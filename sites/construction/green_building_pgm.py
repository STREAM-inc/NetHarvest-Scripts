"""
建築物環境計画書制度システム (東京都環境局) — 公表情報検索

取得対象:
    - 建築物環境計画書 (2025年度基準) の公表情報
    - 一覧表の全カラム (建物番号/地域/建物名/所在地/延べ面積/届出状況/工事完了(予定)年月/
      用途/UA値・BPI/BEI/基準年度/再エネ設備(kW)/EV充電器/段階取得割合/環境性能表示マンション)
    - 詳細画面「建築物の概要」の全項目 (建築主・設計者・施工者とその住所、工事期間、
      敷地面積/建築面積/用途別床面積、高さ/階数/構造、各基準の適合状況 など)

取得フロー:
    1. GET  /KSE00101                     … 検索フォームと screenIdentificationKey を取得
    2. POST /KSE00101/search              … 地域=都内全域・届出状態=計画/変更/完了・用途=全て で検索
    3. GET  /KSE00201/paging?currentPageNumber=N&screenIdentificationKey=...
                                          … 50件/ページで全ページ巡回
    4. POST /KSE00201/details?reportManageNo=XXXXXXXX&category=house|other
                                          … 行ごとの詳細 (建築物の概要) を取得して即 yield

補足:
    - 1 建物が複数用途を持つ場合、一覧は rowspan で複数行に分かれる (17セル=親行 / 9セル=子行)。
      本クローラーは 1 行 = 1 レコードとして出力し、建物共通項目は親行から引き継ぐ。
    - 詳細画面の「建築物の概要」は category(house/other) に依らず同一内容のため、
      reportManageNo 単位でキャッシュして再取得しない。
    - 利用規約 (/KSA00501) 第5条にスクレイピング/クローリングの明示的な禁止条項は無い
      (無断転載・複製の禁止のみ)。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/green_building_pgm.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id green_building_pgm
"""

import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 一覧の親行 (rowspan 付き) のセル数と、2用途目以降の子行のセル数
_MAIN_ROW_CELLS = 17
_SUB_ROW_CELLS = 9

# 子行のセル index -> 親行のセル index の対応
_SUB_TO_MAIN = [7, 8, 9, 11, 12, 13, 14, 15, 16]

# 詳細「建築物の概要」の単純な th/td 行 (空白除去後のラベル -> 出力カラム名)
_SIMPLE_LABELS = {
    "建築物環境計画書作成時期": "計画書作成時期",
    "提出根拠": "提出根拠",
    "省エネルギー性能基準に対する適合状況": "省エネ性能基準適合状況",
    "再生可能エネルギー利用設備設置基準に対する適合状況": "再エネ設備設置基準適合状況",
    "電気自動車充電設備整備基準に対する適合状況": "EV充電設備整備基準適合状況",
}

# 「氏名 / 住所」の2行構成になっている関係者
_PARTIES = ("建築主", "設計者", "施工者")

# 用途別床面積の行頭に現れるラベル
_USE_LABELS = (
    "住宅等", "ホテル等", "病院等", "百貨店等", "事務所等",
    "学校等", "飲食店等", "集会所等", "工場等",
)

_SEARCH_RETRY = 3


def _norm(text: str) -> str:
    """ラベル照合用。全角空白・改行を含む空白をすべて除去する。"""
    return re.sub(r"\s+", "", text or "")


def _clean(node) -> str:
    """セルのテキストを 1 行に正規化する。"""
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


class GreenBuildingPgm(StaticCrawler):
    """建築物環境計画書制度システム スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        # --- 一覧表 ---
        "建物番号",
        "地域",
        "延べ面積",
        "届出状況",
        "工事完了(予定)年月",
        "UA値_BPI",
        "BEI",
        "基準年度",
        "再エネ設備_設置合計容量(kW)",
        "再エネ設備_内太陽光発電(kW)",
        "EV充電器",
        "段階取得割合(%)",
        "環境性能表示マンション",
        # --- 詳細 (建築物の概要) ---
        "計画書作成時期",
        "提出根拠",
        "建築主",
        "建築主住所",
        "設計者",
        "設計者住所",
        "施工者",
        "施工者住所",
        "新築・増築・改築の区別",
        "工事着手",
        "工事完了",
        "敷地面積",
        "建築面積",
        "用途別床面積",
        "建築物の高さ",
        "階数_地上",
        "階数_地下",
        "構造",
        "省エネ性能基準適合状況",
        "再エネ設備設置基準適合状況",
        "EV充電設備整備基準適合状況",
    ]

    def prepare(self):
        # reportManageNo -> 詳細項目 dict (house/other で内容が同じため使い回す)
        self._detail_cache: dict[str, dict] = {}

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        origin = "{0.scheme}://{0.netloc}".format(urlsplit(url))

        # 1. 検索フォームを取得 (session.get 経由 = テストランナーの中断ポイント)
        soup = self.get_soup(url)
        if soup is None:
            logger.error("検索フォームを取得できませんでした: %s", url)
            return

        payload = self._build_search_payload(soup)
        if payload is None:
            logger.error("検索条件を組み立てられませんでした: %s", url)
            return

        # 2. 検索を実行して一覧セッション (screenIdentificationKey) を確立
        result = self._post_search(url, payload)
        if result is None:
            return
        key, total, max_page = result
        self.total_items = total
        logger.info("検索結果: %s件 / %sページ", total, max_page)

        # 3. ページを順に巡回。1行ずつ詳細を引いて即 yield する
        for page in range(1, max_page + 1):
            if page == 1:
                page_soup = self._first_page_soup
            else:
                paging_url = (
                    f"{origin}/KSE00201/paging"
                    f"?currentPageNumber={page}&screenIdentificationKey={key}"
                )
                page_soup = self.get_soup(paging_url)
            if page_soup is None:
                logger.warning("ページ取得に失敗しました (page=%s)", page)
                continue

            rows = page_soup.select("table.table tbody tr")
            if not rows:
                logger.info("行が無いため打ち切ります (page=%s)", page)
                break

            parent: list | None = None
            for tr in rows:
                try:
                    item = self._parse_row(tr, parent, origin, key)
                except Exception as exc:  # 1行の失敗で全体を止めない
                    logger.warning("行の解析に失敗しました: %s", exc)
                    continue
                if item is None:
                    continue
                cells, record = item
                if cells is not None:
                    parent = cells
                yield record

    # ------------------------------------------------------------------ #
    # 検索条件の組み立て / 検索 POST
    # ------------------------------------------------------------------ #
    def _build_search_payload(self, soup: bs4.BeautifulSoup) -> list | None:
        """検索フォームから「都内全域 / 全届出状態 / 全用途」の POST データを作る。

        画面上は「都内全域」チェックで JS が配下の地域・市区町村を全てチェックするため、
        サーバ側バリデーションを通すには area / wards も全て送る必要がある。
        """
        form = soup.find("form", class_="search-form")
        if form is None:
            return None

        areas = [i.get("value", "") for i in form.select('input[name="area"]')]
        wards = [i.get("value", "") for i in form.select('input[name="wards"]')]
        porposes = [i.get("value", "") for i in form.select('input[name="porpose"]')]
        key_input = form.select_one('#screenIdentificationKey')
        if not areas or not wards or not porposes or key_input is None:
            return None

        data: list[tuple[str, str]] = [
            ("allArea", "true"), ("_allArea", "on"),
            ("_area", "on"), ("_wards", "on"),
            ("_reportTipe", "on"),
            ("allPorpose", "true"), ("_allPorpose", "on"), ("_porpose", "on"),
        ]
        data += [("area", v) for v in areas]
        data += [("wards", v) for v in wards]
        # 届出状態: 01=計画 / 02=変更 / 03=完了
        data += [("reportTipe", v) for v in ("01", "02", "03")]
        data += [("porpose", v) for v in porposes]
        data.append(("screenIdentificationKey", key_input.get("value", "")))
        return data

    def _post_search(self, url: str, payload: list) -> tuple[str, int, int] | None:
        """検索 POST を実行し (screenIdentificationKey, 総件数, 総ページ数) を返す。"""
        search_url = urljoin(url + "/", "search")
        last_error: Exception | None = None

        for attempt in range(_SEARCH_RETRY):
            try:
                logger.info("検索実行: %s (試行 %s/%s)", search_url, attempt + 1, _SEARCH_RETRY)
                res = self.session.post(search_url, data=payload, timeout=self.TIMEOUT * 3)
                res.raise_for_status()
                soup = bs4.BeautifulSoup(res.text, "html.parser")

                pager_form = soup.select_one("#pagerForm")
                if pager_form is None:
                    errors = [_clean(e) for e in soup.select(".text-danger")]
                    raise RuntimeError(f"検索結果画面を取得できません: {errors[:3]}")

                key_input = pager_form.select_one('input[name="screenIdentificationKey"]')
                max_input = pager_form.select_one("#maxPageNumber")
                key = key_input.get("value", "") if key_input else ""
                max_page = int(max_input.get("value", "1")) if max_input else 1

                total = 0
                count_el = soup.select_one(".paginationCount")
                if count_el:
                    m = re.search(r"/\s*([\d,]+)\s*件中", _clean(count_el))
                    if m:
                        total = int(m.group(1).replace(",", ""))

                self._first_page_soup = soup
                return key, total, max_page
            except Exception as exc:
                last_error = exc
                logger.warning("検索に失敗しました: %s", exc)
                if attempt < _SEARCH_RETRY - 1:
                    time.sleep(min(2 ** attempt, 8))

        raise RuntimeError(f"検索を {_SEARCH_RETRY} 回試行しましたが失敗しました: {last_error}")

    # ------------------------------------------------------------------ #
    # 一覧行の解析
    # ------------------------------------------------------------------ #
    def _parse_row(self, tr, parent: list | None, origin: str, key: str):
        """一覧の 1 行を 1 レコードに変換する。

        Returns:
            (親行セル or None, レコード dict) / 対象外の行は None
        """
        tds = tr.find_all("td", recursive=False)
        if len(tds) >= _MAIN_ROW_CELLS:
            cells = tds
            new_parent = tds
        elif len(tds) == _SUB_ROW_CELLS and parent is not None:
            # 2用途目以降。建物共通カラムは親行から引き継ぐ
            cells = list(parent)
            for sub_idx, main_idx in enumerate(_SUB_TO_MAIN):
                cells[main_idx] = tds[sub_idx]
            new_parent = None
        else:
            return None

        name = _clean(cells[2])
        if not name:
            return None

        address = _clean(cells[3])
        detail_url, report_no, category = self._detail_target(cells[16], origin)
        detail = self._fetch_detail(detail_url, report_no, category, key) if report_no else {}

        record = {
            Schema.NAME: name,
            Schema.PREF: "東京都" if address.startswith("東京都") else "",
            Schema.ADDR: address,
            Schema.CAT_SITE: _clean(cells[7]),
            Schema.URL: detail_url or origin,
            "建物番号": _clean(cells[0]),
            "地域": _clean(cells[1]),
            "延べ面積": _clean(cells[4]),
            "届出状況": _clean(cells[5]),
            "工事完了(予定)年月": _clean(cells[6]),
            "UA値_BPI": _clean(cells[8]),
            "BEI": _clean(cells[9]),
            "基準年度": _clean(cells[10]),
            "再エネ設備_設置合計容量(kW)": _clean(cells[11]),
            "再エネ設備_内太陽光発電(kW)": _clean(cells[12]),
            "EV充電器": _clean(cells[13]),
            "段階取得割合(%)": _clean(cells[14]),
            "環境性能表示マンション": "有" if _clean(cells[15]) else "",
        }
        # 詳細側のカラムを埋める (取得できなかった項目は空文字)
        for col in self.EXTRA_COLUMNS:
            record.setdefault(col, detail.get(col, ""))
        return new_parent, record

    @staticmethod
    def _detail_target(cell, origin: str):
        """詳細ボタンの formaction から (URL, reportManageNo, category) を取り出す。"""
        button = cell.find("button") if cell else None
        action = button.get("formaction", "") if button else ""
        if not action:
            return "", "", ""
        detail_url = urljoin(origin, action)
        report_no = ""
        category = ""
        m = re.search(r"reportManageNo=([^&]+)", action)
        if m:
            report_no = m.group(1)
        m = re.search(r"category=([^&]+)", action)
        if m:
            category = m.group(1)
        return detail_url, report_no, category

    # ------------------------------------------------------------------ #
    # 詳細 (建築物の概要) の取得・解析
    # ------------------------------------------------------------------ #
    def _fetch_detail(self, detail_url: str, report_no: str, category: str, key: str) -> dict:
        """詳細画面の「建築物の概要」を取得する。reportManageNo 単位でキャッシュする。"""
        if report_no in self._detail_cache:
            return self._detail_cache[report_no]

        detail: dict = {}
        try:
            res = self.session.post(
                detail_url.split("?")[0],
                params={"reportManageNo": report_no, "category": category},
                data={"screenIdentificationKey": key},
                timeout=self.TIMEOUT * 3,
            )
            res.raise_for_status()
            soup = bs4.BeautifulSoup(res.text, "html.parser")
            table = soup.select_one("table.building-detail")
            if table is not None:
                detail = self._parse_detail_table(table)
            else:
                logger.warning("詳細に建築物の概要がありません: %s", report_no)
        except Exception as exc:
            self.error_count += 1
            logger.warning("詳細の取得に失敗しました (%s): %s", report_no, exc)

        self._detail_cache[report_no] = detail
        return detail

    def _parse_detail_table(self, table) -> dict:
        """「建築物の概要」テーブルを項目 dict に変換する。"""
        out: dict = {}
        floor_areas: list[str] = []
        party: str | None = None
        pending: list[str] | None = None

        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"], recursive=False)
            if not cells:
                continue
            ths = [_clean(c) for c in cells if c.name == "th"]
            td_nodes = [c for c in cells if c.name == "td"]
            tds = [_clean(c) for c in td_nodes]

            # 直前の行で予約したラベル群に、この行の td を割り当てる
            if pending:
                for label, value in zip(pending, tds):
                    out[label] = value
                pending = None
                continue

            head = _norm(ths[0]) if ths else ""

            if head in _SIMPLE_LABELS:
                out[_SIMPLE_LABELS[head]] = tds[0] if tds else ""
            elif head in _PARTIES:
                party = head
                out[party] = tds[0] if tds else ""
            elif head == "住所" and party:
                out[f"{party}住所"] = tds[0] if tds else ""
                party = None
            elif head == "新築・増築・改築の区別":
                out["新築・増築・改築の区別"] = tds[0] if tds else ""
                pending = ["工事着手", "工事完了"]
            elif head == "建築物の高さ":
                out["建築物の高さ"] = tds[0] if tds else ""
                pending = ["階数_地上", "階数_地下"]
            elif head == "敷地面積":
                for label, value in zip(ths, tds):
                    key = _norm(label)
                    if key in ("敷地面積", "建築面積"):
                        out[key] = value
            elif head == "用途別床面積" or head in _USE_LABELS or head.startswith("その他"):
                labels = ths[1:] if head == "用途別床面積" else ths
                for label, value in zip(labels, tds):
                    value = value.replace("㎡", "").strip()
                    if value:
                        out_label = re.sub(r"\s+", "", label)
                        floor_areas.append(f"{out_label}:{value}㎡")
            elif head == "構造":
                checked = [
                    _clean(td) for td in td_nodes
                    if td.find("input", attrs={"checked": True}) is not None
                ]
                out["構造"] = "/".join(c for c in checked if c)

        if floor_areas:
            out["用途別床面積"] = " / ".join(floor_areas)
        return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GreenBuildingPgm()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://green-building-pgm.metro.tokyo.lg.jp/KSE00101")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

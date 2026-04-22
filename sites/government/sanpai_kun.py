"""
さんぱいくん — 産業廃棄物処理業者検索 (産廃情報ネット)

取得対象:
    - 全国の産業廃棄物処理業者 (約202,901件) の公表情報
    - 運営: 公益財団法人産業廃棄物処理事業振興財団

取得フロー:
    1. searchprm_rsl.php に無条件検索を POST (200件/ページ)
    2. iIDspPage を 1 から最終ページまでインクリメント (1,015ページ想定)
    3. 各行から固有番号・会社名・許可自治体・廃棄物種類・業区分等を抽出

重要な制約:
    - robots.txt: `Crawl-delay: 120` (2分/リクエスト) を厳守
    - /zyohou/status.php (詳細ページ) は robots で Disallow → 取得しない
    - そのため住所・電話・HP 等は取得不可

実行方法:
    # ローカルテスト (極めて時間がかかるので MAX_PAGES を小さくして試す)
    python scripts/sites/government/sanpai_kun.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id sanpai_kun
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www2.sanpainet.or.jp/zyohou"
SEARCH_URL = f"{BASE_URL}/searchprm_rsl.php"
DSP_COUNT = 200
MAX_PAGES = 2000  # 安全上限 (現在の全件数で 1015 ページ)

_ALLCNT_RE = re.compile(r"allCnt\s*=\s*Number\('([\d,]+)'")
_KYOKA_RE = re.compile(r"許可自治体：(.+?)<br>")
_SANGYOU_RE = re.compile(
    r"【産業廃棄物】<br>(.+?)(?:<br>【特別管理産業廃棄物】|<br>業区分：)"
)
_TOKUBETSU_RE = re.compile(
    r"【特別管理産業廃棄物】<br>(.+?)<br>業区分："
)
_GYOUKUBUN_RE = re.compile(r"業区分：(.+?)<br>")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _extract_color1_spans(html_fragment: str) -> list[str]:
    """<span class='color1'>XXX</span> だけ抽出 (color0 = 未許可なので除外)"""
    return [
        _clean(m.group(1))
        for m in re.finditer(
            r"<span class=['\"]color1['\"]>(.+?)</span>", html_fragment
        )
    ]


def _extract_active_gyoukubun(html_fragment: str) -> list[str]:
    """業区分テキストから color0 を除外し、許可を持つもののみ返す"""
    # color0 の span (未許可) を除去してから、カンマ (，) で分割
    without_color0 = re.sub(
        r"<span class=['\"]color0['\"]>[^<]*</span>\s*[，,]?\s*",
        "",
        html_fragment,
    )
    # 残った HTML タグを剥がす
    plain = re.sub(r"<[^>]+>", "", without_color0)
    plain = plain.replace("　", " ")
    items = [_clean(x) for x in re.split(r"[，,]", plain)]
    return [x for x in items if x]


class SanpaiKunScraper(StaticCrawler):
    """さんぱいくん (www2.sanpainet.or.jp) 産廃処理業者検索スクレイパー"""

    # robots.txt: Crawl-delay 120
    DELAY = 120
    EXTRA_COLUMNS = [
        "固有番号", "優良認定", "許可自治体",
        "産業廃棄物種類", "特別管理産業廃棄物種類", "業区分",
        "二次委託先開示可否", "二次委託先開示状況",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            self.logger.info("ページ取得中: %d", page)
            soup = self._post_search(page=page)
            if soup is None:
                break

            html_text = str(soup)

            # 初回ページで total を取得 → total_items 設定
            if page == 1:
                m = _ALLCNT_RE.search(html_text)
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))
                    self.logger.info("総件数: %s 件", m.group(1))

            # 本テーブルの行を抽出 (ヘッダ行は除外)
            rows = soup.select("table.content tr")
            page_item_count = 0
            for tr in rows:
                tds = tr.find_all("td", recursive=False)
                if len(tds) < 2:
                    continue
                # 先頭 td が 6 桁数字かで判定
                first_text = _clean(tds[0].get_text())
                if not re.fullmatch(r"\d{6}", first_text):
                    continue
                try:
                    item = self._parse_row(tds)
                except Exception as e:
                    self.logger.warning("行パース失敗: %s", e)
                    continue
                if not item:
                    continue
                cid = item.get("固有番号") or ""
                if cid and cid in seen_ids:
                    continue
                if cid:
                    seen_ids.add(cid)
                page_item_count += 1
                yield item

            if page_item_count == 0:
                self.logger.info("これ以上レコードがありません。終了します。")
                break

            # 最終ページ検出
            if not self._has_next_page(html_text, page):
                self.logger.info("最終ページに到達しました。")
                break

            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # POST リクエスト
    # ------------------------------------------------------------------

    def _post_search(self, page: int):
        data = {
            "Param1": "01",
            "iIDspCount": str(DSP_COUNT),
            "iIDspPage": str(page),
            "orderType": "0",
        }
        try:
            resp = self.session.post(SEARCH_URL, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("POST 失敗 page=%d: %s", page, e)
            return None
        # 文字コードはサーバが UTF-8 を指定
        resp.encoding = "utf-8"
        import bs4
        return bs4.BeautifulSoup(resp.text, "html.parser")

    # ------------------------------------------------------------------
    # 1 行パース
    # ------------------------------------------------------------------

    def _parse_row(self, tds: list) -> dict | None:
        cid = _clean(tds[0].get_text())
        if not re.fullmatch(r"\d{6}", cid):
            return None

        body_td = tds[1]
        body_html = body_td.decode_contents()

        # 会社名 (a.company)
        a = body_td.select_one("a.company")
        name = _clean(a.get_text()) if a else ""

        # 優良認定: span.komejirushi 内に <img> があれば 優良
        yuryo = ""
        kome = body_td.select_one("span.komejirushi")
        if kome and kome.find("img") is not None:
            yuryo = "優良認定"

        # 許可自治体
        kyoka = ""
        m = _KYOKA_RE.search(body_html)
        if m:
            raw = m.group(1)
            # <br> が間に入ることはない (regex で既に限定済み)
            kyoka = _clean(re.sub(r"<[^>]+>", "", raw))

        # 産業廃棄物種類 (color1 のみ)
        sangyou_kinds = ""
        m = _SANGYOU_RE.search(body_html)
        if m:
            sangyou_kinds = " / ".join(_extract_color1_spans(m.group(1)))

        # 特別管理産業廃棄物種類
        tokubetsu_kinds = ""
        m = _TOKUBETSU_RE.search(body_html)
        if m:
            tokubetsu_kinds = " / ".join(_extract_color1_spans(m.group(1)))

        # 業区分 (color0 除外 → 許可を持つ区分のみ)
        gyoukubun = ""
        gyoukubun_list: list[str] = []
        m = _GYOUKUBUN_RE.search(body_html)
        if m:
            gyoukubun_list = _extract_active_gyoukubun(m.group(1))
            gyoukubun = " / ".join(gyoukubun_list)

        # 二次委託先 開示可否 / 開示状況
        disclose_kahi = _clean(tds[2].get_text()) if len(tds) > 2 else ""
        disclose_status = _clean(tds[3].get_text()) if len(tds) > 3 else ""

        return {
            Schema.NAME: name,
            Schema.URL: f"{BASE_URL}/searchprm_rsl.php?Param0={cid}",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            Schema.CO_NUM: "",
            Schema.REP_NM: "",
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            Schema.CAT_SITE: gyoukubun,
            "固有番号": cid,
            "優良認定": yuryo,
            "許可自治体": kyoka,
            "産業廃棄物種類": sangyou_kinds,
            "特別管理産業廃棄物種類": tokubetsu_kinds,
            "業区分": gyoukubun,
            "二次委託先開示可否": disclose_kahi,
            "二次委託先開示状況": disclose_status,
        }

    # ------------------------------------------------------------------
    # 次ページ判定
    # ------------------------------------------------------------------

    def _has_next_page(self, html_text: str, current: int) -> bool:
        # "DoSubmit(200,{next_page},0)" 形式でリンクがあるか
        pattern = rf"DoSubmit\({DSP_COUNT},{current + 1},"
        return bool(re.search(pattern, html_text))


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SanpaiKunScraper()
    scraper.execute(f"{BASE_URL}/index.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

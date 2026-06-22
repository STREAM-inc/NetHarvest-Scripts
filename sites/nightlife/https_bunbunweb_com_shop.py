"""
ブンブンウェブ — 仙台国分町キャバクラ情報ポータル (店舗情報 + 求人情報)

取得対象:
    - 店舗一覧 (/search/shop.html) に掲載された全店舗
    - 各店舗の詳細ページ (/shop/{id}/) … 住所・TEL・営業時間・定休日・最大収容人数・設備
    - 各店舗の求人ページ (/recruit/{id}/) … 職種・従業員給与・募集年齢 (掲載がある店舗のみ)

取得フロー (一覧 → 詳細 + 求人, 1 件ずつ即 yield する Pattern B):
    1. 引数 url からホストを導出し、店舗一覧 /search/shop.html を取得
    2. section.search_wrap の各アンカーから 店舗ID・名称(カナ)・ジャンル・建物 を取得
    3. 店舗ごとに /shop/{id}/ (詳細) と /recruit/{id}/ (求人) を取得して 1 レコードを yield

備考:
    - 「店舗の詳細リンクによっては求人情報で職種・ジャンル・給料等が載っている」→ /recruit/{id}/ を追加取得
    - 料金システム説明 (shop_system) / 店舗紹介文 / 求人の自由記述「詳細」は
      長文の自由記述 (著作権リスク) のため取得しない
    - SNS リンクはサイト共通のシェアボタン (店舗固有でない) のため取得しない

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/https_bunbunweb_com_shop.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id https_bunbunweb_com_shop
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# /shop/123/ や /shop/123 形式から店舗IDを取り出す
_SHOP_HREF = re.compile(r"/shop/(\d+)/?$")
# 末尾の (カナ) / （カナ） を読み仮名として抽出
_KANA = re.compile(r"[（(]([^（）()]+)[）)]\s*$")
# 先頭の都道府県を抽出
_PREF = re.compile(r"^(北海道|京都府|大阪府|東京都|.{2,3}?[県])")
# 「未掲載」を表すプレースホルダ
_EMPTY_TOKENS = {"", "-", "‐", "−", "ー", "—", "–"}


def _clean(text: str | None) -> str:
    """前後空白を除去し、'-' 等の未掲載プレースホルダを空文字に正規化する。"""
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return "" if text in _EMPTY_TOKENS else text


class BunbunWebShop(StaticCrawler):
    """ブンブンウェブ スクレイパー (仙台国分町ナイトビジネス店舗 + 求人)"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "建物",
        "最大収容人数",
        "設備・オプション",
        "職種",
        "従業員給与",
        "募集年齢",
        "求人URL",
    ]

    def parse(self, url: str):
        # 引数 url を唯一のルートとし、同一ホスト上の一覧ページを導出する
        list_url = urljoin(url, "/search/shop.html")
        soup = self.get_soup(list_url)
        if soup is None:
            logger.warning("一覧ページを取得できませんでした: %s", list_url)
            return

        # section.search_wrap 内のアンカーから店舗を抽出 (ID で重複排除し掲載順を維持)
        seen: set[str] = set()
        items: list[tuple[str, object]] = []
        for a in soup.select('a[href*="/shop/"]'):
            m = _SHOP_HREF.search(a.get("href", ""))
            if not m:
                continue
            sid = m.group(1)
            if sid in seen:
                continue
            seen.add(sid)
            items.append((sid, a))

        self.total_items = len(items)
        logger.info("店舗 %d 件を検出", len(items))

        for sid, anchor in items:
            try:
                record = self._build_record(url, sid, anchor)
            except Exception as e:  # noqa: BLE001 — 個別店舗のエラーはスキップして継続
                logger.warning("店舗 %s の処理でエラー (スキップ): %s", sid, e)
                continue
            if record:
                yield record  # 1 店舗ごとに即 yield (全件収集しない)

    def _build_record(self, root_url: str, sid: str, anchor) -> dict | None:
        """一覧アンカー + 詳細ページ + 求人ページを統合して 1 レコードを作る。"""
        # --- 一覧ページから取得できるフィールド ---
        name_el = anchor.select_one("h3")
        name_full = _clean(name_el.get_text(strip=True)) if name_el else ""
        name, kana = self._split_kana(name_full)

        genre_el = anchor.select_one("p.search_p")
        genre = _clean(genre_el.get_text(strip=True)) if genre_el else ""

        bldg_el = anchor.select_one("address")
        building = _clean(bldg_el.get_text(strip=True)) if bldg_el else ""

        detail_url = urljoin(root_url, f"/shop/{sid}/")

        record = {
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.CAT_SITE: genre,
            Schema.URL: detail_url,
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            "建物": building,
            "最大収容人数": "",
            "設備・オプション": "",
            "職種": "",
            "従業員給与": "",
            "募集年齢": "",
            "求人URL": "",
        }

        # --- 詳細ページ (/shop/{id}/) ---
        detail = self.get_soup(detail_url)
        if detail is not None:
            self._fill_from_detail(record, detail)

        # --- 求人ページ (/recruit/{id}/) ※掲載がある店舗のみ存在 ---
        recruit_url = urljoin(root_url, f"/recruit/{sid}/")
        recruit = self.get_soup(recruit_url)  # 404 の場合は None (CONTINUE_ON_ERROR)
        if recruit is not None:
            self._fill_from_recruit(record, recruit, recruit_url)

        return record

    @staticmethod
    def _split_kana(name_full: str) -> tuple[str, str]:
        """末尾の (カナ) を読み仮名として分離する。無ければ kana は空。"""
        m = _KANA.search(name_full)
        if m:
            return name_full[: m.start()].strip(), m.group(1).strip()
        return name_full, ""

    def _fill_from_detail(self, record: dict, detail) -> None:
        """店舗詳細ページの情報テーブルと設備欄を反映する。"""
        table = detail.select_one(".table-information_container")
        if table:
            headers = [th.get_text(strip=True) for th in table.select("thead th")]
            values = [td.get_text(" ", strip=True) for td in table.select("tbody td")]
            row = dict(zip(headers, values))
            addr = _clean(row.get("住所"))
            if addr:
                pref_m = _PREF.match(addr)
                if pref_m:
                    record[Schema.PREF] = pref_m.group(1)
                    record[Schema.ADDR] = addr[pref_m.end():].strip()
                else:
                    record[Schema.ADDR] = addr
            record[Schema.TEL] = _clean(row.get("TEL"))
            record[Schema.TIME] = _clean(row.get("営業"))
            record[Schema.HOLIDAY] = _clean(row.get("定休日"))
            record["最大収容人数"] = _clean(row.get("最大収容"))

        # 設備・オプション: strong.on (有効な項目) のみを抽出
        opt = detail.select_one(".shop_option")
        if opt:
            active = [
                _clean(s.get_text(strip=True))
                for s in opt.select("strong.on")
            ]
            record["設備・オプション"] = "/".join(filter(None, active))

    def _fill_from_recruit(self, record: dict, recruit, recruit_url: str) -> None:
        """求人ページの dt/dd を反映する (職種・給与・年齢・TEL/ジャンル補完)。"""
        labels: dict[str, str] = {}
        for dt in recruit.select("dl dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                labels[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

        record["求人URL"] = recruit_url
        record["職種"] = _clean(labels.get("職種"))
        record["従業員給与"] = _clean(labels.get("従業員給与"))
        record["募集年齢"] = _clean(labels.get("年齢"))

        # 詳細ページに無かった項目を求人ページで補完
        if not record[Schema.CAT_SITE]:
            record[Schema.CAT_SITE] = _clean(labels.get("ジャンル"))
        if not record[Schema.TEL]:
            record[Schema.TEL] = _clean(labels.get("TEL"))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BunbunWebShop()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url からホストを導出して一覧/詳細/求人 URL を組み立てる。
    scraper.execute("https://bunbunweb.com/shop/455/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

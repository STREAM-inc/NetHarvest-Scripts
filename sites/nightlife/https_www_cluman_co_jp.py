"""
クラマンネット (www.cluman.co.jp) — すすきの・北海道のナイトビジネス店舗クラマン割引一覧

取得対象:
    - /hokkaido/shops の掲載店舗 (キャバクラ / ニュークラブ / パブスナック /
      ガールズバー / コンカフェ 等)
    - 各店舗の詳細ページ (/hokkaido/shop/{id}) から構造化された店舗情報を抽出

取得フロー:
    1. ルート URL (?page=N) をページ送りし、各ページの店舗詳細リンクを列挙
    2. 重複を除いた店舗ごとに詳細ページを取得し、その場で即 yield (Pattern B)

取得カラム:
    Schema  : NAME / URL / TEL / ADDR / PREF / TIME / CAT_SITE (ジャンル)
    EXTRA   : エリア / 予約 / カラオケ / 駐車場 / 収容人数 / 平均年齢 / 喫煙
    ※ 料金表・店舗紹介・キャストの衣装等の自由記述 (プロース) は著作権リスクのため取得しない

実行方法:
    python scripts/sites/nightlife/https_www_cluman_co_jp.py
    python bin/run_flow.py --site-id https_www_cluman_co_jp
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# ルート URL の /{region}/ セグメントから都道府県を導出するためのマップ
_REGION_PREF = {
    "hokkaido": "北海道",
}

# 詳細リンク /.../shop/{id} から店舗 ID を抽出
_SHOP_ID = re.compile(r"/shop/(\d+)")

# 「店舗情報」テーブルの th ラベル → EXTRA カラム名 (構造化された短い値のみ)
_SPEC_LABELS = {
    "収容人数": "収容人数",
    "平均年齢": "平均年齢",
    "喫煙": "喫煙",
}

# 「予約」「カラオケ」「駐車場」等の可否バッジ → EXTRA カラム名
_ETC_LABELS = {
    "予約": "予約",
    "カラオケ": "カラオケ",
    "駐車場": "駐車場",
}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


class ClumanNet(StaticCrawler):
    """クラマンネット スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "予約",
        "カラオケ",
        "駐車場",
        "収容人数",
        "平均年齢",
        "喫煙",
    ]

    def parse(self, url: str):
        # ルート URL の /{region}/ からエリア既定値・都道府県を決める
        m_region = re.search(r"/([a-z]+)/shops", url)
        region = m_region.group(1) if m_region else ""
        pref = _REGION_PREF.get(region, "")

        seen: set[str] = set()
        page = 1
        while True:
            list_url = f"{url}?page={page}"
            soup = self.get_soup(list_url)

            # このページに現れる店舗詳細 URL を収集 (重複除去)
            page_shop_urls: list[str] = []
            for a in soup.select('a[href*="/shop/"]'):
                href = a.get("href") or ""
                mid = _SHOP_ID.search(href)
                if not mid:
                    continue
                detail_url = urljoin(url, f"shop/{mid.group(1)}")
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                page_shop_urls.append(detail_url)

            # 店舗リンクが 1 件も無ければ最終ページを超えたので終了
            if not page_shop_urls:
                break

            if page == 1:
                # 進捗表示の目安 (1 ページ分 × 概算ページ数)。正確な総数は取得しない
                self.total_items = len(page_shop_urls)

            for detail_url in page_shop_urls:
                try:
                    item = self._scrape_detail(detail_url, pref)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("detail failed %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, detail_url: str, pref: str) -> dict | None:
        soup = self.get_soup(detail_url)

        name_el = soup.select_one("a.nav_shopname")
        name = _clean(name_el.get_text()) if name_el else ""
        if not name:
            # 店舗名が取れないページ (削除・非公開等) はスキップ
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.TEL: "",
            Schema.ADDR: "",
            Schema.PREF: pref,
            Schema.TIME: "",
            Schema.CAT_SITE: "",
            "エリア": "",
            "予約": "",
            "カラオケ": "",
            "駐車場": "",
            "収容人数": "",
            "平均年齢": "",
            "喫煙": "",
        }

        # --- 基本情報ブロック (電話 / 営業時間 / 住所) ---
        info = soup.select_one(".cr_shpdtinfodate_pctltm")
        if info:
            tel_b = info.select_one("b")
            if tel_b:
                item[Schema.TEL] = _clean(tel_b.get_text())

            for sp in info.select("span"):
                txt = _clean(sp.get_text(" "))
                if txt.startswith("営業時間"):
                    item[Schema.TIME] = txt.split("：", 1)[-1].strip() if "：" in txt else txt

            addr_a = info.select_one('a[href*="maps"]')
            if addr_a:
                # <a> 直下のテキストノードが住所 (子 span の "MAP" は除外)
                addr = "".join(addr_a.find_all(string=True, recursive=False))
                item[Schema.ADDR] = _clean(addr)

        # --- ジャンル (CAT_SITE): sectors 絞り込みリンクの単独ジャンル名 ---
        for a in soup.select('a[href*="sectors="]'):
            t = _clean(a.get_text())
            if t and "すすきの" not in t and "ランキング" not in t and "割引" not in t:
                item[Schema.CAT_SITE] = t
                break

        # --- エリア: タイトル接頭辞からジャンルを除いた部分 (例: すすきの) ---
        title = _clean(soup.title.get_text()) if soup.title else ""
        m_title = re.match(r"^(.+?)[「｢]", title)
        if m_title:
            prefix = m_title.group(1)
            if item[Schema.CAT_SITE] and item[Schema.CAT_SITE] in prefix:
                item["エリア"] = prefix.replace(item[Schema.CAT_SITE], "").strip()
            else:
                item["エリア"] = prefix

        # --- 予約 / カラオケ / 駐車場 の可否バッジ ---
        for e in soup.select('[class*="cr_shpdtifetc"]'):
            txt = _clean(e.get_text(" "))
            if "：" not in txt:
                continue
            label, _, val = txt.partition("：")
            key = _ETC_LABELS.get(label.strip())
            if key:
                item[key] = val.strip()

        # --- 店舗情報テーブル (収容人数 / 平均年齢 / 喫煙 等の構造化値) ---
        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            key = _SPEC_LABELS.get(_clean(th.get_text()))
            if key and not item[key]:
                item[key] = _clean(td.get_text(" "))

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ClumanNet()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.cluman.co.jp/hokkaido/shops")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

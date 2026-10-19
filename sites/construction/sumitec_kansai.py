"""
コンストマップ (sumitec-kansai.com) — 施工店一覧クローラー (愛知・岐阜・三重)

対象サイト: https://sumitec-kansai.com/contractor/area_cat/aichi

取得対象:
    - エリア一覧 /contractor/area_cat/{aichi,gifu,mie} に掲載される施工店の「店舗情報」

取得フロー:
    1. 引数 url (/contractor/area_cat/aichi) の末尾エリアを aichi → gifu → mie に差し替えて巡回
    2. 各エリアを /page/N で 1 ページ (10 件) ずつ巡回し、詳細 URL (/contractor/{pref}/.../{id}) を抽出
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B)

備考:
    - 姉妹サイト sumitec-kanto.com (sumitec_kanto_2) と同構成。
    - area_cat 一覧には他県の施工店 (例: /contractor/ehime/...) が混入することがあるため、
      詳細 URL の都道府県スラッグが aichi/gifu/mie のものだけ取得する。
    - 詳細ページの紹介文・オススメポイント・求人情報は自由記述プロースのため取得しない。
    - 利用規約 (/term) にスクレイピング・クローリングの明示的な禁止条項は無い。

実行方法:
    python scripts/sites/construction/sumitec_kansai.py
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_TARGET_AREAS = ("aichi", "gifu", "mie")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_DETAIL_PATH = re.compile(r"^/contractor/([^/]+)/.+/(\d+)/?$")
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")


class SumitecKansaiScraper(StaticCrawler):
    """コンストマップ (sumitec-kansai) エリア一覧 → 施工店詳細クローラー"""

    DELAY = 3.0
    MAX_LIST_PAGES = 50
    EXTRA_COLUMNS = ["掲載ID"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()

        for area in _TARGET_AREAS:
            area_url = self._area_url(url, area)
            for page in range(1, self.MAX_LIST_PAGES + 1):
                page_url = area_url if page == 1 else f"{area_url}/page/{page}"
                self.logger.info("[%s page %d] 一覧取得: %s", area, page, page_url)
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                detail_urls: list[tuple[str, str]] = []
                for link in soup.select("section.c_secPostList a[href]"):
                    detail_url = urljoin(page_url, (link.get("href") or "").strip())
                    m = _DETAIL_PATH.match(urlparse(detail_url).path)
                    if not m or m.group(1) not in _TARGET_AREAS:
                        continue
                    post_id = m.group(2)
                    if post_id in seen_ids:
                        continue
                    seen_ids.add(post_id)
                    detail_urls.append((detail_url, post_id))

                for detail_url, post_id in detail_urls:
                    time.sleep(self.DELAY)
                    item = self._scrape_detail(detail_url, post_id)
                    if item:
                        yield item

                # 次ページへのリンクが無ければこのエリアは終了
                next_path = f"/area_cat/{area}/page/{page + 1}"
                if not soup.select_one(f'a[href*="{next_path}"]'):
                    break
                time.sleep(self.DELAY)

    def _scrape_detail(self, detail_url: str, post_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        name, kana = "", ""
        h2 = soup.select_one("header.m_sec_h h2.mainTxt")
        if h2:
            kana_el = h2.select_one("span.sm")
            if kana_el:
                kana = kana_el.get_text(strip=True)
                kana_el.extract()
            name = h2.get_text(" ", strip=True)
        if not name:
            return None

        row_map: dict[str, str] = {}
        for row in soup.select("table.p_table tr"):
            th, td = row.select_one("th"), row.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            if label == "ホームページ":
                link = td.select_one("a[href]")
                if link and (link.get("href") or "").strip():
                    value = link["href"].strip()
                value = value.replace("​", "").strip()
            row_map[label] = value

        address = row_map.get("住所", "")
        m = _PREF_PATTERN.match(address)
        pref = m.group(1) if m else ""

        tel_raw = row_map.get("電話番号", "")
        tel_m = _TEL_PATTERN.search(tel_raw)
        tel = tel_m.group(0) if tel_m else tel_raw

        lob = ",".join(
            a.get_text(strip=True) for a in soup.select("div.m_sec_cBox div.tags li a")
        )

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.HP: row_map.get("ホームページ", ""),
            Schema.LOB: lob,
            "掲載ID": post_id,
        }

    def _area_url(self, url: str, area: str) -> str:
        """引数 url (/contractor/area_cat/{area}) のエリア部分を差し替える。"""
        base = re.sub(r"/page/\d+/?$", "", url.split("?", 1)[0].rstrip("/"))
        return re.sub(r"/area_cat/[^/]+$", f"/area_cat/{area}", base)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SumitecKansaiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://sumitec-kansai.com/contractor/area_cat/aichi")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
映画.com 映画館情報 (eiga.com/theater/) — 全国映画館スクレイパー

取得対象:
    - 全国 47 都道府県の映画館情報

取得フロー:
    1. 都道府県コード 1〜47 の各都道府県ページ (/theater/{code}/) を巡回
    2. dl.theater-area-list から映画館詳細 URL + スクリーン数を収集
    3. 各映画館詳細ページ (/theater/{pref}/{area}/{id}/) から情報を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/portal/eiga_theater.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id eiga_theater
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://eiga.com"
INDEX_URL = f"{BASE_URL}/theater/"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_SCREEN_RE = re.compile(r"（(\d+)）")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_pref_addr(raw: str) -> tuple[str, str]:
    """住所テキストから (都道府県, 住所残り) を返す。"""
    if not raw:
        return "", ""
    m = _PREF_PATTERN.match(raw)
    if m:
        return m.group(1), raw[m.end():].strip()
    return "", raw.strip()


class EigaTheaterScraper(StaticCrawler):
    """映画.com 全国映画館情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "アクセス", "スクリーン数"]

    def parse(self, url: str):
        theater_entries: list[tuple[str, str]] = []  # (detail_url, screens)
        seen: set[str] = set()

        # 都道府県コード 1〜47 を巡回
        for pref_code in range(1, 48):
            pref_url = f"{BASE_URL}/theater/{pref_code}/"
            soup = self.get_soup(pref_url)
            if soup is None:
                continue

            for li in soup.select("dl.theater-area-list li"):
                a = li.select_one("a[href]")
                if not a:
                    continue
                href = a.get("href", "")
                detail_url = BASE_URL + href if href.startswith("/") else href
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                text = a.get_text()
                m = _SCREEN_RE.search(text)
                screens = m.group(1) if m else ""
                theater_entries.append((detail_url, screens))

        self.total_items = len(theater_entries)
        self.logger.info("収集した映画館数: %d", self.total_items)

        for detail_url, screens in theater_entries:
            item = self._scrape_detail(detail_url, screens)
            if item:
                yield item

    def _scrape_detail(self, url: str, screens: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        try:
            name = _clean(soup.select_one("h1").get_text(strip=True)) if soup.select_one("h1") else ""

            # パンくず内の "/theater/" リンクから都道府県・エリアを抽出
            # 例: "東京都の映画館" → pref="東京都", "新宿の映画館" → area="新宿"
            pref = ""
            area = ""
            for a in soup.select("a[href*='/theater/']"):
                text = _clean(a.get_text(strip=True))
                if not text.endswith("の映画館"):
                    continue
                candidate = text[:-4]
                if _PREF_PATTERN.match(candidate):
                    pref = candidate
                else:
                    area = candidate

            # dl.location から所在地・行き方
            raw_addr = ""
            access = ""
            location_dl = soup.select_one("dl.location")
            if location_dl:
                for dt in location_dl.select("dt"):
                    dd = dt.find_next_sibling("dd")
                    if not dd:
                        continue
                    key = dt.get_text(strip=True)
                    val = _clean(dd.get_text(strip=True))
                    if key == "所在地":
                        raw_addr = val
                    elif key == "行き方":
                        access = val

            # 住所から都道府県分離（パンくずで取れなかった場合のフォールバック）
            pref_from_addr, addr = _extract_pref_addr(raw_addr)
            if not pref:
                pref = pref_from_addr

            # 公式HP（テキストに「映画館公式ページ」を含む外部リンク）
            hp = ""
            for a in soup.select("a[href]"):
                if "映画館公式ページ" in a.get_text() and "eiga.com" not in a.get("href", ""):
                    hp = a.get("href", "").strip()
                    break

            # TEL
            tel = ""
            tel_a = soup.select_one("a[href^='tel:']")
            if tel_a:
                tel = tel_a.get("href", "").replace("tel:", "").strip()

            return {
                Schema.URL:  url,
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL:  tel,
                Schema.HP:   hp,
                "エリア":    area,
                "アクセス":  access,
                "スクリーン数": screens,
            }
        except Exception as e:
            self.logger.error("詳細取得失敗 %s: %s", url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EigaTheaterScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

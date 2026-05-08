# scripts/sites/portal/curama.py
"""
くらしのマーケット (curama.jp) — リフォームカテゴリ スクレイパー

取得対象:
    リフォームセクション配下の全カテゴリに登録されているサービス事業者

取得フロー:
    カテゴリ別一覧ページ → ページネーション → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    # ローカルテスト
    python scripts/sites/portal/curama.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id curama
"""

import math
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

BASE_URL = "https://curama.jp"

# リフォームセクション配下の全カテゴリスラッグ（curama.jp/category/ より）
REFORM_CATEGORIES = [
    "wallpaper", "removable-wallpaper", "screen", "tatami", "fusuma", "shoji",
    "balcony-tiling", "window-film-installation", "vinyl-flooring-replacement",
    "wood-flooring-replacement", "carpet-replacement", "floor-tile-replacement",
    "kitchen-wall-panel-replacement", "bath-vinyl-flooring-install", "floor-coating",
    "bathtub-painting", "stucco-painting", "intercom-install", "handrail-install",
    "blind-install", "hosuclean-install", "track-lighting-install", "picture-rail-install",
    "screen-install", "inside-window-install", "socket", "water-heater",
    "toilet-remodel", "toilet-fan-installation", "bathroom-remodel", "bathroom-fan",
    "bathroom-door-replacement", "bathroom-sink-cabinet-remodel",
    "bathroom-door-glass-replacement", "shower-replacement", "kitchen-fan-replacement",
    "built-in-gas-cooktop-installation", "electric-cooktop-installation",
    "kitchen-bathroom-renovation", "front-door-replacement", "door-handle-change",
    "door-closer-replacement", "gutter-repair", "wall-repair", "caulking",
    "flooring-repair", "door-repair", "accessible-home-renovation",
    "japanese-to-western-room-remodeling", "partition-wall",
    "second-hand-apartment-remodel", "home-inspection", "roof-replacement", "roofing",
    "balcony-roof-installation", "roof-deicing-system-installation", "brick-paving",
    "exterior-painting", "deck-maintenance", "concrete-repair",
    "awning-window-shade-installation", "shutter-automation", "lattice-installation",
    "carport-installation", "cycle-shelter-installation", "driveway-gate-installation",
    "nameplate-installation", "washer-pan-installation", "safety-mirror-installation",
    "movable-shelf-installation", "sliding-door-replacement", "kitchen-counter-installation",
    "parking-block", "laundry-pole", "wall-shelf-installation", "underfloor-ventilation-fan",
    "art-installation", "tv-antenna-terminal", "hanger-pipe-installation",
    "hanging-cabinet-installation", "eaves-installation", "outside-socket",
    "gate-light-installation", "indoor-vent-installation", "outside-vent-installation",
    "tv-antenna-removal", "mail-slot-replacement", "hammock-installation",
    "toilet-paper-holder-installation", "pull-up-bar-installation",
    "bathroom-lighting-installation", "safety-net-installation",
    "basketball-goal-installation", "storage-painting", "dance-pole-installation",
    "shutter-painting", "towel-hanger-installation", "ground-anchor-installation",
    "kamidana-installation", "delivery-box-installation", "mailbox-installation",
    "kitchen-sink-repair", "front-door-painting", "storage", "floor-noise-repair",
    "gate-painting", "bathroom-tv-installation", "parking",
    "projector-screen-installation", "fire-alarm-installation",
    "tv-board-wall-mounting", "asphalt-repair", "block-wall-removal",
    "house-window-glass-coating", "block-wall-painting", "bathroom-sink-repair",
    "door-adjustment", "dinoc-film-installation", "lan-socket-extension",
]

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class CuramaScraper(StaticCrawler):
    """くらしのマーケット リフォームカテゴリ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX", "メール"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/")
        seen_ids: set[str] = set()
        for category in REFORM_CATEGORIES:
            self.logger.info("カテゴリ取得開始: /%s/", category)
            yield from self._scrape_category(base_url, category, seen_ids)

    def _scrape_category(
        self, base_url: str, category: str, seen_ids: set
    ) -> Generator[dict, None, None]:
        """1カテゴリ分のページネーション処理"""
        page = 1
        max_pages = 1

        while page <= max_pages:
            list_url = f"{base_url}/{category}/?page={page}"
            self.logger.info("一覧ページ取得: %s", list_url)

            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 初回のみ総件数からmax_pagesを算出
            if page == 1:
                count_h2 = next(
                    (h2 for h2 in soup.find_all("h2") if "件中" in h2.get_text()), None
                )
                if count_h2:
                    m = re.search(r"(\d[\d,]*)\s*件中", count_h2.get_text())
                    if m:
                        total = int(m.group(1).replace(",", ""))
                        max_pages = math.ceil(total / 50)

            # SERIDを持つ詳細リンクを収集
            detail_hrefs = {
                a["href"]
                for a in soup.find_all("a", href=re.compile(r"/SER\d+/"))
            }
            if not detail_hrefs:
                break

            for href in sorted(detail_hrefs):
                ser_m = re.search(r"SER(\d+)", href)
                if not ser_m:
                    continue
                ser_id = ser_m.group(1)
                if ser_id in seen_ids:
                    continue
                seen_ids.add(ser_id)

                detail_url = base_url + href
                time.sleep(self.DELAY)
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

            page += 1
            if page <= max_pages:
                time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページからサービス事業者情報を取得"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # 名称（h1）
        h1 = soup.find("h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # サイト定義業種（titleタグの括弧内: "xxx(業種 / 種別) - くらしのマーケット"）
        title_tag = soup.find("title")
        if title_tag:
            m = re.search(
                r"\(([^)]+)\)\s*-\s*くらしのマーケット", title_tag.get_text()
            )
            if m:
                item[Schema.CAT_SITE] = m.group(1).strip()

        # 代表者名（"店長：xxx" を含む p タグ）
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if "店長：" in text or "店長:" in text:
                m = re.search(r"店長[：:]\s*(.+)", text)
                if m:
                    item[Schema.REP_NM] = m.group(1).strip()
                break

        # 所在地（h3「所在地」の次の兄弟要素）
        addr_text = self._h3_sibling_text(soup, "所在地")
        if addr_text:
            post_m = re.search(r"〒(\d{3}-\d{4})", addr_text)
            if post_m:
                item[Schema.POST_CODE] = post_m.group(1)
            addr_clean = re.sub(r"〒\d{3}-\d{4}\s*", "", addr_text).strip()
            pref_m = _PREF_RE.match(addr_clean)
            if pref_m:
                item[Schema.PREF] = pref_m.group(1)
                item[Schema.ADDR] = addr_clean[len(pref_m.group(1)):].strip()
            else:
                item[Schema.ADDR] = addr_clean

        # インボイス登録番号 → 法人番号
        invoice_text = self._h3_sibling_text(soup, "インボイス登録番号")
        if invoice_text:
            item[Schema.CO_NUM] = invoice_text.strip()

        if Schema.NAME not in item:
            return None

        return item

    def _h3_sibling_text(self, soup, label: str) -> str | None:
        """h3見出しラベルに対応する直後の兄弟要素テキストを返す"""
        for h3 in soup.find_all("h3"):
            if label in h3.get_text(strip=True):
                sibling = h3.find_next_sibling()
                if sibling:
                    return sibling.get_text(" ", strip=True)
        return None


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CuramaScraper()
    scraper.execute(BASE_URL)

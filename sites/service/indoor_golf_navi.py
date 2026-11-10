"""
INDOOR GOLF NAVI — 全国インドアゴルフスタジオ比較サイト
対象サイト: https://indoor-golf-navi.com/studio/

取得対象:
    - 全国のインドアゴルフスタジオ（約50施設）

取得フロー:
    一覧ページ (/studio/ → /studio/page/N/, 1ページ30件, 範囲外は404) の
    article.ign-card から詳細ページURLを集め、各詳細ページを取得して即 yield する (Pattern B)。
    - 詳細: h1=名称, 「施設情報」dl.ign-dl (住所/アクセス/電話番号/営業時間/定休日/公式サイト),
            「料金」「設備・サービス」dl.ign-dl, 特徴タグ(.ign-feature-tags)
    - フッターの運営会社（ワークウェル株式会社）はサイト運営者のため取得しない。
    - スタジオ紹介・料金目安は自由記述のため取得しない（著作権リスク）。

実行方法:
    # ローカルテスト
    python scripts/sites/service/indoor_golf_navi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id indoor_golf_navi
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

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"^〒?\s*(\d{3}-?\d{4})\s*")

_MAX_PAGES = 50


class IndoorGolfNaviScraper(StaticCrawler):
    """INDOOR GOLF NAVI スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "エリア",          # パンくずの市区町村エリア
        "アクセス",        # 最寄り駅等
        "月額料金",
        "体験料金",
        "シミュレーター",
        "打席数",
    ]

    def _page_url(self, url: str, page: int) -> str:
        """引数 url を起点に WordPress のページ送りURL (/page/N/) を生成する。"""
        if page == 1:
            return url
        return urljoin(url if url.endswith("/") else url + "/", f"page/{page}/")

    def parse(self, url: str):
        seen: set[str] = set()
        for page in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(self._page_url(url, page))
            if soup is None:
                break
            cards = soup.select("article.ign-card")
            if not cards:
                break

            new_urls = []
            for card in cards:
                a = card.select_one("h3.ign-card-title a[href]") or card.select_one("a[href]")
                if not a:
                    continue
                detail_url = urljoin(url, a["href"])
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                new_urls.append(detail_url)

            if not new_urls:
                break

            for detail_url in new_urls:
                item = self._parse_detail(detail_url)
                if item:
                    yield item

            if not soup.select_one(f'a[href*="/page/{page + 1}/"]'):
                break

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        h1 = soup.select_one(".ign-single h1") or soup.find("h1")
        name = h1.get_text(" ", strip=True) if h1 else ""
        if not name:
            return None

        # 全 dl.ign-dl の dt/dd をラベル→値で収集（施設情報・料金・設備）
        fields: dict[str, str] = {}
        hp = ""
        tel = ""
        for dl in soup.select("dl.ign-dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = dt.get_text(strip=True)
                fields.setdefault(label, dd.get_text(" ", strip=True))
                if label == "公式サイト" and not hp:
                    a = dd.find("a", href=True)
                    hp = a["href"] if a else dd.get_text(strip=True)
                if label == "電話番号" and not tel:
                    a = dd.find("a", href=re.compile(r"^tel:"))
                    tel = a["href"][4:] if a and a["href"][4:] else dd.get_text(strip=True)

        crumbs = soup.select(".ign-crumb a")
        area = crumbs[-1].get_text(strip=True) if len(crumbs) >= 3 else ""

        tags = [
            t.get_text(strip=True).lstrip("✓").strip()
            for t in soup.select(".ign-feature-tags a")
        ]

        addr = fields.get("住所", "")
        post_code = ""
        m = _POST_PATTERN.match(addr)
        if m:
            post_code = m.group(1)
            addr = addr[m.end():].strip()
        pref = ""
        m = _PREF_PATTERN.match(addr)
        if m:
            pref = m.group(1)
            addr = addr[m.end():].strip()

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HOLIDAY: fields.get("定休日", ""),
            Schema.CAT_SITE: "/".join(tags),
            "エリア": area,
            "アクセス": fields.get("アクセス", ""),
            "月額料金": fields.get("月額料金", ""),
            "体験料金": fields.get("体験料金", ""),
            "シミュレーター": fields.get("シミュレーター", ""),
            "打席数": fields.get("打席数", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    scraper = IndoorGolfNaviScraper()
    scraper.execute("https://indoor-golf-navi.com/studio/")

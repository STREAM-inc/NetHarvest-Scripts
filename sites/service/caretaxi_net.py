"""
介護タクシー案内所 (caretaxi-net.com) — 全国の介護・福祉タクシー事業者スクレイパー

取得対象:
    - taxi-sitemap*.xml に列挙される事業者詳細ページ
      (/taxi/{region}/{pref}/{city}/{id}/)
    - 各詳細ページの dl.taxi-detail に掲載される事業者情報

取得フロー:
    1. /sitemap.xml から taxi-sitemap*.xml を列挙
    2. 各 taxi-sitemap*.xml から事業者詳細 URL を抽出
       (/taxi/region/pref/city/数字ID/ の形だけを対象。地域/県/市の一覧ページは除外)
    3. 詳細ページを1件取得するごとに即 yield (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/service/caretaxi_net.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id caretaxi_net
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
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 事業者詳細 URL: /taxi/地方/県/市区町村/数字ID/
_DETAIL_RE = re.compile(r"/taxi/[a-z0-9_]+/[a-z0-9_]+/[a-z0-9_]+/\d+/?$")
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.S)


class CaretaxiNetScraper(StaticCrawler):
    """介護タクシー案内所 (caretaxi-net.com) スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "主なサービス",        # 車いす等のアイコンに対応する設備タグ(短い構造化ラベル)
        "その他のサービス",    # 民間救急 / 観光 / クレジットカード 等の短いタグ
        "車種",                # 車種名
        "利用料金",            # 距離制/時間制運賃・割引などの料金体系(数値主体の構造化情報)
        "主な設備・資器材",    # 車いす / ストレッチャー / 酸素 等の短いタグ
        "地方",                # URL から導出する地方名(kanto 等)
    ]

    _SITEMAP_INDEX = "https://caretaxi-net.com/sitemap.xml"

    # dt ラベル -> Schema 定数 のマッピング
    _SCHEMA_LABELS = {
        "所在地": Schema.ADDR,
        "電話番号": Schema.TEL,
        "営業時間": Schema.TIME,
        "代表者名": Schema.REP_NM,
        "ホームページ": Schema.HP,
    }
    # dt ラベル -> EXTRA カラム名 のマッピング
    _EXTRA_LABELS = {
        "主なサービス": "主なサービス",
        "その他のサービス": "その他のサービス",
        "車種": "車種",
        "利用料金": "利用料金",
        "主な設備・資器材": "主な設備・資器材",
    }
    # 取得しないラベル(著作権リスクのある自由記述・ノイズ)
    #   利用者の皆様へ : 事業者が書いた自由記述の紹介文(プロース) -> 著作権リスクで除外
    #   紹介画像・動画 : 画像/動画キャプション(LINE 誘導等の自由文) -> 除外
    #   アプリ予約     : 固定の案内文(ノイズ) -> 除外

    def parse(self, url: str):
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("事業者詳細 URL 総数: %d", self.total_items)

        for detail_url in detail_urls:
            try:
                record = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if record:
                yield record

    def _collect_detail_urls(self) -> list[str]:
        """sitemap index -> taxi-sitemap*.xml -> 事業者詳細 URL を列挙"""
        index_soup = self.get_soup(self._SITEMAP_INDEX)
        if not index_soup:
            return []

        sub_sitemaps = [
            loc for loc in _LOC_RE.findall(index_soup.decode())
            if re.search(r"/taxi-sitemap\d*\.xml$", loc)
        ]

        seen: set[str] = set()
        ordered: list[str] = []
        for sm_url in sub_sitemaps:
            sm_soup = self.get_soup(sm_url)
            if not sm_soup:
                continue
            for loc in _LOC_RE.findall(sm_soup.decode()):
                loc = loc.strip()
                if _DETAIL_RE.search(loc) and loc not in seen:
                    seen.add(loc)
                    ordered.append(loc)
        return ordered

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: "介護タクシー",
        }

        # 地方 (URL: /taxi/{region}/...)
        m = re.search(r"/taxi/([a-z0-9_]+)/", detail_url)
        if m:
            item["地方"] = m.group(1)

        # 名称
        name_el = soup.select_one(".single_taxi h2") or soup.select_one(
            ".single_post h2"
        )
        if not name_el:
            for h2 in soup.select("h2"):
                if h2.get_text(strip=True):
                    name_el = h2
                    break
        if name_el:
            item[Schema.NAME] = name_el.get_text(strip=True)

        dl = soup.select_one("dl.taxi-detail")
        if dl:
            dts = dl.select("dt")
            dds = dl.select("dd")
            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True)
                self._apply_field(item, label, dd)

        if not item.get(Schema.NAME):
            return None
        return item

    def _apply_field(self, item: dict, label: str, dd) -> None:
        # 所在地 -> 住所 / 都道府県 / 郵便番号
        if label == "所在地":
            addr = dd.get_text(" ", strip=True)
            pm = _POST_RE.search(addr)
            if pm:
                item[Schema.POST_CODE] = pm.group(1)
                addr = _POST_RE.sub("", addr).strip()
            item[Schema.ADDR] = addr
            prefm = _PREF_PATTERN.match(addr)
            if prefm:
                item[Schema.PREF] = prefm.group(1)
            return

        # 電話番号 -> tel: アンカー優先、なければテキストから "電話する" 等を除去
        if label == "電話番号":
            tel_a = dd.select_one('a[href^="tel:"]')
            if tel_a:
                item[Schema.TEL] = tel_a.get("href", "").replace("tel:", "").strip()
            else:
                txt = dd.get_text(" ", strip=True)
                txt = re.sub(r"電話する.*$", "", txt).strip()
                item[Schema.TEL] = txt
            return

        # ホームページ -> 外部リンク URL
        if label == "ホームページ":
            hp_a = dd.select_one("a[href]")
            item[Schema.HP] = (
                hp_a.get("href").strip() if hp_a else dd.get_text(strip=True)
            )
            return

        # 主なサービス -> 設備アイコンの title 属性を収集(短い構造化ラベル)
        if label == "主なサービス":
            titles = [
                img.get("title", "").strip()
                for img in dd.select("img[title]")
                if img.get("title", "").strip()
            ]
            value = "、".join(dict.fromkeys(titles)) if titles else dd.get_text(
                " ", strip=True
            )
            if value:
                item["主なサービス"] = value
            return

        # その他の Schema フィールド (営業時間 / 代表者名)
        if label in self._SCHEMA_LABELS:
            value = dd.get_text(" ", strip=True)
            if value:
                item[self._SCHEMA_LABELS[label]] = value
            return

        # EXTRA フィールド
        if label in self._EXTRA_LABELS:
            value = dd.get_text(" ", strip=True)
            if value:
                item[self._EXTRA_LABELS[label]] = value
            return


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CaretaxiNetScraper()
    scraper.execute("https://caretaxi-net.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

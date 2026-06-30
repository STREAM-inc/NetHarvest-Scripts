"""
タウンライフリフォーム — タウンライフ家づくり 加盟店(住宅会社)情報スクレイパー

取得対象:
    - タウンライフ家づくり (www.town-life.jp/home) に掲載される加盟店の会社概要。
      各加盟店の詳細ページ `/home/shopdetail{N}.html` の「基本情報」テーブルから
      企業名・所在地・問い合わせ先・代表者・資本金・従業員数などを取得する。

取得フロー:
    1. ルート URL から `/home/sitemap.xml` を派生して取得し、
       `/home/shopdetail{N}.html` 形式の加盟店ページ URL を列挙する (約1,456件)。
    2. 各詳細ページにアクセスし、「基本情報」テーブル (h3 "基本情報" 直後の table) を解析。
    3. 1件取得するごとに即 yield する (Pattern B)。

備考:
    - 「こだわりポイント」「対応可能工法」「アフター保証」は自由記述の長文(プロース)のため
      著作権リスク回避の観点から取得しない。
    - 「対応可能エリア」「FAX」「各種資格者」「各種免許」は構造化された短いラベルのため EXTRA で取得する。

実行方法:
    python scripts/sites/construction/town_life.py
    docker compose exec worker python /app/bin/run_flow.py --site-id town_life
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 都道府県プレフィックス (所在地から PREF を切り出す)
_PREF_RE = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_POSTCODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"TEL[:：]?\s*([\d\-()（）]+)", re.IGNORECASE)
_FAX_RE = re.compile(r"FAX[:：]?\s*([\d\-()（）]+)", re.IGNORECASE)
_TIME_RE = re.compile(r"(\d{1,2}[:：]\d{2}\s*[〜～\-－]\s*\d{1,2}[:：]\d{2})")

# 詳細ページ URL のパターン
_SHOP_RE = re.compile(r"/home/shopdetail\d+\.html$")


class TownLifeScraper(StaticCrawler):
    """タウンライフ家づくり 加盟店情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "対応可能エリア",
        "FAX",
        "各種資格者",
        "各種免許",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルート url から /home/sitemap.xml を派生 (url は唯一の起点)
        sitemap_url = urljoin(url, "../sitemap.xml")
        shop_urls = self._collect_shop_urls(sitemap_url)
        self.total_items = len(shop_urls)
        self.logger.info("加盟店URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得エラー: %s — %s", shop_url, e)
                continue

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        """sitemap.xml から shopdetail ページの URL を列挙する。"""
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if href and _SHOP_RE.search(href) and href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 「基本情報」見出し直後のテーブルを会社概要として解析する
        heading = soup.find(
            lambda tag: tag.name in ("h2", "h3")
            and tag.get_text(strip=True) == "基本情報"
        )
        table = heading.find_next("table") if heading else None
        if table is None:
            return None

        data: dict = {Schema.URL: url}

        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            if not value:
                continue

            if label == "企業名":
                data[Schema.NAME] = value
            elif label == "所在地":
                self._parse_address(value, data)
            elif label == "問い合わせ先":
                self._parse_contact(value, data)
            elif label == "設立":
                data[Schema.OPEN_DATE] = value
            elif label == "代表者氏名":
                data[Schema.REP_NM] = value
            elif label == "資本金":
                data[Schema.CAP] = value
            elif label == "従業員数":
                data[Schema.EMP_NUM] = value
            elif label == "会社URL":
                link = td.find("a", href=True)
                data[Schema.HP] = link["href"].strip() if link else value
            elif label == "対応可能エリア":
                data["対応可能エリア"] = value
            elif label == "各種資格者":
                data["各種資格者"] = value
            elif label == "各種免許":
                data["各種免許"] = value
            # こだわりポイント/対応可能工法/アフター保証 等の自由記述は取得しない

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _parse_address(value: str, data: dict) -> None:
        """所在地から郵便番号・都道府県・住所を切り出す。"""
        pc = _POSTCODE_RE.search(value)
        if pc:
            data[Schema.POST_CODE] = pc.group(1)
            value = value[pc.end():].strip()
        value = value.lstrip("〒").strip()
        m = _PREF_RE.match(value)
        if m:
            data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = value[m.end():].strip()
        else:
            data[Schema.ADDR] = value

    @staticmethod
    def _parse_contact(value: str, data: dict) -> None:
        """問い合わせ先から TEL・FAX・営業時間を切り出す。"""
        tel = _TEL_RE.search(value)
        if tel:
            data[Schema.TEL] = tel.group(1).strip()
        fax = _FAX_RE.search(value)
        if fax:
            data["FAX"] = fax.group(1).strip()
        tm = _TIME_RE.search(value)
        if tm:
            data[Schema.TIME] = tm.group(1).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TownLifeScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.town-life.jp/home/main/chatform.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

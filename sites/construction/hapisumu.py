"""
ハピすむ — リフォーム加盟店（会社）情報スクレイパー

取得対象:
    - ハピすむ (https://hapisumu.jp/) のリフォーム加盟店（会社）詳細ページ
      `/company/{id}/` の会社概要（社名・所在地・電話・代表者・資本金 等）。

取得フロー:
    1. company-detail-sitemap.xml（登録加盟店）と
       company-detail-guest-sitemap.xml（ゲスト掲載）から全会社詳細URLを収集（重複排除）。
    2. 各詳細ページ `/company/{id}/` を 1 件取得するたびに即 yield（一覧→詳細 Pattern B）。
    3. 会社概要は `dl.divide-y`（dt/dd）から、本社所在地は 〒 を含む <p> ブロックから抽出。

備考:
    - 「会社の特徴・強み」「口コミ・お客さまの声」等の自由記述プロースは著作権リスクのため取得しない。

実行方法:
    python scripts/sites/construction/hapisumu.py
    docker compose exec worker python /app/bin/run_flow.py --site-id hapisumu
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


# 会社詳細URLを列挙するサイトマップ（url からの相対で解決する）
_SITEMAP_PATHS = [
    "company-detail-sitemap.xml",        # 登録加盟店
    "company-detail-guest-sitemap.xml",  # ゲスト掲載
]

_DETAIL_RE = re.compile(r"/company/\d+/?$")

_PREF_RE = re.compile(
    r"(東京都|北海道|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 会社概要 dl のラベル → Schema 定数
_LABEL_TO_SCHEMA = {
    "電話番号": Schema.TEL,
    "従業員数": Schema.EMP_NUM,
    "事業内容": Schema.LOB,
    "創業年": Schema.OPEN_DATE,
    "営業時間": Schema.TIME,
    "代表者名": Schema.REP_NM,
    "資本金": Schema.CAP,
    "売上高": Schema.SALES,
    "決済手法": Schema.PAYMENTS,
    # 会社URL は href を優先するため個別処理
}

# 会社概要 dl のラベル → EXTRA_COLUMNS（構造化された短い値のみ）
_LABEL_TO_EXTRA = [
    "建設業許可内容",
    "資格",
    "保険",
    "加盟団体・協会",
    "補助金・優待制度の対応可否",
    "提携ローン",
]


class HapisumuScraper(StaticCrawler):
    """ハピすむ リフォーム加盟店スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "会社ID",
        "対応エリア",
        *_LABEL_TO_EXTRA,
        "更新日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("会社詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別ページのエラーはスキップして継続
                self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                continue

    def _collect_detail_urls(self, url: str) -> list[str]:
        """サイトマップから会社詳細URLを収集し、重複排除して返す。"""
        seen: set[str] = set()
        ordered: list[str] = []
        for path in _SITEMAP_PATHS:
            sitemap_url = urljoin(url, path)
            soup = self.get_soup(sitemap_url)
            if soup is None:
                self.logger.warning("サイトマップ取得失敗: %s", sitemap_url)
                continue
            for loc in soup.find_all("loc"):
                href = loc.get_text(strip=True)
                if not href or not _DETAIL_RE.search(href):
                    continue
                normalized = href.split("?")[0].split("#")[0]
                if normalized not in seen:
                    seen.add(normalized)
                    ordered.append(normalized)
        return ordered

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        m_id = re.search(r"/company/(\d+)/?$", url)
        if m_id:
            data["会社ID"] = m_id.group(1)

        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(" ", strip=True)

        # 会社概要 dl（dt/dd）
        for dl in soup.select("dl.divide-y"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True)
                if label == "会社URL":
                    link = dd.find("a", href=True)
                    value = link["href"].strip() if link else dd.get_text(strip=True)
                    if value:
                        data[Schema.HP] = value
                    continue
                value = dd.get_text(" ", strip=True)
                if not value:
                    continue
                if label in _LABEL_TO_SCHEMA:
                    data[_LABEL_TO_SCHEMA[label]] = value
                elif label in _LABEL_TO_EXTRA:
                    data[label] = value

        # 本社所在地（〒 を含む <p> とその次の <p>）
        self._extract_hq_address(soup, data)

        # 対応エリア（data 属性）
        area_el = soup.select_one("[data-company-detail-service-area-text]")
        if area_el:
            area = area_el.get_text(" ", strip=True)
            if area:
                data["対応エリア"] = area

        # 都道府県のフォールバック（本社所在地が無い場合は表示用所在地から）
        if not data.get(Schema.PREF):
            loc_el = soup.select_one("[data-company-detail-location]")
            if loc_el:
                loc_txt = loc_el.get_text(" ", strip=True).lstrip("：: ").strip()
                pm = _PREF_RE.search(loc_txt)
                if pm:
                    data[Schema.PREF] = pm.group(1)
                    if not data.get(Schema.ADDR):
                        data[Schema.ADDR] = loc_txt

        # 更新日
        upd = soup.find(string=re.compile(r"更新日"))
        if upd:
            um = re.search(r"(\d{4}/\d{1,2}/\d{1,2})", upd)
            if um:
                data["更新日"] = um.group(1)

        if not data.get(Schema.NAME):
            return None
        return data

    def _extract_hq_address(self, soup, data: dict) -> None:
        """〒 を含む <p> を起点に本社の郵便番号・住所・都道府県を抽出する。"""
        pc_p = soup.find(
            "p", string=re.compile(r"^\s*〒\s*[\d０-９]{3}[-－][\d０-９]{4}")
        )
        if not pc_p:
            return
        post_code = pc_p.get_text(strip=True).lstrip("〒").strip()
        if post_code:
            data[Schema.POST_CODE] = post_code

        addr_p = pc_p.find_next_sibling("p")
        if not addr_p:
            return
        addr = addr_p.get_text(" ", strip=True)
        if not addr:
            return
        pm = _PREF_RE.match(addr)
        if pm:
            data[Schema.PREF] = pm.group(1)
            data[Schema.ADDR] = addr[pm.end():].strip()
        else:
            data[Schema.ADDR] = addr


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HapisumuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://hapisumu.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

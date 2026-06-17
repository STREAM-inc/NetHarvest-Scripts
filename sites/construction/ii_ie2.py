"""
いい家ネット — 工務店・リフォーム会社一覧

取得対象:
    - 工務店・リフォーム会社の会社情報（全国、12974件）

取得フロー:
    1. /koumuten?page=N で一覧を巡回（865ページ、15件/ページ）
    2. 各詳細ページ /koumuten_detail/{ID} から会社情報を取得・即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/construction/ii_ie2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ii_ie2
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
_POST_PATTERN = re.compile(r"〒(\d{3}-\d{4})")

_SNS_MAP = {
    "hpリンク": Schema.HP,
    "x": Schema.X,
    "instagram": Schema.INSTA,
    "line": Schema.LINE,
    "facebook": Schema.FB,
}

_BASE = "https://www.ii-ie2.net"


class IiIe2(StaticCrawler):
    """いい家ネット 工務店・リフォーム会社スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["取扱工事", "施工エリア", "免許・許可"]

    def parse(self, url: str):
        page = 1
        while True:
            soup = self.get_soup(f"{url}?page={page}")
            links = soup.select("a.item-box__link")
            if not links:
                break

            if page == 1:
                total_el = soup.select_one(".shop-search__result-num")
                if total_el:
                    m = re.search(r"(\d[\d,]*)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for link in links:
                detail_href = link.get("href", "")
                if not detail_href:
                    continue
                detail_url = urljoin(_BASE, detail_href)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
        except Exception as e:
            self.logger.warning(f"detail fetch failed: {url}: {e}")
            return None

        name_el = soup.select_one(".shop-about__name-st")
        if not name_el:
            return None

        # 住所・郵便番号・都道府県の分解
        addr_el = soup.select_one(".shop-about__add-value")
        raw_addr = addr_el.get_text(strip=True) if addr_el else ""
        post_code = ""
        pref = ""
        addr = raw_addr
        m_post = _POST_PATTERN.search(raw_addr)
        if m_post:
            post_code = m_post.group(1)
            addr = raw_addr[m_post.end():].strip()
        m_pref = _PREF_PATTERN.match(addr)
        if m_pref:
            pref = m_pref.group(1)
            addr = addr[m_pref.end():].strip()

        # 会社概要のラベル→値マッピング
        overview: dict[str, str] = {}
        for item in soup.select(".overview-list__item"):
            label_el = item.select_one(".overview-list__label")
            value_el = item.select_one(".overview-list__value")
            if label_el and value_el:
                overview[label_el.get_text(strip=True)] = value_el.get_text(strip=True)

        # 電話番号
        tel_el = soup.select_one("a.fixed-nav__btn-link[href^='tel:']")
        tel = tel_el["href"].replace("tel:", "") if tel_el else ""

        # HP・SNSリンク（テキストでマッピング）
        sns: dict = {}
        for a in soup.select("a.sns-list__link"):
            key = a.get_text(strip=True).lower()
            schema_col = _SNS_MAP.get(key)
            if schema_col and schema_col not in sns:
                sns[schema_col] = a.get("href", "")

        return {
            Schema.URL: url,
            Schema.NAME: name_el.get_text(strip=True),
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: overview.get("代表者", ""),
            Schema.OPEN_DATE: overview.get("設立", ""),
            Schema.CAP: overview.get("資本金", ""),
            Schema.EMP_NUM: overview.get("従業員数", ""),
            Schema.HP: sns.get(Schema.HP, ""),
            Schema.LINE: sns.get(Schema.LINE, ""),
            Schema.INSTA: sns.get(Schema.INSTA, ""),
            Schema.X: sns.get(Schema.X, ""),
            Schema.FB: sns.get(Schema.FB, ""),
            "取扱工事": overview.get("取扱工事", ""),
            "施工エリア": overview.get("施工エリア", ""),
            "免許・許可": overview.get("免許・許可", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = IiIe2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.ii-ie2.net/koumuten")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

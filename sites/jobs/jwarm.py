"""
ジェイウォーム (jwarm.net) — 求人・掲載企業情報スクレイパー (StaticCrawler 版)

取得対象:
    掲載企業の基本情報
    (企業名称・郵便番号・住所・電話番号・代表者・資本金・設立・従業員数・売上高・事業内容・HP)

取得フロー:
    1. 一覧ページ /uni_items.php?pg=N&ig=i を pg=1 から巡回
    2. div#itemList 内の span.detail_btn a から詳細URL (uni_item_detail.php?id=...) を収集・重複排除
    3. itemList が無い / リンクが空になったページで巡回終了 (範囲超過)
    4. 各詳細ページの div#kigyou_data テーブル (<th><span>ラベル</span></th><td>値</td>) を抽出

★ なぜ StaticCrawler / Playwright 不要か (切り分け済み):
    - 当サイトは完全サーバーサイドレンダリング。詳細ページの div#kigyou_data は
      初期 HTML に含まれ、リファラ無しのコールドアクセスでも requests で全件取得可。
    - ただし python-requests 等の bot 系 UA は 403 で弾かれる → 実ブラウザ UA が必須。
    - Content-Type に charset が無く body は UTF-8 → 明示しないと文字化けし
      <th> ラベルが一致せずパースが空になる。よって encoding を utf-8 に固定する。
    - Playwright(ヘッドレス)経由だと詳細ページ取得が不安定/空になるため使わない。

実行方法:
    python scripts/sites/jobs/jwarm_scraper.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.jwarm.net"

# jwarm.net は python-requests / bot 系 UA を 403 で弾く。実ブラウザ UA が必須。
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# itemList が永遠に空にならない異常時の無限ループ保険 (実データは数十ページ程度)
_MAX_PAGES = 500


class JwarmScraper(StaticCrawler):
    """ジェイウォーム 掲載企業スクレイパー (jwarm.net)"""

    DELAY = 1.0
    # 代表者(REP_NM)/資本金(CAP)/売上高(SALES)/従業員数(EMP_NUM) は Schema 標準項目。
    # EXTRA には Schema に無い 設立日・事業内容 のみを追加する。
    EXTRA_COLUMNS = ["設立日", "事業内容"]

    def prepare(self):
        """セッション初期化 (フレームワークが parse 前に呼ぶ。__init__ のオーバーライドは禁止)"""
        self._session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        self._session.mount("https://", HTTPAdapter(max_retries=retries))
        self._session.headers.update(
            {
                "User-Agent": _BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en;q=0.9",
            }
        )

    def _fetch_soup(self, url: str):
        """セッションで取得して soup を返す (UA / 文字化け対策込み)。失敗時 None。"""
        if getattr(self, "_session", None) is None:
            self.prepare()
        try:
            resp = self._session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("取得失敗 (%s): %s", url, e)
            return None
        # Content-Type に charset が無いと requests は ISO-8859-1 と誤認する。body は UTF-8。
        if "charset=" not in resp.headers.get("Content-Type", "").lower():
            resp.encoding = "utf-8"
        return bs4.BeautifulSoup(resp.text, "html.parser")

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        for i, detail_url in enumerate(detail_urls, 1):
            item = self._scrape_detail(detail_url)
            if item:
                yield item
            if i % 20 == 0:
                self.logger.info("詳細取得 %d/%d", i, len(detail_urls))

    def _collect_detail_urls(self, base_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        parsed = urlparse(base_url)
        params = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}

        for page in range(1, _MAX_PAGES + 1):
            params["pg"] = str(page)
            page_url = urlunparse(parsed._replace(query=urlencode(params)))

            soup = self._fetch_soup(page_url)
            if soup is None:
                break

            item_list = soup.find("div", id="itemList")
            if not item_list:
                break

            links = [a.get("href", "") for a in item_list.select("span.detail_btn a") if a.get("href")]
            if not links:  # ページ範囲超過 = 終了
                break

            for link in links:
                full = urljoin(base_url, link)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

            self.logger.info("page %d: 累計 %d 件", page, len(urls))
        else:
            self.logger.warning("ページ上限 %d に到達。巡回を打ち切りました。", _MAX_PAGES)

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self._fetch_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 電話番号は <span class='Tel'> (シングルクオート) で複数ある。最初の1つを採用。
        tel_span = soup.find("span", class_="Tel")
        if tel_span:
            data[Schema.TEL] = tel_span.get_text(strip=True)

        target_div = soup.find("div", id="kigyou_data")
        if target_div:
            for row in target_div.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = re.sub(r"[　\xa0]", " ", td.get_text(strip=True)).strip()

                if label == "企業名称":
                    data[Schema.NAME] = value
                elif label == "掲載住所":
                    if value.startswith("〒"):
                        parts = re.split(r"\s+", value, maxsplit=1)
                        if len(parts) == 2:
                            data[Schema.POST_CODE] = parts[0]
                            data[Schema.ADDR] = parts[1].strip()
                        else:
                            data[Schema.ADDR] = value
                    else:
                        data[Schema.ADDR] = value
                elif label == "設立":
                    data["設立日"] = value
                elif label == "URL":
                    data[Schema.HP] = value
                elif label == "代表者":
                    data[Schema.REP_NM] = value
                elif label == "資本金":
                    data[Schema.CAP] = value
                elif label == "事業内容":
                    data["事業内容"] = value
                elif label == "売上高":
                    data[Schema.SALES] = value
                elif label == "従業員数":
                    data[Schema.EMP_NUM] = value

        if not data.get(Schema.NAME):
            self.logger.warning("企業名称が取得できませんでした: %s", url)
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JwarmScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jwarm.net/uni_items.php?pg=1&ig=i")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

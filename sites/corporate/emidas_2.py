"""
エミダス(emidas / NC Network) — 製造業工場データベース

取得対象:
    - ja.nc-net.or.jp に登録されている全製造業企業（約23,608社、日本・海外含む）

取得フロー:
    /search/search/?pno=N を全ページ巡回 → 各企業詳細ページ /company/{id}/ を取得

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/emidas_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id emidas_2
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# onclick="redirect_company_hp('https://example.com')" などを想定
_HP_RE = re.compile(r"redirect_company_hp\s*\(\s*['\"]([^'\"]+)['\"]")

# 住所セルに混入する「[地図を見る]」リンクや先頭の国名「日本」を除去
_MAP_LINK_RE = re.compile(r"\[?\s*地図を見る\s*\]?")
_COUNTRY_PREFIX_RE = re.compile(r"^日本\s*")


def _clean_address(val: str) -> str:
    """住所セルから '[地図を見る]' リンクや先頭の '日本' を除去して整形する。"""
    val = _MAP_LINK_RE.sub("", val)
    val = _COUNTRY_PREFIX_RE.sub("", val)
    return re.sub(r"\s+", " ", val).strip()


class Emidas2(StaticCrawler):
    """エミダス(NC Network) 製造業企業スクレイパー"""

    DELAY = 1.5
    # 低速サイト対策: get_soup の既定 20 秒ではリストページがタイムアウトしやすいため延長する。
    TIMEOUT = 60
    EXTRA_COLUMNS = ["fax", "industry", "annual_sales", "main_products"]

    PER_PAGE = 10               # 1 検索ページあたりの企業件数
    PAGE_RETRY = 4              # リスト/詳細ページの再取得試行回数
    # 総件数が取得できなかった場合のページ上限（無限ループ防止のフェイルセーフ）。
    MAX_PAGES_FALLBACK = 5000

    def parse(self, url: str):
        # url = https://ja.nc-net.or.jp/?cntry=999 を起点に検索エンドポイントを派生
        search_base = urljoin(url, "/search/search/")
        page = 1
        total_pages = None
        consecutive_failures = 0
        self.total_items = None

        while True:
            page_url = f"{search_base}?pno={page}"
            # タイムアウト等で None が返ってもクロール全体を止めないようリトライする。
            soup = self._get_soup_with_retry(page_url)

            if soup is None:
                # リトライ上限まで失敗。ここで break すると以降のページを全て取り逃すため、
                # ページをスキップして継続する（総ページ数 or フェイルセーフ上限で必ず終了する）。
                consecutive_failures += 1
                self.logger.warning(
                    "リストページの取得に失敗しました（リトライ上限到達, 連続%d回）。"
                    "スキップして継続します: %s",
                    consecutive_failures, page_url,
                )
                if total_pages is not None:
                    if page >= total_pages:
                        break
                elif consecutive_failures >= self.PAGE_RETRY:
                    # 総ページ数が不明なまま連続失敗が続く場合は打ち切る。
                    self.logger.error("リストページ取得の連続失敗が上限に達したため終了します。")
                    break
                page += 1
                continue

            consecutive_failures = 0

            # 初回ページで総件数・総ページ数を取得
            if self.total_items is None:
                total_el = soup.select_one("div.subject-display em.em-02")
                if total_el:
                    m = re.search(r"[\d,]+", total_el.get_text())
                    if m:
                        self.total_items = int(m.group().replace(",", ""))
                        total_pages = -(-self.total_items // self.PER_PAGE)  # 切り上げ
                        self.logger.info(
                            "総件数 %d 件 / 全 %d ページを巡回します。",
                            self.total_items, total_pages,
                        )

            # 企業リンクを取得
            company_links = soup.select("h2.ttl-h3-03 a[href^='/company/']")
            if not company_links:
                # 正常に取得できたが企業リンクが無い → 検索結果の末尾に到達
                break

            for link in company_links:
                detail_url = urljoin(url, link["href"])
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(f"詳細取得エラー {detail_url}: {e}")
                    continue

            # 終了判定（保険）: 総ページ数到達、または総件数不明時のフェイルセーフ上限
            if total_pages is not None:
                if page >= total_pages:
                    break
            elif page >= self.MAX_PAGES_FALLBACK:
                self.logger.warning(
                    "ページ数がフェイルセーフ上限(%d)に達したため終了します。",
                    self.MAX_PAGES_FALLBACK,
                )
                break

            page += 1

    def _get_soup_with_retry(self, page_url: str):
        """get_soup を複数回試行する。タイムアウト等で None が返る間はバックオフして再取得する。

        get_soup 自体は 5xx に対してのみ自動リトライするため、接続エラーやタイムアウト
        （CONTINUE_ON_ERROR=True 時は None を返す）はここで吸収して取りこぼしを防ぐ。
        """
        for attempt in range(self.PAGE_RETRY):
            soup = self.get_soup(page_url)
            if soup is not None:
                return soup
            if attempt < self.PAGE_RETRY - 1:
                wait = self.DELAY * (attempt + 2)  # 漸増バックオフ
                self.logger.warning(
                    "ページ再取得 (%d/%d, %.1f秒待機): %s",
                    attempt + 1, self.PAGE_RETRY, wait, page_url,
                )
                time.sleep(wait)
        return None

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self._get_soup_with_retry(detail_url)
        if soup is None:
            self.logger.warning("詳細ページの取得に失敗しました（リトライ上限到達）: %s", detail_url)
            return None

        # 企業名 (h1 優先、なければページタイトルから)
        name_el = soup.select_one("h1")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        data = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMP_NUM: "",
            Schema.CAP: "",
            Schema.REP_NM: "",
            Schema.OPEN_DATE: "",
            Schema.HP: "",
            "fax": "",
            "industry": "",
            "annual_sales": "",
            "main_products": "",
        }

        # テーブルの th/td ペアからフィールドを抽出
        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = td.get_text(separator=" ", strip=True)

            if re.search(r"電話|TEL", key, re.I):
                if not data[Schema.TEL]:
                    data[Schema.TEL] = val
            elif "FAX" in key:
                data["fax"] = val
            elif re.search(r"住所|所在地", key):
                addr = _clean_address(val)
                m = _PREF_RE.match(addr)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = addr[m.end():].strip()
                else:
                    data[Schema.ADDR] = addr
            elif re.search(r"資本金", key):
                data[Schema.CAP] = val
            elif re.search(r"社員|従業員", key):
                data[Schema.EMP_NUM] = val
            elif re.search(r"代表", key):
                data[Schema.REP_NM] = val
            elif re.search(r"設立|創業", key):
                data[Schema.OPEN_DATE] = val
            elif re.search(r"産業分類|業種", key):
                data["industry"] = val
            elif re.search(r"売上", key):
                data["annual_sales"] = val
            elif re.search(r"主要.*品目|品目", key):
                data["main_products"] = val

        # HP URL (onclick="redirect_company_hp(...)" 形式)
        hp_el = soup.select_one("a[onclick*='redirect_company_hp']")
        if hp_el:
            m = _HP_RE.search(hp_el.get("onclick", ""))
            data[Schema.HP] = m.group(1) if m else hp_el.get("href", "")

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Emidas2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://ja.nc-net.or.jp/?cntry=999")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

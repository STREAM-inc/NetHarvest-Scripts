"""
人材サービス総合サイト — 労働者派遣事業者スクレイパー（株式会社）

取得対象:
    人材サービス総合サイトの開業届出・事業所一覧検索から
    全国 × 事業主名称「株式会社」部分一致 × 区分「労働者派遣事業」
    に絞り込んだ全事業所の詳細情報

取得フロー（2フェーズ並列化）:
    Phase1: 「株式会社」全ページを逐次POSTしてURLを収集・重複排除
    Phase2: 詳細ページを MAX_WORKERS 並列で GET

実行方法:
    python scripts/sites/portal/jinzai_service_rodosha_haken_kabushikikaisha.py

    Prefect Flow 経由:
    python bin/run_flow.py --site-id jinzai_service_rodosha_haken_kabushikikaisha
"""

import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# ─────────────────────────────────────────────────
# URL定数
# ─────────────────────────────────────────────────
BASE_URL = "https://jinzai.hellowork.mhlw.go.jp"
TOP_URL = BASE_URL + "/JinzaiWeb/GICB101010.do"
SEARCH_URL = BASE_URL + "/JinzaiWeb/GICB102010.do"

SITE_NAME = "人材サービス総合サイト_労働者派遣事業_株式会社"

# ─────────────────────────────────────────────────
# 都道府県抽出パターン
# ─────────────────────────────────────────────────
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:神奈川|埼玉|千葉|茨城|栃木|群馬|山梨|長野|新潟|富山|石川|福井|"
    r"静岡|愛知|岐阜|三重|滋賀|兵庫|奈良|和歌山|"
    r"青森|岩手|宮城|秋田|山形|福島|"
    r"鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|"
    r"福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# ─────────────────────────────────────────────────
# スレッドローカルな requests.Session
# ─────────────────────────────────────────────────
_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """スレッドごとに1つの Session を生成・再利用する（スレッドセーフ）。"""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        _thread_local.session = s
    return _thread_local.session


def _split_pref_addr(full_addr: str) -> tuple[str, str]:
    """住所文字列を都道府県と市区町村以降に分割する。"""
    m = _PREF_PATTERN.match(full_addr)
    if m:
        pref = m.group(1)
        return pref, full_addr[len(pref):]
    return "", full_addr


def _is_error_page(soup: bs4.BeautifulSoup) -> bool:
    """サーバーエラーページかどうかを判定する。"""
    body = soup.get_text()
    return (
        "エラーが発生しました" in body
        or "サーバーでエラー" in body
        or "システムを最初から" in body
    )


class JinzaiServiceRodoshaHakenKabushikikaishaScaper(StaticCrawler):
    """人材サービス総合サイト 労働者派遣事業者スクレイパー（株式会社 × 並列取得）"""

    DELAY = 0
    MAX_WORKERS = 15
    LIST_PAGE_DELAY = 0.5
    MAX_PAGE_RETRIES = 5

    KEYWORDS = ["株式会社"]

    EXTRA_COLUMNS = [
        "許可受理番号",
        "許可受理年月日",
        "事業所名称",
        "派遣先件数",
        "派遣料金平均額",
        "賃金平均額",
        "マージン率",
        "労使協定締結",
        "キャリア形成支援",
        "その他情報",
        "備考",
    ]

    # ─────────────────────────────────────────────────
    # フック
    # ─────────────────────────────────────────────────

    def prepare(self):
        """Hello Work セッションを初期化する。"""
        self.logger.info("Hello Work セッションを初期化しています...")
        self._init_session()

    def _init_session(self):
        """セッションを（再）初期化する。セッション切れ時の再接続にも使う。"""
        self.session.get(TOP_URL, timeout=20)
        self.session.post(
            TOP_URL,
            data={"screenId": "GICB101010", "action": "initDisp"},
            timeout=20,
        )
        self.session.post(
            TOP_URL,
            data={"screenId": "GICB101010", "action": "transition", "params": "0"},
            timeout=20,
        )

    # ─────────────────────────────────────────────────
    # メインロジック
    # ─────────────────────────────────────────────────

    def parse(self, url: str):
        """
        Phase1: キーワードごとに全URL収集 → 重複排除
        Phase2: ThreadPoolExecutor で並列取得 → yield
        """
        all_detail_urls = self._collect_all_detail_urls()
        self.total_items = len(all_detail_urls)
        self.logger.info(
            "Phase2 開始: %d件（重複排除済み）を %d並列で取得します",
            self.total_items,
            self.MAX_WORKERS,
        )
        yield from self._fetch_details_parallel(all_detail_urls)

    # ─────────────────────────────────────────────────
    # Phase1: 全URL収集（キーワード × 全ページ）
    # ─────────────────────────────────────────────────

    def _collect_all_detail_urls(self) -> list[str]:
        url_set: set[str] = set()

        for keyword in self.KEYWORDS:
            self.logger.info("=== キーワード「%s」のURL収集を開始 ===", keyword)
            keyword_urls = self._collect_urls_for_keyword(keyword)
            before = len(url_set)
            url_set.update(keyword_urls)
            added = len(url_set) - before
            self.logger.info(
                "「%s」完了: %d件収集 / 重複 %d件 / 累計 %d件",
                keyword, len(keyword_urls), len(keyword_urls) - added, len(url_set),
            )

        self.logger.info("Phase1 完了: 全URL %d件（重複排除済み）", len(url_set))
        return list(url_set)

    def _collect_urls_for_keyword(self, keyword: str) -> list[str]:
        """1キーワード分の全ページを逐次POSTして詳細URLリストを返す。"""
        search_params = {
            "screenId": "GICB102010",
            "action": "search",
            "params": "",
            "cbZenkoku": "1",
            "txtJigyonushiName": keyword,
            "cbJigyonushiName": "1",
            "cbJigyoshoKbnHan": "1",
            "hfScrollTop": "0",
        }

        soup = self._post_with_retry(SEARCH_URL, search_params, page_label="1ページ目初回")
        if soup is None:
            self.logger.error("「%s」の1ページ目取得に失敗しました", keyword)
            return []

        count_el = soup.find(id="ID_lbSearchCount")
        total = int(count_el.text.strip()) if count_el else 0
        total_pages = max(1, (total + 19) // 20)
        self.logger.info("  「%s」: %d件 / %dページ", keyword, total, total_pages)

        all_urls: list[str] = []
        hrefs = self._extract_detail_hrefs(soup)
        all_urls.extend(urljoin(SEARCH_URL, h) for h in hrefs)

        for page_num in range(2, total_pages + 1):
            time.sleep(self.LIST_PAGE_DELAY)
            page_data = {
                "screenId": "GICB102010",
                "action": "page",
                "params": str(page_num),
                "hfScrollTop": "0",
            }
            soup = self._post_with_retry(
                SEARCH_URL, page_data,
                page_label=f"{page_num}/{total_pages}ページ",
                search_params=search_params,
                resume_page=page_num,
            )
            if soup is None:
                self.logger.warning(
                    "  「%s」 %d/%dページの取得を最終的に断念します",
                    keyword, page_num, total_pages,
                )
                continue

            hrefs = self._extract_detail_hrefs(soup)
            all_urls.extend(urljoin(SEARCH_URL, h) for h in hrefs)

            if page_num % 200 == 0 or page_num == total_pages:
                self.logger.info(
                    "  URL収集: %d/%dページ完了 (累計 %d件)",
                    page_num, total_pages, len(all_urls),
                )

        return all_urls

    def _post_with_retry(
        self,
        url: str,
        data: dict,
        page_label: str = "",
        search_params: dict | None = None,
        resume_page: int | None = None,
    ) -> bs4.BeautifulSoup | None:
        """
        POST リクエストをリトライ付きで実行する。

        エラーページ・例外が発生した場合:
          1. セッションを再初期化
          2. search_params が渡されていれば検索からやり直し、resume_page まで page アクションで進める
          3. それでも失敗したら None を返す
        """
        for attempt in range(1, self.MAX_PAGE_RETRIES + 1):
            try:
                r = self.session.post(url, data=data, timeout=30)
                soup = bs4.BeautifulSoup(r.content, "html.parser")

                if _is_error_page(soup):
                    raise RuntimeError("サーバーエラーページを検出")

                count_el = soup.find(id="ID_lbSearchCount")
                hrefs = self._extract_detail_hrefs(soup)
                if count_el is None and not hrefs and data.get("action") == "search":
                    raise RuntimeError("検索結果ページの取得に失敗（count_el なし）")

                return soup

            except Exception as e:
                self.logger.warning(
                    "  %s 取得失敗 (試行 %d/%d): %s",
                    page_label, attempt, self.MAX_PAGE_RETRIES, e,
                )
                if attempt < self.MAX_PAGE_RETRIES:
                    wait = 2 ** attempt
                    self.logger.info("  %d秒待機後にセッション再初期化して再試行します", wait)
                    time.sleep(wait)
                    try:
                        self._init_session()
                        if search_params and resume_page and resume_page > 1:
                            self.logger.info(
                                "  セッション再初期化完了。検索から %dページ目まで高速スキップします",
                                resume_page,
                            )
                            self.session.post(SEARCH_URL, data=search_params, timeout=30)
                            for p in range(2, resume_page):
                                self.session.post(
                                    SEARCH_URL,
                                    data={
                                        "screenId": "GICB102010",
                                        "action": "page",
                                        "params": str(p),
                                        "hfScrollTop": "0",
                                    },
                                    timeout=30,
                                )
                                time.sleep(0.1)
                    except Exception as reinit_e:
                        self.logger.warning("  セッション再初期化に失敗: %s", reinit_e)

        self.logger.error("  %s の取得を %d回試みましたが失敗しました", page_label, self.MAX_PAGE_RETRIES)
        return None

    # ─────────────────────────────────────────────────
    # Phase2: 並列詳細取得
    # ─────────────────────────────────────────────────

    def _fetch_details_parallel(self, detail_urls: list[str]):
        """ThreadPoolExecutor で MAX_WORKERS 並列に詳細ページを取得する。"""
        done_count = 0

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_one_detail, url): url
                for url in detail_urls
            }
            for future in as_completed(futures):
                url = futures[future]
                try:
                    item = future.result()
                    if item:
                        done_count += 1
                        if done_count % 1000 == 0:
                            self.logger.info(
                                "  詳細取得: %d/%d件完了", done_count, len(detail_urls)
                            )
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s / %s", url, e)

    def _fetch_one_detail(self, url: str) -> dict | None:
        """1件の詳細ページをスレッドローカル Session で取得してパースする。"""
        session = _get_thread_session()
        r = session.get(url, timeout=20)
        soup = bs4.BeautifulSoup(r.content, "html.parser")
        return self._parse_detail(soup, url)

    # ─────────────────────────────────────────────────
    # パース共通処理
    # ─────────────────────────────────────────────────

    def _extract_detail_hrefs(self, soup: bs4.BeautifulSoup) -> list[str]:
        """一覧ページの結果テーブルから詳細ページの href を抽出する。"""
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue
            header_cells = rows[0].find_all(["th", "td"])
            if not header_cells:
                continue
            if "許可" not in header_cells[0].get_text():
                continue
            hrefs = []
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                a = cells[0].find("a", href=True)
                if a:
                    hrefs.append(a["href"])
            return hrefs
        return []

    def _parse_detail(self, soup: bs4.BeautifulSoup, url: str) -> dict | None:
        """詳細ページの soup からデータを抽出して dict で返す。"""
        main = soup.find(id="main")
        if not main:
            return None

        table = main.find("table")
        if not table:
            return None

        data: dict[str, str] = {}
        hp_url = ""
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                val = re.sub(r"\s+", " ", cells[1].get_text(separator=" ", strip=True)).strip()
                data[key] = val
                if key == "事業主名称":
                    a = cells[1].find("a", href=True)
                    if a and a["href"].startswith("http"):
                        hp_url = a["href"]

        full_addr = data.get("事業所所在地", "")
        pref, addr = _split_pref_addr(full_addr)

        return {
            Schema.URL: url,
            Schema.NAME: data.get("事業主名称", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: data.get("電話番号", ""),
            Schema.HP: hp_url,
            Schema.CAT_SITE: data.get("得意とする職種", ""),
            Schema.EMP_NUM: data.get("派遣労働者数", ""),
            "許可受理番号": data.get("許可・届出受理番号", ""),
            "許可受理年月日": data.get("許可届出受理年月日", ""),
            "事業所名称": data.get("事業所名称", ""),
            "派遣先件数": data.get(
                "労働者派遣の役務の提供を受けた者の数（派遣先件数）", ""
            ),
            "派遣料金平均額": data.get("派遣料金の平均額", ""),
            "賃金平均額": data.get("派遣労働者の賃金の平均額", ""),
            "マージン率": data.get("マージン率", ""),
            "労使協定締結": data.get("労使協定の締結", ""),
            "キャリア形成支援": data.get(
                "派遣労働者のキャリア形成支援制度に関する事項", ""
            ),
            "その他情報": data.get("その他の情報", ""),
            "備考": data.get("備考", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JinzaiServiceRodoshaHakenKabushikikaishaScaper()
    scraper.site_name = SITE_NAME
    scraper.execute(SEARCH_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ナイトジョブ(Night Job) — キャバクラ・クラブ・ニュークラブ・ラウンジ・朝&昼キャバの求人サイト

取得対象:
    - エリアタブ (大阪 / 兵庫 / 京都 / 東京 / 広島 / 沖縄) 配下の掲載店舗 全件
    - 店舗名 / カナ / 都道府県 / エリア / 業種 / 時給 / 営業時間 / 定休日 /
      待遇 / 応募条件 / 職種 / アクセス / 特徴アイコン / TEL / 店舗ページURL

取得フロー:
    1. エリアタブ (都道府県) ごとのアーカイブ一覧 /{pref}-all/{pref}-archive/ を
       page/N/ で全ページ巡回し、div.store_list の店舗ページURLを収集する
    2. 収集した店舗ページを 1 件取得するごとに即 yield する (Pattern B)
    3. 仕上げに /store/ (全店舗アーカイブ, 8ページ) を巡回し、エリアタブに
       紐付いていない店舗を取りこぼさないよう補完する (都道府県は空文字)

サイト固有の注意点:
    - Cloudflare のマネージドチャレンジが有効。requests では全ページ 403 になるため
      DynamicCrawler (Playwright) 必須。さらに *同一 Cookie で連続遷移すると
      チャレンジ画面 (403 / "しばらくお待ちください") に落ちる* ため、
      get_soup() では毎回 context.clear_cookies() してから遷移する。
    - store-sitemap.xml / sitemap.xml は Playwright で開いてもチャレンジが解けず
      403 のままなので列挙には使えない。HTML アーカイブから列挙する。
    - 東京都・沖縄県のアーカイブは 2026-08 時点で掲載 0 件 (サイト側の在庫ゼロ)。
    - 店舗固有の電話番号は掲載されておらず、「応募方法」欄の代理店受付番号
      (全店舗共通) のみ。Schema.TEL にはこの番号を入れる。
    - robots.txt は全許可 (Yoast: Disallow 空)、利用規約ページは存在しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/night_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id night_job
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

logger = logging.getLogger(__name__)

# エリアタブ (都道府県) → アーカイブページの相対パス。
# 兵庫のみスラッグが hyogo-all/hyougo-archive と不揃いなので定数で持つ。
_AREA_TABS: list[tuple[str, str]] = [
    ("大阪府", "osaka-all/osaka-archive/"),
    ("兵庫県", "hyogo-all/hyougo-archive/"),
    ("京都府", "kyoto-all/kyoto-archive/"),
    ("東京都", "tokyo-all/tokyo-archive/"),
    ("広島県", "hiroshima-all/hiroshima-archive/"),
    ("沖縄県", "okinawa-all/okinawa-archive/"),
]

# 全店舗アーカイブ (エリアタブ未紐付けの店舗を拾う保険)
_ALL_STORE_PATH = "store/"

# 1 エリアあたりのページ巡回上限 (無限ループ防止)
_MAX_PAGES = 30

# Cloudflare チャレンジ画面の判別文字列
_CHALLENGE_MARKERS = ("しばらくお待ちください", "just a moment", "attention required")
_MAX_FETCH_ATTEMPTS = 3

# 「〜で見つかった求人は N 件です」から総件数を読む
_HIT_COUNT_PATTERN = re.compile(r"見つかった求人は\s*([\d,]+)\s*件")

# 店舗名「GALLE（ガレ）」→ 名称 / カナ
_NAME_KANA_PATTERN = re.compile(r"^(.*?)[（(]([^（）()]+)[）)]\s*$")

# 応募方法欄に書かれた受付電話番号
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")

# 詳細ページの store_details_02 テーブルのラベル
_LABEL_STORE_NAME = "店舗名"
_LABEL_WAGE = "時給"
_LABEL_TIME = "時間"
_LABEL_ACCESS = "アクセス"
_LABEL_INDUSTRY = "業種"
_LABEL_JOB = "職種"
_LABEL_QUALIFICATION = "資格"
_LABEL_HOLIDAY = "定休日"
_LABEL_TREATMENT = "待遇"
_LABEL_HOW_TO_APPLY = "応募方法"
_LABEL_AREA = "エリア"


def _clean(value) -> str:
    """全角スペース・連続空白を潰した文字列を返す。"""
    if value is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(value)).strip()


def _cell_text(cell) -> str:
    """td 内の改行区切りリストを ' / ' 連結の 1 行にまとめる。"""
    if cell is None:
        return ""
    parts = [_clean(t) for t in cell.get_text("\n").split("\n")]
    return " / ".join(p for p in parts if p)


class NightJobScraper(DynamicCrawler):
    """ナイトジョブ(Night Job) スクレイパー"""

    DELAY = 1.5
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "エリア",
        "時給",
        "職種",
        "アクセス",
        "待遇",
        "応募条件",
        "特徴アイコン",
        "採用担当直通ダイヤル",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Cloudflare チャレンジを通すため実ブラウザ相当の設定で Playwright を起動する。

        フレームワーク既定の起動設定 (Mac Chrome 120 UA / locale 未設定) では
        初回遷移からチャレンジに落ちるため上書きする。
        """
        from playwright.sync_api import sync_playwright

        logger.debug("Playwright を起動します (ナイトジョブ向け設定)...")
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
            viewport={"width": 1400, "height": 900},
        )
        self.page = self.context.new_page()

    # ------------------------------------------------------------------ fetch

    def get_soup(self, url: str, wait_until: str = "domcontentloaded") -> BeautifulSoup | None:
        """Cookie を毎回破棄してから遷移し、Cloudflare チャレンジを回避する。

        同一 Cookie のまま連続遷移すると 2 リクエスト目以降が 403 チャレンジに
        落ちるため、遷移前に context.clear_cookies() を呼ぶ。
        チャレンジ画面を掴んだ場合は指数バックオフで最大 3 回まで再試行する。
        """

        def _fetch() -> str:
            last_title = ""
            for attempt in range(_MAX_FETCH_ATTEMPTS):
                self.context.clear_cookies()
                logger.info("取得中 (Playwright): %s", url)
                self.page.goto(url, wait_until=wait_until, timeout=60000)
                last_title = (self.page.title() or "").lower()
                if not any(m in last_title for m in _CHALLENGE_MARKERS):
                    return self.page.content()
                logger.warning(
                    "Cloudflare チャレンジを検出 (%d/%d): %s",
                    attempt + 1,
                    _MAX_FETCH_ATTEMPTS,
                    url,
                )
                time.sleep(min(2**attempt, 8))
            raise RuntimeError(f"Cloudflare チャレンジを突破できませんでした: {url} ({last_title})")

        try:
            html = self._fetch_html_cached(url, variant=wait_until, fetcher=_fetch)
            return BeautifulSoup(html, "html.parser") if html else None
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                logger.warning("ページ取得エラー (スキップして継続): %s — %s", url, e)
                return None
            raise

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0

        # 1) エリアタブ (都道府県) ごとに一覧 → 詳細 を即 yield
        for pref, path in _AREA_TABS:
            archive_url = urljoin(url, path)
            for page_no in range(1, _MAX_PAGES + 1):
                page_url = archive_url if page_no == 1 else urljoin(archive_url, f"page/{page_no}/")
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                if page_no == 1:
                    hit = _HIT_COUNT_PATTERN.search(soup.get_text(" ", strip=True))
                    if hit:
                        total += int(hit.group(1).replace(",", ""))
                        self.total_items = total
                    logger.info("エリアタブ %s: %s", pref, hit.group(0) if hit else "件数不明")

                store_urls = self._extract_store_urls(soup, page_url)
                if not store_urls:
                    break

                for store_url in store_urls:
                    if store_url in seen:
                        continue
                    seen.add(store_url)
                    item = self._scrape_detail(store_url, pref)
                    if item:
                        yield item

        # 2) 全店舗アーカイブで取りこぼしを補完 (都道府県は不明なので空文字)
        all_store_url = urljoin(url, _ALL_STORE_PATH)
        for page_no in range(1, _MAX_PAGES + 1):
            page_url = all_store_url if page_no == 1 else urljoin(all_store_url, f"page/{page_no}/")
            soup = self.get_soup(page_url)
            if soup is None:
                break
            store_urls = self._extract_store_urls(soup, page_url)
            if not store_urls:
                break
            for store_url in store_urls:
                if store_url in seen:
                    continue
                seen.add(store_url)
                item = self._scrape_detail(store_url, "")
                if item:
                    yield item

    # ------------------------------------------------------------------ list

    def _extract_store_urls(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """一覧ページ (div.store_list) から店舗詳細ページの URL を重複なしで返す。"""
        urls: list[str] = []
        for block in soup.select("div.store_list"):
            link = block.select_one("h2 a[href]")
            if not link:
                continue
            store_url = urljoin(base_url, link["href"])
            if "/store/" in store_url and store_url not in urls:
                urls.append(store_url)
        return urls

    # ---------------------------------------------------------------- detail

    def _scrape_detail(self, url: str, pref: str) -> dict | None:
        """店舗詳細ページ 1 件を辞書化する。取得できなければ None。"""
        try:
            soup = self.get_soup(url)
            if soup is None:
                return None

            details = self._label_map(soup)

            # 店舗名 / カナ — 「GALLE（ガレ）」形式。無ければ h1 から補完
            raw_name = details.get(_LABEL_STORE_NAME, "")
            if not raw_name:
                h1 = soup.select_one("h1")
                raw_name = _clean(h1.get_text(" ") if h1 else "").replace("｜求人情報", "")
            name, kana = raw_name, ""
            matched = _NAME_KANA_PATTERN.match(raw_name)
            if matched:
                name, kana = _clean(matched.group(1)), _clean(matched.group(2))
            if not name:
                logger.warning("店舗名を取得できませんでした: %s", url)
                return None

            # 応募方法欄の受付番号 (全店舗共通の代理店ダイヤル)
            tel_matched = _TEL_PATTERN.search(details.get(_LABEL_HOW_TO_APPLY, ""))
            tel = tel_matched.group(0) if tel_matched else ""

            # ヘッダ/一覧の「採用担当直通ダイヤル」リンク
            hotline = ""
            tel_link = soup.select_one('a[href^="tel:"]')
            if tel_link:
                hotline = _clean(tel_link["href"].replace("tel:", ""))

            icons = [
                _clean(a.get_text(" "))
                for a in soup.select("div.store_details_01 ul li a")
                if _clean(a.get_text(" "))
            ]

            return {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.NAME_KANA: kana,
                Schema.PREF: pref,
                Schema.TEL: tel,
                Schema.CAT_SITE: details.get(_LABEL_INDUSTRY, ""),
                Schema.TIME: details.get(_LABEL_TIME, ""),
                Schema.HOLIDAY: details.get(_LABEL_HOLIDAY, ""),
                "エリア": details.get(_LABEL_AREA, ""),
                "時給": details.get(_LABEL_WAGE, ""),
                "職種": details.get(_LABEL_JOB, ""),
                "アクセス": details.get(_LABEL_ACCESS, ""),
                "待遇": details.get(_LABEL_TREATMENT, ""),
                "応募条件": details.get(_LABEL_QUALIFICATION, ""),
                "特徴アイコン": " / ".join(dict.fromkeys(icons)),
                "採用担当直通ダイヤル": hotline,
            }
        except Exception as e:
            logger.warning("詳細ページの解析に失敗 (スキップ): %s — %s", url, e)
            return None

    def _label_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """詳細ページの 2 つのテーブルからラベル→値のマップを作る。

        - div.store_details_01: 時給 / 時間 / エリア / 業種 (1 行に th/td が複数組)
        - div.store_details_02: 店舗名 / 時給 / 時間 / アクセス / 業種 / 職種 /
                                資格 / 定休日 / 待遇 / 応募方法
        """
        data: dict[str, str] = {}
        for container in ("div.store_details_01", "div.store_details_02"):
            table = soup.select_one(f"{container} table")
            if not table:
                continue
            for row in table.select("tr"):
                cells = row.find_all(["th", "td"], recursive=False)
                label = ""
                for cell in cells:
                    if cell.name == "th":
                        label = _clean(cell.get_text(" "))
                        continue
                    if not label:
                        continue
                    value = _cell_text(cell)
                    # 先に入った値 (store_details_01) を空値で上書きしない
                    if value or label not in data:
                        data.setdefault(label, "")
                        if value:
                            data[label] = value
                    label = ""
        return data


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NightJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nightjob.info/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

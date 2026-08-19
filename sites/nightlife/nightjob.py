"""
ナイトジョブ (Night Job) — キャバクラ・クラブ・ニュークラブ・ラウンジの求人/体入情報サイト

取得対象:
    掲載店舗 全件 (2026-08 時点 約 95 件)
    店舗名 / カナ / 都道府県 / 業種 / 時間 / 定休日 /
    エリア / 時給 / 職種 / アクセス / 特徴タグ / 各種受付ダイヤル

取得フロー (Pattern B: 詳細 1 件取得ごとに即 yield):
    1. ルートページのグローバルナビから都道府県タブ (`/{pref}-all/{pref}-archive/`) を
       動的に検出する。アンカーテキスト「大阪府全て」から都道府県名を得る。
       (兵庫のみ slug が hyogo-all/hyougo-archive と不揃いなため、slug 決め打ちではなく
        href をそのまま使う。将来県が増えても自動追従する)
    2. 都道府県タブを `page/N/` で巡回し、div.store_list の店舗ページ URL を取得。
       1 件取得するごとに詳細ページを開いて即 yield する。
    3. 仕上げに全店舗アーカイブ `/store/` (12 件 × 8 ページ) を巡回し、
       都道府県タブに紐付いていない店舗を取りこぼさないよう補完する
       (この経路で拾った店舗は都道府県が判らないため PREF は空文字)。

サイト固有の注意点:
    - Cloudflare のマネージドチャレンジ配下。requests では全 HTML パスが 403
      (robots.txt だけ 200) になるため DynamicCrawler (Playwright) 必須。
    - Playwright でも *同一 Cookie のまま連続遷移すると 2 リクエスト目以降が
      403 チャレンジ* に落ちる。page.goto() の直前に context.clear_cookies() を
      呼んで毎回 1st-visit 扱いにすると全ページ 200 で取得できる。
      チャレンジの自動解決待ちでは解けないので待つのは無駄。
    - store-sitemap.xml / sitemap.xml は Playwright でも 403 のままで列挙に使えない。
      列挙は HTML アーカイブから行う。
    - 東京都・沖縄県のタブは 2026-08 時点で掲載 0 件 (サイト側の在庫ゼロ)。
    - 店舗固有の電話番号は掲載されていない。「応募方法」欄の 06-6770-5518 と
      ヘッダの 06-4400-1561 はいずれも全店舗共通の代理店ダイヤルなので、
      Schema.TEL には入れず EXTRA_COLUMNS に分けて出力する。
    - 「待遇」「資格」「応募方法」本文および一覧カードの h3 キャッチコピーは
      店舗が書いた長文の自由記述 (著作権リスク) のため取得しない。
      応募方法からは電話番号のみを抽出する。
    - 掲載されている Instagram / LINE はサイト運営者の共通アカウントであり
      店舗固有ではないため取得しない。住所・郵便番号はサイト上に非掲載。
    - robots.txt は全許可 (Yoast: Disallow 空)、利用規約ページは存在しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/nightjob.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nightjob
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

# 全店舗アーカイブ (都道府県タブ未紐付けの店舗を拾う保険)
_ALL_STORE_PATH = "store/"

# ルートページからタブを検出できなかったときのフォールバック
# (都道府県名, ルートからの相対パス)
_FALLBACK_AREA_TABS: list[tuple[str, str]] = [
    ("大阪府", "osaka-all/osaka-archive/"),
    ("兵庫県", "hyogo-all/hyougo-archive/"),
    ("京都府", "kyoto-all/kyoto-archive/"),
    ("東京都", "tokyo-all/tokyo-archive/"),
    ("広島県", "hiroshima-all/hiroshima-archive/"),
    ("沖縄県", "okinawa-all/okinawa-archive/"),
]

# 1 アーカイブあたりのページ巡回上限 (無限ループ防止)
_MAX_PAGES = 30

# Cloudflare チャレンジ画面の判別文字列 (title を小文字化して照合)
_CHALLENGE_MARKERS = ("しばらくお待ちください", "just a moment", "attention required")
_MAX_FETCH_ATTEMPTS = 3

# 「〜で見つかった求人は N 件です」から総件数を読む
_HIT_COUNT_PATTERN = re.compile(r"見つかった求人は\s*([\d,]+)\s*件")

# タブのアンカーテキスト「大阪府全て」→ 都道府県名
_PREF_LABEL_PATTERN = re.compile(r"([^\s　]+?[都道府県])\s*全て")

# 店舗名「CLUB ARROW（クラブ アロー）」→ 名称 / カナ
_NAME_KANA_PATTERN = re.compile(r"^(.*?)[（(]([^（）()]+)[）)]\s*$")
_KATAKANA_PATTERN = re.compile(r"[゠-ヿ]")

# 応募方法欄に書かれた受付電話番号
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")

# 詳細ページ th のラベル
_LABEL_STORE_NAME = "店舗名"
_LABEL_WAGE = "時給"
_LABEL_TIME = "時間"
_LABEL_ACCESS = "アクセス"
_LABEL_INDUSTRY = "業種"
_LABEL_JOB = "職種"
_LABEL_HOLIDAY = "定休日"
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
    """ナイトジョブ (Night Job) スクレイパー"""

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
        "特徴タグ",
        "応募受付ダイヤル",
        "採用担当直通ダイヤル",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Cloudflare を通すため実ブラウザ相当の設定で Playwright を起動する。

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
        チャレンジを掴んだ場合は指数バックオフで最大 _MAX_FETCH_ATTEMPTS 回まで
        再試行し、それでも突破できなければ例外を投げる (無限リトライはしない)。
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
        """引数 url を唯一のルートとして、都道府県タブ → 全店舗アーカイブの順に巡回する。"""
        seen: set[str] = set()
        total = 0

        # 1) 都道府県タブごとに 一覧 → 詳細 を即 yield
        for pref, archive_url in self._discover_area_tabs(url):
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
                    logger.info("都道府県タブ %s: %s", pref, hit.group(0) if hit else "件数不明")

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

        # 2) 全店舗アーカイブで取りこぼしを補完 (都道府県は判らないので空文字)
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

    # -------------------------------------------------------------- area tabs

    def _discover_area_tabs(self, url: str) -> list[tuple[str, str]]:
        """ルートページのナビから (都道府県名, アーカイブURL) を重複なしで取得する。

        検出できなければ _FALLBACK_AREA_TABS を url からの相対解決で返す。
        """
        soup = self.get_soup(url)
        tabs: list[tuple[str, str]] = []
        if soup is not None:
            seen_urls: set[str] = set()
            for link in soup.select('a[href*="-archive/"]'):
                matched = _PREF_LABEL_PATTERN.search(_clean(link.get_text(" ")))
                if not matched:
                    continue
                archive_url = urljoin(url, link["href"])
                if archive_url in seen_urls:
                    continue
                seen_urls.add(archive_url)
                tabs.append((matched.group(1), archive_url))

        if tabs:
            logger.info("都道府県タブを %d 件検出しました: %s", len(tabs), [t[0] for t in tabs])
            return tabs

        logger.warning("都道府県タブを検出できませんでした。フォールバック定数を使用します")
        return [(pref, urljoin(url, path)) for pref, path in _FALLBACK_AREA_TABS]

    # ------------------------------------------------------------------- list

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

    # ----------------------------------------------------------------- detail

    def _scrape_detail(self, url: str, pref: str) -> dict | None:
        """店舗詳細ページ 1 件を辞書化する。取得できなければ None。"""
        try:
            soup = self.get_soup(url)
            if soup is None:
                return None

            details = self._label_map(soup)
            name, kana = self._split_name(details.get(_LABEL_STORE_NAME, ""), soup)
            if not name:
                logger.warning("店舗名を取得できませんでした: %s", url)
                return None

            # 「応募方法」欄の受付番号 (全店舗共通の代理店ダイヤル)。本文自体は取得しない
            apply_matched = _TEL_PATTERN.search(details.get(_LABEL_HOW_TO_APPLY, ""))
            apply_tel = apply_matched.group(0) if apply_matched else ""

            # 特徴タグ (10代 / ブランクOK / 全額日払いOK ... の短いラベル群)
            tags = [
                _clean(a.get_text(" "))
                for a in soup.select("div.store_details_01 ul li a")
                if _clean(a.get_text(" "))
            ]

            return {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.NAME_KANA: kana,
                Schema.PREF: pref,
                # 店舗固有の電話番号はサイトに非掲載 (共通ダイヤルは EXTRA へ)
                Schema.TEL: "",
                Schema.CAT_SITE: details.get(_LABEL_INDUSTRY, ""),
                Schema.TIME: details.get(_LABEL_TIME, ""),
                Schema.HOLIDAY: details.get(_LABEL_HOLIDAY, ""),
                "エリア": details.get(_LABEL_AREA, ""),
                "時給": details.get(_LABEL_WAGE, ""),
                "職種": details.get(_LABEL_JOB, ""),
                "アクセス": details.get(_LABEL_ACCESS, ""),
                "特徴タグ": " / ".join(dict.fromkeys(tags)),
                "応募受付ダイヤル": apply_tel,
                "採用担当直通ダイヤル": self._hotline(soup),
            }
        except Exception as e:
            logger.warning("詳細ページの解析に失敗 (スキップ): %s — %s", url, e)
            return None

    def _split_name(self, raw_name: str, soup: BeautifulSoup) -> tuple[str, str]:
        """「CLUB ARROW（クラブ アロー）」を (名称, カナ) に分解する。

        店舗名セルが空の場合は h1「CLUB ARROW｜求人情報 クラブ　アロー」から補完する。
        括弧内にカタカナが 1 文字も無い場合は読み仮名ではないと判断し、名称に残す。
        """
        raw_name = _clean(raw_name)
        if not raw_name:
            h1 = soup.select_one("h1")
            h1_text = _clean(h1.get_text(" ") if h1 else "")
            head, _, tail = h1_text.partition("｜")
            raw_name = _clean(head)
            kana_from_h1 = _clean(tail.replace("求人情報", "").replace("求人・体入情報", ""))
            if raw_name and _KATAKANA_PATTERN.search(kana_from_h1):
                return raw_name, kana_from_h1

        matched = _NAME_KANA_PATTERN.match(raw_name)
        if matched and _KATAKANA_PATTERN.search(matched.group(2)):
            return _clean(matched.group(1)), _clean(matched.group(2))
        return raw_name, ""

    def _hotline(self, soup: BeautifulSoup) -> str:
        """ヘッダの「採用担当直通ダイヤル」を返す。ハイフン付き表記を優先する。"""
        numbers = [
            _clean(a["href"].replace("tel:", ""))
            for a in soup.select('a[href^="tel:"]')
            if _clean(a.get("href", "")) != "tel:"
        ]
        for number in numbers:
            if "-" in number:
                return number
        return numbers[0] if numbers else ""

    def _label_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """詳細ページの 2 つのテーブルからラベル→値のマップを作る。

        - div.store_details_01: 時給 / 時間 / エリア / 業種 (1 行に th/td が複数組)
        - div.store_details_02: 店舗名 / 時給 / 時間 / アクセス / 業種 / 職種 /
                                資格 / 定休日 / 待遇 / 応募方法
        後から現れる空値で、先に入った値を上書きしない。
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

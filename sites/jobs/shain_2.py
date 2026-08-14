"""
助太刀社員 (shain_2) — 建築/建設業に特化した求人・転職サイト (shain.suke-dachi.jp)

取得対象:
    - 掲載中の建設業・建築業の求人 (2026-08 時点で 631 件 / 20 件 × 32 ページ)
    - 1 求人 = 1 レコード (同一企業が複数求人を出していれば複数行になる)

取得フロー:
    /search?page=N の一覧カード (div.JobSearchResultItem) から詳細リンクと
    掲載開始日を取り出し、/detail/{求人ID} を 1 件ずつ取得して即 yield する。
    「直近 3 ヶ月以内の掲載を優先。件数不足時は期間条件を撤廃」という指示に従い、
    まず 3 ヶ月以内の求人を流し、3 ヶ月より古い求人は後段でまとめて流す
    (結果として全件取得されるが、新しい順に先に yield される)。

サイト固有の注意点:
    - 全パスが Vercel Attack Challenge (Security Checkpoint / HTTP 429) 配下。
      requests では取得できないため DynamicCrawler が必須。さらにフレームワーク
      既定のブラウザ設定ではチャレンジを通過できないので _setup() を上書きし、
      `--disable-blink-features=AutomationControlled` + Windows Chrome の UA を使う。
    - 詳細ページの主データは JSON-LD (JobPosting) と、
      div[role=row] > div[role=rowheader] / div[role=cell] のラベル表 の 2 系統。
      Chakra UI のクラス名 (css-xxxx) はビルド毎に変わるため一切使わない。
    - 「連絡先」(応募先電話番号) は掲載企業によっては存在しない (3 件中 1 件は無し)。
      無い場合は空文字を入れる。
    - 詳細ページ下部に「あなたが探している求人と似ている求人」の他求人カードが
      あるため、「勤務地：」等の本文テキストは先頭 1 件のみを採用する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/shain_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id shain_2
"""

import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from playwright.sync_api import Error as PlaywrightError

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

logger = logging.getLogger(__name__)

# Vercel のボットチャレンジ画面を判別するためのタイトル文字列
_CHECKPOINT_TITLE = "Vercel Security Checkpoint"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 一覧カードの「掲載期間：2026/08/07 - 2026/11/06」から掲載開始日を拾う
_PERIOD_PATTERN = re.compile(r"掲載期間[：:]\s*(\d{4})/(\d{1,2})/(\d{1,2})")
_TOTAL_PATTERN = re.compile(r"検索結果\s*([\d,]+)\s*件")
_JOB_ID_PATTERN = re.compile(r"/detail/(\d+)")
_TEL_PATTERN = re.compile(r"0[\d\-()－ー\s]{8,}")
_WORK_AREA_PATTERN = re.compile(r"^勤務地[：:]\s*(.+)$")
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# 「休日」欄の見出し行 (括弧の種類は掲載企業ごとに揺れる)
_SECTION_HEADING_PATTERN = re.compile(r"^[【《〈\[]")
_HOLIDAY_HEADING_PATTERN = re.compile(r"[【《〈\[]\s*休日\s*[】》〉\]]")

# JSON-LD の employmentType を日本語表記に変換する
_EMPLOYMENT_TYPE_JA = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣・契約社員",
    "INTERN": "インターン",
    "OTHER": "その他",
}

_SALARY_UNIT_JA = {"MONTH": "月給", "YEAR": "年収", "DAY": "日給", "HOUR": "時給"}

_SNS_MATCHERS = (
    (Schema.INSTA, re.compile(r"instagram\.com", re.I)),
    (Schema.X, re.compile(r"(?:twitter\.com|//x\.com)", re.I)),
    (Schema.FB, re.compile(r"facebook\.com", re.I)),
    (Schema.LINE, re.compile(r"line\.me", re.I)),
    (Schema.TIKTOK, re.compile(r"tiktok\.com", re.I)),
)

# 「この求人の特徴」ブロックに含まれる小見出し (タグ本体ではないので除外する)
_FEATURE_HEADINGS = frozenset(
    {
        "この求人の特徴",
        "雇用形態",
        "賃金",
        "福利厚生・働き方",
        "経験・年齢",
        "時間・休日・通勤",
        "その他",
    }
)

# --- 建設業・建築業フィルタ用 ---------------------------------------------
# サイトの職種タクソノミ (/search/o_* の全 96 職種)。JSON-LD の title は
# この職種名を「、」で連結した文字列なので、いずれかに一致すれば建設業と判定する。
_CONSTRUCTION_OCCUPATIONS = frozenset(
    {
        # 建築/躯体
        "躯体/型枠大工", "躯体/鉄筋工", "クレーン", "躯体/雑工", "左官(土間)",
        "ポンプ", "躯体/測量", "解体", "アンカー", "躯体/鳶 (足場)",
        "躯体/鳶 (鉄骨)", "屋根", "ハツリ", "溶接・鍛冶工",
        # 建築/仕上げ
        "LGS", "ボード", "クロス", "塗装", "ALC", "大工", "左官", "シール",
        "ガラス", "鳶 (重量)", "貼床", "置床(OAフロア)", "揚重", "内装/警備員",
        "クリーニング", "建具", "サイディング", "サッシ", "防災(避難器具)",
        "サイン", "大工(展示会)", "表具(展示会)", "アスベスト除去", "ウッドデッキ",
        "ルーバー", "補修（リペア）", "家具施工", "塗床", "防水", "タイル",
        "ELV・エスカレーター",
        # 設備
        "設備/雑工", "空調(配管)", "空調(ダクト)", "空調(保温)", "空調(冷媒)",
        "空調(計装)", "衛生(配管工)", "衛生(ガス)", "衛生(水道)",
        "キッチン・ユニットバス", "造作", "防災（スプリンクラー）", "防災（消火栓）",
        # 電気
        "強電", "弱電", "自火報", "太陽光", "仮設", "避雷針",
        # 土木
        "土工", "重機オペレーター", "電気", "インターロッキング", "石工",
        "土木/測量", "造園", "杭打ち", "舗装", "土木/警備員", "水道工事",
        "エクステリア・外構", "土木/鳶 (足場)", "土木/鳶 (鉄骨)", "鍛治鳶",
        "橋梁鳶", "土木/型枠大工", "土木/鉄筋工",
        # 施工管理
        "施工管理(電気)", "施工管理(土木)", "施工管理(建築)", "施工管理(管工事)",
        "施工管理(造園)",
    }
)

# タクソノミに無い新職種が追加された場合の保険 (建設/建築系キーワード)
_CONSTRUCTION_KEYWORDS = re.compile(
    r"施工管理|建築|建設|土木|工事|躯体|内装|設備|鳶|大工|左官|塗装|防水"
    r"|解体|配管|空調|電気|土工|舗装|造園|測量|重機|鉄筋|型枠|防災|溶接"
)


def _clean(value) -> str:
    """空白・全角スペースを正規化して 1 行の文字列にする。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("　", " ")).strip()


def _first_line(value: str) -> str:
    """複数行テキストの先頭の非空行だけを返す。"""
    for line in (value or "").splitlines():
        line = _clean(line)
        if line:
            return line
    return ""


def _job_id(detail_url: str) -> str:
    m = _JOB_ID_PATTERN.search(urlparse(detail_url).path or "")
    return m.group(1) if m else ""


def _months_ago(base: datetime, months: int) -> datetime:
    """base から months ヶ月前の日時を返す (月末日は月初側に丸める)。"""
    month_index = base.month - 1 - months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    # 3/31 の 1 ヶ月前のような不正日付を避けるため日を切り詰める
    day = min(base.day, 28)
    return datetime(year, month, day)


def _card_posted_date(card) -> datetime | None:
    """一覧カードの「掲載期間」から掲載開始日 (= 求人掲載日) を返す。"""
    m = _PERIOD_PATTERN.search(card.get_text(" ", strip=True))
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _job_posting_ld(soup: BeautifulSoup) -> dict:
    """詳細ページの JSON-LD から JobPosting ノードを取り出す。"""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        for node in data if isinstance(data, list) else [data]:
            if isinstance(node, dict) and node.get("@type") == "JobPosting":
                return node
    return {}


def _label_rows(soup: BeautifulSoup) -> dict[str, str]:
    """div[role=row] のラベル/値ペアを辞書化する (募集要項・応募詳細・企業情報)。"""
    rows: dict[str, str] = {}
    for row in soup.select('div[role="row"]'):
        header = row.select_one('[role="rowheader"]')
        cell = row.select_one('[role="cell"]')
        if not header or not cell:
            continue
        label = _clean(header.get_text())
        # 同名ラベルは先勝ち (本文が先、similar-jobs 等が後に来るため)
        if label and label not in rows:
            rows[label] = cell.get_text("\n", strip=True)
    return rows


def _row_cell(soup: BeautifulSoup, label: str):
    """指定ラベルの値セル (bs4 Tag) を返す。SNS のようにリンクを見たい場合に使う。"""
    for row in soup.select('div[role="row"]'):
        header = row.select_one('[role="rowheader"]')
        if header and _clean(header.get_text()) == label:
            return row.select_one('[role="cell"]')
    return None


def _iso_to_date(value: str) -> str:
    """JSON-LD の ISO 日時を YYYY-MM-DD に丸める。"""
    value = _clean(value)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else value


class Shain2Scraper(DynamicCrawler):
    """助太刀社員 (shain.suke-dachi.jp) 建設・建築求人スクレイパー"""

    DELAY = 1.0

    # Vercel チャレンジを通過できる UA (フレームワーク既定の Mac Chrome 120 では弾かれる)
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 掲載日がこの月数以内の求人を先に取得する (0 で無効化 = 掲載順のまま)
    RECENT_MONTHS = 3
    # 1 ページあたりのリトライ上限 (無限ループ防止)
    MAX_ATTEMPTS = 3
    # チャレンジ画面が本ページに切り替わるまでの最大待機秒数
    CHECKPOINT_WAIT_SEC = 30
    # ページャが読めなかった場合の保険 (実サイトは 32 ページ前後)
    MAX_PAGES = 200

    EXTRA_COLUMNS = [
        "求人ID",
        "募集職種",
        "雇用形態",
        "勤務地エリア",
        "給与下限",
        "給与上限",
        "給与単位",
        "歓迎資格",
        "求人特徴",
        "求人掲載日",
        "掲載終了日",
        "企業ページURL",
    ]

    # ------------------------------------------------------------ ブラウザ設定

    def _setup(self):
        """フレームワーク既定の起動設定では Vercel チャレンジを通過できないため上書きする。

        headless のまま `--disable-blink-features=AutomationControlled` と
        実ブラウザ相当の UA / locale / viewport を与えると、チャレンジ用 JS が
        数秒で自動的に解決され、以降は Cookie により素通しになる。
        """
        logger.debug("Playwright を起動します (助太刀社員向け設定)...")
        from playwright.sync_api import sync_playwright

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
            viewport={"width": 1280, "height": 900},
        )
        self.page = self.context.new_page()

    def get_soup(
        self,
        url: str,
        wait_until: str = "domcontentloaded",
        wait_selector: str | None = None,
    ) -> BeautifulSoup | None:
        """Vercel チャレンジの通過を待ってから HTML を返す。

        Args:
            url: 取得先 URL
            wait_until: page.goto の待機条件
            wait_selector: 描画完了の判定に使う CSS セレクタ (任意)
        """

        def _fetch() -> str:
            last_error: Exception | None = None
            for attempt in range(self.MAX_ATTEMPTS):
                try:
                    logger.info("取得中 (Playwright): %s", url)
                    self.page.goto(url, wait_until=wait_until, timeout=60_000)
                    if self._wait_for_content(wait_selector):
                        return self.page.content()
                    last_error = RuntimeError(
                        f"Vercel チャレンジを通過できませんでした: {url}"
                    )
                except PlaywrightError as e:
                    last_error = e
                    logger.warning(
                        "ページ取得に失敗 (%d/%d): %s — %s",
                        attempt + 1,
                        self.MAX_ATTEMPTS,
                        url,
                        e,
                    )
                # 指数バックオフ (最大 8 秒)
                self.page.wait_for_timeout(min(2**attempt, 8) * 1000)
            raise RuntimeError(f"取得に失敗しました: {url} — {last_error}")

        try:
            html = self._fetch_html_cached(url, variant=wait_until, fetcher=_fetch)
            return BeautifulSoup(html, "html.parser")
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                logger.warning("ページ取得エラー (スキップして継続): %s — %s", url, e)
                return None
            logger.error("ページ取得エラー: %s", e)
            raise

    def _wait_for_content(self, wait_selector: str | None) -> bool:
        """チャレンジ画面が本来のページに切り替わるまで待つ。通過できれば True。"""
        for _ in range(self.CHECKPOINT_WAIT_SEC * 2):
            title = self.page.title() or ""
            if _CHECKPOINT_TITLE not in title and not title.startswith("Loading "):
                if not wait_selector:
                    return True
                try:
                    self.page.wait_for_selector(wait_selector, timeout=15_000)
                    return True
                except PlaywrightError:
                    return False
            self.page.wait_for_timeout(500)
        return False

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧 → 詳細を巡回し、1 件取得するごとに即 yield する。

        Args:
            url: sites.yml に登録された正規 URL (= サイトのルート)
        """
        list_root = urljoin(url, "search")
        cutoff = (
            _months_ago(datetime.now(), self.RECENT_MONTHS)
            if self.RECENT_MONTHS
            else None
        )

        seen_ids: set[str] = set()
        # 直近 RECENT_MONTHS ヶ月より古い掲載 (期間条件を撤廃して後段で取得する)
        deferred: list[str] = []
        last_page: int | None = None
        page = 1

        while page <= self.MAX_PAGES:
            page_url = list_root if page == 1 else f"{list_root}?page={page}"
            soup = self.get_soup(page_url, wait_selector="div.JobSearchResultItem")
            if soup is None:
                logger.warning("一覧ページを取得できませんでした: %s", page_url)
                break

            if page == 1:
                self.total_items = self._parse_total(soup)
                last_page = self._parse_last_page(soup)
                logger.info("総件数=%s / 総ページ数=%s", self.total_items, last_page)

            cards = soup.select("div.JobSearchResultItem")
            if not cards:
                logger.info("求人カードが無いため終了します: %s", page_url)
                break

            for card in cards:
                link = card.select_one('a[href*="/detail/"]')
                if not link or not link.get("href"):
                    continue
                detail_url = urljoin(url, link["href"])
                job_id = _job_id(detail_url)
                # PR 求人枠は全ページに重複して現れるため求人 ID で一意化する
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                posted = _card_posted_date(card)
                if cutoff and posted and posted < cutoff:
                    deferred.append(detail_url)
                    continue

                item = self._scrape_detail(url, detail_url)
                if item:
                    yield item

            if last_page and page >= last_page:
                break
            page += 1

        # 件数確保のため、期間条件を撤廃して 3 ヶ月より古い掲載も取得する
        if deferred:
            logger.info(
                "直近%dヶ月より古い求人 %d 件を取得します", self.RECENT_MONTHS, len(deferred)
            )
        for detail_url in deferred:
            item = self._scrape_detail(url, detail_url)
            if item:
                yield item

    # ------------------------------------------------------------- 一覧の補助

    @staticmethod
    def _parse_total(soup: BeautifulSoup) -> int | None:
        """「検索結果 631 件 1 ~ 20 件を表示」から総件数を読む。

        件数は <span> に分かれて入るため、まとめたテキストに対して照合する。
        """
        m = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
        if not m:
            return None
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _parse_last_page(soup: BeautifulSoup) -> int | None:
        """ページャの button[data-page] から最終ページ番号を読む。"""
        pages = []
        for button in soup.select("button[data-page]"):
            try:
                pages.append(int(button["data-page"]))
            except (KeyError, ValueError, TypeError):
                continue
        return max(pages) if pages else None

    # --------------------------------------------------------------- 詳細取得

    def _scrape_detail(self, root_url: str, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url, wait_selector="h1")
        if soup is None:
            return None

        try:
            return self._build_item(root_url, detail_url, soup)
        except Exception as e:  # 1 件の不備で全体を止めない
            self.error_count += 1
            logger.warning("詳細ページの解析に失敗 (スキップ): %s — %s", detail_url, e)
            return None

    def _build_item(
        self, root_url: str, detail_url: str, soup: BeautifulSoup
    ) -> dict | None:
        ld = _job_posting_ld(soup)
        rows = _label_rows(soup)
        org = ld.get("hiringOrganization") or {}
        ld_address = (ld.get("jobLocation") or {}).get("address") or {}

        occupations = _clean(ld.get("title"))
        # 建設業・建築業の求人のみを対象にする (指示によるフィルタ)
        if not self._is_construction(occupations):
            logger.info("建設・建築業以外のため除外: %s (%s)", detail_url, occupations)
            return None

        # --- 会社名 (企業情報テーブル優先、無ければ JSON-LD)
        name = _clean(rows.get("会社名")) or _clean(org.get("name"))
        if not name:
            logger.warning("会社名が取得できないためスキップ: %s", detail_url)
            return None

        # --- 住所 (企業情報テーブル優先、無ければ JSON-LD の PostalAddress を合成)
        address = _clean(rows.get("住所"))
        if not address:
            address = _clean(
                f"{ld_address.get('addressRegion', '')}"
                f"{ld_address.get('addressLocality', '')}"
                f"{ld_address.get('streetAddress', '')}"
            )
        m = _PREF_PATTERN.match(address)
        pref = m.group(1) if m else _clean(ld_address.get("addressRegion"))

        # --- 郵便番号 (JSON-LD は空のことが多いので面接場所の〒表記も見る)
        post_code = _clean(ld_address.get("postalCode"))
        if not post_code:
            m = _POST_CODE_PATTERN.search(rows.get("面接場所", ""))
            post_code = m.group(1) if m else ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.TEL: self._apply_tel(rows),
            Schema.EMP_NUM: _clean(rows.get("従業員数")),
            Schema.OPEN_DATE: _clean(rows.get("設立年")),
            Schema.HP: _clean(rows.get("ホームページ")) or _clean(org.get("sameAs")),
            Schema.CAT_SITE: occupations,
            Schema.TIME: _first_line(rows.get("勤務時間", "")),
            Schema.HOLIDAY: self._holiday(rows.get("休日", "")),
            "求人ID": _job_id(detail_url),
            "募集職種": occupations,
            "雇用形態": _EMPLOYMENT_TYPE_JA.get(
                _clean(ld.get("employmentType")), _clean(ld.get("employmentType"))
            ),
            "勤務地エリア": self._work_area(soup),
            "歓迎資格": _clean(rows.get("歓迎資格")),
            "求人特徴": self._features(soup),
            "求人掲載日": _iso_to_date(ld.get("datePosted", "")),
            "掲載終了日": _iso_to_date(ld.get("validThrough", "")),
            "企業ページURL": self._company_url(root_url, soup),
        }
        item.update(self._salary(ld))
        item.update(self._sns(soup))
        return item

    # --------------------------------------------------------- フィールド抽出

    @staticmethod
    def _is_construction(occupations: str) -> bool:
        """職種がサイトの建設・建築タクソノミに含まれるか判定する。

        JSON-LD の title は「衛生(ガス)、強電、土工」のように職種名を「、」で
        連結した文字列。1 つでもタクソノミに一致すれば建設業の求人とみなす。
        タクソノミが更新された場合に取りこぼさないよう、キーワードでも補完する。
        """
        if not occupations:
            return False
        parts = [_clean(p) for p in occupations.split("、") if _clean(p)]
        if any(p in _CONSTRUCTION_OCCUPATIONS for p in parts):
            return True
        return bool(_CONSTRUCTION_KEYWORDS.search(occupations))

    @staticmethod
    def _apply_tel(rows: dict[str, str]) -> str:
        """応募詳細の「連絡先」から応募先電話番号を取り出す。

        値は「0426223772\\n営業電話は禁止しております。…」のように注意書きが
        続くため、先頭行だけを見て電話番号らしき並びを拾う。
        掲載企業によっては「連絡先」行そのものが存在しない (その場合は空文字)。
        """
        contact = rows.get("連絡先", "")
        if not contact:
            return ""
        m = _TEL_PATTERN.search(_first_line(contact))
        return _clean(m.group(0)) if m else ""

    @staticmethod
    def _holiday(value: str) -> str:
        """「休日」欄の【休日】直下の短い値だけを返す (長文の休暇説明は取らない)。"""
        lines = [_clean(x) for x in (value or "").splitlines()]
        for i, line in enumerate(lines):
            # 見出しの括弧は掲載企業によって【】/《》/〈〉と揺れる
            if not _HOLIDAY_HEADING_PATTERN.fullmatch(line):
                continue
            for nxt in lines[i + 1 :]:
                if not nxt:
                    continue
                # 値が無いまま次の見出しに入った場合は空
                if _SECTION_HEADING_PATTERN.match(nxt):
                    return ""
                return nxt if len(nxt) <= 40 else ""
        # 休日見出しが無い場合は先頭行を、短くかつ見出しでないときだけ採用する
        head = _first_line(value)
        if not head or len(head) > 40 or _SECTION_HEADING_PATTERN.match(head):
            return ""
        return head

    @staticmethod
    def _work_area(soup: BeautifulSoup) -> str:
        """見出し直下の「勤務地：東京, 埼玉」(都道府県の短いリスト) を返す。

        ページ下部の「似ている求人」カードにも同じ表記があるため、
        本文が先に出現する最初の 1 件のみを採用する。
        """
        for text in soup.find_all(string=_WORK_AREA_PATTERN):
            m = _WORK_AREA_PATTERN.match(_clean(text))
            if m:
                return _clean(m.group(1))
        return ""

    @staticmethod
    def _features(soup: BeautifulSoup) -> str:
        """「この求人の特徴」ブロックの短いタグ群を「、」区切りで返す。"""
        heading = soup.find(
            lambda tag: tag.name in ("h2", "h3")
            and _clean(tag.get_text()) == "この求人の特徴"
        )
        container = heading.parent if heading else None
        if container is None:
            return ""
        labels = [
            _clean(t)
            for t in container.stripped_strings
            if _clean(t) and _clean(t) not in _FEATURE_HEADINGS
        ]
        return "、".join(dict.fromkeys(labels))

    @staticmethod
    def _company_url(root_url: str, soup: BeautifulSoup) -> str:
        """企業ページ (/company/{uuid}) への絶対 URL を返す。"""
        link = soup.select_one('a[href^="/company/"]')
        return urljoin(root_url, link["href"]) if link and link.get("href") else ""

    @staticmethod
    def _salary(ld: dict) -> dict:
        base = ld.get("baseSalary") or {}
        value = base.get("value") or {}
        unit = _clean(value.get("unitText"))
        return {
            "給与下限": _clean(value.get("minValue")),
            "給与上限": _clean(value.get("maxValue")),
            "給与単位": _SALARY_UNIT_JA.get(unit, unit),
        }

    @staticmethod
    def _sns(soup: BeautifulSoup) -> dict:
        """企業情報の「SNSアカウント」欄のリンクを種別ごとに振り分ける。"""
        result = {key: "" for key, _ in _SNS_MATCHERS}
        cell = _row_cell(soup, "SNSアカウント")
        if cell is None:
            return result
        for a in cell.select("a[href]"):
            href = _clean(a["href"])
            for key, pattern in _SNS_MATCHERS:
                if not result[key] and pattern.search(href):
                    result[key] = href
        return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Shain2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://shain.suke-dachi.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ガテン職(info) — 建設業の求人・転職サイト GATEN職 (https://gaten.info)

取得対象:
    - 求人一覧 (/list) に掲載された全求人の企業/募集情報
    - 会社名・所在地・電話番号・従業員数・HP・担当者・勤務時間・休日・業種 等

取得フロー:
    1. 一覧 /list の 1 ページ目から総件数を読み、総ページ数 (約315p) を確定する
    2. 全一覧ページ /list?page=N を並行取得し、各カードから
       業種 (.company-industory-type) と詳細リンク /job/{id} を収集する
    3. 収集した詳細ページ /job/{id} を並行取得し、構造化フィールドを抽出して
       取得でき次第ストリーミングで yield する

備考 (重要 — 過去に 314 件で打ち切られていた原因):
    - Cloudflare の managed challenge が有効なため requests / cf_clearance 流用は 403。
      **1 コンテキスト内の 2 回目以降の遷移は必ず challenge("Just a moment") にブロック**され、
      初回 goto だけが通過する。よって **リクエストごとに新しいコンテキスト**を張る。
    - 旧実装は `while True: … if not cards: break` で一覧を辿っていたため、
      ある一覧ページが一過性で challenge/空になった瞬間に **クロール全体が break** し、
      約13ページ=約314件で停止していた。→ 本実装は総ページ数を先に確定し、
      各ページ/詳細を **リトライ付きで並行取得**して一過性失敗でクロールを止めない。
    - 旧実装は DELAY=1.5 の逐次取得(実質同時1)で約8,000ページを捌けず時間切れだった。
      → CPU コア数(challenge JS が CPU バウンド)に合わせた控えめな並行 + 画像等の
      リソースブロックで高速化する。DELAY は 0 (並行数で流量を制御する)。
    - 求人紹介文・仕事内容・企業理念など自由記述(プロース)は著作権リスク回避のため取得しない。

実行方法:
    python scripts/sites/jobs/info.py
    docker compose exec worker python /app/bin/run_flow.py --site-id info
"""

import asyncio
import math
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 都道府県 (所在地文字列の先頭から抽出)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,3}[-(]?\d{1,4}[-)]?\d{3,4}")

# 一覧の 1 ページあたりの掲載件数 (総件数 → 総ページ数の算出に使用)
_PAGE_SIZE = 25
# 取得完了を表す番兵
_SENTINEL = object()


class GatenInfo(DynamicCrawler):
    """ガテン職(info) スクレイパー — 並行 (async Playwright) 実装。"""

    DELAY = 0.0  # 流量は並行数で制御する (逐次 sleep はしない)

    # --- 並行チューニング ---
    # challenge JS は CPU バウンドで、この環境は少コア。過剰な並行は逆に遅くなる & CF の
    # レート起因ブロックを誘発しうるため控えめに。固定数のワーカープールで捌く
    # (タスクを大量に作らない = 低メモリ & 後片付けが軽い)。
    LIST_WORKERS = 3         # 一覧ページを取得するワーカー数 (重い: challenge)
    DETAIL_WORKERS = 6       # 詳細ページを取得するワーカー数 (軽い)
    CARD_QUEUE_MAX = 800     # 詳細待ちカードの上限 (超えたら一覧取得をバックプレッシャで抑制)
    MAX_RETRY = 3            # 一過性 challenge/空応答のリトライ回数
    NAV_TIMEOUT = 45000      # goto タイムアウト(ms)
    SELECTOR_TIMEOUT = 10000  # 目的セレクタ待ちタイムアウト(ms)
    # 描画に不要な重いリソースはブロックして各遷移を高速化する (challenge script は素通し)
    _BLOCK_TYPES = {"image", "media", "font", "stylesheet"}

    EXTRA_COLUMNS = [
        "職種タグ",
        "契約形態",
        "給与報酬",
        "勤務地",
        "募集業種",
        "対象となる方",
        "担当者カナ",
    ]

    # ------------------------------------------------------------------
    # リソース管理 — async Playwright を parse() 内で自前管理するため、
    # 親 DynamicCrawler の同期ブラウザ起動は使わない (no-op でオーバーライド)。
    # ------------------------------------------------------------------
    def _setup(self):
        self.page = None  # smoke_test の goto ガードが参照するため明示的に None
        self._closed = False
        self._loop = None
        self._pw = None
        self._browser = None

    def _teardown_resources(self):
        self._cleanup()

    def _cleanup(self):
        """イベントループ・ブラウザ・全タスクを冪等に解放する。"""
        if getattr(self, "_closed", False):
            return
        self._closed = True
        loop = getattr(self, "_loop", None)
        if loop is None or loop.is_closed():
            return

        async def _shutdown():
            # まずブラウザを閉じて実行中の goto を全て中断させる → 各ワーカーは即座に
            # 例外で終了する。停止しないタスクを gather で待たない (ハング回避)。
            if self._browser is not None:
                await self._browser.close()
            if self._pw is not None:
                await self._pw.stop()

        try:
            # ブラウザ停止が万一固まっても全体を止めないよう時間で打ち切る。
            loop.run_until_complete(asyncio.wait_for(_shutdown(), timeout=30))
        except Exception as e:  # noqa: BLE001
            self.logger.warning("クリーンアップ中の例外 (無視): %s", e)
        finally:
            # 残った未完了タスクは gather せず破棄 (loop.close が回収)。
            for t in asyncio.all_tasks(loop):
                t.cancel()
            try:
                loop.close()
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # メインループ — 総ページ数を確定し、一覧/詳細を並行取得してストリーミング yield
    # ------------------------------------------------------------------
    def parse(self, url: str):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._boot())
            self._seen: set[str] = set()
            self._item_q: asyncio.Queue = asyncio.Queue()
            self._loop.create_task(self._orchestrate(url))

            while True:
                item = self._loop.run_until_complete(self._item_q.get())
                if item is _SENTINEL:
                    break
                yield item
        finally:
            self._cleanup()

    async def _boot(self):
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=True)

    async def _orchestrate(self, url: str):
        """一覧を生産→詳細ワーカーが消費、の固定プール。全消化後に番兵を投入。

        タスクは (LIST_WORKERS + DETAIL_WORKERS) 個だけ。大量タスクを作らないので
        メモリも後片付け(キャンセル)も軽い。
        """
        card_q: asyncio.Queue = asyncio.Queue(maxsize=self.CARD_QUEUE_MAX)
        workers = [
            self._loop.create_task(self._detail_worker(card_q))
            for _ in range(self.DETAIL_WORKERS)
        ]
        try:
            await self._produce_lists(url, card_q)
        except Exception as e:  # noqa: BLE001
            self.logger.warning("一覧生産で例外 (取得済みで継続): %s", e)
        finally:
            # 生産完了 → 各詳細ワーカーに終了合図 (None) を送る
            for _ in workers:
                await card_q.put(None)
            await asyncio.gather(*workers, return_exceptions=True)
            await self._item_q.put(_SENTINEL)

    async def _produce_lists(self, url: str, card_q: asyncio.Queue):
        """1 ページ目で総ページ数を確定し、残りを LIST_WORKERS で並行取得してカードを投入。"""
        html1 = await self._fetch(url, ".companylist_outer", "companylist_outer")
        pages = 1
        if html1:
            soup = BeautifulSoup(html1, "html.parser")
            total = self._read_total(soup)
            if total:
                self.total_items = total
                pages = math.ceil(total / _PAGE_SIZE)
            await self._enqueue_cards(soup, url, card_q)

        if pages > 1:
            page_q: asyncio.Queue = asyncio.Queue()
            for n in range(2, pages + 1):
                page_q.put_nowait(f"{url}?page={n}")
            lw = [
                self._loop.create_task(self._list_worker(page_q, card_q))
                for _ in range(self.LIST_WORKERS)
            ]
            for _ in lw:
                page_q.put_nowait(None)
            await asyncio.gather(*lw, return_exceptions=True)
        else:
            # フォールバック: 総件数が読めない場合のみ、空ページが続くまで逐次で辿る
            n, empty_streak = 2, 0
            while empty_streak < 2:
                found = await self._list_page(f"{url}?page={n}", card_q)
                empty_streak = 0 if found else empty_streak + 1
                n += 1

    async def _list_worker(self, page_q: asyncio.Queue, card_q: asyncio.Queue):
        while True:
            page_url = await page_q.get()
            try:
                if page_url is None:
                    return
                await self._list_page(page_url, card_q)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("一覧取得失敗 (継続): %s — %s", page_url, e)
            finally:
                page_q.task_done()

    async def _list_page(self, page_url: str, card_q: asyncio.Queue) -> bool:
        """一覧 1 ページを取得しカードを card_q に投入。カードを 1 件でも得たら True。"""
        html = await self._fetch(page_url, ".companylist_outer", "companylist_outer")
        if not html:
            return False
        soup = BeautifulSoup(html, "html.parser")
        return await self._enqueue_cards(soup, page_url, card_q) > 0

    async def _enqueue_cards(self, soup, base_url: str, card_q: asyncio.Queue) -> int:
        """一覧カードを抽出し、未処理の詳細 URL を card_q へ投入する (満杯なら待機=背圧)。"""
        count = 0
        for card in self._extract_cards(soup):
            link = card.select_one('a[href*="/job/"]')
            if not link or not link.get("href"):
                continue
            detail_href = re.sub(r"#.*$", "", link["href"])
            detail_url = urljoin(base_url, detail_href)
            if detail_url in self._seen:
                continue
            self._seen.add(detail_url)

            industries = [
                x.get_text(strip=True)
                for x in card.select(".company-industory-type")
                if x.get_text(strip=True)
            ]
            list_name_el = card.select_one("h3.companyname")
            list_name = list_name_el.get_text(strip=True) if list_name_el else ""

            await card_q.put((detail_url, industries, list_name))
            count += 1
        return count

    @staticmethod
    def _extract_cards(soup) -> list:
        # デスクトップ版カードを優先 (モバイル版と二重掲載のため)。無ければ全カードで代替。
        cards = soup.select(".companylist.d-none.d-xl-block .companylist_outer")
        if not cards:
            cards = soup.select(".companylist_outer")
        return cards

    async def _detail_worker(self, card_q: asyncio.Queue):
        while True:
            work = await card_q.get()
            try:
                if work is None:
                    return
                detail_url, industries, list_name = work
                html = await self._fetch(detail_url, "h2.detail-title", "detail-title")
                if not html:
                    continue
                item = self._parse_detail(
                    BeautifulSoup(html, "html.parser"), detail_url, industries, list_name
                )
                if item:
                    await self._item_q.put(item)
            except Exception as e:  # noqa: BLE001 — 個別詳細の失敗は握って継続
                self.error_count += 1
                self.logger.warning("詳細処理失敗 (継続): %s", e)
            finally:
                card_q.task_done()

    # ------------------------------------------------------------------
    # 取得 (fresh context ごとに 1 遷移。challenge/空はリトライ)
    # ------------------------------------------------------------------
    async def _fetch(self, url: str, wait_selector: str, required: str) -> str | None:
        for attempt in range(self.MAX_RETRY):
            html = await self._nav(url, wait_selector)
            if html and "Just a moment" not in html and required in html:
                return html
            if attempt < self.MAX_RETRY - 1:
                self.logger.info("再取得 (試行%d, challenge/空): %s", attempt + 2, url)
                await asyncio.sleep(1.0 + attempt)
        self.error_count += 1
        self.logger.warning("取得失敗 (リトライ上限): %s", url)
        return None

    async def _nav(self, url: str, wait_selector: str) -> str | None:
        """まっさらなコンテキストで 1 回だけ遷移し HTML を返す (常に「初回遷移」)。"""
        context = await self._browser.new_context(user_agent=self.USER_AGENT)
        await context.route("**/*", self._route)
        try:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=self.NAV_TIMEOUT)
            html = await page.content()
            if wait_selector.lstrip(".").split()[-1] not in html:
                try:
                    await page.wait_for_selector(wait_selector, timeout=self.SELECTOR_TIMEOUT)
                    html = await page.content()
                except Exception:  # noqa: BLE001 — レンダリング待ちタイムアウトは許容
                    pass
            return html
        except Exception as e:  # noqa: BLE001 — 個別ページの取得失敗はリトライに委ねる
            self.logger.debug("goto 失敗: %s — %s", url, e)
            return None
        finally:
            await context.close()

    async def _route(self, route):
        if route.request.resource_type in self._BLOCK_TYPES:
            await route.abort()
        else:
            await route.continue_()

    # ------------------------------------------------------------------
    # 詳細ページの構造化抽出 (ネットワーク非依存の純パース)
    # ------------------------------------------------------------------
    def _parse_detail(self, soup, url: str, industries: list, list_name: str) -> dict | None:
        item = {
            Schema.URL: url,
            Schema.NAME: "",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMP_NUM: "",
            Schema.REP_NM: "",
            Schema.HP: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.CAT_SITE: " / ".join(industries),
            "職種タグ": "",
            "契約形態": "",
            "給与報酬": "",
            "勤務地": "",
            "募集業種": "",
            "対象となる方": "",
            "担当者カナ": "",
        }

        # 会社名
        name_el = soup.select_one("h2.detail-title")
        item[Schema.NAME] = name_el.get_text(strip=True) if name_el else list_name

        # 企業概要 (従業員数 / 所在地 / ウェブサイト)
        company = self._pairs(
            soup, "li.detail-company-info-item",
            ".detail-company-info-label", ".detail-company-info-text",
        )
        item[Schema.EMP_NUM] = company.get("従業員数", "")

        addr_raw = company.get("所在地", "")
        if addr_raw:
            m = _POST_PATTERN.search(addr_raw)
            if m:
                item[Schema.POST_CODE] = m.group(1)
                addr_raw = addr_raw[m.end():].strip()
            addr_raw = addr_raw.lstrip("〒 　").strip()
            pm = _PREF_PATTERN.search(addr_raw)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr_raw[pm.end():].strip() or addr_raw
            else:
                item[Schema.ADDR] = addr_raw

        # ウェブサイト (href 優先)
        for li in soup.select("li.detail-company-info-item"):
            lab = li.select_one(".detail-company-info-label")
            if lab and "ウェブサイト" in lab.get_text(strip=True):
                a = li.select_one("a[href]")
                item[Schema.HP] = a["href"].strip() if a and a.get("href") else \
                    company.get("ウェブサイト", "")
                break

        # 応募・選考 (連絡先 / 担当者)
        recruit = self._pairs(
            soup, "li.detail-recruitment-info-item",
            ".detail-recruitment-info-label", ".detail-recruitment-info-text",
        )
        tel_src = recruit.get("連絡先", "")
        if tel_src:
            tels = []
            for t in _TEL_PATTERN.findall(tel_src):
                if t not in tels:
                    tels.append(t)
            item[Schema.TEL] = " / ".join(tels)

        rep = recruit.get("担当者", "")
        if rep:
            km = re.search(r"[（(]([^）)]+)[）)]", rep)
            if km:
                item["担当者カナ"] = km.group(1).strip()
                rep = re.sub(r"[（(][^）)]+[）)]", "", rep)
            item[Schema.REP_NM] = rep.strip()

        # 募集要項 (guideline)
        guide = self._guideline(soup)
        item[Schema.TIME] = guide.get("勤務時間", "")
        item[Schema.HOLIDAY] = guide.get("休日休暇", "")
        item["契約形態"] = guide.get("契約形態", "")
        item["給与報酬"] = guide.get("給与/報酬", "")
        item["勤務地"] = guide.get("勤務地", "")
        item["募集業種"] = guide.get("募集業種", "")
        item["対象となる方"] = guide.get("対象となる方", "")

        # 職種タグ
        tags = [
            li.get_text(strip=True)
            for li in soup.select(".detail-inner-tag .tag-list li")
            if li.get_text(strip=True)
        ]
        item["職種タグ"] = " / ".join(dict.fromkeys(tags))

        return item

    @staticmethod
    def _pairs(soup, item_sel: str, label_sel: str, text_sel: str) -> dict:
        out = {}
        for li in soup.select(item_sel):
            lab = li.select_one(label_sel)
            val = li.select_one(text_sel)
            if not lab:
                continue
            key = lab.get_text(strip=True)
            out[key] = re.sub(r"\s+", " ", val.get_text(" ", strip=True)).strip() if val else ""
        return out

    @staticmethod
    def _guideline(soup) -> dict:
        out = {}
        for title in soup.select(".detail-recruitment-guideline-title"):
            li = title.find_parent("li") or title.parent
            val = li.select_one(".detail-recruitment-guideline-text") if li else None
            key = title.get_text(strip=True)
            out[key] = re.sub(r"\s+", " ", val.get_text(" ", strip=True)).strip() if val else ""
        return out

    @staticmethod
    def _read_total(soup) -> int:
        el = soup.select_one(".search-detail") or soup.select_one(".search-title")
        if el:
            m = re.search(r"([\d,]+)\s*件", el.get_text())
            if m:
                return int(m.group(1).replace(",", ""))
        return 0


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GatenInfo()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://gaten.info/list")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

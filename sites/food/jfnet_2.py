# ===== 【pip install -e . を実行していない場合のみ必要】===========
import sys
from pathlib import Path
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================
"""
日本フードサービス協会 (JF) — 会員企業一覧スクレイパー (正会員 + 賛助会員)

取得対象:
    - 全国の正会員企業 + 賛助会員企業 の一覧
    - 社名 / 公式サイトURL / 会員区分 / ブランド・店舗名 / カナ頭文字
    - さらに各社の公式サイト (企業HP) をたどり、会社概要から取得できる
      基本カラム (住所・都道府県・郵便番号・TEL・代表者名・資本金・
      設立/創業・従業員数・事業内容・売上高 等) を可能な限り補完する。

取得フロー:
    1. ルートURL (https://www.jfnet.or.jp/) から会員一覧ページ
       /about-jf/membership-list/ を派生して取得 (静的・単一ページ)。
    2. 同一ページ内に 2 つの会員リストがサーバーレンダリングされている:
         div.membership__list[x-show="page == 1"] … 正会員 (369社)
         div.membership__list[x-show="page == 2"] … 賛助会員 (279社)
       各エントリ div[x-show^="furigana"] から 社名 / 企業HP / ブランド名 /
       索引カナ を取得する。
    3. 各社の企業HP (外部サイト) にアクセスし、会社概要/企業情報ページを
       探して label:value 形式 (table th/td, dl dt/dd) から基本カラムを抽出。
       外部サイトは構造がまちまちのため、キーワードによるベストエフォート抽出。
       取得できた生の label:value は「会社概要」列に全て残す。
       ※ 企業HPへの到達失敗・WAF・タイムアウト等が起きても行自体は
         一覧由来の情報だけで必ず yield する (握り潰して継続)。

実行方法:
    python scripts/sites/food/jfnet_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id jfnet_2
"""

import re
from urllib.parse import urljoin

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 会員一覧ページのパス (ルートURLから派生させる)
_LIST_PATH = "/about-jf/membership-list/"

# x-show="furigana == 'て'" から索引カナを取り出す
_FURIGANA_RE = re.compile(r"furigana\s*==\s*['\"](.+?)['\"]")

# 会員区分ごとの一覧ブロック (Alpine.js の page 切替タブ)
_MEMBER_BLOCKS = [
    ("正会員", "div.membership__list[x-show='page == 1']"),
    ("賛助会員", "div.membership__list[x-show='page == 2']"),
]

# 都道府県 (住所先頭からの判定用)
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 企業HP内の「会社概要」ページへのリンク候補 (テキスト / href)
_OVERVIEW_TEXT_KW = ["会社概要", "会社案内", "企業情報", "会社情報", "企業概要", "会社データ", "企業データ"]
_OVERVIEW_HREF_KW = ["company", "corporate", "overview", "profile", "outline", "about", "gaiyo", "kaisha"]

# 表(th/td)・定義リスト(dl)を持たず、li 等に「見出し + 値」を並べる構造 (Studio.Design/
# Nuxt 等の JS 生成サイトに多い) からラベル値を拾うための「会社概要ラベル」ホワイトリスト。
# 沿革の年号やナビ項目など概要以外の行を除外し、会社概要列を汚さないために使う。
_PROFILE_LABEL_KWS = [
    "会社名", "社名", "商号", "屋号", "名称", "英文", "住所", "所在地", "本社", "本店",
    "拠点", "支店", "営業所", "代表", "社長", "資本金", "設立", "創業", "創立",
    "従業員", "社員", "売上", "事業", "業務", "業態", "電話", "tel", "ｔｅｌ",
    "fax", "ｆａｘ", "郵便", "番号", "登録", "許可", "免許",
]

# label(見出し) → Schema カラム のマッピング (キーワード部分一致・先頭ほど優先)
_LABEL_MAP = [
    (Schema.ADDR,      ["所在地", "本社", "住所", "本店", "所在"]),
    (Schema.TEL,       ["電話", "tel", "ｔｅｌ"]),
    (Schema.REP_NM,    ["代表者", "代表取締役", "代表", "社長"]),
    (Schema.CAP,       ["資本金"]),
    (Schema.OPEN_DATE, ["設立", "創業", "創立"]),
    (Schema.EMP_NUM,   ["従業員", "社員数", "従業者"]),
    (Schema.SALES,     ["売上", "売り上げ"]),
    (Schema.LOB,       ["事業内容", "事業案内", "事業目的", "主要事業", "業務内容", "営業内容", "業態"]),
]

_TEL_RE = re.compile(r"0\d{1,4}[-(]\d{1,4}[-)]\d{3,4}")
_POST_RE = re.compile(r"〒\s*(\d{3}-?\d{4})")
_ADDR_CUT_RE = re.compile(r"TEL|ＴＥＬ|電話|FAX|ＦＡＸ|Google|MAP|地図|営業時間")


class JfnetMembershipScraper(StaticCrawler):
    """日本フードサービス協会 会員企業一覧 (正会員+賛助会員) スクレイパー"""

    DELAY = 1.0
    # 外部企業サイト取得用の短めタイムアウト (遅い/無反応サイトで停滞しないため)
    EXTERNAL_TIMEOUT = 12
    # JS 生成サイト用の Playwright レンダリングのタイムアウト(秒)
    RENDER_TIMEOUT = 25

    EXTRA_COLUMNS = [
        "会員区分",         # 正会員 / 賛助会員
        "ブランド・店舗名",  # 各社が展開する店舗ブランド名の列挙
        "カナ頭文字",       # 五十音索引の頭文字カナ
        "会社概要",         # 企業HPから抽出した label:value の生データ (全項目)
    ]

    # -------------------------------------------------------------- 一覧
    def parse(self, url: str):
        list_url = urljoin(url, _LIST_PATH)
        soup = self.get_soup(list_url)
        if soup is None:
            return

        # 会員区分ごとにエントリを収集
        collected = []
        for kubun, selector in _MEMBER_BLOCKS:
            block = soup.select_one(selector)
            if block is None:
                continue
            for entry in block.select("div[x-show^='furigana']"):
                collected.append((kubun, entry))

        self.total_items = len(collected)

        for kubun, entry in collected:
            try:
                a = entry.select_one("a[href]")
                if a is None:
                    continue
                name = a.get_text(strip=True)
                if not name:
                    continue

                hp = (a.get("href") or "").strip()

                text_el = entry.select_one("div.--text")
                brands = text_el.get_text(strip=True) if text_el else ""

                kana = ""
                m = _FURIGANA_RE.search(entry.get("x-show", ""))
                if m:
                    kana = m.group(1)

                item = {
                    Schema.NAME: name,
                    Schema.HP: hp,
                    Schema.URL: list_url,
                    "会員区分": kubun,
                    "ブランド・店舗名": brands,
                    "カナ頭文字": kana,
                }

                # 企業HP をたどって基本カラムを補完 (失敗しても一覧情報は残す)
                if hp.startswith("http"):
                    try:
                        item.update(self._enrich_from_company_site(hp))
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("企業HP解析に失敗 (%s): %s", hp, e)

                yield item
            except Exception as e:  # noqa: BLE001
                self.logger.warning("エントリの解析に失敗: %s", e)
                continue

    # -------------------------------------------------------------- 企業HP補完
    def _fetch(self, target: str) -> bs4.BeautifulSoup | None:
        """外部サイトを requests で静的取得。どんな失敗 (WAF/タイムアウト/404) も None に丸める。"""
        try:
            resp = self.session.get(target, timeout=self.EXTERNAL_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200 or not resp.text:
                return None
            ctype = resp.headers.get("Content-Type", "")
            if ctype and "html" not in ctype.lower():
                return None
            if "charset=" not in ctype.lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:  # noqa: BLE001
            self.logger.info("外部サイト取得失敗 (%s): %s", target, e)
            return None

    def _render(self, target: str) -> bs4.BeautifulSoup | None:
        """外部サイトを Playwright でレンダリングして取得する (JS 生成サイト用)。

        Nuxt/Studio.Design 等のクライアントレンダリング型サイトは requests では
        本文が空になり会社概要を取得できない。その場合のフォールバックとして
        ヘッドレスブラウザで描画後の DOM を返す。失敗は全て None に丸める。
        """
        try:
            self._ensure_browser()
        except Exception as e:  # noqa: BLE001
            self.logger.info("ブラウザ起動に失敗 (動的取得を断念): %s", e)
            return None
        try:
            from playwright.sync_api import TimeoutError as PWTimeout
            try:
                self._page.goto(target, wait_until="networkidle", timeout=self.RENDER_TIMEOUT * 1000)
            except PWTimeout:
                # networkidle まで落ち着かなくても、描画済みの内容は拾えることが多い
                pass
            self._page.wait_for_timeout(1500)
            html = self._page.content()
            return bs4.BeautifulSoup(html, "html.parser")
        except Exception as e:  # noqa: BLE001
            self.logger.info("動的レンダリング失敗 (%s): %s", target, e)
            return None

    def _ensure_browser(self):
        """Playwright ブラウザを遅延起動する (JS 生成サイトに遭遇した初回のみ)。"""
        if getattr(self, "_page", None) is not None:
            return
        from playwright.sync_api import sync_playwright
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)
        self._context = self._browser.new_context(user_agent=self.USER_AGENT)
        self._page = self._context.new_page()

    def _teardown_resources(self):
        """遅延起動した Playwright リソースがあれば安全に解放する。"""
        for attr in ("_page", "_context", "_browser"):
            obj = getattr(self, attr, None)
            if obj is not None:
                try:
                    obj.close()
                except Exception:  # noqa: BLE001
                    pass
        pw = getattr(self, "_playwright", None)
        if pw is not None:
            try:
                pw.stop()
            except Exception:  # noqa: BLE001
                pass
        super()._teardown_resources()

    def _load_with_fallback(self, target: str) -> tuple[bs4.BeautifulSoup | None, list[tuple[str, str]]]:
        """静的取得を試し、ラベル値が取れなければ Playwright レンダリングにフォールバック。"""
        soup = self._fetch(target)
        pairs = self._extract_pairs(soup) if soup is not None else []
        if pairs:
            return soup, pairs
        # 静的で概要が取れない (JS 生成サイト等) → ブラウザレンダリングで再挑戦
        dyn = self._render(target)
        if dyn is not None:
            dyn_pairs = self._extract_pairs(dyn)
            if dyn_pairs:
                return dyn, dyn_pairs
            return dyn, pairs
        return soup, pairs

    def _enrich_from_company_site(self, hp: str) -> dict:
        """企業HPトップ → 会社概要ページ を辿って基本カラムを抽出する。"""
        top, pairs = self._load_with_fallback(hp)
        if top is None:
            return {}

        # トップに概要が無ければ会社概要ページを辿る (取れていても補完を試みる)
        overview_url = self._find_overview_link(top, hp)
        if overview_url and overview_url.rstrip("/") != hp.rstrip("/"):
            _ov_soup, ov_pairs = self._load_with_fallback(overview_url)
            if ov_pairs:
                pairs = ov_pairs

        return self._pairs_to_columns(pairs)

    @staticmethod
    def _find_overview_link(soup: bs4.BeautifulSoup, base: str) -> str | None:
        best = None
        best_score = 0
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if not href or href.startswith(("#", "javascript", "mailto", "tel:")):
                continue
            text = a.get_text(strip=True)
            hl = href.lower()
            score = 0
            for i, kw in enumerate(_OVERVIEW_TEXT_KW):
                if kw in text:
                    score = max(score, 100 - i)
            for i, kw in enumerate(_OVERVIEW_HREF_KW):
                if kw in hl:
                    score = max(score, 50 - i)
            if score > best_score:
                best_score = score
                best = urljoin(base, href)
        return best

    @staticmethod
    def _extract_pairs(soup: bs4.BeautifulSoup) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        def _add(label: str, value: str):
            label = (label or "").strip()
            value = (value or "").strip()
            if not label or not value:
                return
            key = (label, value)
            if key in seen:
                return
            seen.add(key)
            pairs.append((label, value))

        # 1) 表: th/td
        for row in soup.select("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                _add(th.get_text(" ", strip=True), td.get_text(" ", strip=True))

        # 2) 定義リスト: dt/dd
        for dl in soup.select("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                _add(dt.get_text(" ", strip=True), dd.get_text(" ", strip=True))

        # 3) li 等に「見出し + 値」を並べる構造 (JS 生成サイトに多い)。
        #    ナビや沿革を拾わないよう、見出しが会社概要ラベルの時だけ採用する。
        for li in soup.select("li"):
            units = [u for u in li.stripped_strings]
            if len(units) < 2:
                continue
            label = units[0]
            if len(label) > 20:
                continue
            ll = label.lower()
            if not any(kw in ll for kw in _PROFILE_LABEL_KWS):
                continue
            _add(label, " ".join(units[1:]))

        return pairs

    def _pairs_to_columns(self, pairs: list[tuple[str, str]]) -> dict:
        out: dict = {}
        for label, value in pairs:
            ll = label.lower()
            for field, kws in _LABEL_MAP:
                if field in out:
                    continue
                if any(kw in ll for kw in kws):
                    out[field] = value[:200]
                    break

        # 住所から都道府県 / 郵便番号 / TEL を派生
        addr_raw = out.get(Schema.ADDR, "")
        if addr_raw:
            post = _POST_RE.search(addr_raw)
            if post:
                out[Schema.POST_CODE] = post.group(1)
            tel = _TEL_RE.search(addr_raw)
            if tel and Schema.TEL not in out:
                out[Schema.TEL] = tel.group()
            clean = _ADDR_CUT_RE.split(addr_raw)[0]
            clean = _POST_RE.sub("", clean).strip(" 　")
            out[Schema.ADDR] = clean
            pref = next((p for p in _PREFS if p in clean[:8]), "")
            if pref:
                out[Schema.PREF] = pref

        # TEL 単独項目の正規化
        if Schema.TEL in out:
            tel = _TEL_RE.search(out[Schema.TEL])
            if tel:
                out[Schema.TEL] = tel.group()

        # 生の会社概要 (全項目) を残す
        if pairs:
            out["会社概要"] = " / ".join(f"{l}:{v}" for l, v in pairs)[:1000]

        return out


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JfnetMembershipScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jfnet.or.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

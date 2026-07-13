"""
情報サービス産業協会（JISA）会員一覧 (jisa.or.jp) — 会員企業一覧スクレイパー

取得対象:
    - 法人会員・団体会員・賛助会員のすべて（約 522 件）
    - 会員名・会員企業HP・会員区分
    - 各会員の HP へアクセスして取得した企業の基本情報:
        会社名（正式名称）・代表者名・郵便番号・都道府県・住所・電話番号・
        FAX番号・メールアドレス・設立年月・資本金・事業内容

取得フロー:
    1. /about_jisa/list/tabid/739/Default.aspx の単一ページを解析。
       div.memberList が 3 ブロック（法人 / 団体 / 賛助）あり、各ブロック内の
       div[id$=houjin|dantai|sanjyo] セクションのテーブル行から名称と HP リンクを抽出。
       ページネーションは無し（全件が 1 ページに掲載）。
    2. 抽出した各会員の HP トップページにアクセスし、企業の基本情報を取得する。
       トップページで情報が揃わない場合は「会社概要 / 会社情報 / 企業情報 /
       アクセス / お問い合わせ / 特定商取引法に基づく表記」等のリンクを
       複数辿って不足項目を補う（best-effort）。

備考:
    - 各会員の外部 HP には統一構造が無いため、th/td テーブル・dl/dt/dd の
      ラベル-値ペアと、〒 郵便番号・電話・FAX・メール等の正規表現で
      ヒューリスティックに抽出する。HP に明記が無い項目は空欄となる
      （推測・補完はしない）。

実行方法:
    python scripts/sites/corporate/jisa.py
    python bin/run_flow.py --site-id jisa
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 会員区分: セクション id の接尾辞 → 日本語ラベル
_CATEGORY_BY_SUFFIX = {
    "houjin": "法人会員",
    "dantai": "団体会員",
    "sanjyo": "賛助会員",
}

# 都道府県（住所抽出のアンカー）
_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_POSTAL_RE = re.compile(r"〒?\s*(\d{3})[\-‐‑–—―ー－\s]{0,2}(\d{4})")
# 〒 記号必須版（散文中の数字を郵便番号と誤認しないため全文抽出で使う）
_POSTAL_MARK_RE = re.compile(r"〒\s*(\d{3})[\-‐‑–—―ー－\s]{0,2}(\d{4})")
_ADDR_RE = re.compile("(" + _PREFS + r")([^\s　]{2,40})")
# 電話・FAX（市外局番始まり / 括弧・ハイフン許容）。0120 フリーダイヤルも許容。
_TEL_RE = re.compile(r"0\d{1,4}[\-\(\)‐‑–—―－\s]{1,3}\d{1,4}[\-\)‐‑–—―－\s]{1,3}\d{3,4}")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# 会社概要ページを探すためのキーワード（リンク文言 / href）
_COMPANY_LINK_TEXT = (
    "会社概要", "会社案内", "会社情報", "企業情報", "会社データ", "会社紹介",
    "アクセス", "お問い合わせ", "お問合せ", "問い合わせ", "コンタクト",
    "特定商取引", "コーポレート", "会社",
)
_COMPANY_LINK_HREF = (
    "company", "corporate", "about", "profile", "outline", "overview",
    "access", "contact", "inquiry", "tokutei", "tokusho", "info",
)

# ラベル → 抽出項目。値側テキストにこれらのキーワードが含まれる th/dt を対応付ける。
# （長いラベルを先に判定するため tuple の順序に意味を持たせる）
_LABEL_MAP = (
    ("rep", ("代表者", "代表取締役", "代表理事", "代表社員", "理事長", "社長", "代表")),
    ("cap", ("資本金",)),
    ("founded", ("設立", "創業", "創立")),
    ("fax", ("fax", "ｆａｘ", "ファックス", "ファクシミリ", "ファクス")),
    ("tel", ("tel", "ｔｅｌ", "電話", "phone", "電話番号")),
    ("email", ("メール", "mail", "e-mail", "ｅ-ｍａｉｌ")),
    ("addr", ("所在地", "住所", "本社", "所在")),
    ("lob", ("事業内容", "事業概要", "業務内容", "主な事業", "事業")),
    ("name", ("会社名", "商号", "社名", "名称", "法人名")),
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


class JisaScraper(StaticCrawler):
    """情報サービス産業協会（JISA）会員一覧 スクレイパー"""

    DELAY = 1.5
    # 会員 HP 取得は 1 件ずつ短めのタイムアウトで（応答の遅いサイト対策）
    HP_TIMEOUT = 12
    # 1 会員あたりで辿る会社概要系サブページの最大数（best-effort / 過負荷防止）
    MAX_SUBPAGES = 4
    EXTRA_COLUMNS = ["会員区分", "HP掲載社名", "FAX番号"]

    # 取得項目のキー（空文字で初期化）
    _INFO_KEYS = ("name", "rep", "post", "pref", "addr", "tel", "fax",
                  "email", "founded", "cap", "lob")

    # -----------------------------------------------------------------
    # 会員 HP からの基本情報抽出
    # -----------------------------------------------------------------
    def _get_hp_soup(self, url: str) -> bs4.BeautifulSoup | None:
        """会員 HP を取得して BeautifulSoup を返す（失敗時は None）。"""
        try:
            resp = self.session.get(url, timeout=self.HP_TIMEOUT)
            resp.raise_for_status()
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.info("HP 取得失敗 (スキップ): %s — %s", url, e)
            return None

    @staticmethod
    def _extract_hp_name(soup: bs4.BeautifulSoup) -> str:
        """HP 掲載の社名を推定（og:site_name 優先、無ければ <title>）。"""
        og = soup.find("meta", attrs={"property": "og:site_name"})
        if og and _clean(og.get("content", "")):
            return _clean(og["content"])
        if soup.title:
            return _clean(soup.title.get_text())
        return ""

    @staticmethod
    def _iter_kv_pairs(soup: bs4.BeautifulSoup):
        """th/td テーブル・dl/dt/dd から (ラベル, 値) ペアを列挙する。"""
        # <table> の th→td（同一行）
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                yield _clean(th.get_text(" ", strip=True)), _clean(td.get_text(" ", strip=True))
        # <dl> の dt→dd
        for dl in soup.find_all("dl"):
            children = [c for c in dl.find_all(["dt", "dd"], recursive=False)]
            label = ""
            for c in children:
                if c.name == "dt":
                    label = _clean(c.get_text(" ", strip=True))
                elif c.name == "dd" and label:
                    yield label, _clean(c.get_text(" ", strip=True))

    @staticmethod
    def _classify_label(label: str) -> str | None:
        """ラベル文字列を取得項目キーに分類する（該当なしは None）。"""
        low = label.lower()
        for key, kws in _LABEL_MAP:
            if any(kw in low for kw in kws):
                return key
        return None

    @classmethod
    def _split_addr(cls, value: str, require_marker: bool = False) -> tuple[str, str, str]:
        """住所文字列から (郵便番号, 都道府県, 住所) を切り出す。

        require_marker=True の場合、〒 記号を伴う郵便番号のみを採用する
        （散文中の数字を誤って郵便番号扱いしないため。全文フォールバック時に使う）。
        """
        pat = _POSTAL_MARK_RE if require_marker else _POSTAL_RE
        pref = addr = ""
        addr_start = None
        m = _ADDR_RE.search(value)
        if m and re.search(r"[0-9０-９市区郡町村]", m.group(0)):
            pref, addr = m.group(1), m.group(2)
            addr_start = m.start()

        # 郵便番号は「採用した住所の直前にあるもの」を選ぶ。
        # 支社を併記するサイトで本社郵便番号と支社住所がずれるのを防ぐ。
        post = ""
        candidates = list(pat.finditer(value))
        chosen = None
        if addr_start is not None:
            before = [mp for mp in candidates if mp.end() <= addr_start]
            chosen = before[-1] if before else (candidates[0] if candidates else None)
        elif candidates:
            chosen = candidates[0]
        if chosen:
            post = f"{chosen.group(1)}-{chosen.group(2)}"
        return post, pref, addr

    @classmethod
    def _extract_from_soup(cls, soup: bs4.BeautifulSoup) -> dict:
        """1 ページから取得可能な基本情報を抽出して dict で返す。"""
        info = {k: "" for k in cls._INFO_KEYS}
        addr_locked = False  # ラベル付き住所を採用したら全文フォールバックで上書きしない

        # 1) ラベル-値ペア（会社概要テーブル等）が最も信頼できる
        for label, value in cls._iter_kv_pairs(soup):
            key = cls._classify_label(label)
            if not key or not value:
                continue
            if key == "addr":
                if addr_locked:
                    continue
                # 郵便番号・都道府県・住所は同一の値から一括で切り出し整合させる
                post, pref, addr = cls._split_addr(value)
                if addr:
                    # 実住所が取れた行のみ採用・確定（以降の行/全文では上書きしない）
                    info["post"], info["pref"], info["addr"] = post, pref, addr
                    addr_locked = True
                elif post and not info["post"]:
                    # 郵便番号のみの行（住所は別行）。郵便番号だけ拾い探索は継続
                    info["post"] = post
            elif key == "tel":
                m = _TEL_RE.search(value)
                if not info["tel"]:
                    info["tel"] = _clean(m.group(0)) if m else value
            elif key == "fax":
                m = _TEL_RE.search(value)
                if not info["fax"]:
                    info["fax"] = _clean(m.group(0)) if m else value
            elif key == "email":
                m = _EMAIL_RE.search(value)
                if not info["email"]:
                    info["email"] = m.group(0) if m else value
            elif not info[key]:
                info[key] = value

        # 2) ラベルで住所が取れなかった場合のみ、全文から（〒必須で）フォールバック抽出。
        #    ラベル付き住所を採用済みなら、整合性維持のため上書きしない。
        flat = soup.get_text(" ", strip=True)
        if not addr_locked and not info["addr"]:
            post, pref, addr = cls._split_addr(flat, require_marker=True)
            info["post"] = info["post"] or post
            info["pref"] = info["pref"] or pref
            info["addr"] = info["addr"] or addr
        if not info["email"]:
            # mailto: リンクを優先
            a = soup.select_one('a[href^="mailto:"]')
            if a:
                info["email"] = a["href"].split("mailto:", 1)[-1].split("?")[0].strip()
            else:
                m = _EMAIL_RE.search(flat)
                if m:
                    info["email"] = m.group(0)
        return info

    def _find_company_pages(self, soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        """会社概要・アクセス・お問い合わせ・特商法等のリンク先 URL を列挙する。"""
        seen: set[str] = set()
        pages: list[str] = []
        base_norm = base_url.rstrip("/")
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"].strip()
            if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            href_l = href.lower()
            if not (any(k in text for k in _COMPANY_LINK_TEXT)
                    or any(k in href_l for k in _COMPANY_LINK_HREF)):
                continue
            target = urljoin(base_url, href)
            if not target.startswith("http"):
                continue
            norm = target.rstrip("/")
            if norm == base_norm or norm in seen:
                continue
            seen.add(norm)
            pages.append(target)
            if len(pages) >= self.MAX_SUBPAGES:
                break
        return pages

    def _merge(self, info: dict, extra: dict) -> None:
        """extra の値で info の空欄のみを埋める。"""
        for k in self._INFO_KEYS:
            if not info.get(k) and extra.get(k):
                info[k] = extra[k]

    def _fetch_member_info(self, hp: str) -> dict:
        """会員 HP にアクセスし、企業の基本情報を取得する。

        トップページ → 会社概要 / アクセス / お問い合わせ / 特商法 等の
        サブページを複数辿り、不足項目を補完する（best-effort）。
        """
        info = {k: "" for k in self._INFO_KEYS}
        soup = self._get_hp_soup(hp)
        if soup is None:
            return info

        info["name"] = self._extract_hp_name(soup)
        self._merge(info, self._extract_from_soup(soup))

        # 主要項目が揃うまで（または上限まで）サブページを辿る
        for sub_url in self._find_company_pages(soup, hp):
            if info["addr"] and info["tel"] and info["rep"] and info["lob"]:
                break
            sub = self._get_hp_soup(sub_url)
            if sub is None:
                continue
            self._merge(info, self._extract_from_soup(sub))

        return info

    # -----------------------------------------------------------------
    # メイン
    # -----------------------------------------------------------------
    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        member_lists = soup.find_all("div", class_="memberList")

        # 進捗表示用に総件数を先に算出（データ行のみ）
        total = 0
        for ml in member_lists:
            for sec in ml.find_all("div", id=True):
                if not any(sec.get("id", "").endswith(s) for s in _CATEGORY_BY_SUFFIX):
                    continue
                for tr in sec.select("table tr"):
                    if tr.find("th"):
                        continue
                    if len(tr.find_all("td")) >= 2:
                        total += 1
        self.total_items = total

        for ml in member_lists:
            for sec in ml.find_all("div", id=True):
                sec_id = sec.get("id", "")
                category = next(
                    (label for suf, label in _CATEGORY_BY_SUFFIX.items() if sec_id.endswith(suf)),
                    "",
                )
                if not category:
                    continue

                for tr in sec.select("table tr"):
                    # 区分見出し行 (<th colspan=2>ア行</th>) はスキップ
                    if tr.find("th"):
                        continue
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue

                    name_cell = tds[-1]
                    name = _clean(name_cell.get_text(" ", strip=True))
                    if not name:
                        continue

                    hp = ""
                    a = name_cell.find("a", href=True)
                    if a and a["href"].strip().startswith("http"):
                        hp = a["href"].strip()

                    try:
                        # 各会員の HP にアクセスして企業の基本情報を取得
                        info = self._fetch_member_info(hp) if hp else {}

                        yield {
                            Schema.URL: url,
                            Schema.NAME: name,
                            Schema.HP: hp,
                            Schema.REP_NM: info.get("rep", ""),
                            Schema.POST_CODE: info.get("post", ""),
                            Schema.PREF: info.get("pref", ""),
                            Schema.ADDR: info.get("addr", ""),
                            Schema.TEL: info.get("tel", ""),
                            Schema.EMAIL: info.get("email", ""),
                            Schema.OPEN_DATE: info.get("founded", ""),
                            Schema.CAP: info.get("cap", ""),
                            Schema.LOB: info.get("lob", ""),
                            "会員区分": category,
                            "HP掲載社名": info.get("name", ""),
                            "FAX番号": info.get("fax", ""),
                        }
                    except Exception:
                        self.logger.exception("行解析失敗: %s", name)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JisaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jisa.or.jp/about_jisa/list/tabid/739/Default.aspx")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

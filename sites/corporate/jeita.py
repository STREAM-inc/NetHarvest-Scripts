"""
電子情報技術産業協会（JEITA）会員 — 会員企業の基本情報取得

取得の考え方:
    JEITA 会員一覧 (list.cgi) は「会社名」と「各社ホームページ URL」のディレクトリで
    あり、代表者・住所・電話番号・資本金・設立年月等の基本情報は掲載されていない。
    そのため本クローラーは list.cgi を起点として会員企業を列挙し、各社のホームページを
    実際に辿って企業の基本情報を取得する。

取得フロー:
    1. list.cgi (parse に渡される url = 起点) を取得
       - `[data-tab-content]` の 2 タブ (1=正会員 / 2=賛助会員)
       - `.member__list-box` (あいうえお順の頭文字グループ) → `.member__list li`
       - 各 <li> の <a href> = 会社 HP URL、テキスト = 会社名
    2. 会員 1 件ごとに、その HP を起点に会社概要 / 会社情報 / アクセス /
       お問い合わせ / 特定商取引法に基づく表記 / フッター等を辿り、以下を抽出:
         会社名 / 代表者名 / 郵便番号 / 住所 / 電話番号 / FAX / メールアドレス /
         設立年月 / 資本金 / 事業内容
    3. 会員 1 件 = 1 レコードとして yield

ルール:
    - HP に明記されている情報のみを取得し、推測・補完はしない。
    - 見つからない項目は空欄。
    - HP が取得できない会員も、JEITA 側の会社名 / HP URL / 会員区分は yield する。

実行方法:
    python scripts/sites/corporate/jeita.py
    docker compose exec worker python /app/bin/run_flow.py --site-id jeita
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# --- HP 内で辿る候補ページのキーワード (href / リンクテキストのどちらかに含まれれば対象) ---
_PROFILE_KW = [
    "会社概要", "会社情報", "企業情報", "会社案内", "企業概要", "会社データ", "会社",
    "about", "company", "profile", "corporate", "overview", "outline",
]
_ACCESS_KW = ["アクセス", "access"]
_CONTACT_KW = ["お問い合わせ", "問い合わせ", "問合せ", "contact", "inquiry"]
_LEGAL_KW = ["特定商取引", "tokushoho", "tokutei", "torihiki"]

# --- 会社概要テーブル等のラベル → 出力キー のマッピング (先に一致したものを優先) ---
_LABEL_MAP = [
    (Schema.REP_NM, ["代表者", "代表取締役", "代表 者", "社長", "代表者名"]),
    (Schema.ADDR, ["所在地", "本社所在地", "本店所在地", "住所", "本社", "本店"]),
    (Schema.TEL, ["電話", "tel", "ｔｅｌ", "℡", "電話番号"]),
    ("FAX", ["fax", "ｆａｘ", "ファックス", "ファクシミリ"]),
    (Schema.EMAIL, ["メール", "e-mail", "email", "電子メール", "mail"]),
    ("設立年月", ["設立年月", "設立", "創立", "創業"]),
    (Schema.CAP, ["資本金"]),
    (Schema.LOB, ["事業内容", "事業概要", "業務内容", "主な事業", "主要事業", "事業目的"]),
]


def _clean(text: str) -> str:
    """空白 (全角含む) を 1 個に正規化してトリムする。"""
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


def _first_postal(text: str) -> str:
    """テキストから郵便番号 (〒付き優先 / ddd-dddd) を 1 件抽出。"""
    m = re.search(r"〒\s*(\d{3})[\-ー－\s]?\s*(\d{4})", text)
    if not m:
        m = re.search(r"(?<!\d)(\d{3})-(\d{4})(?!\d)", text)
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _first_phone(text: str) -> str:
    """テキストから電話番号らしき文字列 (0始まり・合計10〜11桁) を 1 件抽出。"""
    for m in re.finditer(r"0[\d\-\(\)（）\s]{8,15}", text):
        raw = m.group()
        digits = re.sub(r"\D", "", raw)
        if 10 <= len(digits) <= 11:
            norm = re.sub(r"[\s\(\)（）]+", "-", raw)
            return re.sub(r"-+", "-", norm).strip("-")
    return ""


def _first_email(soup, text: str) -> str:
    """mailto: リンク優先でメールアドレスを 1 件抽出。画像ファイル名等は除外。"""
    for a in soup.select("a[href^='mailto:']"):
        addr = a.get("href", "")[7:].split("?")[0].strip()
        if "@" in addr:
            return addr
    for m in re.finditer(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text):
        cand = m.group()
        if re.search(r"\.(png|jpe?g|gif|webp|svg)$", cand, re.I) or "@2x" in cand or "@3x" in cand:
            continue
        return cand
    return ""


class Jeita(StaticCrawler):
    """電子情報技術産業協会（JEITA）会員 — 各社 HP から企業基本情報を取得。"""

    DELAY = 1.5
    # FAX / 設立年月 は Schema 未定義のため独自カラムとして宣言。頭文字は会員一覧由来。
    EXTRA_COLUMNS = ["FAX", "設立年月", "頭文字"]

    # 1 会員あたり HP 内で追加取得するページ数の上限 (トップページを除く)
    MAX_DETAIL_PAGES = 5

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        # ナビ (data-tab-menu) から会員区分ラベルを取得: {"1": "正会員", "2": "賛助会員"}
        tab_labels = {}
        for nav in soup.select(".member__nav-item[data-tab-menu]"):
            key = nav.get("data-tab-menu")
            link = nav.select_one(".member__nav-link")
            if not (key and link):
                continue
            sub = link.select_one(".sub")
            if sub:
                sub.extract()
            tab_labels[key] = link.get_text(strip=True)

        self.total_items = len(soup.select("[data-tab-content] .member__list > li"))

        for tab in soup.select("[data-tab-content]"):
            member_type = tab_labels.get(tab.get("data-tab-content"), "")

            for box in tab.select(".member__list-box"):
                title_el = box.select_one(".member__list-title")
                initial = title_el.get_text(strip=True) if title_el else ""

                for li in box.select(".member__list > li"):
                    a = li.find("a", href=True)
                    if a is not None:
                        name = _clean(a.get_text())
                        hp = a.get("href", "").strip()
                    else:
                        name = _clean(li.get_text())
                        hp = ""

                    if not name:
                        continue

                    # ベースレコード (JEITA ディレクトリ由来)
                    record = {
                        Schema.NAME: name,
                        Schema.HP: hp,
                        Schema.CAT_SITE: member_type,
                        Schema.URL: url,
                        "頭文字": initial,
                    }

                    # 各社 HP を辿って基本情報を補完 (失敗しても record は yield)
                    if hp.startswith("http"):
                        try:
                            details = self._scrape_company(hp)
                        except Exception as e:  # noqa: BLE001 — 外部サイト依存のため握り潰して継続
                            self.logger.warning("HP 解析に失敗 (%s): %s", hp, e)
                            details = {}
                        for k, v in details.items():
                            if v and not record.get(k):
                                record[k] = v

                    yield record

    # ------------------------------------------------------------------
    # 各社 HP からの基本情報抽出
    # ------------------------------------------------------------------
    def _scrape_company(self, hp_url: str) -> dict:
        """HP トップから会社概要等を辿り、企業基本情報の辞書を返す。"""
        top = self.get_soup(hp_url)
        if top is None:
            return {}

        base_host = urlparse(hp_url).netloc.lower()

        # 辿るページを分類ごとに収集 (トップページ上のリンクから)
        buckets = {"profile": [], "access": [], "contact": [], "legal": []}
        seen = set()
        for a in top.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            full = urljoin(hp_url, href)
            if urlparse(full).netloc.lower() != base_host:
                continue  # 同一ドメインのみ
            key = full.split("#")[0]
            if key in seen or key == hp_url:
                continue

            haystack = (href + " " + _clean(a.get_text())).lower()
            if any(kw.lower() in haystack for kw in _PROFILE_KW):
                bucket = "profile"
            elif any(kw.lower() in haystack for kw in _ACCESS_KW):
                bucket = "access"
            elif any(kw.lower() in haystack for kw in _CONTACT_KW):
                bucket = "contact"
            elif any(kw.lower() in haystack for kw in _LEGAL_KW):
                bucket = "legal"
            else:
                continue
            seen.add(key)
            buckets[bucket].append(key)

        # 会社概要を最優先で走査。件数を上限で制限。
        candidates = (
            buckets["profile"] + buckets["access"]
            + buckets["contact"] + buckets["legal"]
        )[: self.MAX_DETAIL_PAGES]

        result: dict = {}
        # トップページ (フッター含む) より会社概要ページを優先するため candidates を先に走査
        for page_url in candidates:
            page = self.get_soup(page_url)
            if page is not None:
                self._extract_into(page, result)
        # 最後にトップページのフッター等で不足分を補完
        self._extract_into(top, result)
        return result

    def _extract_into(self, soup, result: dict):
        """1 ページからラベル→値ペアを抽出し、未取得の項目のみ result に格納する。"""
        pairs = self._label_value_pairs(soup)

        for label, value in pairs:
            lbl = label.lower()
            for key, keywords in _LABEL_MAP:
                if result.get(key):
                    continue
                if not any(kw in lbl for kw in keywords):
                    continue
                cleaned = self._normalize_field(key, value, soup)
                if cleaned:
                    result[key] = cleaned
                break  # このラベルは 1 項目に割り当て済み

        # --- 正規表現フォールバック (ラベルで拾えなかった郵便番号) ---
        if not result.get(Schema.POST_CODE):
            postal = _first_postal(soup.get_text(" ", strip=True))
            if postal:
                result[Schema.POST_CODE] = postal

    def _normalize_field(self, key: str, value: str, soup) -> str:
        """項目種別に応じて値を整形する。"""
        value = _clean(value)
        if not value:
            return ""

        if key == Schema.POST_CODE:
            return _first_postal(value)
        if key in (Schema.TEL, "FAX"):
            return _first_phone(value)
        if key == Schema.EMAIL:
            return _first_email(soup, value) or (value if "@" in value else "")
        if key == Schema.ADDR:
            # 住所内に含まれる郵便番号は POST_CODE 側で拾うため住所文字列からは除去
            return _clean(re.sub(r"〒\s*\d{3}[\-ー－\s]?\s*\d{4}", "", value))
        return value

    @staticmethod
    def _label_value_pairs(soup):
        """<table> と <dl> からラベル・値のペア一覧を抽出する。"""
        pairs = []

        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find_all("th")
                td = tr.find_all("td")
                if th and td:
                    label = _clean(th[0].get_text(" ", strip=True))
                    value = _clean(td[0].get_text(" ", strip=True))
                elif len(td) >= 2:
                    label = _clean(td[0].get_text(" ", strip=True))
                    value = _clean(td[1].get_text(" ", strip=True))
                else:
                    continue
                if label:
                    pairs.append((label, value))

        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = _clean(dt.get_text(" ", strip=True))
                value = _clean(dd.get_text(" ", strip=True))
                if label:
                    pairs.append((label, value))

        return pairs


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Jeita()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jeita.or.jp/cgi-bin/member/list.cgi")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ちとせの仕事 — 千歳市が運営するしごと情報発信サイトの掲載企業クローラー

対象サイト: https://www.chitose-shigoto.jp/article-company

取得対象:
    - 掲載企業 全社 (フィルタなし)。2026-11 時点で一覧 11 ページ / 129 社、
      company-sitemap.xml には 157 社 (一覧に出ない掲載期間外の企業も詳細ページは公開)

取得フロー:
    1. {url} → {url}/page/N (WordPress wp-pagenavi, 12社/ページ)。
       範囲外ページは 302 でトップへ飛ぶため、a.nextpostslink の有無で終了判定する
    2. 各カードの /article-company/{ID}.html を詳細取得 → 即 yield
    3. 一覧巡回後、{url} と同一ホストの /company-sitemap.xml から未取得の企業を補完

詳細ページ:
    - 企業名: .jumbotron h3 strong / 業種: .jumbotron h3 span.label (カンマ区切りで複数あり)
    - 会社概要: 最初の table.table の th/td
      (代表者名/所在地/電話番号/資本金/従業員数/設立年/事業内容/HPアドレス)
    - 以降の table は募集要項 (求人ごと) のため取得しない

備考:
    - 依頼時の備考では「全111ページ・約1,300社」とあったが、実際は 11 ページ・総数 157 社
      (WP REST /wp-json/wp/v2/company の X-WP-Total でも 157)。
    - 所在地は「〒066-0077 千歳市上長都1-12」のように都道府県を省略した表記が大半。
      登録事業者は千歳市内に事業所を持つことが条件 (規約第3条) のため、
      都道府県の記載が無い場合は「北海道」を補完する (道外の政令市名は対応県に補正)。
    - 所在地欄は「■本社 … ■千歳営業所 …」のように複数拠点・TEL が混在する自由記述が多い。
      郵便番号/住所は最初に現れる 1 拠点分を採り、欄の全文は EXTRA「所在地(原文)」に残す。
    - 一部企業 (8972〜8986 等) は会社概要が未入力で、企業名・業種以外は空になる。
    - 会社紹介・メッセージ・求める人物像等の自由記述は著作権リスクのため取得しない。
      事業内容は備考で明示指定されているため Schema.LOB に出力する。
    - 利用規約 (terms.html) は転載禁止 (第10条) の規定のみで、スクレイピング/クローリングの
      明示的な禁止条項はない。

実行方法:
    python scripts/sites/jobs/chitose_shigoto.py
"""

import re
import sys
import unicodedata
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_MAX_PAGES = 300  # 安全上限 (12社/ページ)

_DETAIL_RE = re.compile(r"/article-company/(\d+)\.html")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_ZIP_RE = re.compile(r"(\d{3}-\d{4})")
_PREF = r"(北海道|東京都|京都府|大阪府|[一-龥]{2,3}県)"
# 所在地欄から最初の住所片を抜き出す: [都道府県] 市区郡 + 区切り記号までの残り
_ADDR_RE = re.compile(r"(?:" + _PREF + r"\s*)?([一-龥ぁ-んァ-ヶ]{1,8}?[市郡区])([^■◆●【】〒（()）／/|：:<＜]*)")
# 住所片の末尾に続く連絡先・注記を切り落とす
_ADDR_TAIL_RE = re.compile(r"\s*(?:TEL|FAX|電話|本社|※).*$")
# 都道府県を省略しがちな道外の政令市
_CITY_PREF = {
    "大阪市": "大阪府", "名古屋市": "愛知県", "横浜市": "神奈川県", "川崎市": "神奈川県",
    "相模原市": "神奈川県", "さいたま市": "埼玉県", "千葉市": "千葉県", "仙台市": "宮城県",
    "神戸市": "兵庫県", "京都市": "京都府", "福岡市": "福岡県", "北九州市": "福岡県",
    "広島市": "広島県", "静岡市": "静岡県", "浜松市": "静岡県", "新潟市": "新潟県",
    "岡山市": "岡山県", "熊本市": "熊本県", "堺市": "大阪府",
}
_DATE_RE = re.compile(r"(\d{4})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")
_POS_RE = re.compile(
    r"^(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|"
    r"支社長|支店長|社長|会長|所長|工場長|店長|院長|園長|施設長|営業所長|総支配人|代表者?)[\s　:：]*"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(text)).strip()


def _norm(text: str) -> str:
    """全角英数・各種ハイフンを半角に寄せ、ハイフン前後の空白を詰める。"""
    text = unicodedata.normalize("NFKC", _clean(text))
    text = re.sub(r"[‐‑‒–—―−ー－](?=\s*\d)", "-", text)
    return re.sub(r"(\d)\s*-\s*(\d)", r"\1-\2", text)


def _split_address(raw: str) -> tuple[str, str, str]:
    """所在地欄 → (郵便番号, 都道府県, 住所)。

    複数拠点・連絡先が混在する自由記述のため、最初の郵便番号と
    最初に現れる住所片 (市区郡で始まり ■【〒（／ 等の区切りまで) を採用する。
    """
    text = _norm(raw)
    m = _ZIP_RE.search(text)
    zip_code = m.group(1) if m else ""

    m = _ADDR_RE.search(text)
    if not m:
        return zip_code, "", ""
    pref, city, rest = m.group(1) or "", m.group(2), m.group(3)
    rest = _ADDR_TAIL_RE.sub("", rest).strip()
    if not pref:
        pref = next((p for c, p in _CITY_PREF.items() if city.endswith(c)), "北海道")
    return zip_code, pref, _clean(city + rest)


def _split_rep(raw: str) -> tuple[str, str]:
    """'代表取締役　曙 嘉輝' → (役職, 氏名)"""
    text = _clean(raw)
    m = _POS_RE.match(text)
    if m and text[m.end():]:
        return m.group(1), _clean(text[m.end():])
    return "", text


def _normalize_date(raw: str) -> str:
    """'1952年9月26日' → '1952-09-26' / '2014年' → '2014'"""
    m = _DATE_RE.search(raw)
    if not m:
        return ""
    y, mo, d = m.groups()
    if mo and d:
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    if mo:
        return f"{y}-{int(mo):02d}"
    return y


class ChitoseShigotoScraper(StaticCrawler):
    """ちとせの仕事 掲載企業 スクレイパー"""

    DELAY = 3.0

    EXTRA_COLUMNS = [
        "企業ID",
        "設立年",
        "所在地(原文)",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        seen: set[str] = set()

        # 1) 一覧ページ巡回 (詳細取得ごとに即 yield)
        for page in range(1, _MAX_PAGES + 1):
            list_url = base if page == 1 else f"{base}/page/{page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            links = self._list_links(soup, url)
            new_links = [(cid, href) for cid, href in links if cid not in seen]
            if not new_links:
                break
            for cid, href in new_links:
                seen.add(cid)
                item = self._parse_detail(href, cid)
                if item:
                    yield item

            if not soup.select_one("a.nextpostslink"):
                break

        # 2) 一覧に出ない (掲載期間外) 企業を company-sitemap.xml から補完
        sitemap_url = urljoin(url, "/company-sitemap.xml")
        sm = self.get_soup(sitemap_url)
        if sm is None:
            return
        host = urlparse(url).netloc
        for loc in sm.find_all("loc"):
            href = _clean(loc.get_text())
            m = _DETAIL_RE.search(href)
            if not m or urlparse(href).netloc != host or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            item = self._parse_detail(href, m.group(1))
            if item:
                yield item

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    @staticmethod
    def _list_links(soup: bs4.BeautifulSoup, url: str) -> list[tuple[str, str]]:
        """一覧カードから (企業ID, 詳細URL) を出現順・重複なしで返す。"""
        links: list[tuple[str, str]] = []
        found: set[str] = set()
        for a in soup.select("a[href*='/article-company/']"):
            m = _DETAIL_RE.search(a["href"])
            if not m or m.group(1) in found:
                continue
            found.add(m.group(1))
            links.append((m.group(1), urljoin(url, a["href"])))
        return links

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, company_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        head = soup.select_one(".jumbotron h3")
        name = ""
        category = ""
        if head is not None:
            strong = head.find("strong")
            name = _clean(strong.get_text()) if strong else ""
            label = head.select_one("span.label")
            category = _clean(label.get_text()) if label else ""
        if not name:
            h2 = soup.select_one("h2.h3")
            name = _clean(h2.get_text()) if h2 else ""

        fields: dict[str, bs4.Tag] = {}
        table = self._overview_table(soup)
        if table is not None:
            for tr in table.find_all("tr"):
                th, td = tr.find("th"), tr.find("td")
                if th and td:
                    fields.setdefault(_clean(th.get_text()), td)

        def val(label: str) -> str:
            td = fields.get(label)
            return _clean(td.get_text(" ")) if td is not None else ""

        addr_raw = val("所在地")
        zip_code, pref, addr = _split_address(addr_raw)
        pos, rep = _split_rep(val("代表者名"))

        # 電話番号欄 → 無ければ所在地欄の TEL 記載で補完 (「-」等のみの欄あり)
        tel_m = _TEL_RE.search(_norm(val("電話番号"))) or _TEL_RE.search(_norm(addr_raw))

        hp = ""
        hp_td = fields.get("HPアドレス")
        if hp_td is not None:
            a = hp_td.find("a", href=True)
            hp = a["href"].strip() if a else val("HPアドレス")

        founded = val("設立年")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.CAT_SITE: category,
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.POST_CODE: zip_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel_m.group(0) if tel_m else "",
            Schema.HP: hp,
            Schema.CAP: val("資本金"),
            Schema.EMP_NUM: val("従業員数"),
            Schema.OPEN_DATE: _normalize_date(founded),
            Schema.LOB: val("事業内容"),
            "企業ID": company_id,
            "設立年": founded,
            "所在地(原文)": addr_raw,
        }

    @staticmethod
    def _overview_table(soup: bs4.BeautifulSoup) -> bs4.Tag | None:
        """会社概要テーブル (「代表者名」「所在地」等を持つ table) を返す。"""
        for table in soup.select("table.table"):
            ths = {_clean(th.get_text()) for th in table.find_all("th")}
            if ths & {"代表者名", "所在地", "資本金", "設立年"}:
                return table
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ChitoseShigotoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.chitose-shigoto.jp/article-company")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
イージア掲載事業者 (easier.jp) — サングローブの集客支援サービス「Easier」掲載事業者一覧

取得対象:
    - easier.jp に PR ページ / コンテンツページを掲載している事業者 (約866社)
    - 事業者名・代表者名・郵便番号・都道府県・住所・TEL・公式サイトURL・掲載サービス名
    - 1事業者1行 (8文字英数字の事業者IDでユニーク化)

取得フロー:
    sitemap-index.xml → 3本のサブ sitemap → 各 <loc> の第1パス要素 (8文字ID) で
    事業者単位にグルーピングする (サイトにディレクトリ一覧ページが存在しないため)。
    事業者ごとに 掲載ページを1〜4件だけ取得し、
      1) og:site_name / <title> から 事業者表示名・掲載サービス名
      2) 「特定商取引法に基づく表記」ブロック (会社名/代表者/所在地/電話番号 等) から会社情報
    を抽出して即 yield する。特商法ページは footer リンク → 同一IDの /content/ ページの
    順にフォールバックする。
    記事本文のプロース (サービス紹介の長文) は著作権配慮のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/easier.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id easier
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_RE = re.compile("(" + "|".join(_PREFS) + ")")
_POST_RE = re.compile(r"(\d{3})\s*[-‐−ー―]\s*(\d{4})")
_TEL_RE = re.compile(r"(0\d{1,4}[-‐−ー\(\)\s]?\d{1,4}[-‐−ー\)\s]?\d{3,4})")
_MAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_URL_RE = re.compile(r"https?://[^\s\"'<>）\)]+")

# 事業者ID = URL 第1パス要素の8文字英数字
_ID_RE = re.compile(r"^https?://easier\.jp/([0-9A-Za-z_-]{8})(?:/|$)")

# 404 ページはステータス200で返るため本文で判定する
_NOT_FOUND_TEXT = "お探しのページは見つかりませんでした"

# SNS ドメイン → Schema カラム
_SNS_MAP = [
    ("instagram.com", Schema.INSTA),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("facebook.com", Schema.FB),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("tiktok.com", Schema.TIKTOK),
]
# 公式サイト候補から除外するドメイン
_NOT_HP = (
    "easier.jp", "cloudfront.net", "google.com", "google.co.jp", "gstatic.com",
    "googleapis.com", "youtube.com", "youtu.be", "sungrove.co.jp",
    "sungrovetech.vn", "apple.com", "instagram.com", "facebook.com",
    "twitter.com", "x.com", "tiktok.com", "line.me", "lin.ee", "note.com",
)

# 「特定商取引法に基づく表記」で使われるラベル (表記揺れを吸収)
_LABEL_RES = [
    ("post", re.compile(r"^郵便番号$")),
    ("hp", re.compile(r"(ホームページ|ホームぺージ|公式サイト|公式HP|公式ＨＰ|WEBサイト|Webサイト|ウェブサイト|サイトURL|URL)")),
    ("mail", re.compile(r"(メールアドレス|メール|E-?mail|e-?mail)")),
    ("tel", re.compile(r"^(電話番号|お電話番号|電話|TEL|Tel|tel|ＴＥＬ|連絡先電話番号|連絡先)$")),
    ("rep", re.compile(r"^(代表者名?|代表|代表取締役|代表責任者名?|運営統括責任者名?|統括責任者名?|運営責任者名?|責任者名?|業務責任者|販売責任者)$")),
    ("addr", re.compile(r"^(所在地|住所|本社所在地|本社住所|事業所所在地|所在地住所)$")),
    ("lob", re.compile(r"^(事業内容|業種|業務内容|サービス内容|取扱商品|取扱サービス)$")),
    ("hours", re.compile(r"^(営業時間|受付時間|受付営業時間|営業日時)$")),
    ("name", re.compile(r"^(?:販売|運営|提供|サービス提供)?(?:事業者|事業所|会社|法人|商号|屋号|店舗|販売者|販売業者|運営会社|運営者|店名)(?:名|名称)?$")),
]


def _norm_label(text: str) -> str:
    """行頭の装飾記号・末尾のコロンを落としてラベル名を正規化する。"""
    s = (text or "").replace("　", " ").replace("\xa0", " ").strip()
    s = re.sub(r"^[■●◆▼★・\*\-\s【\[（\(]+", "", s)
    s = re.sub(r"[】\]）\)\s]*[：:]*\s*$", "", s)
    return s.strip()


def _label_of(text: str) -> str | None:
    """行がラベルならフィールドキーを、そうでなければ None を返す。"""
    nl = _norm_label(text)
    if not nl or len(nl) > 20:
        return None
    for key, rx in _LABEL_RES:
        if rx.search(nl):
            return key
    return None


def _parse_kv_lines(lines: list[str]) -> list[tuple[str, str]]:
    """ラベル行 / 値行の並びから (キー, 値) を抽出する。

    「ラベルがn連続 → 値がn連続」の2カラムレイアウト
    (例: 電話番号 / メールアドレス / 03-.... / info@...) にも対応する。
    """
    seq = [(_label_of(line), line.strip()) for line in lines if line and line.strip()]
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(seq):
        if seq[i][0] is None:
            i += 1
            continue
        keys: list[str] = []
        while i < len(seq) and seq[i][0] is not None:
            keys.append(seq[i][0])
            i += 1
        values: list[str] = []
        limit = max(4, len(keys) + 1)
        while i < len(seq) and seq[i][0] is None and len(values) < limit:
            values.append(seq[i][1])
            i += 1
        if not values:
            continue
        if len(keys) >= 2:
            # 2カラムレイアウト: ラベル群と値群を順に対応付ける
            for key, value in zip(keys, values):
                out.append((key, value))
        else:
            # 単一ラベル: 続く数行を値として結合 (郵便番号+住所の改行分割に対応)
            out.append((keys[0], "\n".join(values[:4])))
    return out


class Easier(StaticCrawler):
    """イージア掲載事業者 (easier.jp) スクレイパー"""

    DELAY = 1.0
    # 1事業者あたりに取得するページ数の上限 (会社情報が揃うまでフォールバック)
    MAX_PAGES_PER_COMPANY = 4
    # サイト固有列。記事本文などのプロース列は著作権配慮のため含めない
    EXTRA_COLUMNS = ["事業者ID", "サイト表示名", "掲載ページ数", "特定商取引法ページURL"]

    # ------------------------------------------------------------------ 列挙
    def _sitemap_locs(self, sitemap_url: str) -> list[str]:
        """sitemap (index / urlset) の <loc> を返す。"""
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []
        return [loc.get_text(strip=True) for loc in soup.find_all("loc") if loc.get_text(strip=True)]

    def _collect_companies(self, url: str) -> dict[str, list[str]]:
        """sitemap-index から 事業者ID → 掲載URL一覧 の辞書を組み立てる。"""
        companies: dict[str, list[str]] = {}
        sub_sitemaps = [loc for loc in self._sitemap_locs(url) if loc.endswith(".xml")]
        if not sub_sitemaps:
            # 引数 url が urlset そのものだった場合のフォールバック
            sub_sitemaps = [url]
        for sub in sub_sitemaps:
            for loc in self._sitemap_locs(sub):
                m = _ID_RE.match(loc)
                if m:
                    companies.setdefault(m.group(1), []).append(loc)
        return companies

    # ------------------------------------------------------------------ 取得
    def _get_page(self, page_url: str) -> bs4.BeautifulSoup | None:
        """掲載ページを取得する。ステータス200で返る404ページは None 扱いにする。"""
        soup = self.get_soup(page_url)
        if soup is None:
            return None
        body = soup.body
        if body is not None and _NOT_FOUND_TEXT in body.get_text(" ", strip=True):
            return None
        return soup

    def _collect_kv(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        """DOM の title/desc ペア・table・本文テキストの3経路から会社情報を集約する。"""
        got: dict[str, str] = {}

        def put(key: str, value: str) -> None:
            value = (value or "").strip()
            if value and key not in got:
                got[key] = value

        # (1) 特定商取引法モジュール等の __title / __desc ペア
        for item in soup.select('[class*="__item"]'):
            title = item.select_one('[class*="__title"]')
            desc = item.select_one('[class*="__desc"]')
            if title is not None and desc is not None:
                key = _label_of(title.get_text(strip=True))
                if key:
                    put(key, desc.get_text("\n", strip=True))

        # (2) 表形式
        for tr in soup.select("table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th is not None and td is not None:
                key = _label_of(th.get_text(strip=True))
                if key:
                    put(key, td.get_text("\n", strip=True))

        # (3) <br> 区切りのフリーテキストブロック (最も多いパターン)
        for block in soup.select("p, li, dd, td, div"):
            if block.find(["p", "li", "dd", "table", "div"]) is not None:
                continue
            text = block.get_text("\n", strip=True)
            if not text or "\n" not in text:
                continue
            for key, value in _parse_kv_lines(text.split("\n")):
                put(key, value)

        # (4) ページ全体の行列で 2カラムレイアウトを救済 (footer の 事業所名 等も含む)
        if soup.body is not None:
            lines = soup.body.get_text("\n").split("\n")
            for key, value in _parse_kv_lines(lines):
                put(key, value)

        return got

    # -------------------------------------------------------------- 整形処理
    @staticmethod
    def _first_line(value: str, max_len: int) -> str:
        line = (value or "").split("\n")[0].strip()
        line = re.sub(r"[\s\xa0]+", " ", line)
        if not line or len(line) > max_len:
            return ""
        return line

    @staticmethod
    def _clean_name(value: str) -> str:
        line = Easier._first_line(value, 80)
        # 「株式会社○○ 御中」等の余計な語尾は付かないが、前後の記号のみ除去
        return line.strip("　 ・:：")

    @staticmethod
    def _clean_rep(value: str) -> str:
        line = Easier._first_line(value, 24)
        # 見出し・キャッチコピーの誤検出を弾く (氏名に数字/装飾記号は入らない)
        if not line or re.search(r"[。！？「」【】0-9０-９●■◆★]", line):
            return ""
        if re.match(r"^[はがのをにでとも、]", line):
            return ""
        return line

    @staticmethod
    def _clean_tel(value: str) -> str:
        m = _TEL_RE.search(Easier._first_line(value, 60) or (value or "").split("\n")[0])
        return m.group(1).strip() if m else ""

    @staticmethod
    def _clean_url(value: str) -> str:
        for m in _URL_RE.finditer(value or ""):
            candidate = m.group(0).rstrip("/。、,")
            if not any(dom in candidate for dom in _NOT_HP):
                return candidate
        return ""

    @staticmethod
    def _clean_mail(value: str) -> str:
        m = _MAIL_RE.search(value or "")
        return m.group(0) if m else ""

    @staticmethod
    def _clean_lob(value: str) -> str:
        """事業内容。長文プロースは著作権配慮のため採用しない。"""
        text = re.sub(r"\n+", " / ", (value or "").strip())
        text = re.sub(r"[\s\xa0]+", " ", text).strip()
        # 4文字未満は見出しの断片、150文字超は長文プロースとみなして採用しない
        if len(text) < 4 or len(text) > 150:
            return ""
        if re.match(r"^[はがのをにでとも、]", text):
            return ""
        return text

    @staticmethod
    def _split_address(value: str) -> tuple[str, str, str]:
        """所在地の値から (郵便番号, 都道府県, 住所) を切り出す。"""
        raw = re.sub(r"[\xa0]+", " ", value or "")
        post = ""
        m = _POST_RE.search(raw)
        if m:
            post = f"{m.group(1)}-{m.group(2)}"
        pm = _PREF_RE.search(raw)
        if pm is None:
            return post, "", ""
        pref = pm.group(1)
        rest = raw[pm.start():]
        # 住所は最大2行 (建物名が改行される場合がある)。文章行が続く場合は打ち切る
        lines: list[str] = []
        for line in rest.split("\n"):
            line = re.sub(r"[\s]+", " ", line).strip()
            if not line:
                continue
            if lines and (len(line) > 40 or re.search(r"[。！？]", line)):
                break
            lines.append(line)
            if len(lines) >= 2:
                break
        addr = " ".join(lines).strip()
        if len(addr) > 120:
            addr = addr[:120]
        return post, pref, addr

    @staticmethod
    def _official_link(soup: bs4.BeautifulSoup) -> str:
        """公式サイトURL。footer/header の設定リンク、または「公式サイト」等の
        アンカーテキストを持つ外部リンクに限定する (本文中の外部リンク誤検出を避ける)。"""
        anchors = list(soup.select("footer a[href], header a[href], #page-footer a[href], #page-header a[href]"))
        anchors += [
            a for a in soup.select("a[href]")
            if re.search(r"(公式サイト|公式HP|公式ＨＰ|ホームページ|運用会社サイト|運営会社サイト|会社サイト|コーポレートサイト)", a.get_text(strip=True) or "")
        ]
        for a in anchors:
            href = (a.get("href") or "").strip()
            if href.startswith("http") and not any(dom in href for dom in _NOT_HP):
                return href.rstrip("/")
        return ""

    # ------------------------------------------------------------------ 本体
    def _build_item(self, company_id: str, page_urls: list[str]) -> dict | None:
        """1事業者分の行を組み立てる (掲載ページを最大 MAX_PAGES_PER_COMPANY 件取得)。"""
        # トップページ → /pr/ → /content/ の順で当たる
        ordered = sorted(page_urls, key=lambda u: (u.count("/") != 3, "/content/" in u))

        soup = None
        first_url = ""
        fetched = 0
        for candidate in ordered:
            soup = self._get_page(candidate)
            fetched += 1
            if soup is not None:
                first_url = candidate
                break
            if fetched >= self.MAX_PAGES_PER_COMPANY:
                break
        if soup is None:
            return None

        title = soup.title.get_text(strip=True) if soup.title else ""
        og = soup.find("meta", property="og:site_name")
        display_name = (og.get("content") or "").strip() if og else ""
        if not display_name and " - " in title:
            display_name = title.rsplit(" - ", 1)[-1].strip()
        # 掲載サービス名 (ページ見出し部分) をサイト定義業種として扱う
        service = title.rsplit(" - ", 1)[0].strip() if " - " in title else ""
        if service in ("TOP", "top", display_name):
            service = ""

        sns: dict[str, str] = {}
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("http"):
                continue
            for domain, column in _SNS_MAP:
                if domain in href:
                    sns.setdefault(column, href)
                    break
        hp_link = self._official_link(soup)

        # 「特定商取引法に基づく表記」ページ
        tokusho_url = ""
        for a in soup.select("a[href]"):
            text = a.get_text(strip=True)
            if "特定商取引" in text or "特商法" in text:
                candidate = urljoin(first_url, a.get("href") or "")
                if candidate.rstrip("/") != first_url.rstrip("/"):
                    tokusho_url = candidate
                break

        kv = self._collect_kv(soup)

        fallbacks = ([tokusho_url] if tokusho_url else []) + [
            u for u in ordered if u != first_url and u != tokusho_url and "/content/" in u
        ]
        used_tokusho = ""
        for candidate in fallbacks:
            if kv.get("name") and kv.get("addr"):
                break
            if fetched >= self.MAX_PAGES_PER_COMPANY:
                break
            sub = self._get_page(candidate)
            fetched += 1
            if sub is None:
                continue
            for key, value in self._collect_kv(sub).items():
                kv.setdefault(key, value)
            if not hp_link:
                hp_link = self._official_link(sub)
            if kv.get("name") or kv.get("addr"):
                used_tokusho = candidate

        post, pref, addr = self._split_address(kv.get("addr", ""))
        if not post:
            post_only = _POST_RE.search(kv.get("post", ""))
            if post_only:
                post = f"{post_only.group(1)}-{post_only.group(2)}"

        name = self._clean_name(kv.get("name", "")) or display_name
        if not name:
            return None

        item = {
            Schema.URL: first_url,
            Schema.NAME: name,
            Schema.REP_NM: self._clean_rep(kv.get("rep", "")),
            Schema.POST_CODE: post,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: self._clean_tel(kv.get("tel", "")),
            Schema.HP: self._clean_url(kv.get("hp", "")) or hp_link,
            Schema.LOB: self._clean_lob(kv.get("lob", "")),
            Schema.CAT_SITE: service,
            Schema.TIME: self._first_line(kv.get("hours", ""), 60),
            Schema.INSTA: sns.get(Schema.INSTA, ""),
            Schema.LINE: sns.get(Schema.LINE, ""),
            Schema.X: sns.get(Schema.X, ""),
            Schema.FB: sns.get(Schema.FB, ""),
            Schema.TIKTOK: sns.get(Schema.TIKTOK, ""),
            "事業者ID": company_id,
            "サイト表示名": display_name,
            Schema.EMAIL: self._clean_mail(kv.get("mail", "")),
            "掲載ページ数": str(len(page_urls)),
            "特定商取引法ページURL": used_tokusho or tokusho_url,
        }
        return item

    def parse(self, url: str) -> Generator[dict, None, None]:
        companies = self._collect_companies(url)
        self.total_items = len(companies)
        self.logger.info("事業者ID: %d 件 (sitemap 総URL数から集約)", len(companies))

        for company_id, page_urls in companies.items():
            try:
                item = self._build_item(company_id, page_urls)
            except Exception as e:  # noqa: BLE001 — 1社の失敗で全体を止めない
                self.logger.warning("事業者 %s の取得に失敗: %s", company_id, e)
                continue
            if item:
                yield item


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Easier()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://easier.jp/sitemap-index.xml")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

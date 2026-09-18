"""
株式会社サングローブ 制作実績 (sanguro) — 制作サイト (顧客企業) の悉皆列挙クローラー

取得対象:
    - https://www.sungrove.co.jp/works/ (全 31 ページ / 1 ページ 5 件・約 155 件)
      に掲載されている顧客サイトの「社名 (会社名/店名/店舗名)」と「サイト URL」

取得フロー (Pattern B: 1 件取得ごとに即 yield):
      1. 引数 url の一覧ページを 1 ページずつ取得 (2 ページ目以降は `<url>page/N/`)
      2. 各カード (li.c-results__list-item) から社名・URL・業種・契約形態・カラーを抽出
      3. その顧客サイトのトップページを 1 リクエストだけ取得し、
         サングローブ制作サイトのコードフィンガープリント (専用プラグイン /
         テーマパス) と「スクール表記」を照合して付与する
      4. 1 件ずつ即 yield する

フィンガープリント (2026-09 実測・依頼元指定):
    - /wp-content/plugins/sungrove/            … 専用プラグイン (最も決定的)
    - /wp-content/themes/sungrove_*            … 社名入りテーマ
    - /wp-content/themes/sg<数字3桁>           … 新系統テーマ (sg071, sg004m 等)
    - /wp-content/themes/<数字3〜5桁 or ec###> … 旧系統テーマ。単独では汎用的すぎるため
      同一テーマ配下の共通アセット (css/responce.css ※ "responce" の綴り誤りが特徴的 /
      js/css3-mediaqueries.js / js/html5.js / js/script.js) との複合条件で判定する。
    ※ fonts.googleapis.com・fontawesome・addtoany・jQuery CDN・wp-pagenavi は
      他社サイトにも広く存在するため指紋に使わない。

規約・robots:
    - 利用規約/プライバシーポリシーにスクレイピング・自動収集・クローリングを
      禁止する記載は無い (2026-09 確認)。
    - robots.txt は User-agent:* に対し Disallow:/wp-admin のみ。
      ただし Scrapy / MJ12bot の UA は名指しで Disallow されているため、
      Scrapy 系 UA は使わず、UA を明示し 1 リクエスト/秒・単一接続で巡回する
      (DELAY = 1.0 / requests.Session の単一コネクション)。

取得できない項目:
    - 制作実績一覧には TEL・住所・代表者名が一切掲載されていない。
      これらは後工程で「HP (URL) のホスト一致」を第一キーに STX と名寄せして付与する
      (顧客サイト側に tel: リンク・〒住所がある場合のみ補助的に取得している)。
    - 顧客サイトの紹介文 (p.c-post-works__desc) は長文の自由記述プロースのため
      著作権リスク回避として取得しない。
    - 自社 CMS「イージア」(easier.jp) のテナントサイトは本工程の対象外 (依頼元指定)。

実行方法:
    python scripts/sites/corporate/sanguro.py
    python bin/run_flow.py --site-id sanguro
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- フィンガープリント ---------------------------------------------------- #
_FP_PLUGIN = "/wp-content/plugins/sungrove/"
_FP_THEME_NAMED = re.compile(r"/wp-content/themes/(sungrove_[A-Za-z0-9_\-]+)")
_FP_THEME_SG = re.compile(r"/wp-content/themes/(sg[0-9]{3}[a-z]?)", re.I)
_FP_THEME_NUM = re.compile(r"/wp-content/themes/((?:ec)?[0-9]{3,5})/")
# 旧系統 (数字のみテーマ) の複合条件となる共通アセット
_FP_COMMON_ASSET = re.compile(
    r"/wp-content/themes/(?:ec)?[0-9]{3,5}/"
    r"(?:css/responce\.css|js/css3-mediaqueries\.js|js/html5\.js|js/script\.js)"
)
_THEME_ANY = re.compile(r"/wp-content/themes/([A-Za-z0-9_\-]+)")
_SCHOOL_RE = re.compile(r"(スクール|ｽｸｰﾙ|school|アカデミー|養成|認定校|講座)", re.I)

# --- 顧客サイトからの補助抽出 (名寄せキー) -------------------------------- #
_TEL_HREF_RE = re.compile(r"tel:\+?([0-9\-\(\)\s]{9,20})")
_TEL_NORM_RE = re.compile(r"^(0\d{1,4})(\d{1,4})(\d{4})$")
_POST_ADDR_RE = re.compile(
    r"〒?\s*(\d{3}[-－]?\d{4})[\s　]*"
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)([^\s<>　]{2,40})"
)
# 名称ラベル (ページによって「会社名」「店名」「店舗名」「作品名」と揺れる)
_NAME_LABELS = ("会社名", "店名", "店舗名", "法人名", "サイト名", "作品名")
# 対象外ドメイン (自社 CMS イージアのテナント / 自社サイト)
_EXCLUDE_HOSTS = ("easier.jp", "sungrove.co.jp")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _norm_tel(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if not digits.startswith("0"):
        if digits.startswith("81"):
            digits = "0" + digits[2:]
        else:
            return ""
    if not (9 <= len(digits) <= 11):
        return ""
    if len(digits) == 11:                       # 携帯/フリーダイヤル系
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if digits.startswith(("03", "06")):
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def _host(url: str) -> str:
    try:
        netloc = urlsplit(url).netloc.lower().split(":")[0]
        return netloc[4:] if netloc.startswith("www.") else netloc
    except ValueError:
        return ""


class SanguroScraper(StaticCrawler):
    """サングローブ 制作実績 + 制作サイト指紋逆引きスクレイパー"""

    # robots.txt の指定は無いが、依頼元方針に合わせ 1 リクエスト/秒で巡回する
    DELAY = 1.0
    # 顧客サイト取得のタイムアウト (閉鎖済みドメインで長く待たない)
    CLIENT_TIMEOUT = 12
    MAX_PAGES = 60          # ページネーション暴走防止 (実測 31 ページ)

    EXTRA_COLUMNS = [
        "掲載社名",        # 一覧の「会社名/店名/店舗名」欄の表記
        "契約形態",        # フルスクラッチ / CMS など
        "サイトカラー",    # ベージュ / ブルー など (works_color タクソノミ)
        "ヒットテーマ",    # /wp-content/themes/<name>
        "スクール表記",    # スクール・講座等の表記の有無 (ヒットしたキーワード)
        "指紋種別",        # plugin:sungrove / theme:sungrove_* / theme:sg### / theme:旧系統
        "指紋判定",        # ヒット / 非ヒット / 取得失敗
    ]

    # ------------------------------------------------------------------ #
    # URL 導出 (引数 url を唯一のルートとする / SSOT = sites.yml の url)
    # ------------------------------------------------------------------ #
    def _page_url(self, url: str, page: int) -> str:
        base = url if url.endswith("/") else url + "/"
        return base if page <= 1 else f"{base}page/{page}/"

    # ------------------------------------------------------------------ #
    # 顧客サイトの指紋照合 (1 ドメイン 1 リクエスト)
    # ------------------------------------------------------------------ #
    def _fetch_client_html(self, hp: str) -> str:
        """顧客サイトのトップページ HTML を取得する。失敗時は空文字。"""
        try:
            resp = self.session.get(hp, timeout=self.CLIENT_TIMEOUT, allow_redirects=True)
            if resp.status_code >= 400:
                self.logger.warning("顧客サイト取得失敗 (%s): HTTP %s", hp, resp.status_code)
                return ""
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return resp.text or ""
        except Exception as e:        # 閉鎖ドメイン・SSL エラー等は欠測扱いで継続
            self.logger.warning("顧客サイト取得失敗 (%s): %s", hp, e)
            return ""

    def _fingerprint(self, html: str) -> dict:
        """サングローブ制作サイトの指紋を照合する。"""
        kinds, theme = [], ""

        if _FP_PLUGIN in html:
            kinds.append("plugin:sungrove")

        m = _FP_THEME_NAMED.search(html)
        if m:
            theme = theme or m.group(1)
            kinds.append("theme:sungrove_*")

        m = _FP_THEME_SG.search(html)
        if m:
            theme = theme or m.group(1)
            kinds.append("theme:sg###")

        m = _FP_THEME_NUM.search(html)
        if m:
            theme = theme or m.group(1)
            # 数字のみテーマは単独では汎用的すぎるため共通アセットとの複合条件にする
            if _FP_COMMON_ASSET.search(html):
                kinds.append("theme:旧系統(数字)+共通アセット")

        if not theme:
            m = _THEME_ANY.search(html)
            theme = m.group(1) if m else ""

        school = ""
        ms = _SCHOOL_RE.search(html)
        if ms:
            school = ms.group(1)

        return {
            "ヒットテーマ": theme,
            "スクール表記": school,
            "指紋種別": " / ".join(kinds),
            "指紋判定": "ヒット" if kinds else "非ヒット",
        }

    def _extract_contact(self, html: str) -> dict:
        """顧客サイトから名寄せ補助情報 (TEL / 郵便番号 / 都道府県 / 住所) を抽出する。"""
        tel = ""
        for m in _TEL_HREF_RE.finditer(html):
            tel = _norm_tel(m.group(1))
            if tel:
                break
        post = pref = addr = ""
        m = _POST_ADDR_RE.search(re.sub(r"<[^>]+>", " ", html))
        if m:
            p = re.sub(r"[-－]", "", m.group(1))
            post = f"{p[:3]}-{p[3:]}"
            pref = m.group(2)
            addr = _clean(m.group(3))
        return {
            Schema.TEL: tel,
            Schema.POST_CODE: post,
            Schema.PREF: pref,
            Schema.ADDR: addr,
        }

    # ------------------------------------------------------------------ #
    # 一覧カードのパース
    # ------------------------------------------------------------------ #
    def _parse_card(self, card, page_url: str) -> dict | None:
        ttl = card.select_one(".c-post-works__ttl")
        title = _clean(ttl.get_text()) if ttl else ""

        # dl.c-post-works__meta の dt ラベル → dd 値 (ラベルはページごとに揺れる)
        meta, hp = {}, ""
        for item in card.select(".c-post-works__meta-item"):
            dt = item.select_one(".c-post-works__meta-ttl")
            dd = item.select_one(".c-post-works__meta-desc")
            if not dt or not dd:
                continue
            label = _clean(dt.get_text())
            a = dd.select_one("a[href]")
            if label == "URL":
                hp = _clean(a["href"]) if a else _clean(dd.get_text())
            else:
                meta[label] = _clean(a.get_text() if a else dd.get_text())

        listed_name = ""
        for label in _NAME_LABELS:
            if meta.get(label):
                listed_name = meta[label]
                break
        name = listed_name or re.sub(r"\s*様$", "", title)
        if not name and not hp:
            return None

        # タクソノミ (業種 / 契約形態 / カラー) をリンク先パスで振り分ける
        industry, contract, colors = [], [], []
        for a in card.select(".c-post-works__term-link[href]"):
            href = a.get("href", "")
            txt = _clean(a.get_text())
            if not txt:
                continue
            if "/works_industry/" in href:
                industry.append(txt)
            elif "/works_contract/" in href:
                contract.append(txt)
            elif "/works_color/" in href:
                colors.append(txt)

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.HP: hp,
            Schema.CAT_SITE: " / ".join(industry),
            "掲載社名": listed_name,
            "契約形態": " / ".join(contract),
            "サイトカラー": " / ".join(colors),
        }

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        seen_hosts: set[str] = set()
        seen_names: set[str] = set()

        for page in range(1, self.MAX_PAGES + 1):
            page_url = self._page_url(url, page)
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", page_url)
                break

            cards = soup.select("li.c-results__list-item")
            if not cards:
                self.logger.info("カードが無いため終了: %s", page_url)
                break

            # 総件数の目安 (ページネーション最終ページ × 1ページ件数) を初回のみ設定
            if page == 1 and self.total_items is None:
                pages = [
                    int(a.get_text(strip=True))
                    for a in soup.select(".c-pagination a.page-numbers")
                    if a.get_text(strip=True).isdigit()
                ]
                if pages:
                    self.total_items = max(pages) * len(cards)

            for card in cards:
                row = self._parse_card(card, page_url)
                if not row:
                    continue

                hp = row[Schema.HP]
                host = _host(hp)
                # 自社 CMS イージアのテナント / 自社サイトは対象外 (依頼元指定)
                if host and any(host == x or host.endswith("." + x) for x in _EXCLUDE_HOSTS):
                    continue
                # 登録可能ドメイン単位で重複除去 (URL 無しは名称で判定)
                key = host or row[Schema.NAME]
                if host:
                    if host in seen_hosts:
                        continue
                    seen_hosts.add(host)
                else:
                    if key in seen_names:
                        continue
                    seen_names.add(key)

                fp = {"ヒットテーマ": "", "スクール表記": "", "指紋種別": "", "指紋判定": "取得失敗"}
                contact = {Schema.TEL: "", Schema.POST_CODE: "", Schema.PREF: "", Schema.ADDR: ""}
                if hp.startswith("http"):
                    html = self._fetch_client_html(hp)
                    if html:
                        fp = self._fingerprint(html)
                        contact = self._extract_contact(html)
                row.update(contact)
                row.update(fp)
                yield row
        else:
            self.logger.warning("MAX_PAGES (%s) に到達したため打ち切りました", self.MAX_PAGES)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SanguroScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.sungrove.co.jp/works/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

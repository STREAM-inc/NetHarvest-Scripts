"""
日本フランチャイズチェーン協会（JFA）正会員 — 各正会員企業サイトの基本情報スクレイパー

取得対象:
    - JFA 正会員一覧 (https://www.jfa-fc.or.jp/particle/38.html) に掲載された
      正会員企業について、一覧に載る 企業名 / 企業名(英語) / ホームページURL /
      チェーン名 / 業種(サイト定義業種) に加え、各社の企業サイトへアクセスして
      取得する 会社名(正式名称) / 代表者名 / 郵便番号 / 都道府県 / 住所 /
      電話番号 / FAX番号 / メールアドレス / 設立年月 / 資本金 / 事業内容。

取得フロー:
    - 起点(url)は単一の会員一覧ページ (ページネーション無し)。五十音「あ」〜「わ」
      行ごとのネストしたテーブルに正会員が掲載されている。
    - 各正会員行は「企業名セル(HPリンク付き) + チェーン名・営業内容セル」の2セル構成。
    - 準会員・賛助会員は企業名にHPリンクが無い（＝リンク付き行は正会員のみ）。
      さらに JFA 自ドメインのナビ/サイドバーリンクを除外することで、
      正会員のみを確実に抽出する。
    - 抽出した各社 HP (Schema.HP) へアクセスし、会社概要 / 会社案内 / お問い合わせ /
      アクセス / 特定商取引法に基づく表記 / フッター 等から基本情報を取得する。
      情報が複数ページに分散している場合は、会社概要系リンクを優先的に数ページ辿る。

備考(方針):
    - HP に明記されている情報のみを取得し、推測・補完はしない。見つからない項目は空欄。
    - 各社サイトは個社ごとに構造が全く異なり固定セレクタでは汎用取得できないため、
      table(th/td・td/td) / dl(dt/dd) / 「ラベル：値」テキスト行のラベル駆動抽出と、
      本文全体からの正規表現抽出を併用する。取得できない項目は空欄のまま yield する。
    - 外部サイトはダウン/WAF/SSL等で取得失敗しうるため、失敗時も一覧情報のみで
      継続する (CONTINUE_ON_ERROR=True, get_soup は None を返す)。

実行方法:
    # ローカルテスト
    python scripts/sites/agency_franchise/jfa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jfa
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_HTTP = re.compile(r"^https?://", re.I)

# 47 都道府県 (住所抽出用)
_PREF = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)
# 都道府県 + 市区郡(必須) + 以降 (改行/記号/全角空白で打ち切り)。
# 市区郡トークンを必須にすることで、商品説明等に紛れた都道府県名の誤検出を防ぐ。
_ADDR_STOP = r"[^\s　、。｜|<>【】（）()\n\r]"
_ADDR_RE = re.compile(rf"({_PREF})({_ADDR_STOP}{{0,10}}?[市区郡]{_ADDR_STOP}{{1,40}})")
# 実住所らしさの担保: 番地に相当する数字/丁目/番を含むこと。
_ADDR_HAS_NUM = re.compile(r"[0-9０-９]|丁目|番地|番")
# 郵便番号: 〒マーカー付き (誤検出しにくい)。マーカー無しは住所直前に限り採用。
_POST_MARK_RE = re.compile(r"〒\s*(\d{3})[\-‐ー－−–]?\s?(\d{4})(?!\d)")
_POST_BARE_RE = re.compile(r"(\d{3})[\-‐ー－−–]\s?(\d{4})(?!\d)")
# 電話/FAX番号 (市外局番 0始まり)
_TEL_RE = re.compile(r"(0\d{1,3})[\-‐ー－(（]\s?(\d{1,4})[\-‐ー－)）]\s?(\d{3,4})(?!\d)")
# FAX (ラベル付きテキストから)
_FAX_TEXT_RE = re.compile(
    r"(?:FAX|Fax|ﾌｧｸｽ|ファックス|ファクシミリ)[^0-9]{0,8}"
    r"(0\d{1,3})[\-‐ー－(（]\s?(\d{1,4})[\-‐ー－)）]\s?(\d{3,4})(?!\d)",
    re.I,
)
# メールアドレス
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_EMAIL_NG = re.compile(r"\.(png|jpe?g|gif|svg|webp)$|example\.|sentry|wixpress", re.I)

# 追加のサイト固有カラム (Schema に無いもの)
_EN_COL = "企業名_英語"
_CHAIN_COL = "チェーン名"
_FAX_COL = "FAX番号"

# ラベル駆動抽出の仕様: (ラベル正規表現, 出力キー, 抽出種別)。上から順に評価する。
_LABEL_SPECS = [
    (re.compile(r"会社名|商\s*号|正式名称|社\s*名"), Schema.NAME, "name"),
    (re.compile(r"代表者|代表取締役|代\s*表\s*者|社\s*長"), Schema.REP_NM, "text"),
    (re.compile(r"郵便番号|〒"), Schema.POST_CODE, "post"),
    (re.compile(r"住\s*所|所\s*在\s*地|本\s*社|本\s*店"), Schema.ADDR, "addr"),
    (re.compile(r"FAX|Fax|ﾌｧｸｽ|ファックス|ファクシミリ"), _FAX_COL, "tel"),
    (re.compile(r"TEL|Tel|電\s*話|Phone|お電話", re.I), Schema.TEL, "tel"),
    (re.compile(r"E-?mail|Email|e-mail|メール", re.I), Schema.EMAIL, "email"),
    (re.compile(r"設\s*立|創\s*業|創\s*立"), Schema.OPEN_DATE, "text"),
    (re.compile(r"資\s*本\s*金"), Schema.CAP, "text"),
    (re.compile(r"事業内容|事業案内|事業概要|業務内容|事\s*業"), Schema.LOB, "lob"),
]

# 会社概要系ページへのリンク候補 (優先度が小さいほど先に辿る)
_ABOUT_PRIORITY = [
    (re.compile(
        r"company|corporate|corp|about|profile|outline|overview|gaiyo|gaiyou|kaisha|"
        r"会社概要|会社案内|企業情報|会社情報|会社紹介|会社データ",
        re.I,
    ), 0),
    (re.compile(r"tokushoho|tokusho|特定商取引", re.I), 1),
    (re.compile(r"contact|inquiry|inquire|お問い?合わせ|問い合わせ", re.I), 2),
    (re.compile(r"access|アクセス", re.I), 3),
]


class JfaScraper(StaticCrawler):
    """日本フランチャイズチェーン協会（JFA）正会員スクレイパー"""

    DELAY = 1.0
    # 外部各社サイトはレスポンスが遅い場合があるため短めに (スモークテストの制限秒対策)
    TIMEOUT = 15
    # 業種(営業内容)は Schema.CAT_SITE。英語名・チェーン名・FAX はサイト固有カラム。
    EXTRA_COLUMNS = [_EN_COL, _CHAIN_COL, _FAX_COL]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("一覧ページの取得に失敗: %s", url)
            return

        rows = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) != 2:
                continue
            # 企業名セル(左)に外部HPリンクを持つ行のみ = 正会員（準/賛助会員は非リンク）
            a = tds[0].find("a", href=_HTTP)
            if not a:
                continue
            href = a.get("href", "")
            if "jfa-fc.or.jp" in href:  # JFA自ドメインのナビ/バナー等を除外
                continue
            if tds[0].find("th"):
                continue
            rows.append((tds, a))

        self.total_items = len(rows)
        self.logger.info("正会員 抽出: %d 件", len(rows))

        for tds, a in rows:
            try:
                item = self._build_item(url, tds, a)
                if item:
                    yield item
            except Exception as e:  # 個別行の失敗は握りつぶさずログして継続
                self.logger.warning("行の解析に失敗: %s", e)
                continue

    def _build_item(self, url: str, tds, a) -> dict | None:
        name = a.get_text(strip=True)
        if not name:
            return None
        hp = a.get("href", "").strip()

        span = tds[0].find("span", class_="f10px1")
        en = span.get_text(strip=True) if span else ""

        # チェーン名・営業内容セル: 1行目=チェーン名, 2行目以降=業種(サイト定義業種)
        lines = [t.strip() for t in tds[1].get_text("\n", strip=True).split("\n") if t.strip()]
        chain = lines[0] if lines else ""
        biz = " ".join(lines[1:]) if len(lines) > 1 else ""

        item = {
            Schema.NAME: name,
            Schema.HP: hp,
            Schema.CAT_SITE: biz,
            _EN_COL: en,
            _CHAIN_COL: chain,
            Schema.URL: url,
        }

        # 各社サイトへアクセスして基本情報を補完 (HP明記の情報のみ)
        if hp:
            enriched = self._enrich_from_company_site(hp)
            # サイト側で値が取れた項目のみ上書き (一覧由来の企業名は取れなければ残す)
            for k, v in enriched.items():
                if v:
                    item[k] = v

        return item

    def _enrich_from_company_site(self, hp: str) -> dict:
        """企業サイト (トップ + 会社概要系ページ) から基本情報を抽出する。"""
        soup = self.get_soup(hp)
        if soup is None:
            return {}

        info = self._extract_basic(soup)

        # 主要項目が欠けていれば会社概要系ページを優先度順に数ページ辿って補完する。
        def _missing() -> bool:
            core = (Schema.ADDR, Schema.REP_NM, Schema.CAP, Schema.LOB, Schema.TEL)
            return not all(info.get(k) for k in core)

        if _missing():
            for about_url in self._find_about_urls(soup, hp, limit=3):
                if not _missing():
                    break
                about_soup = self.get_soup(about_url)
                if about_soup is None:
                    continue
                for k, v in self._extract_basic(about_soup).items():
                    if v and not info.get(k):
                        info[k] = v
        return info

    def _find_about_urls(self, soup, base_url: str, limit: int = 3) -> list[str]:
        base_host = urlparse(base_url).netloc
        base_norm = base_url.rstrip("/")
        candidates: dict[str, int] = {}
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            text = tag.get_text(strip=True)
            prio = None
            for rx, p in _ABOUT_PRIORITY:
                if rx.search(href) or rx.search(text):
                    prio = p if prio is None else min(prio, p)
            if prio is None:
                continue
            full = urljoin(base_url, href)
            if not full.lower().startswith(("http://", "https://")):
                continue
            # 別ドメイン(SNS等)へ飛ぶリンクは避け、同一サイト内に限定
            if urlparse(full).netloc != base_host:
                continue
            full = full.split("#")[0]
            if full.rstrip("/") == base_norm:
                continue
            if full not in candidates or prio < candidates[full]:
                candidates[full] = prio
        ordered = sorted(candidates.items(), key=lambda kv: kv[1])
        return [u for u, _ in ordered[:limit]]

    # ------------------------------------------------------------------ 抽出
    def _extract_basic(self, soup) -> dict:
        """HTML から基本情報 (ラベル駆動 + 正規表現) を抽出する。"""
        info: dict = {}
        pairs = self._collect_pairs(soup)

        # ラベル駆動: 各項目について、最初にラベルが一致したペアの値を採用する。
        for rx, key, kind in _LABEL_SPECS:
            if info.get(key):
                continue
            for label, value in pairs:
                if not rx.search(label):
                    continue
                extracted = self._extract_value(kind, self._clean(value))
                if extracted:
                    info[key] = extracted
                    break

        # 住所が取れていれば都道府県を派生 (未取得時のみ)
        if info.get(Schema.ADDR) and not info.get(Schema.PREF):
            m = _PREF_RE.search(info[Schema.ADDR])
            if m:
                info[Schema.PREF] = m.group(0)

        # 正規表現フォールバック (ラベルで取れなかった項目のみ本文全体から補う)
        self._regex_fallbacks(soup, info)
        return info

    def _collect_pairs(self, soup) -> list[tuple[str, str]]:
        """table(th/td・td/td) / dl(dt/dd) / 「ラベル：値」テキスト行を (ラベル, 値) 化する。"""
        pairs: list[tuple[str, str]] = []

        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"], recursive=False)
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(" ", strip=True)
                value = " ".join(c.get_text(" ", strip=True) for c in cells[1:])
                if label:
                    pairs.append((label, value))

        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = dt.get_text(" ", strip=True)
                value = dd.get_text(" ", strip=True)
                if label:
                    pairs.append((label, value))

        # 非テーブルレイアウト (フッター等) の「ラベル：値」行
        for line in soup.get_text("\n").split("\n"):
            line = line.strip()
            m = re.match(r"(.{1,14}?)\s*[：:]\s*(\S.*)", line)
            if m:
                pairs.append((m.group(1).strip(), m.group(2).strip()))

        return pairs

    def _extract_value(self, kind: str, value: str) -> str:
        if not value:
            return ""
        if kind == "name":
            return value[:100]
        if kind == "text":
            return value[:80]
        if kind == "lob":
            return value[:300]
        if kind == "post":
            m = _POST_MARK_RE.search(value) or _POST_BARE_RE.search(value)
            return f"{m.group(1)}-{m.group(2)}" if m else ""
        if kind == "addr":
            m = _ADDR_RE.search(value)
            if m and _ADDR_HAS_NUM.search(m.group(2)):
                return (m.group(1) + m.group(2)).strip()
            # ラベルが住所である以上、都道府県を含めば全文を採用
            return value[:120] if _PREF_RE.search(value) else ""
        if kind == "tel":  # 電話・FAX共通
            m = _TEL_RE.search(value)
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""
        if kind == "email":
            m = _EMAIL_RE.search(value)
            return m.group(0) if m and not _EMAIL_NG.search(m.group(0)) else ""
        return value[:120]

    def _regex_fallbacks(self, soup, info: dict) -> None:
        text = soup.get_text(" ", strip=True)

        # 住所 + 都道府県: 番地(数字/丁目/番)を含む最初の候補
        if not info.get(Schema.ADDR):
            for m in _ADDR_RE.finditer(text):
                if _ADDR_HAS_NUM.search(m.group(2)):
                    info[Schema.PREF] = m.group(1)
                    info[Schema.ADDR] = (m.group(1) + m.group(2)).strip()
                    break

        # 郵便番号: 〒マーカー付き優先、無ければ住所直前の裸パターン
        if not info.get(Schema.POST_CODE):
            m_post = _POST_MARK_RE.search(text)
            if not m_post and info.get(Schema.ADDR):
                idx = text.find(info[Schema.ADDR])
                if idx > 0:
                    m_post = _POST_BARE_RE.search(text[max(0, idx - 20):idx])
            if m_post:
                info[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"

        # FAX: ラベル付き番号のみ (電話番号との取り違え防止)
        if not info.get(_FAX_COL):
            m_fax = _FAX_TEXT_RE.search(text)
            if m_fax:
                info[_FAX_COL] = f"{m_fax.group(1)}-{m_fax.group(2)}-{m_fax.group(3)}"

        # TEL: FAX番号や「FAX」直前の番号は避けて最初の電話番号を採用
        if not info.get(Schema.TEL):
            fax_val = info.get(_FAX_COL, "")
            for m in _TEL_RE.finditer(text):
                tel = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                if tel == fax_val:
                    continue
                prefix = text[max(0, m.start() - 8):m.start()]
                if re.search(r"FAX|Fax|ﾌｧｸｽ|ファ", prefix):
                    continue
                info[Schema.TEL] = tel
                break

        # メールアドレス
        if not info.get(Schema.EMAIL):
            for m in _EMAIL_RE.finditer(text):
                if not _EMAIL_NG.search(m.group(0)):
                    info[Schema.EMAIL] = m.group(0)
                    break

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", s or "").strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JfaScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jfa-fc.or.jp/particle/38.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

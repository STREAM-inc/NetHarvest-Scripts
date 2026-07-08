"""
建産協 (J-CHIF) — 会員企業・団体リンク集スクレイパー

取得対象:
    - 会社/団体名 (NAME) / ホームページURL (HP) / 会員種別 (EXTRA)
    - 各会員のHPを開いて取得: 都道府県 (PREF) / 住所 (ADDR) / 郵便番号 (POST_CODE) / TEL

取得フロー (一覧 → 外部HP で完結):
    1. link.html (会員リンク集) を 1 回取得し、5 つの会員種別セクション
       (企業正会員 / 中小企業正会員 / 団体正会員 / 企業賛助会員 / 団体賛助会員) から
       会員名 + HP URL を全件抜き出す。
    2. 会員 1 件ごとに、その会員自身の HP を開いて住所・TEL・郵便番号を
       ベストエフォートで抽出し、即 yield する
       (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。

住所・TEL の抽出はサイトごとに構造が異なるため、固定セレクタではなく
テキストからの正規表現ヒューリスティックで行う。トップページで見つからない場合は
「会社概要 / 会社案内 / 企業情報 / アクセス / お問い合わせ」等のサブページを
1 ページだけ辿って再抽出する。取得できないフィールドは空欄のままとする
(会員により掲載が無い / 画像化されている場合があるため)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/kensankyo.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kensankyo
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

# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
# 郵便番号 (例: "〒108-0075")。住所抽出の起点として最優先で使う。
_POST_RE = re.compile(r"〒\s*(\d{3}[-－]\d{4})")
# 郵便番号の直後に続く住所を捕捉する (改行・空白をまたいで都道府県から)
_ADDR_AFTER_POST_RE = re.compile(
    rf"〒\s*\d{{3}}[-－]\d{{4}}[\s　]*({_PREF})(.{{2,50}})"
)
# 郵便番号が無い場合のフォールバック: 都道府県から始まる住所らしき文字列
_ADDR_PREF_RE = re.compile(rf"({_PREF})(.{{2,50}})")
# 住所の妥当性判定 (フォールバック用)。都道府県の直後が地名 (先頭に句読点や
# 記号を含まない) で、早い位置に 市/区/郡 が現れ、番地/丁目/号/数字 を伴うものだけ
# 住所とみなす。ニュース等の散文 ("奈良県・明日香村…のもと") の誤検出を避ける。
_ADDR_VALID_RE = re.compile(r"^[^\s　、。，・「『【（(＜<]{0,5}?[市区郡].{0,20}?(丁目|番地|[0-9０-９]|号)")
# 住所を打ち切る区切り (この手前までを住所とする)
_ADDR_CUT_RE = re.compile(r"(TEL|Tel|tel|FAX|Fax|電話|℡|〒|Copyright|©|地図|MAP|Map|Google|営業|受付|、|。|｜|\||【|》|＞|>)")
# 日本の固定/フリーダイヤル電話番号 (半角化後に判定)
_TEL_RE = re.compile(r"0\d{1,4}[-(]\d{1,4}[-)]\d{3,4}")
# TEL ラベル付きの番号 (優先的に採用したい)
_TEL_LABELED_RE = re.compile(r"(?:TEL|Tel|tel|電話|℡|ＴＥＬ)[^\d]{0,8}(0[\d\-()]{7,14})")

# 会員種別 h3 見出しから会員数の接尾辞を除去する用
_MEMBER_COUNT_RE = re.compile(r"[（(]\s*\d+\s*[社団体]*\s*[)）]\s*$")

# 会社概要系サブページのランク付けキーワード (高スコアほど住所が載っている可能性が高い)
# (キーワード, スコア)。href とリンク文言の両方に対して判定する。
_PROFILE_STRONG = ("会社概要", "会社案内", "企業概要", "企業情報", "会社情報",
                   "団体概要", "組織概要", "会社データ", "法人概要", "事業所")
_PROFILE_MID = ("概要", "アクセス", "所在地", "company", "corporate", "about",
                "outline", "gaiyo", "overview", "access", "profile")
_PROFILE_WEAK = ("お問い合わせ", "お問合せ", "問い合わせ", "contact", "inquiry")
# これらを含むリンクは会社概要ではないので除外する
_PROFILE_EXCLUDE = ("ir", "recruit", "saiyo", "採用", "news", "topics", "product",
                    "sample", "privacy", "policy", "sitemap", "login", "search",
                    "faq", "blog", "column", "case", "event", "seminar", "pdf")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


def _to_halfwidth(s: str) -> str:
    """全角英数記号を半角へ (電話番号/郵便番号の正規化用)。"""
    return s.translate(
        {ord(c): ord(c) - 0xFEE0 for c in "０１２３４５６７８９（）－"}
    ).replace("−", "-").replace("ー", "-").replace("‐", "-")


class KensankyoScraper(StaticCrawler):
    """建産協 (J-CHIF) 会員リンク集スクレイパー (一覧 → 各会員HP)。"""

    # yield 1 件ごとに sleep。会員ごとに別ドメインの HP を叩くため負荷は分散するが、
    # 念のため 1 秒あける。
    DELAY = 1.0
    EXTRA_COLUMNS = ["会員種別"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        members = self._collect_members(soup, url)
        self.total_items = len(members)
        self.logger.info("会員リンクを %d 件抽出しました。各HPを巡回します。", len(members))

        for m in members:
            info = self._scrape_hp(m["hp"])
            item = {
                Schema.NAME: m["name"],
                Schema.HP: m["hp"],
                "会員種別": m["category"],
            }
            item.update(info)
            yield item

    # ------------------------------------------------------------------
    # 一覧 (link.html) の解析
    # ------------------------------------------------------------------
    def _collect_members(self, soup, root_url: str) -> list[dict]:
        """link.html の各会員種別セクションから 会員名 + HP + 種別 を全件取り出す。"""
        members: list[dict] = []
        seen: set[str] = set()

        for sec in soup.select("div.sec__inn"):
            title_el = sec.select_one(".sec__inn--title")
            category = ""
            if title_el:
                category = _MEMBER_COUNT_RE.sub(
                    "", _clean(title_el.get_text(" ", strip=True))
                ).strip()

            for a in sec.select("a.sec__inn--item--list--link[href]"):
                name = _clean(a.get_text(" ", strip=True))
                hp = urljoin(root_url, a.get("href", "").strip())
                if not name or not hp:
                    continue
                if hp in seen:
                    continue
                seen.add(hp)
                members.append({"name": name, "hp": hp, "category": category})

        return members

    # ------------------------------------------------------------------
    # 各会員 HP からの住所 / TEL 抽出 (ベストエフォート)
    # ------------------------------------------------------------------
    def _scrape_hp(self, hp_url: str) -> dict:
        """会員HP (トップ→必要なら会社概要系サブページ) から住所・TEL・郵便番号を抽出。"""
        soup = self.get_soup(hp_url)
        if soup is None:
            return {}

        info = self._extract_contact(soup)

        # トップで住所も TEL も揃っていればそれで十分。欠けていれば会社概要系
        # サブページをスコア順に最大 2 ページ辿って補完する。
        if not (info.get(Schema.ADDR) and info.get(Schema.TEL)):
            for sub_url in self._find_profile_links(soup, hp_url, limit=2):
                sub_soup = self.get_soup(sub_url)
                if sub_soup is None:
                    continue
                sub_info = self._extract_contact(sub_soup)
                # 会社概要ページの値を優先 (トップより正確なことが多い)
                for k, v in sub_info.items():
                    if v:
                        info[k] = v
                if info.get(Schema.ADDR) and info.get(Schema.TEL):
                    break

        return info

    def _find_profile_links(self, soup, base_url: str, limit: int = 2) -> list[str]:
        """会社概要 / アクセス等、住所が載っていそうな同一ドメイン内リンクをスコア順に返す。"""
        base_host = urlparse(base_url).netloc
        scored: list[tuple[int, str]] = []
        seen: set[str] = set()

        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            full = urljoin(base_url, href)
            if urlparse(full).netloc and urlparse(full).netloc != base_host:
                continue  # 外部ドメインへは飛ばない
            if full in seen:
                continue

            text = a.get_text(" ", strip=True) or ""
            hay = (text + " " + href).lower()
            if any(x in hay for x in _PROFILE_EXCLUDE):
                continue

            score = 0
            if any(k in text for k in _PROFILE_STRONG) or any(
                k in hay for k in _PROFILE_STRONG if k.isascii()
            ):
                score = 100
            elif any(k in text for k in _PROFILE_MID) or any(
                k in hay for k in _PROFILE_MID if k.isascii()
            ):
                score = 60
            elif any(k in text for k in _PROFILE_WEAK) or any(
                k in hay for k in _PROFILE_WEAK if k.isascii()
            ):
                score = 30
            if score == 0:
                continue

            seen.add(full)
            scored.append((score, full))

        scored.sort(key=lambda t: t[0], reverse=True)
        return [u for _, u in scored[:limit]]

    @staticmethod
    def _refine_addr(raw: str) -> str:
        """住所候補文字列を区切り文字の手前で打ち切り、前後の空白を整える。"""
        raw = _clean(raw)
        cut = _ADDR_CUT_RE.search(raw)
        if cut:
            raw = raw[: cut.start()]
        return _clean(raw)

    def _extract_contact(self, soup) -> dict:
        """1 ページの可視テキストから 郵便番号 / 都道府県 / 住所 / TEL を抽出。"""
        # script/style を除いた可視テキスト
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = _to_halfwidth(soup.get_text("\n", strip=True))

        info: dict = {}

        # --- 郵便番号 ---
        mpost = _POST_RE.search(text)
        if mpost:
            info[Schema.POST_CODE] = mpost.group(1).replace("－", "-")

        # --- 住所 ---
        # 優先: 郵便番号の直後に続く「都道府県 + 住所」。会社概要では
        #       〒108-0075\n東京都港区… の形が多く、これが最も誤検出が少ない。
        pref, addr = "", ""
        m = _ADDR_AFTER_POST_RE.search(text)
        if m:
            pref, addr = m.group(1), self._refine_addr(m.group(2))
        # フォールバック: 都道府県から始まり、市区郡+番地/数字を含む文字列
        if not addr:
            for mp in _ADDR_PREF_RE.finditer(text):
                cand = self._refine_addr(mp.group(2))
                if cand and _ADDR_VALID_RE.search(cand):
                    pref, addr = mp.group(1), cand
                    break
        if pref:
            info[Schema.PREF] = pref
        if addr:
            info[Schema.ADDR] = addr

        # --- TEL (ラベル付きを優先、無ければ最初の電話番号) ---
        tel = ""
        mlabeled = _TEL_LABELED_RE.search(text)
        if mlabeled:
            mt = _TEL_RE.search(mlabeled.group(1))
            if mt:
                tel = mt.group(0)
        if not tel:
            mt = _TEL_RE.search(text)
            if mt:
                tel = mt.group(0)
        if tel:
            info[Schema.TEL] = tel.replace("(", "-").replace(")", "-")

        return info


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KensankyoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.kensankyo.org/link.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

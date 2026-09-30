"""
ASITA インドネシア旅行業協会 会員一覧 — asita

取得対象:
    ASITA (Association of The Indonesian Tours and Travel Agencies) の会員名簿
    https://asita.id/anggota/ (WordPress + Business Directory Plugin / 263 ページ・5,250 社)。
    一覧カードから 会社名・州 (Listing Category)・責任者・住所・メール・
    固定電話・携帯を、詳細ページから 会員番号 (NIA) を取得する。

取得フロー:
    1. ルート (会員一覧) を取得する。1 ページ 20 件 (div.wpbdp-listing.excerpt)。
    2. カード 1 件ごとに詳細ページ (/anggota/{id}/{slug}/) を取得し、
       一覧の値とマージして即 yield する (Pattern B)。
    3. ページ送りは div.wpbdp-pagination span.next > a (= {root}page/N/)。
       リンクが無い / カードが 0 件のページに達したら終了する
       (末尾を超えたページは HTTP 200 で空の一覧が返るため件数で判定する)。

ページ構造:
    一覧・詳細とも WPBDP のフィールド div が並ぶ。
        <div class="... wpbdp-field-{key} wpbdp-field-{assoc} ...">
            <span class="field-label">ラベル</span>
            <div class="value">値</div>
        </div>
    key と出力カラムの対応:
        nama_perusahaan          会社名          (一覧のみ。詳細では <h1>/パンくずのみ)
        listing_category         州             "Provinsi Bali" 等
        penanggung_jawab         責任者
        nomor_induk_anggota_nia  会員番号 (NIA)  (詳細のみ)
        alamat_perusahaan        住所
        email_perusahaan         メールアドレス
        nomor_hp                 携帯
        nomor_telp_kantor        TEL

備考:
    - 国は「インドネシア」固定 (Schema.PREF は日本の都道府県用のため使用せず、
      州・都市・国は EXTRA カラムに出す)。
    - 都市は住所末尾の市名を採る。住所が通り名のみ等で市名を取れない場合は
      州名で代替する。
    - 電話番号は +62 を含む国際表記へ正規化する
      ("(0911) 352139" → "+62-911-352139"、"0361-735820" → "+62-361-735820")。
      市外局番が無い番号 ("3353131" 等) は州から補わず原文のまま残し、
      EXTRA "備考" に「市外局番欠落」と記録する (後段の HP 補完で解決する想定)。
    - 1 社に複数番号が併記される場合があるが、Schema.TEL / Schema.PHONE は
      正規化 (数字とハイフン以外を除去) が掛かり連結されてしまうため先頭 1 件のみ入れ、
      2 件目以降は EXTRA "備考" に「他TEL:」「他携帯:」として残す。
    - メールアドレスは Cloudflare の email-protection で難読化されている
      (a.__cf_email__[data-cfemail])。先頭 1 バイトを鍵とする XOR で復号する。
      フッターの協会事務局アドレス (dpp@asita.id) は会員データではないため、
      抽出範囲を会員ブロック内に限定したうえで明示的に除外する。
    - アウトバウンド/インバウンドの区分はサイト上に存在しない (ASTINDO 等
      他ソースとの突合が必要) ため本クローラーでは取得しない。
    - 会社紹介文などの自由記述フィールドはサイト上に無く、取得対象にも含めない。
    - origin が一時的に HTTP 521 (Cloudflare: origin unreachable) を返すことがある。
      一覧ページは _get_list_soup() で最大 3 回リトライし、それでも駄目なら例外にする
      (空の一覧 = 末尾到達と区別できなくなるため、無言で打ち切らない)。
    - robots.txt は /wp-admin/ のみ Disallow。利用規約ページは存在しない
      (sitemap 上にも規約系ページ無し) ため、スクレイピングを禁ずる明示条項は無い。
      過負荷を避けるため DELAY = 0.3 とする。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/asita.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id asita
"""

import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# WPBDP のフィールド div のクラスから「フィールド名」を取り出す際に無視する構造トークン
# 例: "wpbdp-field-display wpbdp-field-value wpbdp-field-nama_perusahaan
#      wpbdp-field-title wpbdp-field-type-textfield wpbdp-field-association-title"
#      → 残る "nama_perusahaan" がフィールド名
_STRUCTURAL_TOKENS = {"display", "value", "meta", "content", "title", "category"}

# 番号の区切り (複数番号の併記: "3353131, 4366100" / "0361-1111 / 0361-2222")
_NUMBER_SPLIT_PATTERN = re.compile(r"\s*(?:,|/|;|\bdan\b|\batau\b)\s*", re.IGNORECASE)

# 番号内のグループ区切り (ハイフン・括弧・スラッシュ以外の記号)
_GROUP_SPLIT_PATTERN = re.compile(r"[^0-9+]+")

# 住所セグメントのうち市名として採用しないもの (通り・建物・区画の表記)
_NON_CITY_PREFIX_PATTERN = re.compile(
    r"^(?:jl\.?|jln\.?|jalan|gg\.?|gang|komp(?:lek|leks)?\.?|perum(?:ahan)?|ruko|"
    r"gedung|graha|wisma|plaza|mall|blok|kav\.?|rt\b|rw\b|no\.?|lt\.?|lantai|"
    r"desa|dusun|kel\.?|kelurahan|kec\.?|kecamatan)\b",
    re.IGNORECASE,
)

# 市名の頭に付く行政区分の接頭辞 (除去して市名だけにする)
_CITY_PREFIX_PATTERN = re.compile(
    r"^(?:kota(?:madya)?|kab(?:\.|upaten)?)\s+", re.IGNORECASE
)

# 州名の先頭に付く "Provinsi"
_PROVINCE_PREFIX_PATTERN = re.compile(r"^provinsi\s+", re.IGNORECASE)

# 州の略称 (住所末尾に州の略記が付くことがある。市名として採らない)
#   例: "... No.138 Cimahi Jabar" の "Jabar" = Jawa Barat
_PROVINCE_ABBREVIATIONS = {
    "jabar", "jateng", "jatim", "jakut", "jaksel", "jakbar", "jaktim", "jakpus",
    "jakarta", "dki", "diy", "sumut", "sumbar", "sumsel", "lampung",
    "kalsel", "kalteng", "kaltim", "kalbar", "kaltara",
    "sulsel", "sulut", "sulteng", "sultra", "sulbar",
    "babel", "kepri", "ntb", "ntt", "papua", "malut",
}

# 住所末尾の 1 語を市名として採る際に弾く一般語 (方角・通り名の一部など)
_CITY_STOPWORDS = {
    "raya", "utara", "selatan", "timur", "barat", "tengah", "pusat",
    "indah", "baru", "jaya", "permai", "asri", "blok", "lantai", "gedung",
    "komplek", "kompleks", "perum", "perumahan", "ruko", "jalan", "dalam",
    "atas", "bawah", "depan", "belakang", "samping", "suite", "tower",
    "indonesia",
}

# 直前に来ると「末尾 1 語」が建物名になる語 (市名ではないので採らない)
#   例: "( Samping Hotel Wijaya )" の "Wijaya"
_BUILDING_MARKERS = {
    "hotel", "gedung", "gd", "ruko", "wisma", "graha", "plaza", "mall",
    "apartemen", "apartment", "tower", "blok", "komplek", "kompleks",
    "pertokoan", "kav", "lt", "lantai", "hall", "centre", "center",
}


class Asita(StaticCrawler):
    """ASITA インドネシア旅行業協会 会員一覧 スクレイパー"""

    DELAY = 0.3          # 約 5,300 社 × (一覧 + 詳細) のため過負荷を避ける
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "会員番号",
        "州",
        "都市",
        "国",
        "備考",
    ]

    # 国は固定 (インドネシアの協会)
    COUNTRY = "インドネシア"

    # 協会事務局のアドレス (フッター掲載。会員データではないため除外)
    _OFFICE_EMAIL = "dpp@asita.id"

    # 想定外のページ送りループを避けるための上限 (2026-09 時点で 263 ページ / 5,250 社)
    MAX_PAGES = 400

    # 一覧ページ取得のリトライ回数 (origin が不安定で 521 を返すことがある)
    MAX_PAGE_ATTEMPTS = 3

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        page_url = url
        seen: set[str] = set()

        for page_no in range(1, self.MAX_PAGES + 1):
            # 取得できなければ例外にする。ここで黙って return すると
            # 一過性の 5xx でクロールが途中終了したことに気付けない。
            soup = self._get_list_soup(page_url)

            cards = soup.select("div.wpbdp-listing.excerpt")
            logger.info("一覧 %d ページ目: %d 件 (%s)", page_no, len(cards), page_url)
            if not cards:
                # 末尾を超えたページは HTTP 200 で空の一覧を返す
                return

            for card in cards:
                detail_url = self._detail_url(card, page_url)
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_member(detail_url, card)
                except Exception as e:  # 1 件の失敗で全体を止めない
                    self.error_count += 1
                    logger.warning("詳細ページの取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            next_url = self._next_page_url(soup, page_url)
            if not next_url or next_url == page_url:
                return
            page_url = next_url

    # ------------------------------------------------------------------ #
    # 一覧ページ
    # ------------------------------------------------------------------ #
    def _get_list_soup(self, page_url: str) -> bs4.BeautifulSoup:
        """一覧ページを取得する (回数上限付きリトライ)。

        asita.id の origin は一時的に HTTP 521 (Cloudflare: origin unreachable) を返すことがある。
        521 は StaticCrawler の自動リトライ対象 (500/502/503/504) に含まれないため、
        ここで明示的にリトライする。全試行失敗したら例外にして失敗を表に出す
        (無言で打ち切ると「末尾に到達した」と区別できず、取りこぼしに気付けない)。
        """
        for attempt in range(self.MAX_PAGE_ATTEMPTS):
            soup = self.get_soup(page_url)
            if soup is not None:
                return soup
            if attempt < self.MAX_PAGE_ATTEMPTS - 1:
                wait = min(2 ** (attempt + 1), 30)
                logger.warning(
                    "一覧ページの取得に失敗 (%d/%d, %d 秒待機): %s",
                    attempt + 1, self.MAX_PAGE_ATTEMPTS, wait, page_url,
                )
                time.sleep(wait)
        raise RuntimeError(f"一覧ページを取得できませんでした: {page_url}")

    @staticmethod
    def _detail_url(card: bs4.Tag, page_url: str) -> str:
        """カードの見出しリンクから詳細 URL を作る。"""
        link = card.select_one("div.listing-title a[href]")
        if link is None:
            link = card.select_one(".wpbdp-field-nama_perusahaan a[href]")
        if link is None:
            return ""
        return urljoin(page_url, (link.get("href") or "").strip())

    @staticmethod
    def _next_page_url(soup: bs4.BeautifulSoup, page_url: str) -> str:
        """ページャの「Next →」から次ページ URL を作る。"""
        link = soup.select_one("div.wpbdp-pagination span.next a[href]")
        if link is None:
            return ""
        return urljoin(page_url, (link.get("href") or "").strip())

    # ------------------------------------------------------------------ #
    # 会員 1 件 (一覧カード + 詳細ページ)
    # ------------------------------------------------------------------ #
    def _scrape_member(self, detail_url: str, card: bs4.Tag) -> dict | None:
        fields = self._extract_fields(card)

        detail_soup = self.get_soup(detail_url)
        if detail_soup is not None:
            block = detail_soup.select_one("div.wpbdp-listing.single") or detail_soup
            detail_fields = self._extract_fields(block)
            # 会員番号 (NIA) は詳細にしか無い。他は詳細を優先しつつ一覧で欠落を埋める
            for key, value in detail_fields.items():
                if value:
                    fields[key] = value
            if not fields.get("nama_perusahaan"):
                fields["nama_perusahaan"] = self._name_from_detail(detail_soup)

        name = fields.get("nama_perusahaan", "")
        if not name:
            logger.warning("会社名を取得できませんでした (スキップ): %s", detail_url)
            return None

        notes: list[str] = []
        category = fields.get("listing_category", "")
        province = _PROVINCE_PREFIX_PATTERN.sub("", category).strip()
        address = fields.get("alamat_perusahaan", "")

        tel, tel_notes = self._format_numbers(fields.get("nomor_telp_kantor", ""), "他TEL")
        mobile, mobile_notes = self._format_numbers(fields.get("nomor_hp", ""), "他携帯")
        notes.extend(tel_notes)
        notes.extend(mobile_notes)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.REP_NM: fields.get("penanggung_jawab", ""),
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.PHONE: mobile,
            Schema.EMAIL: self._pick_email(fields.get("email_perusahaan", "")),
            Schema.CAT_SITE: category,
            "会員番号": fields.get("nomor_induk_anggota_nia", ""),
            "州": province,
            "都市": self._extract_city(address, province),
            "国": self.COUNTRY,
            "備考": " / ".join(notes),
        }

    # ------------------------------------------------------------------ #
    # WPBDP フィールド抽出
    # ------------------------------------------------------------------ #
    def _extract_fields(self, container: bs4.Tag) -> dict[str, str]:
        """WPBDP のフィールド div 群を {フィールド名: 値} に変換する。

        メールは Cloudflare 難読化のため、値テキストではなく data-cfemail から復号する。
        """
        fields: dict[str, str] = {}
        for div in container.select("div.wpbdp-field-display"):
            key = self._field_key(div)
            if not key:
                continue
            value_node = div.select_one("div.value")
            if value_node is None:
                continue

            cf = value_node.select_one("a.__cf_email__[data-cfemail]")
            if cf is not None:
                value = self._decode_cf_email(cf.get("data-cfemail", ""))
            else:
                value = self._clean(value_node.get_text(" ", strip=True))

            if value and not fields.get(key):
                fields[key] = value
        return fields

    @staticmethod
    def _field_key(div: bs4.Tag) -> str:
        """クラス列から WPBDP のフィールド名を取り出す (構造トークンは無視)。"""
        for cls in div.get("class", []):
            if not cls.startswith("wpbdp-field-"):
                continue
            token = cls[len("wpbdp-field-"):]
            if not token or token in _STRUCTURAL_TOKENS:
                continue
            if token.startswith("type-") or token.startswith("association-"):
                continue
            return token
        return ""

    @staticmethod
    def _name_from_detail(soup: bs4.BeautifulSoup) -> str:
        """詳細ページの JSON-LD / <title> から会社名を補う。"""
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = script.string or script.get_text() or ""
            m = re.search(r'"name"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
            if m:
                name = bs4.BeautifulSoup(m.group(1), "html.parser").get_text()
                name = name.replace("\\/", "/").strip()
                if name:
                    return name
        title = soup.find("title")
        if title:
            return re.split(r"\s+[–—|]\s+", title.get_text(strip=True))[0].strip()
        return ""

    @staticmethod
    def _decode_cf_email(encoded: str) -> str:
        """Cloudflare email-protection の data-cfemail を復号する (先頭 1 バイトが鍵)。"""
        try:
            data = bytes.fromhex(encoded)
        except ValueError:
            return ""
        if len(data) < 2:
            return ""
        key = data[0]
        return "".join(chr(b ^ key) for b in data[1:])

    def _pick_email(self, email: str) -> str:
        """協会事務局のアドレスは会員データではないため落とす。"""
        email = email.strip()
        if not email or email.lower() == self._OFFICE_EMAIL:
            return ""
        return email

    # ------------------------------------------------------------------ #
    # 電話番号の国際表記化
    # ------------------------------------------------------------------ #
    def _format_numbers(self, raw: str, note_label: str) -> tuple[str, list[str]]:
        """併記された番号を +62 表記に正規化し、(先頭の番号, 備考リスト) を返す。

        Schema.TEL / Schema.PHONE は「数字とハイフン以外を除去」する正規化が
        後段で掛かるため、複数番号を 1 セルに詰めると連結されてしまう。
        先頭 1 件のみをカラムに入れ、残りは備考に回す。
        """
        notes: list[str] = []
        formatted: list[str] = []
        missing_area = False

        for part in _NUMBER_SPLIT_PATTERN.split(raw or ""):
            part = part.strip()
            if not part:
                continue
            number, has_area = self._to_international(part)
            if not number:
                continue
            if not has_area:
                missing_area = True
            if number not in formatted:
                formatted.append(number)

        if not formatted:
            return "", notes

        if missing_area:
            notes.append("市外局番欠落")
        if len(formatted) > 1:
            notes.append(f"{note_label}: " + " / ".join(formatted[1:]))
        return formatted[0], notes

    @staticmethod
    def _to_international(raw: str) -> tuple[str, bool]:
        """1 本の番号を "+62-{市外局番}-{加入者番号}" 形式にする。

        Returns:
            (整形後の番号, 市外局番を特定できたか)
            市外局番が無い番号は州から補わず、原文の数字をそのまま返す。
        """
        # 内線表記 ("021-1234567 ext 12") は本体のみ残す
        raw = re.split(r"\b(?:ext|extension|hunting)\b\.?", raw, maxsplit=1, flags=re.IGNORECASE)[0]

        # 括弧・ハイフン・スラッシュをグループ区切りとして扱い、各グループは数字のみ残す
        groups = [re.sub(r"\D", "", g) for g in _GROUP_SPLIT_PATTERN.split(raw.replace("+", "-"))]
        groups = [g for g in groups if g]
        if not groups:
            return "", False

        digits = "".join(groups)
        if not digits:
            return "", False

        # すでに国番号付き ("62..." / "+62...")
        if digits.startswith("62") and len(digits) > 8:
            groups[0] = groups[0][2:] if groups[0].startswith("62") else groups[0]
            groups = [g for g in groups if g]
            if not groups:
                return "", False
            rest = groups
            if rest[0].startswith("0"):
                rest[0] = rest[0].lstrip("0")
            rest = [g for g in rest if g]
            return "+62-" + "-".join(rest) if rest else "", bool(rest)

        # 国内表記 ("0361-735820" / "(0911) 352139" / "0822-3645-7373")
        if groups[0].startswith("0"):
            area = groups[0].lstrip("0")
            rest = [g for g in groups[1:] if g]
            if not area:
                return digits, False
            if not rest:
                # 区切りが無く 1 塊のとき ("0361735820") は市外局番を切り出せない
                return "+62-" + area, True
            return "+62-" + "-".join([area] + rest), True

        # 市外局番なし ("3353131" 等) は州から補わず原文の数字を残す
        return "-".join(groups), False

    # ------------------------------------------------------------------ #
    # 住所 → 都市
    # ------------------------------------------------------------------ #
    @classmethod
    def _extract_city(cls, address: str, province: str) -> str:
        """住所末尾の市名を採る。取れない場合は州名で代替する。"""
        if not address:
            return province

        segments = [s.strip(" .") for s in re.split(r"[,–—]|\s+-\s+", address)]
        segments = [s for s in segments if s]

        province_key = cls._key(province)
        for segment in reversed(segments):
            candidate = _CITY_PREFIX_PATTERN.sub("", segment).strip(" .")
            if not candidate or len(candidate) < 3:
                continue
            if _NON_CITY_PREFIX_PATTERN.match(candidate):
                continue
            if re.search(r"\d", candidate):
                continue
            key = cls._key(candidate)
            # 州名そのもの (州の再掲) は市名として採らず、1 つ手前を見る
            if province_key and (key in province_key or province_key in key):
                continue
            return candidate

        # 区切りが無く通り名に市名が続く住所 ("... No. 79 kepahiang") への最終手段。
        # 末尾 1 語だけを見る (それ以上遡ると通り名・建物名を拾ってしまう)。
        for segment in reversed(segments or [address]):
            trailing = cls._trailing_city(segment, province_key)
            if trailing:
                return trailing

        return province

    @classmethod
    def _trailing_city(cls, segment: str, province_key: str) -> str:
        """住所末尾の 1 語が市名として妥当ならそれを返す。"""
        words = [w.strip(" .,()") for w in segment.split()]
        words = [w for w in words if w]
        if not words:
            return ""
        candidate = words[-1]
        if len(candidate) < 4 or not candidate.isalpha():
            return ""
        key = cls._key(candidate)
        if key in _PROVINCE_ABBREVIATIONS or key in _CITY_STOPWORDS:
            return ""
        # 建物名の一部 ("Hotel Wijaya" 等) は市名ではない
        if len(words) >= 2 and cls._key(words[-2]) in _BUILDING_MARKERS:
            return ""
        if province_key and (key in province_key or province_key in key):
            return ""
        return candidate

    @staticmethod
    def _key(text: str) -> str:
        """比較用キー (英小文字と数字のみ)。"""
        return re.sub(r"[^a-z0-9]", "", (text or "").lower())

    # ------------------------------------------------------------------ #
    # 共通
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """全角スペース・改行・重複空白を畳み、末尾の区切り文字を落とす。"""
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text.strip(" ;,")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Asita()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://asita.id/anggota/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

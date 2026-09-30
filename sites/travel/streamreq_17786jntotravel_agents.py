"""
【STREAMREQ-17786】JNTO公式Travel Agents（ニュージーランド）
 — streamreq_17786jntotravel_agents

取得対象:
    日本政府観光局 (JNTO) オーストラリア/ニュージーランド版 公式サイトの
    Travel Agents ページ (https://www.japan.travel/en/au/travel-agents/) に
    掲載された旅行代理店・ツアーオペレーター一覧 (2026-09 時点 101 社)。
    掲載企業は豪州 (AU) とニュージーランド (NZ) が混在するため、依頼の備考どおり
    **NZ 所在の企業のみ** を抽出し AU 企業は除外する (2026-09 時点で 9 社)。

サイト構造 (Phase 1 調査結果):
    - ルート URL 1 ページに全社が静的 HTML の <table id="data-table"> として
      出力済み。JS (DataTables) は表示の絞り込み/並べ替えを行うだけで追加の
      データ取得はしない → StaticCrawler で全件取得できる (Dynamic 不要)。
    - ページネーションは無い (1 ページ = 全 101 行, thead は見出しのみ)。
    - 詳細ページは存在しない (行内に a タグが 0 本)。各社の情報は 1 行 (<tr>) に
      全て入っている。
        td.thb            … ロゴ画像 (101 行中 85 行に存在)
        td.title          … 会社名
        td.note           … Company Profile (長文の自由記述 → 取得しない)
        td.phone (1 列目) … 主に豪州の電話番号
        td.url            … 公式サイト URL
        td.email          … メールアドレス
        td.address        … 住所 (国名が入る行と入らない行がある。空の行が 12 行)
        td.locations      … 掲載エリアコード (ACT/NSW/NT/NZ/QLD/SA/TAS/VIC/WA の
                            カンマ区切り。所在地ではなく「対応エリア」に近い)
        td.product-types  … 取扱商品タグ (School Groups, Ski & Snowboard 等)
        td.phone (2 列目) … 主に NZ の電話番号 (0800 / +64)

NZ 判定ロジック (_is_new_zealand):
    locations は「対応エリア」であり所在地ではない (実データ確認: Intrepid Travel /
    Wendy Wu Tours / Kintetsu 等は locations に NZ を含むが豪州所在) ため、
    住所を最優先で判定する。
        1. 住所に NZ を示す語 (New Zealand / Auckland 等の主要都市) → NZ
        2. 住所に AU を示す語 (Australia / 州コード / 主要都市) → 除外
        3. 住所が空なら URL・メールのドメイン (.nz / .au) で判定
           (例: Japan Junket / World Journeys は住所空だが .com.au → 除外)
        4. それでも判別できない場合は locations が NZ 単独のときだけ NZ とみなす
           (例: Mountainwatch Travel は locations="NZ,NSW" の豪州企業 → 除外)
    ※ locations が空でも住所が NZ の企業が存在する
      (例: House of Travel Christchurch City) ため locations は必須条件にしない。

データ上の注意点 (実データで確認済み):
    - 会社名・住所にノーブレークスペース (\\xa0) が混ざる行がある → 空白正規化する。
    - メールアドレスに入力ミスの空白が混ざる ("info@ example.com") → 除去する。
    - td.url にスキームが無い ("www.example.com") 行がある → https を補う。
    - td.address に URL が入っている行がある → 住所として扱わない。
    - 住所に国名が無い行がある → 末尾に "New Zealand" を補完する。
    - 電話は td.phone が 2 列あるため、NZ 番号 (+64 / 0800) を Schema.TEL に、
      残りを EXTRA「電話番号(その他)」に入れる。
    - メールアドレスは依頼の備考どおり Schema.EMAIL (メールアドレス) に保持する。

取得しないフィールド (除外):
    - Company Profile (td.note): 各社が書いた長文の自由記述 (プロース) のため
      著作権リスクを避けて取得しない。

名寄せ:
    対象が海外事業者のため STX 名寄せは対象外 (依頼指示)。

利用規約 / robots.txt (2026-09 再確認):
    - robots.txt: Disallow は /admin/, /*/travel-directory/*, /jp/ のみで
      "Allow: /"。本ページ (/en/au/travel-agents/) はクロール許可対象。
    - https://www.japan.travel/en/terms-of-use/ に scraping / crawling / robot /
      automated / spider / data mining 等の禁止文言は無い (著作権条項のみ)。
    → 収集継続可能と判断。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_17786jntotravel_agents.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py \
        --site-id streamreq_17786jntotravel_agents
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- ニュージーランド / オーストラリア 判定パターン -------------------------
_NZ_ADDR_PATTERN = re.compile(
    r"\bNew\s+Zealand\b|\bAotearoa\b|"
    r"\b(?:Auckland|Wellington|Christchurch|Queenstown|Hamilton|Dunedin|"
    r"Tauranga|Napier|Hastings|Rotorua|Nelson|Wanaka|Whangarei|Invercargill|"
    r"Palmerston\s+North|New\s+Plymouth|Taupo|Gisborne|Timaru|Blenheim|"
    r"Newmarket|Rosedale|Mangere|Albany)\b",
    re.IGNORECASE,
)
_AU_ADDR_PATTERN = re.compile(
    r"\bAustralia\b|\b(?:NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\b|"
    r"\b(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Hobart|Darwin|"
    r"Gold\s+Coast|Cairns|Newcastle|Wollongong|Geelong|Townsville)\b",
    re.IGNORECASE,
)
_NZ_DOMAIN_PATTERN = re.compile(r"\.nz\b", re.IGNORECASE)
_AU_DOMAIN_PATTERN = re.compile(r"\.au\b", re.IGNORECASE)

# NZ の電話番号 (国番号 +64 / フリーダイヤル 0800) 判定
_NZ_TEL_PATTERN = re.compile(r"^(?:\+?64|0800)")

# td.address に URL が入っている行の判定
_URL_LIKE_PATTERN = re.compile(r"^(?:https?://|www\.)", re.IGNORECASE)


class StreamReq17786JntoTravelAgents(StaticCrawler):
    """JNTO AU/NZ 版 Travel Agents のうち NZ 所在企業のみを取得するスクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "国",
        "掲載エリア",
        "電話番号(その他)",
        "ロゴ画像URL",
    ]

    COUNTRY = "ニュージーランド"
    COUNTRY_EN = "New Zealand"

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """ルート URL (= sites.yml の url) の一覧テーブルを 1 行ずつ処理する。

        ページネーション・詳細ページは無いため、この 1 ページで完結する。
        NZ 判定に通った行はその場で即 yield する (Pattern B)。
        """
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("一覧ページを取得できませんでした: %s", url)
            return

        table = soup.select_one("table#data-table") or soup.select_one(
            ".mod-data-table table"
        )
        if table is None:
            self.logger.warning("一覧テーブルが見つかりません: %s", url)
            return

        body = table.find("tbody") or table
        rows = body.find_all("tr")
        self.logger.info("一覧 %d 行を検出 (NZ 所在のみ抽出します): %s", len(rows), url)

        hit = 0
        for tr in rows:
            item = self._parse_row(tr, url)
            if item:
                hit += 1
                yield item

        self.logger.info("NZ 所在と判定した企業: %d 件", hit)

    # ------------------------------------------------------------------ #
    # 1 行のパース
    # ------------------------------------------------------------------ #
    def _parse_row(self, tr: bs4.Tag, url: str) -> dict | None:
        """1 行を辞書化する。見出し行・NZ 所在でない行は None を返す。"""
        name = self._cell_text(tr, "title")
        if not name:
            return None

        address = self._normalize_address(self._cell_text(tr, "address"))
        locations = self._cell_text(tr, "locations")
        hp = self._normalize_url(self._cell_text(tr, "url"))
        email = self._normalize_email(self._cell_text(tr, "email"))
        product_types = self._cell_text(tr, "product-types")

        # --- 備考の指示: NZ 所在企業のみ抽出し AU 企業は除外する ---
        if not self._is_new_zealand(address, locations, hp, email):
            return None

        tel, tel_other = self._pick_tel(tr)

        logo = ""
        img = tr.select_one("td.thb img[src]")
        if img:
            logo = urljoin(url, img["src"].strip())

        # 掲載エリア・取扱商品はカンマ区切りのため空要素を落として整形する
        area = ",".join(part.strip() for part in locations.split(",") if part.strip())
        genre = ",".join(p.strip() for p in product_types.split(",") if p.strip())

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.ADDR: self._with_country(address),
            Schema.TEL: tel,
            Schema.EMAIL: email,
            Schema.HP: hp,
            Schema.CAT_SITE: genre,
            "国": self.COUNTRY,
            "掲載エリア": area,
            "電話番号(その他)": tel_other,
            "ロゴ画像URL": logo,
        }

    # ------------------------------------------------------------------ #
    # ヘルパー
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """ノーブレークスペース等を通常空白にし、連続空白を 1 つに畳む。"""
        return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()

    @classmethod
    def _cell_text(cls, tr: bs4.Tag, class_name: str) -> str:
        """指定クラスの td テキストを取得 (複数列ある場合は最初の非空セル)。"""
        for td in tr.select(f"td.{class_name}"):
            text = cls._clean(td.get_text(" ", strip=True))
            if text:
                return text
        return ""

    @classmethod
    def _normalize_address(cls, address: str) -> str:
        """住所セルの前置ラベルを除去する。URL が入っている行は住所なしとする。"""
        if not address or _URL_LIKE_PATTERN.match(address):
            return ""
        # "Address: Suite 4, ..." のようにラベルが入る行がある
        return re.sub(r"^Address\s*:\s*", "", address, flags=re.IGNORECASE).strip()

    @classmethod
    def _pick_tel(cls, tr: bs4.Tag) -> tuple[str, str]:
        """td.phone は 2 列あり後列に NZ 番号が入る傾向のため NZ 番号を優先する。

        Returns:
            (Schema.TEL に入れる番号, EXTRA「電話番号(その他)」に入れる番号)
        """
        phones: list[str] = []
        for td in tr.select("td.phone"):
            text = cls._clean(td.get_text(" ", strip=True))
            if text and text not in phones:
                phones.append(text)
        if not phones:
            return "", ""

        for phone in phones:
            # 空白・記号を除いた先頭で NZ 番号 (+64 / 0800) かを判定する
            compact = re.sub(r"[\s()\-]", "", phone)
            if _NZ_TEL_PATTERN.match(compact):
                others = [p for p in phones if p != phone]
                return phone, " / ".join(others)

        # NZ 番号が無ければ後列 (NZ 用の列) を優先する
        return phones[-1], " / ".join(phones[:-1])

    @staticmethod
    def _normalize_email(email: str) -> str:
        """メールアドレス内部の誤入力空白 ("info@ example.com") を除去する。"""
        if not email:
            return ""
        return re.sub(r"\s+", "", email)

    @staticmethod
    def _normalize_url(hp: str) -> str:
        """スキームが欠けた URL ("www.example.com") に https を補う。"""
        hp = hp.strip()
        if not hp:
            return ""
        if hp.startswith(("http://", "https://")):
            return hp
        return f"https://{hp}"

    @classmethod
    def _with_country(cls, address: str) -> str:
        """住所に国名を含める (末尾に New Zealand が無ければ付与)。"""
        if not address:
            return cls.COUNTRY_EN
        if re.search(r"\bNew\s+Zealand\b", address, re.IGNORECASE):
            return address
        return f"{address.rstrip(', ')}, {cls.COUNTRY_EN}"

    @staticmethod
    def _is_new_zealand(address: str, locations: str, hp: str, email: str) -> bool:
        """住所 → ドメイン → 掲載エリア の順に NZ 所在かを判定する。

        locations は「所在地」ではなく「対応エリア」であり、シドニー所在でも
        NZ を含む企業があるため、住所を最優先の根拠とする。
        """
        if address:
            if _NZ_ADDR_PATTERN.search(address):
                return True
            if _AU_ADDR_PATTERN.search(address):
                return False

        domains = f"{hp} {email}"
        if _NZ_DOMAIN_PATTERN.search(domains):
            return True
        if _AU_DOMAIN_PATTERN.search(domains):
            return False

        # 住所もドメインも手掛かりが無い場合は掲載エリアが NZ 単独のときのみ採用
        areas = {part.strip().upper() for part in locations.split(",") if part.strip()}
        return areas == {"NZ"}


if __name__ == "__main__":
    scraper = StreamReq17786JntoTravelAgents()
    scraper.execute("https://www.japan.travel/en/au/travel-agents/")

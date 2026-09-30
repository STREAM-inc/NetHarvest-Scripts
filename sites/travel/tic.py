"""
TIC (Travel Industry Council of Hong Kong / 香港旅遊業議會) 会員旅行代理店名簿 — tic

取得対象:
    香港旅遊業議會 (TIC) の "Find a Travel Agent" 検索結果。
    引数 url は Nature of Business (業務区分) フィルタ nature[66]〜nature[74] を
    全てチェックした状態 = 業務区分が登録されている全会員旅行代理店の一覧。
    2026-09 時点で 73 ページ (15 件/ページ) 約 1,095 社。

取得フロー:
    1. 一覧 (引数 url の `page=N` を 0 始まりのページ番号に差し替え) を取得。
       1 社 = div.ta-details-rows で、会社名 (英/中)・屋号 (英/中)・免許番号・
       詳細リンク (a[href^="/en/travel-agent-information/"]) が載る。
    2. 各社の詳細ページを取得し、住所・TEL・FAX・Website・Email・
       業務区分 (Nature of Business)・加盟団体・会員種別・支店数を補完する。
       1 社パースするごとに即 yield する (Pattern B / 早期 yield)。
    3. 一覧に行が無くなった時点で終了 (範囲外ページは 0 件で返る)。

備考 (依頼元の指示) の反映:
    - 国カラムは "香港特別行政区" で固定。
    - 都市は住所末尾の地区 (HONG KONG / KOWLOON / NEW TERRITORIES) から導出する。
    - 会社名は英語表記を Schema.NAME、中国語表記を EXTRA "会社名_中国語" に格納する。
    - TEL / FAX は +852 を含む国際表記に正規化する (香港は市外局番なしの 8 桁。
      例 23481222 → +852-2348-1222)。
    - 業務区分・免許番号は後工程 (重複除去・判定) の作業用カラムとして保持する。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とする。ページ送りは url 内の
      `page=N` プレースホルダを置換して派生させ、詳細 URL は urljoin で派生させる。
    - メールアドレスは Cloudflare Email Protection で難読化されている
      (span.__cf_email__[data-cfemail])。先頭 1 バイトを鍵とした XOR で復号する。
    - 未登録項目はサイト上 "-" で表示されるため空文字に落とす。
    - 会員紹介文のような長文の自由記述は掲載が無く、出力もしない。
    - 利用規約に相当するのは /en/disclaimer のみで、免責事項のみ。スクレイピング /
      クローリングを禁止する条項は無い。robots.txt も /admin/ /search/ 等のみ
      Disallow で本パス (/en/agents/, /en/travel-agent-information/) は許可。
      (2026-09 確認)

実行方法:
    # ローカルテスト
    python scripts/sites/travel/tic.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tic
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 一覧 1 行 (1 社)
_ROW_SELECTOR = "div.ta-details-rows"
# 詳細ページへのリンク
_DETAIL_HREF_PREFIX = "/en/travel-agent-information/"

# 住所末尾の地区 → 都市名。TIC の住所は全て大文字で地区名が末尾に付く。
# サイト側に "NEW TERRITOIRES" 等のスペルゆれがあるため許容する。
_DISTRICT_MAP = [
    (
        re.compile(r"(NEW\s+TERRITOI?R(?:IES|ES|Y|E)|\bN\.?\s*T\.?)\s*$", re.IGNORECASE),
        "New Territories",
    ),
    (re.compile(r"KOWLOON\s*$", re.IGNORECASE), "Kowloon"),
    (re.compile(r"HONG\s*KONG\s*$", re.IGNORECASE), "Hong Kong"),
]

# 未登録を表すプレースホルダ
_EMPTY_VALUES = {"", "-", "‐", "―", "N/A", "n/a"}

# 住所欄が「事業所なし」を示すプレースホルダ (住所として扱わず空にする)
_NO_ADDRESS_PATTERN = re.compile(r"^\(?\s*NO\s+BUSINESS\s+ADDRESS\s*\)?$", re.IGNORECASE)

# ページ送りプレースホルダ (引数 url に含まれる `page=N`)
_PAGE_PLACEHOLDER = re.compile(r"([?&]page=)N\b")

# 安全弁: 想定ページ数 (2026-09 時点 73) を大きく超えたら打ち切る
_MAX_PAGES = 300


class Tic(StaticCrawler):
    """香港旅遊業議會 (TIC) 会員旅行代理店名簿スクレイパー"""

    DELAY = 0.5

    EXTRA_COLUMNS = [
        "会社名_中国語",   # 中国語 (繁体字) の会社名
        "屋号",            # Trade Name (英語)
        "屋号_中国語",     # Trade Name (中国語)
        "免許番号",        # Licence No. (作業用: 重複除去・判定材料)
        "FAX",             # +852 正規化済み
        "業務区分",        # Outbound Travel / Inbound Travel / Local Tour (作業用)
        "業務内容",        # 業務区分配下の取扱区分ラベル
        "加盟団体",        # Association Membership (HATA / ICTA / OTOA 等)
        "会員種別",        # Membership Type (Ordinary 等)
        "支店数",          # Branches セクションの支店件数
        "国",              # "香港特別行政区" 固定
        "都市",            # 住所末尾の地区
    ]

    # ------------------------------------------------------------------
    def parse(self, url: str):
        seen: set[str] = set()

        for page in range(_MAX_PAGES):
            list_url = self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧を取得できませんでした: %s", list_url)
                break

            rows = soup.select(_ROW_SELECTOR)
            if not rows:
                self.logger.info("一覧の終端に到達しました (page=%s)", page)
                break

            for row in rows:
                listed = self._parse_row(row, list_url)
                detail_url = listed.pop("detail_url", "")
                key = detail_url or f'{listed["name"]}|{listed["licence"]}'
                if key in seen:
                    continue
                seen.add(key)

                detail = self._parse_detail(detail_url) if detail_url else {}
                yield self._build_item(listed, detail, detail_url or list_url)

    # ------------------------------------------------------------------
    @staticmethod
    def _page_url(url: str, page: int) -> str:
        """引数 url の `page=N` プレースホルダを実ページ番号 (0 始まり) に置換する。"""
        if _PAGE_PLACEHOLDER.search(url):
            return _PAGE_PLACEHOLDER.sub(rf"\g<1>{page}", url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}page={page}"

    # ------------------------------------------------------------------
    def _parse_row(self, row: bs4.Tag, list_url: str) -> dict:
        """一覧 1 行から会社名 (英/中)・屋号 (英/中)・免許番号・詳細URLを取得する。"""
        link = row.select_one(f'a[href^="{_DETAIL_HREF_PREFIX}"]')
        return {
            "name": self._field(row, "views-field-field-company-name-en"),
            "name_chi": self._field(row, "views-field-field-company-name-chi"),
            "trade_name": self._trade_name(row, "views-field-field-trade-name"),
            "trade_name_chi": self._trade_name(row, "views-field-field-trade-name-chi"),
            "licence": self._field(row, "views-field-field-licence-no"),
            "detail_url": urljoin(list_url, link["href"]) if link else "",
        }

    def _field(self, row: bs4.Tag, cls: str) -> str:
        el = row.select_one(f"div.{cls} div.field-content")
        return self._clean(el.get_text(" ", strip=True)) if el else ""

    def _trade_name(self, row: bs4.Tag, cls: str) -> str:
        """屋号はラベル ("Trade Name:") と値が同じ field-content 内に入る。"""
        el = row.select_one(f"div.{cls} div.trade-name-val")
        return self._clean(el.get_text(" ", strip=True)) if el else ""

    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str) -> dict:
        """詳細ページから住所・連絡先・業務区分・会員情報を取得する。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページを取得できませんでした: %s", detail_url)
            return {}

        head = soup.select_one("div.head-office-section")
        out = {
            "addr": self._address(head),
            "tel": self._detail_field(head, "field_callable_tel"),
            "fax": self._detail_field(head, "field_callable_fax"),
            "hp": self._detail_link(head, "field_website"),
            "email": self._cf_email(head),
            "licence": self._detail_field(soup, "field_licence_no"),
            "membership": self._detail_field(soup, "field_association_membership"),
            "membership_type": self._detail_field(soup, "field_membership_type"),
        }

        # 会社名 (英/中) は詳細側にもあるので一覧が空のときの保険に使う
        for key, cls in (("name", "ta-title-en"), ("name_chi", "ta-title-chi")):
            el = soup.select_one(f"div.{cls}")
            out[key] = self._clean(el.get_text(" ", strip=True)) if el else ""

        # Nature of Business: div.nob-type が区分、配下の li.nob-title が取扱区分
        natures, services = [], []
        for box in soup.select("div.nature-of-business-section div.nob-container"):
            nob_type = box.select_one("div.nob-type")
            if nob_type:
                natures.append(self._clean(nob_type.get_text(" ", strip=True)))
            services += [
                self._clean(li.get_text(" ", strip=True))
                for li in box.select("li.nob-title")
            ]
        out["nature"] = " / ".join(n for n in natures if n)
        out["services"] = " / ".join(dict.fromkeys(s for s in services if s))

        out["branches"] = str(len(soup.select("div.branches-section div.field_branches")))
        return out

    def _address(self, scope: bs4.Tag | None) -> str:
        """本社住所。「(NO BUSINESS ADDRESS)」は住所ではないため空文字に落とす。"""
        addr = self._detail_field(scope, "field_address")
        return "" if _NO_ADDRESS_PATTERN.match(addr) else addr

    def _detail_field(self, scope: bs4.Tag | None, cls: str) -> str:
        if scope is None:
            return ""
        el = scope.select_one(f".{cls}")
        return self._clean(el.get_text(" ", strip=True)) if el else ""

    def _detail_link(self, scope: bs4.Tag | None, cls: str) -> str:
        """Website は href を優先し、無ければ表示テキストを使う。"""
        if scope is None:
            return ""
        wrapper = scope.select_one(f".{cls}")
        if wrapper is None:
            return ""
        a = wrapper if wrapper.name == "a" else wrapper.find("a", href=True)
        if a is not None and a.get("href"):
            return self._clean(a["href"])
        return self._clean(wrapper.get_text(" ", strip=True))

    # ------------------------------------------------------------------
    def _cf_email(self, scope: bs4.Tag | None) -> str:
        """Cloudflare Email Protection (data-cfemail) を復号する。"""
        if scope is None:
            return ""
        el = scope.select_one("span.__cf_email__[data-cfemail]")
        if el is None:
            return ""
        encoded = el["data-cfemail"]
        try:
            key = int(encoded[:2], 16)
            decoded = "".join(
                chr(int(encoded[i:i + 2], 16) ^ key) for i in range(2, len(encoded), 2)
            )
        except ValueError:
            self.logger.warning("cfemail の復号に失敗しました: %s", encoded)
            return ""
        return decoded if "@" in decoded else ""

    # ------------------------------------------------------------------
    @staticmethod
    def _format_tel(raw: str) -> str:
        """香港の電話番号を +852-XXXX-XXXX の国際表記に正規化する。"""
        digits = re.sub(r"\D", "", raw or "")
        if digits.startswith("852") and len(digits) == 11:
            digits = digits[3:]
        if len(digits) != 8:
            return raw.strip() if raw else ""
        return f"+852-{digits[:4]}-{digits[4:]}"

    @staticmethod
    def _city(addr: str) -> str:
        """住所末尾の地区名から都市を導出する。"""
        for pattern, city in _DISTRICT_MAP:
            if pattern.search(addr or ""):
                return city
        return ""

    @classmethod
    def _clean(cls, text: str) -> str:
        """NBSP・連続空白を整え、未登録プレースホルダ ("-") を空文字に落とす。"""
        text = (text or "").replace("\xa0", " ").replace("​", "")
        text = re.sub(r"\s+", " ", text).strip()
        return "" if text in _EMPTY_VALUES else text

    # ------------------------------------------------------------------
    def _build_item(self, listed: dict, detail: dict, source_url: str) -> dict:
        name = listed["name"] or detail.get("name", "")
        addr = detail.get("addr", "")
        return {
            Schema.NAME: name,
            Schema.ADDR: addr,
            Schema.TEL: self._format_tel(detail.get("tel", "")),
            Schema.EMAIL: detail.get("email", ""),
            Schema.HP: detail.get("hp", ""),
            Schema.CAT_SITE: detail.get("nature", ""),
            Schema.URL: source_url,
            "会社名_中国語": listed["name_chi"] or detail.get("name_chi", ""),
            "屋号": listed["trade_name"],
            "屋号_中国語": listed["trade_name_chi"],
            "免許番号": listed["licence"] or detail.get("licence", ""),
            "FAX": self._format_tel(detail.get("fax", "")),
            "業務区分": detail.get("nature", ""),
            "業務内容": detail.get("services", ""),
            "加盟団体": detail.get("membership", ""),
            "会員種別": detail.get("membership_type", ""),
            "支店数": detail.get("branches", ""),
            "国": "香港特別行政区",
            "都市": self._city(addr),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Tic()
    # 🔒 sites.yml の url と完全一致 (SSOT = sites.yml)
    scraper.execute(
        "https://www.tichk.org/en/agents/find-agent?combine=&nature%5B66%5D=66"
        "&nature%5B67%5D=67&nature%5B68%5D=68&nature%5B69%5D=69&nature%5B70%5D=70"
        "&nature%5B71%5D=71&nature%5B72%5D=72&nature%5B73%5D=73&nature%5B74%5D=74"
        "&find-ta-location=&page=N"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
SHTA (Sint Maarten Hospitality & Trade Association) 会員名簿 — shta_3

取得対象:
    カリブ海 シント・マールテン (Sint Maarten) のホスピタリティ団体 SHTA の
    会員名簿 (SHTA Members & Partners Directory) のうち、
    **旅行代理店・ツアーオペレーター区分のみ** を抽出する。

    - "Travel Agents & OTA's" (/travel-agents-otas/)
        … 区分全体が旅行代理店 / OTA のため掲載会員を全件採用
    - "Activities" (/activities/)
        … ツアー・エクスカーション事業者とカジノ・ゴルフ協会・パデルクラブ等が
          混在する区分のため、ツアーオペレーターに該当する会員のみを採用

    ※ Accommodations (ホテル) / Dining & Night Life (レストラン) /
      Air Travel (航空会社・空港) / Retail 等の他業種区分は取得しない。

取得フロー:
    1. ルート (引数 url = 会員名簿トップ) を取得し、区分ブロック
       (div.et_pb_text_inner) の見出し <h1><a href="/{slug}/"> から
       「区分名 / 区分ページ URL / 区分内の会員名リスト」を収集する。
    2. 対象区分の区分ページを取得。1 会員 = 1 ブロック (div.et_pb_blurb) で、
         - h4.et_pb_module_header … 会員名
         - div.et_pb_blurb_description … 紹介文 + "A:/Address:/T:/E:/W:" の
           ラベル行 (住所・TEL・メール・Web サイト)
         - div.et_pb_main_blurb_image img … ロゴ画像
       1 件パースするごとに即 yield する (Pattern B / 早期 yield)。
    3. 区分ページに個別ブロックが無く、名簿トップの一覧にのみ名前がある会員は
       名称のみのレコードとして補完 yield する (掲載漏れ対策)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、区分ページ URL は
      名簿トップのリンクから urljoin で派生させる。別 URL はハードコードしない。
    - ページネーションは無し (名簿トップ 1 枚 + 区分ページ)。会員個別の詳細
      ページも存在せず、会員名リンクはすべて区分ページを指す。
    - 会員紹介文は長文の自由記述 (著作権リスク) のため出力しない。
      区分が混在する "Activities" のツアーオペレーター判定にのみ使用する。
    - 所在地はシント・マールテン国内のため Schema.PREF (都道府県) は使わない。
    - 利用規約ページは存在しない (/terms/, /terms-of-use/, /privacy-policy/ は
      いずれも 404)。robots.txt は /wp-admin/ のみ Disallow で本パスの取得は許可。
      (2026-09 確認)
    - 同一サイト・同一方針の scripts/sites/travel/shta.py / shta_2.py が既に
      存在する (いずれも sites.yml 未登録)。重複登録に注意。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/shta_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id shta_3
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 連絡先ラベル行 ("A: 〜" / "Address: 〜" / "T: 〜" / "E: 〜" / "W: 〜")
# サイト側の表記ゆれ (Adress / 全角コロン / コロンが <strong> の内外) を吸収する
_LABEL_PATTERN = re.compile(
    r"^(a|address|adress|adresse|location|"
    r"t|tel|telephone|phone|"
    r"us number|mobile|cell|m|"
    r"e|email|e-mail|"
    r"w|web|website|url)\s*[::]\s*(.+)$",
    re.IGNORECASE,
)

_ADDR_LABELS = {"a", "address", "adress", "adresse", "location"}
_TEL_LABELS = {"t", "tel", "telephone", "phone"}
_TEL2_LABELS = {"us number", "mobile", "cell", "m"}
_EMAIL_LABELS = {"e", "email", "e-mail"}

_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

# 区分見出しの判定: 旅行代理店 / OTA 区分 (区分全体を採用)
_AGENCY_CATEGORY_PATTERN = re.compile(r"travel agent|\bota|tour operator", re.IGNORECASE)
# 区分見出しの判定: ツアーオペレーターが混在する区分 (会員単位で絞り込み)
_MIXED_CATEGORY_PATTERN = re.compile(r"^activities$", re.IGNORECASE)

# ツアーオペレーター判定: 会員名に現れる語
_TOUR_NAME_PATTERN = re.compile(
    r"\b(tours?|excursions?|travel|charters?|sightseeing|adventures?|sailing|cruises?)\b",
    re.IGNORECASE,
)
# ツアーオペレーター判定: 紹介文に現れる語 (判定にのみ使用し本文は出力しない)
_TOUR_DESC_PATTERN = re.compile(
    r"(tour operator|tour company|travel agency|travel agent|"
    r"destination management|excursions?|guided tours?)",
    re.IGNORECASE,
)

# 名寄せ用に落とす法人格サフィックス
_SUFFIX_PATTERN = re.compile(r"\b(n\.?v\.?|b\.?v\.?|inc|ltd|llc|corp|co)\b", re.IGNORECASE)


class Shta3(StaticCrawler):
    """SHTA 会員名簿 (旅行代理店・ツアーオペレーター区分) スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "その他電話番号",  # "US Number:" / "Mobile:" 等の 2 本目の番号
        "ロゴ画像URL",     # 会員ブロックのロゴ画像
    ]

    def parse(self, url: str):
        root = self.get_soup(url)
        if root is None:
            self.logger.error("会員名簿トップを取得できませんでした: %s", url)
            return

        categories = list(self._iter_target_categories(root, url))
        if not categories:
            self.logger.warning("対象区分 (旅行代理店/ツアーオペレーター) が見つかりません")
            return

        seen: set[str] = set()  # 区分をまたいだ重複掲載を名寄せで排除する

        for cat_name, cat_url, listed_names, filter_needed in categories:
            soup = self.get_soup(cat_url)
            if soup is not None:
                for member in self._iter_members(soup, cat_url):
                    if filter_needed and not self._is_tour_operator(
                        member["name"], member["desc"]
                    ):
                        continue
                    key = self._norm(member["name"])
                    if key in seen:
                        continue
                    seen.add(key)
                    yield {
                        Schema.NAME: member["name"],
                        Schema.ADDR: member["addr"],
                        Schema.TEL: member["tel"],
                        Schema.EMAIL: member["email"],
                        Schema.HP: member["hp"],
                        Schema.CAT_SITE: cat_name,
                        Schema.URL: cat_url,
                        "その他電話番号": member["tel2"],
                        "ロゴ画像URL": member["logo"],
                    }

            # 区分ページに個別ブロックが無く、名簿トップにのみ載る会員を名称のみで補完
            for name in listed_names:
                key = self._norm(name)
                if key in seen:
                    continue
                if filter_needed and not self._is_tour_operator(name, ""):
                    continue
                seen.add(key)
                yield {
                    Schema.NAME: name,
                    Schema.ADDR: "",
                    Schema.TEL: "",
                    Schema.EMAIL: "",
                    Schema.HP: "",
                    Schema.CAT_SITE: cat_name,
                    Schema.URL: url,
                    "その他電話番号": "",
                    "ロゴ画像URL": "",
                }

    # ------------------------------------------------------------------
    def _iter_target_categories(self, root: bs4.BeautifulSoup, url: str):
        """名簿トップから対象区分だけを (区分名, 区分URL, 会員名リスト, 要絞り込み) で返す。

        旅行代理店 / OTA 区分を先に、混在区分 (Activities) を後に処理する。
        同一会員が複数区分に重複掲載されるため、情報量の多い主区分を先に見て
        名寄せで後続の重複を落とす狙い。
        """
        agency, mixed = [], []
        for block in root.select("div.et_pb_text_inner"):
            heading = block.find(["h1", "h2", "h3"])
            if heading is None:
                continue
            anchor = heading.find("a", href=True)
            if anchor is None:
                continue

            cat_name = self._clean(heading.get_text(" ", strip=True))
            cat_url = urljoin(url, anchor["href"])
            if urlparse(cat_url).netloc != urlparse(url).netloc:
                continue

            # 入れ子 <ul> を包むだけのラッパー <li> は全会員名が連結されるため除外
            names = [
                self._clean(li.get_text(" ", strip=True))
                for li in block.find_all("li")
                if li.find(["ul", "ol", "li"]) is None
            ]
            names = [n for n in names if n]

            if _AGENCY_CATEGORY_PATTERN.search(cat_name):
                agency.append((cat_name, cat_url, names, False))
            elif _MIXED_CATEGORY_PATTERN.match(cat_name):
                mixed.append((cat_name, cat_url, names, True))

        return agency + mixed

    def _iter_members(self, soup: bs4.BeautifulSoup, cat_url: str):
        """区分ページの会員ブロック (div.et_pb_blurb) を 1 件ずつ dict にして返す。"""
        content = soup.select_one("div.entry-content") or soup
        for blurb in content.select("div.et_pb_blurb"):
            heading = blurb.select_one("h4.et_pb_module_header") or blurb.find("h4")
            if heading is None:
                continue
            name = self._clean(heading.get_text(" ", strip=True))
            if not name:
                continue

            desc = blurb.select_one("div.et_pb_blurb_description")
            fields = self._parse_description(desc) if desc else {}

            img = blurb.select_one("div.et_pb_main_blurb_image img")
            logo = urljoin(cat_url, img["src"]) if img and img.get("src") else ""

            yield {
                "name": name,
                "desc": fields.get("desc", ""),
                "addr": fields.get("addr", ""),
                "tel": fields.get("tel", ""),
                "tel2": fields.get("tel2", ""),
                "email": fields.get("email", ""),
                "hp": fields.get("hp", ""),
                "logo": logo,
            }

    def _parse_description(self, desc: bs4.Tag) -> dict:
        """紹介文ブロックから住所・TEL・メール・Web サイトを取り出す。

        紹介文本文 (プロース) は区分判定にのみ使い、出力には含めない。
        """
        # <a> の href はテキストより正確なので先に退避しておく
        mailto = ""
        website = ""
        for a in desc.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().startswith("mailto:"):
                if not mailto:
                    mailto = href[7:].split("?")[0].strip()
            elif href.lower().startswith(("http://", "https://")):
                # 自サイト (shta.com) へのリンクは会員の Web サイトではない
                if not website and "shta.com" not in urlparse(href).netloc.lower():
                    website = href

        # <br> / <p> を改行に変換してラベル行を行単位で扱えるようにする
        for br in desc.find_all("br"):
            br.replace_with("\n")
        for p in desc.find_all("p"):
            p.insert_before("\n")
            p.insert_after("\n")
        text = desc.get_text()

        out = {"desc": text, "addr": "", "tel": "", "tel2": "", "email": "", "hp": ""}
        for raw_line in text.split("\n"):
            line = self._clean(raw_line)
            if not line:
                continue
            m = _LABEL_PATTERN.match(line)
            if not m:
                continue
            label = m.group(1).lower()
            value = self._clean(m.group(2))
            if not value:
                continue
            if label in _ADDR_LABELS:
                out["addr"] = out["addr"] or value
            elif label in _TEL_LABELS:
                out["tel"] = out["tel"] or value
            elif label in _TEL2_LABELS:
                out["tel2"] = out["tel2"] or value
            elif label in _EMAIL_LABELS:
                out["email"] = out["email"] or value
            else:  # w / web / website / url
                out["hp"] = out["hp"] or value

        # メール: mailto href を最優先、無ければラベル値 / 本文中のアドレス
        if mailto:
            out["email"] = mailto
        else:
            em = _EMAIL_PATTERN.search(out["email"] or text)
            out["email"] = em.group(0) if em else ""

        # Web サイト: スキーム付きの href を最優先、無ければラベル値を補正
        if website:
            out["hp"] = website
        elif out["hp"]:
            candidate = out["hp"].split()[0].strip(",;")
            out["hp"] = candidate if "." in candidate else ""
            if out["hp"] and not out["hp"].lower().startswith(("http://", "https://")):
                out["hp"] = "https://" + out["hp"]

        return out

    # ------------------------------------------------------------------
    def _is_tour_operator(self, name: str, description: str) -> bool:
        """会員名・紹介文から旅行代理店 / ツアーオペレーターかを判定する。"""
        if _TOUR_NAME_PATTERN.search(name):
            return True
        return bool(description and _TOUR_DESC_PATTERN.search(description))

    @staticmethod
    def _clean(text: str) -> str:
        """NBSP・連続空白・前後の記号を整えた文字列を返す。"""
        text = text.replace("\xa0", " ").replace("\u200b", "")
        return re.sub(r"\s+", " ", text).strip().strip("·|").strip()

    @staticmethod
    def _norm(name: str) -> str:
        """名寄せ用の正規化キー (小文字・記号除去・法人格サフィックス除去)。"""
        key = _SUFFIX_PATTERN.sub(" ", name.lower())
        key = re.sub(r"[^a-z0-9]+", " ", key)
        # "Seagrape Tour" と "Seagrape Tours" のような単複のゆれを吸収する
        words = [re.sub(r"s$", "", w) for w in key.split()]
        return "".join(words)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Shta3()
    # 🔒 sites.yml の url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://shta.com/shta-member-directory/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

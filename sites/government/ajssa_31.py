"""
一般社団法人 広島県警備業協会（AJSSA 会員名簿・広島県 / 広警協）— 会員企業一覧

取得対象:
    - 広島県警備業協会の会員企業（正会員 約172社 + 賛助会員 6社）
    - 会社名 / 郵便番号 / 都道府県(広島県固定) / 住所 / 代表者 / TEL / FAX /
      HP / 業務内容(警備区分) / 地区 / 市区町村 / 主たる業務

取得フロー:
    1. GET /member.html（単一の静的ページ。ページネーション無し）
    2. ページには「会員企業一覧 172社」の五十音順マスタ表と、地区・市区町村別の
       内訳表が同居しており、同じ会員が 2 度出現する。マスタ表(「一覧」見出し配下)は
       スキップし、地区別セクション(広島地区 / 呉・東広島地区 / … / 賛助会員)の
       table.member-table のみを走査して重複を避ける。
    3. 各 table.member-table の 1 行 = 1 会員。td は
       [企業名(a=HP), 代表者名, 住所, 電話・FAX, 業務内容(span=区分)]。
       地区・市区町村は直前の h3.member-area 見出しから引き当てる。
    行を 1 件解析するごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_31.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_31
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_POST_RE = re.compile(r"(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"\d{2,4}-\d{2,4}-\d{3,4}")
_COUNT_RE = re.compile(r"[\s　]*\d+社\s*$")

# 業務内容セルの span クラス → 警備区分のフルネーム
_CAT_MAP = {
    "shisetsu": "施設警備",
    "junkai": "巡回警備",
    "hoan": "保安警備",
    "kikai": "機械警備",
    "kukou": "空港保安警備",
    "koutsu": "交通誘導警備",
    "zatsuto": "雑踏警備",
    "kicho": "貴重品運搬警備",
    "shinpen": "身辺警備",
}


class Ajssa31(StaticCrawler):
    """一般社団法人 広島県警備業協会 会員企業一覧 スクレイパー"""

    DELAY = 1.5
    # FAX / 地区 / 市区町村 / 主たる業務 は Schema に無いため EXTRA。
    EXTRA_COLUMNS = ["FAX", "地区", "市区町村", "主たる業務"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして使う
        soup = self.get_soup(url)
        if not soup:
            return

        # 地区別セクションの会員行のみを対象にするため、まず対象テーブルと
        # その地区・市区町村を文書順に収集する（マスタ「一覧」表はスキップ）。
        region = None
        city = None
        targets: list[tuple] = []  # (table, region, city)
        for el in soup.find_all(["h3", "table"]):
            cls = el.get("class") or []
            if el.name == "h3":
                text = el.get_text(strip=True)
                if "一覧" in text:  # 五十音順マスタ表 → 重複回避のためスキップ
                    region, city = None, None
                elif "地区" in text:
                    region, city = _COUNT_RE.sub("", text), None
                elif "賛助" in text:
                    region, city = "賛助会員", None
                else:  # 市区町村見出し
                    city = _COUNT_RE.sub("", text)
                continue
            if "member-table" not in cls:
                continue
            if region is None:  # マスタ表配下 → スキップ
                continue
            targets.append((el, region, city))

        self.total_items = sum(
            self._count_data_rows(t) for t, _, _ in targets
        )

        for table, region, city in targets:
            for tr in table.select("tr"):
                try:
                    item = self._parse_row(tr, url, region, city)
                    if item:
                        yield item
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員行の解析に失敗しskip: %s", e)
                    continue

    @staticmethod
    def _count_data_rows(table) -> int:
        n = 0
        for tr in table.select("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue
            if "member-table-1" in (tds[0].get("class") or []):
                continue
            if tds[0].get_text(strip=True):
                n += 1
        return n

    def _parse_row(self, tr, source_url: str, region: str, city: str) -> dict | None:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 5:
            return None
        # ヘッダー行 (企業名 / 代表者名 …) を除外
        if "member-table-1" in (tds[0].get("class") or []):
            return None

        name = tds[0].get_text(" ", strip=True)
        if not name:
            return None

        # HP: 企業名セル内のリンク
        hp = ""
        a = tds[0].select_one("a[href]")
        if a and a.get("href"):
            href = a["href"].strip()
            if href and not href.startswith(("#", "javascript:")):
                hp = urljoin(source_url, href)

        rep = tds[1].get_text(" ", strip=True)

        # 住所: 先頭に郵便番号、残りが市区町村＋番地(＋建物名)
        addr_raw = tds[2].get_text(" ", strip=True)
        post_code, addr = self._split_address(addr_raw)

        # 電話・FAX: 2 本並記 (全角スペース or 改行区切り)。1 本目=TEL, 2 本目=FAX
        nums = _TEL_RE.findall(tds[3].get_text(" ", strip=True))
        tel = nums[0] if nums else ""
        fax = nums[1] if len(nums) > 1 else ""

        # 業務内容: span の区分。1 つ目 (◎ 直後) が主たる業務
        cats = []
        for sp in tds[4].select("span"):
            spcls = [c for c in sp.get("class", []) if c in _CAT_MAP]
            if spcls:
                cats.append(_CAT_MAP[spcls[0]])
            else:
                txt = sp.get_text(strip=True)
                if txt:
                    cats.append(txt)
        primary = cats[0] if cats else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: "広島県",
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: hp,
            Schema.CAT_SITE: " / ".join(cats),
            "FAX": fax,
            "地区": region or "",
            "市区町村": city or "",
            "主たる業務": primary,
        }

    @staticmethod
    def _split_address(addr_raw: str) -> tuple[str, str]:
        """住所文字列から 郵便番号 と 住所(市区町村以降) を分離する。"""
        post_code = ""
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = post_code[:3] + "-" + post_code[3:]
            addr_raw = _POST_RE.sub("", addr_raw, count=1)
        addr = addr_raw.replace("〒", "").strip()
        return post_code, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa31()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.hirokeikyo.com/member.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

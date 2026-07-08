"""
一般社団法人 岡山県警備業協会（AJSSA 会員名簿・岡山県 / 岡警協）— 会員企業紹介

取得対象:
    - 岡山県警備業協会の会員警備会社（全社・約108社）
    - 会社名 / 名称カナ / 郵便番号 / 都道府県 / 住所 / TEL / FAX / e-mail /
      代表者 / HP / 業務区分（1号〜4号・機械警備）

取得フロー:
    引数 url (= sites.yml の url = /kaiin/kaiin.html#area) が唯一のルート。
    1. ルート(会員企業紹介トップ)の「地域別」ナビ (section#area) から
       エリア別ページ (備前 / 備中 / 美作 の 3 ページ) のリンクを取得する。
       各エリアページは配下の全市区の会員を含む上位一覧のため、これら 3 ページを
       巡回すれば重複なく全会員を網羅できる (市区別ページはこの部分集合)。
    2. 各エリアページの table.ta1 (名称 / 所在地 / 電話) の各行から会員詳細ページ
       (例: 152-owlsecurity.html) へのリンクを取得する。
    3. 各詳細ページの「会社概要」dl (名称 / 代表者 / 所在地 / 電話 / FAX / e-mail /
       URL) と「業務区分」table (1号〜4号・機械警備の ○ 印) を解析する。
       名称カナは見出し h3「会社名（かな）」の末尾かな部から取得する。
    詳細を 1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_30.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_30
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

# 〒NNN-NNNN もしくは 〒NNNNNNN
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# 住所先頭の都道府県 (岡山県以外の可能性は低いが汎用に)
_PREF_RE = re.compile(r"(北海道|東京都|(?:京都|大阪)府|..県)")
# 見出し末尾の（かな）を抽出 (ひらがな/カタカナ/長音のみ)
_KANA_RE = re.compile(r"（([ぁ-ゖァ-ヺー・\s]+)）\s*$")


class Ajssa30(StaticCrawler):
    """一般社団法人 岡山県警備業協会 会員企業紹介 スクレイパー"""

    DELAY = 1.5
    # 業務区分 = 警備種別の短い構造化ラベル(1号/2号/機械警備) → Schema.CAT_SITE。
    # FAX / e-mail は Schema に無いサイト固有の構造化情報として EXTRA。
    # 「業務内容」は自由記述の長文プロースのため著作権リスクで取得しない。
    EXTRA_COLUMNS = ["FAX", "e-mail"]

    def parse(self, url: str):
        # 引数 url を唯一のルートとしてエリアページ・詳細ページ URL を派生させる
        root = self.get_soup(url)
        if root is None:
            logger.warning("ルートページの取得に失敗: %s", url)
            return

        area_sec = root.find("section", id="area")
        if area_sec is None:
            logger.warning("地域別ナビ(section#area)が見つかりません: %s", url)
            return

        # エリアページ (備前/備中/美作) の直下リンクのみ (市区別リンクは配下の ul で除外)
        area_links = area_sec.select("ul.disc > li > a[href]")
        area_urls = [urljoin(url, a["href"].strip()) for a in area_links if a.get("href")]

        # 全エリアの会員一覧 (詳細 URL + 一覧の名称) を先に集めて総数を確定する
        members: list[tuple[str, str]] = []  # (detail_url, list_name)
        seen: set[str] = set()
        for area_url in area_urls:
            for detail_url, list_name in self._list_members(area_url):
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                members.append((detail_url, list_name))

        self.total_items = len(members)

        for detail_url, list_name in members:
            try:
                item = self._scrape_detail(detail_url, list_name)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip (%s): %s", detail_url, e)
                continue

    def _list_members(self, area_url: str):
        """エリアページの table.ta1 から (詳細URL, 一覧名称) を列挙する。"""
        soup = self.get_soup(area_url)
        if soup is None:
            logger.warning("エリアページの取得に失敗: %s", area_url)
            return
        table = soup.find("table", class_="ta1")
        if table is None:
            return
        for tr in table.find_all("tr"):
            td = tr.find("td")
            if not td:  # ヘッダ行 (th のみ)
                continue
            a = td.find("a", href=True)
            if not a:
                continue
            detail_url = urljoin(area_url, a["href"].strip())
            list_name = a.get_text(strip=True)
            yield detail_url, list_name

    def _scrape_detail(self, url: str, list_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 会社概要 dl (dt=ラベル, dd=値) をラベル駆動で取得
        fields: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            if not dts:
                continue
            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True)
                value = dd.get_text(" ", strip=True)
                if label and label not in fields:
                    fields[label] = value
            if "名称" in fields:  # 会社概要 dl を確認できたら打ち切り
                break

        name = fields.get("名称", "") or list_name
        if not name:
            return None

        # 名称カナ: 見出し h3「会社名（かな）」の末尾かな部
        kana = ""
        h3 = soup.find("h3")
        if h3:
            m = _KANA_RE.search(h3.get_text(strip=True))
            if m:
                kana = m.group(1).strip()

        # 所在地: 〒郵便番号 + 都道府県 + 住所
        loc = fields.get("所在地", "")
        post = ""
        pm = _POST_RE.search(loc)
        if pm:
            post = pm.group(1)
            if "-" not in post:
                post = post[:3] + "-" + post[3:]
            loc = _POST_RE.sub("", loc, count=1)
        loc = loc.replace("〒", "").strip()
        prm = _PREF_RE.search(loc)
        if prm and loc.startswith(prm.group(1)):
            pref = prm.group(1)
            addr = loc[len(pref):].strip()
        else:
            pref = "岡山県"
            addr = loc

        # 代表者: 全角スペースを半角へ
        rep = fields.get("代表者", "").replace("　", " ").strip()

        # URL(HP): dd 内の a[href] を優先、無ければテキスト
        hp = fields.get("URL", "").strip()

        # 業務区分: table.ta1 のヘッダ th と ○ の付いた列を対応付け
        cat_site = self._parse_gyoumu(soup)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.POST_CODE: post,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: fields.get("電話", ""),
            Schema.REP_NM: rep,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
            "FAX": fields.get("FAX", ""),
            "e-mail": fields.get("e-mail", ""),
        }

    @staticmethod
    def _parse_gyoumu(soup) -> str:
        """業務区分 table.ta1 (ヘッダ th + ○ 行) から選択済み区分を「/」連結で返す。"""
        for table in soup.find_all("table", class_="ta1"):
            trs = table.find_all("tr")
            if len(trs) < 2:
                continue
            ths = trs[0].find_all("th")
            tds = trs[1].find_all("td")
            if not ths or not tds:
                continue
            # 業務区分テーブルはヘッダに「号」または「機械警備」を含む
            head_txt = trs[0].get_text()
            if "号" not in head_txt and "機械警備" not in head_txt:
                continue
            labels = []
            for th, td in zip(ths, tds):
                if td.get_text(strip=True):  # ○ 等の印がある列
                    label = re.sub(r"\s+", "", th.get_text(" ", strip=True))
                    labels.append(label)
            return " / ".join(labels)
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa30()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://okakeikyo.or.jp/kaiin/kaiin.html#area")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
一般社団法人 北海道警備業協会（AJSSA 会員名簿・北海道 / HSSA）— 会員検索

取得対象:
    - 北海道警備業協会の会員企業（全社・約428社）
    - 会社名 / 郵便番号 / 住所 / TEL / FAX / 代表者 / HP / 業務種別

取得フロー:
    1. GET /search/index.html?...search=検索する
         → div.search-list 内の dl が 1 会員。会社名・住所・TEL・代表者と
           詳細ページ (post.html?mid=...) へのリンクが並ぶ。ページネーション無し。
    2. 各会員の詳細ページ (post.html?mid=...) を取得し、ラベル駆動の dl から
         住所 / 電話番号 / FAX / URL / 代表者 / 業務種別 を取得。
       会社名は詳細ページの .subtitle-01、無ければ一覧の dt.search-name を採用。
    詳細を 1 件取得するごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_2
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

_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class Ajssa2(StaticCrawler):
    """一般社団法人 北海道警備業協会 会員検索 スクレイパー"""

    DELAY = 1.5
    # 業務種別 → Schema.CAT_SITE。FAX のみ Schema に無いため EXTRA。
    EXTRA_COLUMNS = ["FAX"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして詳細 URL を派生させる
        soup = self.get_soup(url)
        if not soup:
            return

        dls = soup.select("div.search-list > dl")
        self.total_items = len(dls)

        for dl in dls:
            try:
                name_el = dl.select_one("dt.search-name")
                list_name = name_el.get_text(strip=True) if name_el else ""

                a = dl.select_one("dd.search-more a[href]")
                if not a:
                    # 詳細リンクが無い場合は一覧の情報のみで yield
                    item = self._parse_list_only(dl, url, list_name)
                    if item:
                        yield item
                    continue

                detail_url = urljoin(url, a["href"].strip())
                item = self._scrape_detail(detail_url, list_name)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _scrape_detail(self, url: str, list_name: str) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            return None

        name_el = soup.select_one(".subtitle-01")
        name = (name_el.get_text(strip=True) if name_el else "") or list_name
        if not name:
            return None

        # ラベル駆動: <dl><dt>ラベル</dt><dd>値</dd></dl>
        fields: dict[str, str] = {}
        for dl in soup.select(".membersearch-detail dl"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(" ", strip=True)
            fields[label] = value

        addr_raw = fields.get("住所", "")
        post_code, pref, addr = self._split_address(addr_raw)

        hp = ""
        for dl in soup.select(".membersearch-detail dl"):
            dt = dl.select_one("dt")
            if dt and dt.get_text(strip=True) == "URL":
                link = dl.select_one("dd a[href]")
                if link:
                    hp = link["href"].strip()
                break

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: fields.get("電話番号", ""),
            Schema.REP_NM: fields.get("代表者", ""),
            Schema.HP: hp,
            Schema.CAT_SITE: fields.get("業務種別", ""),
            "FAX": fields.get("FAX", ""),
        }

    def _parse_list_only(self, dl, source_url: str, name: str) -> dict | None:
        if not name:
            return None
        addr_raw = ""
        add_el = dl.select_one("dd.search-add")
        if add_el:
            addr_raw = add_el.get_text(" ", strip=True)
        post_code, pref, addr = self._split_address(addr_raw)

        tel = ""
        fax = ""
        tel_el = dl.select_one("dd.search-tel")
        if tel_el:
            tel_txt = tel_el.get_text(" ", strip=True)
            m_tel = re.search(r"TEL[：:]\s*([0-9\-]+)", tel_txt)
            m_fax = re.search(r"FAX[：:]\s*([0-9\-]+)", tel_txt)
            tel = m_tel.group(1) if m_tel else ""
            fax = m_fax.group(1) if m_fax else ""

        rep_el = dl.select_one("dd.search-representative")
        rep = rep_el.get_text(strip=True) if rep_el else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: "",
            Schema.CAT_SITE: "",
            "FAX": fax,
        }

    @staticmethod
    def _split_address(addr_raw: str) -> tuple[str, str, str]:
        """住所文字列から 郵便番号 / 都道府県(北海道固定) / 住所 を分離する。"""
        post_code = ""
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = post_code[:3] + "-" + post_code[3:]
            addr_raw = _POST_RE.sub("", addr_raw, count=1)

        addr_raw = addr_raw.replace("〒", "").strip()
        pref = ""
        addr = addr_raw
        if addr_raw.startswith("北海道"):
            pref = "北海道"
            addr = addr_raw[len("北海道"):].strip()
        return post_code, pref, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute(
        "https://www.hssa.or.jp/search/index.html?company_name_s=&subdivision_s=0&address_s=&search=%E6%A4%9C%E7%B4%A2%E3%81%99%E3%82%8B"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

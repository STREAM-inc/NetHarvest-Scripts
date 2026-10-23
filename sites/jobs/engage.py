"""
engage (エンゲージ) — 企業の採用ページ (会社単位)

対象サイト: https://en-gage.net/#/

取得フロー:
    1. {url のルート}/sitemap_company_index.xml から sitemap_company_NNNN.xml.gz
       (application/x-gzip, 1 本あたり最大 50,000 社 / 計 約 15 万社) を列挙
    2. 各 gz サイトマップを展開し、会社採用ページ /{slug}/ を取り出す
    3. 会社ページを get_soup() で取得し、table.companyTable (会社名 / 事業内容 /
       企業WEBサイト / 企業SNS / 所在地) と採用情報欄から抽出して 1 社ごとに即 yield

備考:
    - url は SPA のハッシュ付き (#/) だが、ハッシュはサーバに送られないため
      urljoin でルートからサイトマップ URL を派生させる。
    - 求人詳細 (/user/search/desc/{id}/, 約 230 万件) には会社の追加項目 (TEL・設立等) が
      無いため、会社ページを 1 レコードとする。TEL / 代表者 / 資本金 は構造化された形では
      掲載されていない (一部企業の自由記述欄のみ) ため取得しない。
    - 事業内容は短い項目行 (箇条書き) のみを採用し、段落形式の文章は著作権リスク回避のため除外。
    - キャッチコピー・MESSAGE・BUSINESS 等の自由記述プロースは取得しない。
    - 利用規約 (https://en-gage.net/user/terms/) はスクレイピングを明示禁止していない。

実行方法:
    python scripts/sites/jobs/engage.py
"""

import gzip
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|.{2,3}県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_LOC_PATTERN = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
# 会社ページは /{slug}/ の 1 階層のみ
_COMPANY_PATH = re.compile(r"^/[^/]+/$")
# 事業内容のうち箇条書きとみなす 1 行の最大長
_LOB_LINE_MAX = 40


class EngageScraper(StaticCrawler):
    """engage 会社採用ページ スクレイパー"""

    DELAY = 3.0
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = [
        "企業ID",
        "最終更新日",
        "採用区分",
        "掲載求人数",
        "緯度経度",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        index_url = urljoin(url, "/sitemap_company_index.xml")
        seen: set[str] = set()
        for sitemap_url in self._locs(self._fetch_xml(index_url)):
            for company_url in self._locs(self._fetch_xml(sitemap_url)):
                if not _COMPANY_PATH.match(urlparse(company_url).path):
                    continue
                if company_url in seen:
                    continue
                seen.add(company_url)
                item = self._parse_company(company_url)
                if item:
                    yield item

    # ------------------------------------------------------------------ sitemap
    @staticmethod
    def _locs(xml: str) -> list[str]:
        return _LOC_PATTERN.findall(xml)

    def _fetch_xml(self, url: str) -> str:
        """サイトマップ (gzip の場合あり) を取得してテキストで返す。"""
        try:
            res = self.session.get(url, timeout=self.TIMEOUT)
            res.raise_for_status()
        except Exception as e:  # noqa: BLE001 — 1 本失敗しても他のサイトマップは続行する
            logger.warning("サイトマップの取得に失敗: %s — %s", url, e)
            return ""
        body = res.content
        if body[:2] == b"\x1f\x8b":  # gzip マジックナンバー
            try:
                body = gzip.decompress(body)
            except OSError as e:
                logger.warning("サイトマップの展開に失敗: %s — %s", url, e)
                return ""
        return body.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------ company
    def _parse_company(self, company_url: str) -> dict | None:
        soup = self.get_soup(company_url)
        if soup is None:
            return None
        table = soup.select_one("table.companyTable")
        if table is None:
            logger.info("会社情報テーブル無し (スキップ): %s", company_url)
            return None

        rows: dict[str, bs4.Tag] = {}
        for tr in table.select("tr"):
            th, td = tr.select_one("th"), tr.select_one("td")
            if th and td:
                rows[th.get_text("", strip=True)] = td

        name = self._text(rows.get("会社名"))
        if not name:
            return None

        addr_raw = self._text(rows.get("所在地"))
        post_code = ""
        m = _POST_PATTERN.search(addr_raw)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"
            addr_raw = (addr_raw[: m.start()] + addr_raw[m.end():]).strip()
        pref = ""
        m = _PREF_PATTERN.match(addr_raw)
        if m:
            pref = m.group(1)
            addr_raw = addr_raw[m.end():].strip()

        website = ""
        site_td = rows.get("企業WEBサイト")
        if site_td:
            a = site_td.select_one("a[href]")
            website = a["href"].strip() if a else self._text(site_td)

        sns = {"is": "", "fb": "", "x": "", "tw": "", "line": "", "tt": ""}
        sns_td = rows.get("企業SNS")
        if sns_td:
            for li in sns_td.select("li.sns"):
                a = li.select_one("a[href]")
                if not a:
                    continue
                for cls in li.get("class", []):
                    if cls in sns:
                        sns[cls] = a["href"].strip()
        insta, fb, x, line, tiktok = sns["is"], sns["fb"], sns["x"] or sns["tw"], sns["line"], sns["tt"]
        # クラス名が想定外の場合はドメインで補完
        if sns_td:
            for a in sns_td.select("a[href]"):
                href = a["href"].strip()
                if "instagram.com" in href and not insta:
                    insta = href
                elif "facebook.com" in href and not fb:
                    fb = href
                elif ("twitter.com" in href or "x.com" in href) and not x:
                    x = href
                elif "line.me" in href and not line:
                    line = href
                elif "tiktok.com" in href and not tiktok:
                    tiktok = href

        updated = ""
        upd = soup.select_one(".el_panel .upDate")
        if upd:
            updated = upd.get_text("", strip=True).replace("最終更新日：", "").strip()

        categories = [dt.get_text(strip=True) for dt in soup.select("dl.jobList dt.category")]
        job_count = len(soup.select("dl.jobList dd.dataArea"))

        latlng = ""
        ll = soup.select_one("#mapMakeLatLng")
        if ll and ll.get("value"):
            latlng = ll["value"].strip()

        return {
            Schema.URL: company_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr_raw,
            Schema.LOB: self._lob(rows.get("事業内容")),
            Schema.HP: website,
            Schema.INSTA: insta,
            Schema.FB: fb,
            Schema.X: x,
            Schema.LINE: line,
            Schema.TIKTOK: tiktok,
            "企業ID": urlparse(company_url).path.strip("/"),
            "最終更新日": updated,
            "採用区分": " / ".join(categories),
            "掲載求人数": str(job_count),
            "緯度経度": latlng,
        }

    @staticmethod
    def _text(td: bs4.Tag | None) -> str:
        if td is None:
            return ""
        return re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()

    @staticmethod
    def _lob(td: bs4.Tag | None) -> str:
        """事業内容から短い箇条書き行のみを抽出 (段落形式の文章は除外)。"""
        if td is None:
            return ""
        lines = []
        for raw in td.get_text("\n").split("\n"):
            line = raw.strip()
            if not line or re.fullmatch(r"【[^】]*】", line):
                continue
            # 区切り線 (-----) 以降は補足の文章になるため打ち切る
            if len(line) >= 3 and set(line) <= set("-－ー=＝―─━"):
                break
            line = line.lstrip("■□●○◆◇・-－ ")
            # 読点止まり = 折り返された段落の始まりとみなし打ち切る
            if line.endswith(("、", "，", ",")):
                break
            # 長い行・句点を含む行 = 箇条書きの間に挟まる説明文として除外
            if len(line) > _LOB_LINE_MAX or "。" in line:
                continue
            if line:
                lines.append(line)
        return " / ".join(lines)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EngageScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://en-gage.net/#/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

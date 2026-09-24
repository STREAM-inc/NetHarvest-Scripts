"""
食品開発展 出展社リスト (informa-japan.com) — 出展社の社名・ブース番号・出展製品・HP を取得

対象サイト:
    https://www.informa-japan.com/hi/complist/
    食品開発展 (Hi / FiT / S-tec / LLj の4展構成、インフォーマ マーケッツ ジャパン主催)
    の出展社リスト (旧システム / アーカイブ)。

取得フロー:
    1. 引数 url (= 既定年の出展社リスト) を取得し、50音セクション (h4 + table.exhibitor)
       の全行を走査する。1ページに全50音行が含まれており、ページ送りは無い。
    2. 続けて index.php?currentYear=YYYY を「今年 → OLDEST_YEAR」の降順で巡回し、
       掲載のある開催年をすべて取得する (掲載が無い年は表が空なので自動的にスキップされる)。
       新しい年を優先するため、社名/出展社IDが既出の場合はスキップする。
    3. 各行ごとに詳細ページ (detail.php?exid=...) を取得し、HP・出展製品・プレゼン日程を
       付与して即 yield する。

備考 (取得カラムに関する調査結果):
    - 「所在地」はサイト上 (一覧・詳細・出展社検索のいずれ) にも一切掲載が無いため取得不可。
    - 「出展分野」は 出展社検索 (searchlist.php) のカテゴリ検索でのみ絞り込めるが、
      出展社ごとのカテゴリは画面に表示されない。代わりに出展展示会区分 (Hi/FiT/S-tec/LLj)
      を Schema.CAT_SITE に、詳細ページの「出展製品」(取扱商品) を EXTRA に格納する。
    - 網羅性: 全167カテゴリを OR 指定した検索結果 (2020年 344社) は
      50音一覧 (2020年 350社) の部分集合であることを確認済み。
      よって50音一覧の巡回だけで全出展社を網羅できる。
    - 「見どころ」およびプレゼンの「タイトル/詳細」は長文の自由記述のため取得対象外。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/informa_japan.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id informa_japan
"""

import datetime
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class InformaJapanCrawler(StaticCrawler):
    """食品開発展 出展社リスト スクレイパー"""

    DELAY = 0.5

    EXTRA_COLUMNS = [
        "開催年",
        "ブース番号",
        "出展製品",
        "五十音区分",
        "新製品・新技術",
        "試飲・試食・デモ",
        "プレゼン参加",
        "プレゼン日程",
        "出展社ID",
    ]

    # 掲載が確認できている最古の開催年 (これより古い年は index.php が空表を返す)
    OLDEST_YEAR = 2018

    # 出展展示会区分 (一覧の img.logo alt) の表記ゆれを揃える
    _KBN_MAP = {"hi": "Hi", "fit": "FiT", "s-tec": "S-tec", "stec": "S-tec", "llj": "LLj"}

    # 法人格など、社名の重複判定では無視する表記
    _CORP_RE = re.compile(r"株式会社|有限会社|合同会社|㈱|㈲|\(株\)|\(有\)")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _txt(node) -> str:
        """ノードのテキストを空白正規化して返す"""
        if node is None:
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

    def _norm_name(self, name: str) -> str:
        """社名の重複判定用キー (全半角・法人格・記号・空白の揺れを吸収する)"""
        key = unicodedata.normalize("NFKC", name)
        key = self._CORP_RE.sub("", key)
        key = re.sub(r"[\s　・（）()＆&,．.\-—―ー]", "", key)
        return key.lower()

    @staticmethod
    def _extract_year(soup) -> str:
        """見出し「出展社リスト (2021年)」から既定の開催年を取り出す"""
        h1 = soup.find("h1")
        m = re.search(r"(20\d{2})\s*年", h1.get_text() if h1 else "")
        return m.group(1) if m else ""

    def _split_lines(self, td) -> list[str]:
        """<br> 区切りの td を行リストに分解する (バッジ span は除去)"""
        for badge in td.select("span.catNew, span.catDemo, span.catPre"):
            badge.decompose()
        for br in td.find_all("br"):
            br.replace_with("\n")
        lines = []
        for raw in td.get_text("\n").split("\n"):
            line = re.sub(r"\s+", " ", raw).strip()
            if line:
                lines.append(line)
        return lines

    def _parse_detail(self, detail_url: str) -> dict:
        """詳細ページから HP / 出展製品 / プレゼン日程 / ブース番号 を取得する"""
        result = {"hp": "", "products": "", "presentations": "", "booth": ""}

        soup = self.get_soup(detail_url)
        if soup is None:
            return result

        for th in soup.select("div#tblBl th"):
            label = self._txt(th)
            td = th.find_next_sibling("td")
            if td is None:
                continue

            if label == "URL":
                a = td.find("a", href=True)
                href = a["href"].strip() if a else ""
                if not href and self._txt(td).startswith("http"):
                    href = self._txt(td)
                result["hp"] = href

            elif label == "出展製品":
                result["products"] = " / ".join(self._split_lines(td))

            elif label == "ブース番号" and not result["booth"]:
                result["booth"] = self._txt(td)

            elif label == "日付":
                # プレゼン日程 (日付 + 会場)。講演タイトル・詳細本文は取得しない
                date = self._txt(td)
                venue_th = td.find_next_sibling("th")
                venue = ""
                if venue_th is not None and self._txt(venue_th) == "会場":
                    venue = self._txt(venue_th.find_next_sibling("td"))
                entry = " ".join(x for x in (date, venue) if x)
                if entry:
                    result["presentations"] = (
                        f"{result['presentations']} / {entry}" if result["presentations"] else entry
                    )

        return result

    def _parse_list(self, soup, page_url: str, year: str, seen: set):
        """1年分の出展社リストページを走査し、1件ずつ yield する"""
        for h4 in soup.find_all("h4"):
            group = self._txt(h4)                     # 五十音区分 (あ/か/さ ...)
            table = h4.find_next_sibling()
            if table is None or table.name != "table":
                continue
            if "exhibitor" not in (table.get("class") or []):
                continue

            for tr in table.find_all("tr"):
                name_td = tr.find("td", class_="name")
                if name_td is None:
                    continue

                a = name_td.find("a", href=True)
                name = self._txt(a) if a else self._txt(name_td)
                if not name:
                    continue

                exid = ""
                detail_url = ""
                if a:
                    detail_url = urljoin(page_url, a["href"])
                    m = re.search(r"exid=(\w+)", a["href"])
                    exid = m.group(1) if m else ""

                # 新しい開催年を優先し、既出の出展社 (ID または社名一致) はスキップする
                keys = {f"name:{self._norm_name(name)}"}
                if exid:
                    keys.add(f"exid:{exid}")
                if keys & seen:
                    continue
                seen |= keys

                logo = name_td.find("img", class_="logo")
                kbn = (logo.get("alt") or "").strip() if logo else ""
                kbn = self._KBN_MAP.get(kbn.lower(), kbn)

                booth = self._txt(tr.find("td", class_="boothno"))
                flag_new = "有" if tr.select_one("td.new span") else ""
                flag_demo = "有" if tr.select_one("td.demo span") else ""
                flag_presen = "有" if tr.select_one("td.presen span") else ""

                detail = {"hp": "", "products": "", "presentations": "", "booth": ""}
                if detail_url:
                    detail = self._parse_detail(detail_url)

                yield {
                    Schema.NAME: name,
                    Schema.CAT_SITE: kbn,
                    Schema.HP: detail["hp"],
                    Schema.URL: detail_url or page_url,
                    "開催年": year,
                    "ブース番号": booth or detail["booth"],
                    "出展製品": detail["products"],
                    "五十音区分": group,
                    "新製品・新技術": flag_new,
                    "試飲・試食・デモ": flag_demo,
                    "プレゼン参加": flag_presen,
                    "プレゼン日程": detail["presentations"],
                    "出展社ID": exid,
                }

    # -------------------------------------------------------------------- parse
    def parse(self, url: str):
        seen: set = set()

        # 1) 引数 url がそのまま返す既定年
        soup = self.get_soup(url)
        default_year = ""
        if soup is None:
            self.logger.error("一覧ページを取得できませんでした: %s", url)
        else:
            default_year = self._extract_year(soup)
            self.logger.info("既定の開催年: %s", default_year or "(不明)")
            yield from self._parse_list(soup, url, default_year, seen)

        # 2) 過去の開催年を新しい順に巡回する (掲載の無い年は表が空なので自然にスキップされる)
        index_url = urljoin(url, "index.php")
        for year in range(datetime.date.today().year, self.OLDEST_YEAR - 1, -1):
            y = str(year)
            if y == default_year:
                continue
            page_url = f"{index_url}?currentYear={y}"
            year_soup = self.get_soup(page_url)
            if year_soup is None:
                continue
            yield from self._parse_list(year_soup, page_url, y, seen)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = InformaJapanCrawler()
    scraper.execute("https://www.informa-japan.com/hi/complist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

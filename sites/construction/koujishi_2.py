"""工事士.com (koujishi_2) — 電気工事求人サイトの掲載企業情報スクレイパー

トップページ (https://koujishi.com/) の「都道府県から探す」リンクを起点に、
47 都道府県の求人一覧 (/list/{pref}/{page}/) を巡回し、求人詳細ページ
(/detail/{id}/) から掲載企業の会社概要・連絡先を取得する。
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭から都道府県を切り出す
PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}県)")
# 一覧カードの詳細リンク (/detail/1500352/ , /detail/1500352/?option 等)
DETAIL_ID_RE = re.compile(r"/detail/(\d+)/")
# 一覧ページの「該当求人件数： 762件」
TOTAL_RE = re.compile(r"該当求人件数[：:]\s*([\d,]+)\s*件")
# 1 ページあたりの掲載件数
PER_PAGE = 10
# 念のためのページ送り上限 (1 都道府県あたり)
MAX_PAGES = 200


class Koujishi2Scraper(StaticCrawler):
    """工事士.com 掲載企業スクレイパー (全国版)"""

    DELAY = 0.8
    EXTRA_COLUMNS = [
        "職種（詳細）",
        "雇用形態",
        "想定年収",
        "主要取引先",
        "会社の特徴",
        "FAX",
        "採用担当者",
        "掲載期間",
        "情報更新日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """トップページ → 都道府県別一覧 → 求人詳細 の順に巡回する。"""
        list_urls = self._collect_list_urls(url)
        self.logger.info("一覧URL: %d 件", len(list_urls))

        seen_ids: set[str] = set()   # 全体の重複排除 (同じ求人が複数県に掲載される)
        for list_url in list_urls:
            page_seen: set[str] = set()   # 都道府県内でのページ送り判定用
            total: int | None = None
            for page in range(1, MAX_PAGES + 1):
                page_url = list_url if page == 1 else f"{list_url}{page}/"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                if total is None:
                    total = self._extract_total(soup)

                page_ids = self._extract_detail_ids(soup)
                fresh = [i for i in page_ids if i not in page_seen]
                if not fresh:
                    # 広告枠しか無い = そのエリアのページ送り終端
                    break
                page_seen.update(fresh)

                for detail_id in fresh:
                    if detail_id in seen_ids:
                        continue
                    seen_ids.add(detail_id)
                    detail_url = urljoin(url, f"detail/{detail_id}/")
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item

                if total is not None and page * PER_PAGE >= total:
                    break

    # ------------------------------------------------------------------ 一覧
    def _collect_list_urls(self, url: str) -> list[str]:
        """トップページの「都道府県から探す」から 47 都道府県の一覧URLを集める。"""
        soup = self.get_soup(url)
        urls: list[str] = []
        if soup is not None:
            for heading in soup.find_all(["h2", "h3"]):
                if "都道府県" not in heading.get_text(strip=True):
                    continue
                container = heading.parent
                if container is None:
                    continue
                for a in container.select('a[href^="/list/"]'):
                    href = a.get("href", "").split("?")[0]
                    if not re.fullmatch(r"/list/[a-z]+/", href):
                        continue
                    full = urljoin(url, href)
                    if full not in urls:
                        urls.append(full)
                if urls:
                    break
        if not urls:
            # 都道府県リンクを取れなかった場合は全国一覧にフォールバック
            urls = [urljoin(url, "list/")]
        return urls

    def _extract_total(self, soup) -> int | None:
        m = TOTAL_RE.search(re.sub(r"\s+", " ", soup.get_text(" ", strip=True)))
        if not m:
            return None
        return int(m.group(1).replace(",", ""))

    def _extract_detail_ids(self, soup) -> list[str]:
        ids: list[str] = []
        for a in soup.select('a[href*="/detail/"]'):
            m = DETAIL_ID_RE.search(a.get("href", ""))
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
        return ids

    # ------------------------------------------------------------------ 詳細
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {key: "" for key in self.EXTRA_COLUMNS}
        data[Schema.URL] = url

        company = self._company_table(soup)
        data[Schema.NAME] = company.get("社名", "")
        if not data[Schema.NAME]:
            return None

        data[Schema.REP_NM] = company.get("代表者", "")
        data[Schema.CAP] = company.get("資本金", "")
        data[Schema.SALES] = company.get("売上高", "")
        data[Schema.EMP_NUM] = company.get("従業員数", "")
        data[Schema.LOB] = self._clean(company.get("事業内容", ""))
        data["主要取引先"] = self._clean(company.get("主要取引先", ""))
        data[Schema.OPEN_DATE] = self._to_date(company.get("設立", ""))
        data[Schema.HP] = self._homepage(soup) or company.get("ホームページ", "")

        post_code, pref, addr = self._split_address(company.get("事業所", ""))
        data[Schema.POST_CODE] = post_code
        data[Schema.PREF] = pref
        data[Schema.ADDR] = addr

        # 連絡先 (表示は伏字だが RSC ペイロードに平文で含まれる)
        contact = self._contact(soup)
        data[Schema.TEL] = contact.get("contactTel", "")
        data[Schema.PHONE] = contact.get("contactMobileTel", "")
        data["FAX"] = contact.get("contactFax", "")
        data[Schema.EMAIL] = contact.get("notificationMail", "")
        data["採用担当者"] = contact.get("contactPerson", "")

        # 募集要項テーブル (ラベル / 値)
        recruit = self._recruit_table(soup)
        data[Schema.CAT_SITE] = self._blank_placeholder(recruit.get("職種カテゴリ", ""))
        data["職種（詳細）"] = self._blank_placeholder(recruit.get("職種（詳細）", ""))
        # 「正社員 ※ 試用期間3ヶ月…」の補足を落として区分だけ残す
        data["雇用形態"] = self._blank_placeholder(
            re.split(r"\s*※", recruit.get("雇用形態", ""))[0].strip())
        data["想定年収"] = self._blank_placeholder(recruit.get("想定年収", ""))

        data["会社の特徴"] = self._company_tags(soup)

        period, updated = self._period(soup)
        data["掲載期間"] = period
        data["情報更新日"] = updated

        return data

    def _company_table(self, soup) -> dict[str, str]:
        """会社概要テーブル (th / td) を辞書化する。"""
        kv: dict[str, str] = {}
        table = soup.select_one('div[class*="CompanyDetailTable"] table')
        if table is None:
            return kv
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = self._clean(th.get_text(" ", strip=True))
            # <br> 区切りの複数事業所を残しつつ、<!-- --> 由来の余分な空白を作らない
            for br in td.find_all("br"):
                br.replace_with("\n")
            value = self._clean_multiline(td.get_text("", strip=False))
            if label:
                kv[label] = value
        return kv

    def _homepage(self, soup) -> str:
        table = soup.select_one('div[class*="CompanyDetailTable"] table')
        if table is None:
            return ""
        for tr in table.select("tr"):
            th = tr.find("th")
            if th and "ホームページ" in th.get_text(strip=True):
                a = tr.find("a", href=True)
                if a:
                    return a["href"].strip()
        return ""

    def _company_tags(self, soup) -> str:
        box = soup.select_one('div[class*="companyTags"]')
        if box is None:
            return ""
        tags = [self._clean(t.get_text(" ", strip=True))
                for t in box.select('a[class*="secTag"], span[class*="secTag__"]')]
        tags = [t for t in tags if t]
        return " / ".join(dict.fromkeys(tags))

    def _recruit_table(self, soup) -> dict[str, str]:
        kv: dict[str, str] = {}
        for tr in soup.select('table[class*="RecruitGroupTable"] tr'):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            label = self._clean(cells[0].get_text(" ", strip=True))
            value = self._clean(cells[1].get_text(" ", strip=True))
            if label and label not in kv:
                kv[label] = value
        return kv

    def _contact(self, soup) -> dict[str, str]:
        """RSC フライトデータから連絡先 (電話・FAX・メール・担当者) を取り出す。"""
        result: dict[str, str] = {}
        script_text = "".join(s.get_text() for s in soup.find_all("script"))
        for key in ("contactTel", "contactMobileTel", "contactFax",
                    "notificationMail", "contactPerson"):
            m = re.search(key + r'\\?"\s*:\s*\\?"([^"\\]*)', script_text)
            if m:
                result[key] = self._clean(m.group(1))
        return result

    def _period(self, soup) -> tuple[str, str]:
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
        period = ""
        updated = ""
        m = re.search(r"掲載期間[：:]\s*(\d{4}/\d{1,2}/\d{1,2})\s*[～~〜]\s*(\d{4}/\d{1,2}/\d{1,2})", text)
        if m:
            period = f"{m.group(1)}～{m.group(2)}"
        m = re.search(r"更新日[：:]\s*(\d{4}/\d{1,2}/\d{1,2})", text)
        if m:
            updated = m.group(1)
        return period, updated

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").replace("\u3000", " ")).strip()

    @classmethod
    def _clean_multiline(cls, text: str) -> str:
        """改行 (=元の <br>) は残したまま、行ごとに空白を整える。"""
        lines = [cls._clean(line) for line in (text or "").split("\n")]
        return "\n".join([line for line in lines if line])

    @staticmethod
    def _blank_placeholder(value: str) -> str:
        """「-」「ー」等のプレースホルダは空文字にする。"""
        return "" if value.strip() in {"-", "ー", "―", "–", "—", "‐"} else value

    def _split_address(self, office: str) -> tuple[str, str, str]:
        """事業所欄の先頭 (本社) から 郵便番号 / 都道府県 / 住所 を切り出す。"""
        if not office:
            return "", "", ""
        head = office.split("\n")[0]
        head = re.sub(r"^\s*[^：:]{0,12}[：:]\s*", "", head).strip()
        head = self._clean(head)
        post_code = ""
        m = re.search(r"〒\s*(\d{3})\s*-\s*(\d{4})", head)
        if m:
            post_code = f"〒{m.group(1)}-{m.group(2)}"
            head = head[m.end():].strip()
        head = re.sub(r"^〒\s*", "", head).strip()
        pref = ""
        m = PREF_RE.search(head)
        if m and m.start() <= 1:
            pref = m.group(1)
        return post_code, pref, head

    @staticmethod
    def _to_date(value: str) -> str:
        """「2019年4月」「1982年7月1日」等を YYYY-MM-DD / YYYY-MM に整形する。"""
        if not value:
            return ""
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", value)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", value)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}"
        m = re.search(r"(\d{4})\s*年", value)
        if m:
            return m.group(1)
        return value


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    Koujishi2Scraper().execute("https://koujishi.com/")

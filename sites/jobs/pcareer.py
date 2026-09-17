"""
対象サイト: https://pcareer.m3.com/ （薬キャリ／薬剤師求人）

トップページ -> 都道府県別求人一覧 (/positions/pr_{pref}) -> 求人詳細 を巡回する。
求人詳細は「コンサルタント経由 (/positions/pr_x/city_y/{id})」と
「直接応募 (/direct/positions/...)」の 2 系統があるが、DOM のクラス名は共通。
"""
import math
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

# 一覧 1 ページあたりの求人件数（サイト仕様）
PER_PAGE = 20
# 暴走防止のページ数上限（1 都道府県あたり）
MAX_PAGES = 2000

_PREF_LINK_RE = re.compile(r"^/positions/pr_[a-z_]+$")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
_COUNT_RE = re.compile(r"([\d,]+)\s*件\s*中")
_NO_RE = re.compile(r"薬剤師求人No\.?\s*(\S+)")
_UPDATE_RE = re.compile(r"更新日[:：]\s*(\d{4})年(\d{1,2})月(\d{1,2})日")
_DATE_FULL_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
_DATE_YM_RE = re.compile(r"(\d{4})年(\d{1,2})月")


class PcareerScraper(StaticCrawler):
    """薬キャリ 薬剤師求人スクレイパー（pcareer.m3.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人番号",
        "更新日",
        "求人形態",
        "情報提供企業",
        "情報提供企業法人名",
        "給与",
        "勤務時間",
        "雇用形態",
        "募集職種",
        "最寄駅",
        "処方枚数",
        "転勤の有無",
        "こだわり条件",
        "事業所数",
        "厚生労働大臣認可",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref_url in self._collect_pref_urls(url):
            yield from self._crawl_pref(pref_url, seen)

    # ------------------------------------------------------------------
    # 一覧側
    # ------------------------------------------------------------------
    def _collect_pref_urls(self, url: str) -> list[str]:
        """トップページから 47 都道府県の求人一覧 URL を集める。"""
        soup = self.get_soup(url)
        pref_urls: list[str] = []
        if soup is not None:
            for a in soup.find_all("a", href=True):
                href = a["href"].split("?")[0].rstrip("/")
                if _PREF_LINK_RE.match(href):
                    full = urljoin(url, href)
                    if full not in pref_urls:
                        pref_urls.append(full)
        if not pref_urls:
            # 万一トップの構成が変わった場合は全国一覧にフォールバック
            pref_urls = [urljoin(url, "/positions")]
        self.logger.info("都道府県一覧: %d 件", len(pref_urls))
        return pref_urls

    def _crawl_pref(self, pref_url: str, seen: set[str]) -> Generator[dict, None, None]:
        max_page = MAX_PAGES
        page = 1
        while page <= max_page:
            page_url = pref_url if page == 1 else f"{pref_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", page_url)
                return

            if page == 1:
                total = self._read_total(soup)
                if total:
                    max_page = min(MAX_PAGES, math.ceil(total / PER_PAGE))
                    self.logger.info("%s: 全 %d 件 / %d ページ", pref_url, total, max_page)

            cards = soup.select("div.m3p-joboffer")
            if not cards:
                return

            for card in cards:
                pos_id = card.get("data-position-id")
                if not pos_id or pos_id in seen:
                    continue
                seen.add(pos_id)
                detail_url = self._card_detail_url(card, page_url)
                if not detail_url:
                    continue
                item = self._scrape_detail(detail_url, self._card_genre(card))
                if item:
                    yield item
            page += 1

    def _read_total(self, soup) -> int | None:
        count_el = soup.select_one(".m3p-pager__count")
        text = count_el.get_text(" ", strip=True) if count_el else soup.get_text(" ", strip=True)
        m = _COUNT_RE.search(text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                return None
        return None

    def _card_detail_url(self, card, base_url: str) -> str | None:
        for a in card.find_all("a", href=True):
            href = a["href"]
            if "/positions/" in href and re.search(r"/\d{4,}(\?|$|/)", href):
                return urljoin(base_url, href)
        return None

    def _card_genre(self, card) -> str:
        """カード左上のステータス欄先頭が施設業種（調剤薬局・ドラッグストア等）。"""
        status = card.select_one(".m3p-joboffer__status li")
        return status.get_text(strip=True) if status else ""

    # ------------------------------------------------------------------
    # 詳細側
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, genre: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        info = self._detail_table(soup)
        company = self._company_table(soup)
        bc_pref, bc_city = self._breadcrumb(soup)

        # 勤務地: 1 行目が住所、2 行目以降が最寄駅
        work_place = info.get("勤務地", "")
        lines = [ln.strip() for ln in work_place.split("\n") if ln.strip()]
        address_raw = lines[0] if lines else ""
        station = " / ".join(lines[1:])

        pref = ""
        m = _PREF_RE.match(address_raw)
        if m:
            pref = m.group(1)
            addr = address_raw[len(pref):].strip()
        else:
            addr = address_raw.strip()
        if not pref:
            pref = bc_pref
        if not addr:
            addr = bc_city

        # 施設名（非公開の求人は空欄）
        copy_el = soup.select_one("h2.m3p-detail__copy")
        facility = copy_el.get_text(" ", strip=True) if copy_el else ""
        if "非公開" in facility:
            facility = ""

        provider, update_date, job_no = self._meta(soup)
        provider_corp = self._provider_corp(soup)

        name = facility or company.get("名称", "") or provider
        if not name:
            return None

        tags = [t.get_text(strip=True) for t in soup.select("ul.m3p-detail__tag li")]
        tags = [t for t in tags if t]

        # 給与は 1 行目の金額レンジのみ採用（以降は補足文のため取得しない）
        salary_first = info.get("給与・手当", "").split("\n")[0]
        salary_first = re.split(r"[※（(]", salary_first)[0].strip()

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.FAC_NAME: facility,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: genre,
            Schema.HOLIDAY: " / ".join(
                ln.strip() for ln in info.get("休日・休暇", "").split("\n") if ln.strip()
            ),
            Schema.TIME: " / ".join(
                ln.strip() for ln in info.get("営業時間", "").split("\n") if ln.strip()
            ),
            Schema.CAP: company.get("資本金", ""),
            Schema.EMP_NUM: company.get("従業員数", ""),
            Schema.OPEN_DATE: self._to_date(company.get("設立", "")),
            "求人番号": job_no,
            "更新日": update_date,
            "求人形態": "直接応募" if "/direct/" in url else "コンサルタント経由",
            "情報提供企業": provider,
            "情報提供企業法人名": provider_corp,
            "給与": salary_first,
            "勤務時間": " / ".join(
                ln.strip() for ln in info.get("勤務時間", "").split("\n") if ln.strip()
            ),
            "雇用形態": info.get("雇用形態", "").replace("\n", " ").strip(),
            "募集職種": info.get("募集職種（人数）", "").replace("\n", " ").strip(),
            "最寄駅": station.lstrip("-－ ").strip(),
            "処方枚数": info.get("処方枚数", "").replace("\n", " ").strip(),
            "転勤の有無": info.get("転勤の有無", "").replace("\n", " ").strip(),
            "こだわり条件": "、".join(tags),
            "事業所数": company.get("事業所数", "") or company.get("事務所数", ""),
            "厚生労働大臣認可": re.sub(
                r"\s+", " ", company.get("厚生労働大臣認可", "")
            ).strip(),
        }

    def _detail_table(self, soup) -> dict:
        """求人条件テーブル（1 行に th/td が 2 組並ぶ場合あり）をラベル辞書にする。"""
        data: dict[str, str] = {}
        for table in soup.select("table.m3p-detail__table"):
            classes = table.get("class") or []
            if "m3p-joblist__dtl-blur" in classes:
                # 会員限定のぼかしテーブル（中身は空）
                continue
            for row in table.find_all("tr"):
                label = ""
                for cell in row.find_all(["th", "td"], recursive=False):
                    if cell.name == "th":
                        label = cell.get_text(" ", strip=True)
                    elif label:
                        value = cell.get_text("\n", strip=True)
                        value = re.sub(r"\n{2,}", "\n", value).replace("\xa0", " ").strip()
                        if value and label not in data:
                            data[label] = value
                        label = ""
        return data

    def _company_table(self, soup) -> dict:
        data: dict[str, str] = {}
        table = soup.select_one("table.m3p-company__table")
        if not table:
            return data
        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                data[th.get_text(strip=True)] = td.get_text("\n", strip=True)
        return data

    def _breadcrumb(self, soup) -> tuple[str, str]:
        items = [li.get_text(strip=True) for li in soup.select("ol.m3-breadcrumbs li")]
        pref = city = ""
        for text in items:
            if not pref and _PREF_RE.match(text):
                pref = _PREF_RE.match(text).group(1)
            elif pref and not city and text not in ("求人詳細", "全国"):
                city = text
        return pref, city

    def _meta(self, soup) -> tuple[str, str, str]:
        """情報提供企業 / 更新日 / 求人番号 を詳細ヘッダから取り出す。"""
        provider = update_date = job_no = ""
        meta = soup.select_one("ul.m3p-detail__info")
        if not meta:
            return provider, update_date, job_no
        text = meta.get_text("\n", strip=True)
        for line in text.split("\n"):
            line = line.strip()
            if "情報提供企業" in line:
                provider = line.split("：", 1)[-1].split(":", 1)[-1].strip()
            m = _UPDATE_RE.search(line)
            if m:
                update_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            m = _NO_RE.search(line)
            if m:
                job_no = m.group(1)
        return provider, update_date, job_no

    def _provider_corp(self, soup) -> str:
        block = soup.select_one("div.m3p-consultant__companies")
        if not block:
            return ""
        span = block.find("span")
        return span.get_text(strip=True) if span else ""

    def _to_date(self, text: str) -> str:
        if not text:
            return ""
        m = _DATE_FULL_RE.search(text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = _DATE_YM_RE.search(text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}"
        return text.strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    PcareerScraper().execute("https://pcareer.m3.com/")

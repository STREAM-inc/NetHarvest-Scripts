"""
ビズケン — 建設業界専門の求人・転職情報サイト (biz-ken.com)
対象サイト: https://biz-ken.com/job_area/miyagi/

取得対象:
    東北・北陸の10県 (青森/岩手/宮城/秋田/山形/福島/新潟/富山/石川/福井) の
    勤務地別求人一覧に掲載されている全求人。

取得フロー:
    勤務地別一覧 /job_area/{県}/ (ページ送り /page/N/, 20件/ページ) を巡回し、
    各求人の詳細ページ /job/{ID}/ を訪問して1件取得するごとに即 yield する (Pattern B)。
    巡回は引数 url の県から開始し、残りの県 URL は url から相対的に導出する。
    複数県に掲載される求人は求人IDで重複除外する。

詳細ページの構造:
    - 企業名: p.introduce span.company
    - 求人タイトル: h1.job-main-title
    - 給与/勤務地(県)/業種: .job-summary-wrapper .terms (アイコン画像名 term-job-{kyuyo,area,gyoshu} で判別)
    - 勤務地住所: h3「勤務地」直後の p.detail (自由記述混在のため住所行のみ抽出)
    - 事業内容/資本金/設立年月日/従業員: .pc-only .card-company (p.summary + table th/td)
    - 職種: パンくず JSON-LD の /job_occupation/ 階層 (無ければ gyoshu アイコン欄。サイト上の名称に反し中身は職種)
    - 業種: タイトルが「社名／勤務地／業種／職種」形式の場合の第3要素 (〜業)。それ以外は空
    TEL・代表者・HP はサイトに掲載なし。

取得しないフィールド (除外):
    仕事内容・応募条件・求める人物像・勤務時間・待遇・休日 — 長文の自由記述のため著作権リスクで除外。

利用規約 (https://biz-ken.com/policy/): スクレイピングの明示的禁止なし。
    第2条6「本サービスの運営の妨げとなる一切の行為」があるため DELAY を長めに取り低負荷で巡回する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/biz_ken.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id biz_ken
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 巡回対象の県スラッグ (サイト上の表記。新潟は "nigata")
_TARGET_PREFS = [
    "aomori", "iwate", "miyagi", "akita", "yamagata",
    "fukushima", "nigata", "toyama", "ishikawa", "fukui",
]

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_JOB_LINK_RE = re.compile(r"/job/(\d+)/?$")
_AREA_RE = re.compile(r"/job_area/([^/]+)/")
_ADDR_LABEL_RE = re.compile(r"(?:住所|所在地)\s*[:：]\s*(.+)")
_INDUSTRY_RE = re.compile(r"業(?:（[^）]*）)?$")
_ERA_OFFSET = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}
_DATE_RE = re.compile(r"(明治|大正|昭和|平成|令和)?\s*(\d{1,4}|元)\s*年\s*(?:(\d{1,2})\s*月)?\s*(?:(\d{1,2})\s*日)?")


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


def _normalize_date(raw: str) -> str:
    """設立年月日を YYYY-MM-DD (日なしは YYYY-MM, 月なしは YYYY) に正規化。解釈不能なら原文。"""
    raw = _clean(raw)
    m = _DATE_RE.search(raw)
    if not m:
        return raw
    era, year, month, day = m.groups()
    year = 1 if year == "元" else int(year)
    if era:
        year += _ERA_OFFSET[era]
    if year < 1800:
        return raw
    out = f"{year:04d}"
    if month:
        out += f"-{int(month):02d}"
        if day:
            out += f"-{int(day):02d}"
    return out


def _extract_address(text: str) -> str:
    """勤務地欄の自由記述から住所1行を抜き出す。"""
    lines = [_clean(l) for l in text.splitlines()]
    lines = [l for l in lines if l]
    for line in lines:
        m = _ADDR_LABEL_RE.search(line)
        if m and _PREF_PATTERN.search(m.group(1)):
            return m.group(1).strip()
    for line in lines:
        m = _PREF_PATTERN.search(line)
        if m and len(line) - m.start() <= 80:
            return line[m.start():].strip()
    return ""


def _industry_from_title(title: str, company: str) -> str:
    """「社名／勤務地／業種／職種」形式のタイトルから業種 (〜業) を取り出す。該当しなければ空。"""
    parts = [p.strip() for p in title.split("／")]
    if len(parts) >= 4 and parts[0] == company and _INDUSTRY_RE.search(parts[2]):
        return parts[2]
    return ""


class BizKenCrawler(StaticCrawler):
    """ビズケン 求人スクレイパー (東北・北陸10県)"""

    DELAY = 3.0  # 規約の運営妨害禁止条項に配慮して低頻度で巡回
    EXTRA_COLUMNS = ["職種", "求人タイトル"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        for area_url in self._area_urls(url):
            yield from self._crawl_area(area_url, seen_ids)

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _area_urls(self, url: str) -> list[str]:
        """引数 url の県を先頭に、対象10県の一覧 URL を url から導出する。"""
        m = _AREA_RE.search(url)
        start = m.group(1) if m else None
        prefs = ([start] if start else []) + [p for p in _TARGET_PREFS if p != start]
        # url と同一オリジンの /job_area/{県}/ を組み立てる
        return [urllib.parse.urljoin(url, f"/job_area/{p}/") for p in prefs]

    def _crawl_area(self, area_url: str, seen_ids: set[str]) -> Generator[dict, None, None]:
        page = 1
        last_page = None
        while True:
            list_url = area_url if page == 1 else f"{area_url}page/{page}/"
            soup = self.get_soup(list_url)
            if soup is None:
                return
            if last_page is None:
                last_page = self._last_page(soup)

            job_urls = []
            for a in soup.select("main .archive a[href]") or soup.select("main a[href]"):
                href = urllib.parse.urljoin(list_url, a["href"])
                jm = _JOB_LINK_RE.search(urllib.parse.urlparse(href).path)
                if jm and href not in job_urls:
                    job_urls.append(href)
            if not job_urls:
                return

            for job_url in job_urls:
                job_id = _JOB_LINK_RE.search(job_url).group(1)
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                item = self._parse_detail(job_url)
                if item:
                    yield item

            page += 1
            if page > last_page:
                return

    @staticmethod
    def _last_page(soup: bs4.BeautifulSoup) -> int:
        nums = [1]
        for a in soup.select('a[href*="/page/"]'):
            pm = re.search(r"/page/(\d+)/", a.get("href", ""))
            if pm:
                nums.append(int(pm.group(1)))
        return max(nums)

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, job_url: str) -> dict | None:
        soup = self.get_soup(job_url)
        if soup is None:
            return None

        company = soup.select_one("p.introduce .company") or soup.select_one(".card-company p.company")
        name = _clean(company.get_text()) if company else ""
        title_el = soup.select_one("h1.job-main-title") or soup.find("h1")
        title = _clean(title_el.get_text()) if title_el else ""

        # 一覧サマリ (アイコン画像名でラベル判別)
        terms: dict[str, str] = {}
        for t in soup.select(".job-summary-wrapper .terms"):
            img = t.select_one(".mini-title img")
            content = t.select_one(".content")
            if not img or not content:
                continue
            km = re.search(r"term-job-([a-z]+)", img.get("src", ""))
            if km and km.group(1) != "yoko":  # yoko = 仕事内容の抜粋 (長文) は除外
                terms[km.group(1)] = _clean(content.get_text())

        # 勤務地住所
        addr_full = ""
        for h3 in soup.select(".main-column h3"):
            if _clean(h3.get_text()) == "勤務地":
                p = h3.find_next_sibling("p")
                if p:
                    addr_full = _extract_address(p.get_text("\n"))
                break

        pref = ""
        pm = _PREF_PATTERN.match(addr_full)
        if pm:
            pref = pm.group(1)
            addr = addr_full[pm.end():].strip()
        else:
            am = _PREF_PATTERN.search(terms.get("area", ""))
            pref = am.group(1) if am else ""
            addr = addr_full

        # 企業情報カード
        card = soup.select_one(".pc-only .card-company") or soup.select_one(".card-company")
        info: dict[str, str] = {}
        lob = ""
        if card:
            summary = card.select_one("p.summary")
            lob = _clean(summary.get_text()) if summary else ""
            for th in card.select("th"):
                td = th.find_next_sibling("td")
                if td:
                    info[_clean(th.get_text())] = _clean(td.get_text())
        emp = info.get("従業員", "")
        if emp.endswith(("名人", "人人")):  # メイン側テーブルは単位が二重になる
            emp = emp[:-1]

        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: _industry_from_title(title, name),
            Schema.LOB: lob,
            Schema.OPEN_DATE: _normalize_date(info.get("設立年月日", "")),
            Schema.EMP_NUM: emp,
            Schema.CAP: info.get("資本金", ""),
            # gyoshu アイコン欄は実際には職種名 (例: 建築施工管理) が入る
            "職種": self._occupation(soup) or terms.get("gyoshu", ""),
            "求人タイトル": title,
        }

    @staticmethod
    def _occupation(soup: bs4.BeautifulSoup) -> str:
        """パンくず JSON-LD の /job_occupation/ 階層を「大 > 小」で返す。"""
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(sc.string or "")
            except json.JSONDecodeError:
                continue
            graph = data.get("@graph", [data]) if isinstance(data, dict) else []
            for node in graph:
                if node.get("@type") != "BreadcrumbList":
                    continue
                names = [
                    _clean(el.get("name", ""))
                    for el in node.get("itemListElement", [])
                    if "/job_occupation/" in (el.get("item") or "")
                ]
                if names:
                    return " > ".join(names)
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizKenCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://biz-ken.com/job_area/miyagi/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
バイトル 事務系(clerk)求人 — 中部(東海)・近畿(関西) (baitoru.com)

取得対象:
    - バイトルの職種カテゴリ「事務系(clerk)」の求人票を、近畿(kansai)と
      中部=東海(tokai)の両エリアで巡回し、求人票単位(1求人=1行)で収集する。

取得フロー:
    1. 引数 url (= /kansai/jlist/clerk/) を唯一のルートとし、配信元(origin)と
       職種サフィックス(clerk)を派生させる。備考の指示に従い、同一サイトの
       中部=東海(/tokai/jlist/clerk/) も同じクローラーで巡回する
       (URL 分割しない)。
    2. 各エリア一覧の総件数(_countValue / "求人件数は、現在N件…")を読み、
       バイトルの1エリア上限(約1万件=最大ページ)を超える場合はページ送りでは
       取りこぼすため、1段深い絞り込みリンク(/{region}/jlist/{pref}/clerk/ 等)へ
       再帰的に降りて分割取得する。上限以下のエリアはページ送りで全ページ巡回する。
    3. 一覧から求人詳細(/jobdetail/{id})を1件ずつ取得し、詳細ページ内の
       JSON-LD(schema.org JobPosting)+会社情報ブロックから構造化フィールドを
       抽出して即 yield する(Pattern B)。

    ※ 求人本文・募集情報・コメント・応募プロセス等の自由記述プロースは
      著作権リスクのため取得しない。備考で明示された 職種(求人職種名)・雇用形態・
      事業内容 のみを構造化フィールドとして取得する。

利用規約 (https://www.baitoru.com/about/kiyaku.html → kiyaku_base.html 第9条)
    にはスクレイピング/クローリングを名指しで禁止する条項は無い(不正アクセス・
    過度な負荷の禁止のみ)。DELAY を確保して過度な負荷を避けつつ巡回する。

実行方法:
    python scripts/sites/jobs/clerk.py
    docker compose exec worker python /app/bin/run_flow.py --site-id clerk
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 備考の指示: 近畿(本URL)に加えて中部=東海も同一クローラーで巡回する。
# 引数 url のリージョンに加えて必ず巡回する相手側リージョン。
CHUBU_TOKAI_REGION = "tokai"

# バイトルが1エリアの一覧で返す上限件数。これを超えるエリアはページ送り
# だけでは取りこぼすため、1段深い絞り込みリンクへ再帰的に降りて分割取得する。
AREA_LIMIT = 10000

# 1エリアあたりの巡回上限ページ数(暴走防止のセーフティ)。30件/ページ。
MAX_PAGES = 400

# 絞り込みの再帰上限(region/pref/city/ward… の想定最大深度)。
MAX_DEPTH = 5

# JSON-LD employmentType コード → 日本語ラベル(雇用形態)。
_EMP_TYPE_MAP = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "契約社員",
    "TEMPORARY": "派遣",
    "INTERN": "インターン",
    "OTHER": "その他",
}

# 求人詳細URL(/jobdetail/{id})。
_JOB_DETAIL_RE = re.compile(r"/jobdetail/(\d+)")
# 総件数テキスト("求人件数は、現在19,225件あります。")。
_COUNT_TEXT_RE = re.compile(r"現在\s*([\d,]+)\s*件")
# 代表TEL のテキストから番号を取り出す。
_TEL_RE = re.compile(r"代表TEL[:：]?\s*([0-9０-９][\d０-９\-‐ー－()（）\s]+)")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class ClerkScraper(StaticCrawler):
    """バイトル 事務系(clerk) 中部・近畿 求人スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "エリア",      # 勤務地の市区町村 (JSON-LD addressLocality)
        "職種",        # 求人職種名 (JSON-LD title。次工程で4職種に絞り込むため取得)
        "雇用形態",    # JSON-LD employmentType の日本語ラベル
    ]

    def prepare(self):
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルートとし、配信元・職種サフィックスを派生。
        parsed = urlparse(url)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        parts = [p for p in parsed.path.split("/") if p]
        # 期待形: [{region}, 'jlist', ..., 'clerk']
        region = parts[0] if parts else "kansai"
        suffix = parts[-1] if parts else "clerk"  # 通常 'clerk'

        # 備考: 近畿(引数url) + 中部=東海 の両リージョンを同一クローラーで巡回。
        regions = [region]
        if CHUBU_TOKAI_REGION not in regions:
            regions.append(CHUBU_TOKAI_REGION)

        seen_jobs: set[str] = set()  # 訪問済み求人ID(重複排除キー)
        seen_areas: set[str] = set()

        for reg in regions:
            base = urljoin(self.origin + "/", f"{reg}/jlist/{suffix}/")
            logger.info("リージョン巡回開始: %s", base)
            yield from self._crawl_area(base, suffix, seen_jobs, seen_areas, depth=0)

        logger.info("収集求人数: %d件", len(seen_jobs))

    # ------------------------------------------------------------------ #
    # エリア巡回(件数に応じてページ送り or 再帰分割)
    # ------------------------------------------------------------------ #
    def _crawl_area(self, base: str, suffix: str, seen_jobs: set,
                    seen_areas: set, depth: int) -> Generator[dict, None, None]:
        base = base.rstrip("/") + "/"
        if base in seen_areas:
            return
        seen_areas.add(base)

        soup = self.get_soup(base)
        if soup is None:
            return

        total = self._total_count(soup)
        logger.info("一覧巡回: %s 総件数=%s depth=%d",
                    base, total if total is not None else "?", depth)

        # 1ページ目の求人は常に先に出力する(最初の1件を速やかに yield)。
        yield from self._emit_jobs(soup, seen_jobs)

        # 上限超過エリアはページ送りせず、1段深い絞り込みへ降りて分割取得する。
        if total is not None and total > AREA_LIMIT and depth < MAX_DEPTH:
            children = self._child_areas(soup, base, suffix)
            if children:
                logger.info("上限超過(%s件)につき %d 個の子エリアへ分割: %s",
                            total, len(children), base)
                for child in children:
                    yield from self._crawl_area(
                        child, suffix, seen_jobs, seen_areas, depth + 1)
                return
            # 子リンクが無い場合は取れる範囲(=上限まで)だけページ送りする。

        # 2ページ目以降を巡回。範囲外は 404 → get_soup が None を返す。
        page_no = 2
        while page_no <= MAX_PAGES:
            list_url = f"{base}page{page_no}/"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            job_ids = self._page_job_ids(soup)
            if not job_ids:
                break
            yield from self._emit_jobs(soup, seen_jobs)
            page_no += 1

    def _emit_jobs(self, soup, seen_jobs: set) -> Generator[dict, None, None]:
        """一覧ページ上の各求人詳細を取得して出力する(重複排除つき)。"""
        for job_id in self._page_job_ids(soup):
            if job_id in seen_jobs:
                continue
            seen_jobs.add(job_id)
            job_url = urljoin(self.origin + "/", f"/jobdetail/{job_id}")
            item = self._scrape_detail(job_url)
            if item and item.get(Schema.NAME):
                yield item

    # ------------------------------------------------------------------ #
    # 一覧ページの解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _total_count(soup) -> int | None:
        """一覧ページの総件数を返す。

        "求人件数は、現在N件あります。" テキスト、無ければ span._countValue から。
        """
        text = soup.get_text(" ", strip=True)
        m = _COUNT_TEXT_RE.search(text)
        if m:
            return int(m.group(1).replace(",", ""))
        counts = []
        for sp in soup.find_all("span", class_=re.compile("countValue")):
            digits = re.sub(r"[^\d]", "", sp.get_text())
            if digits:
                counts.append(int(digits))
        return max(counts) if counts else None

    @staticmethod
    def _page_job_ids(soup) -> list[str]:
        """一覧ページから求人詳細ID(/jobdetail/{id})を抽出(ページ内重複排除)。"""
        ids: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href*='/jobdetail/']"):
            m = _JOB_DETAIL_RE.search(a.get("href", "") or "")
            if not m:
                continue
            jid = m.group(1)
            if jid not in seen:
                seen.add(jid)
                ids.append(jid)
        return ids

    def _child_areas(self, soup, base: str, suffix: str) -> list[str]:
        """現在エリアの「1段深い」絞り込みリンク(子エリア)を抽出する。

        子リンクは /{region}/jlist/{area…}/{suffix}/ 形式で、現在エリアの
        area 部分を接頭辞に持ち、area セグメントがちょうど1つ深いもの
        (例: /kansai/jlist/clerk/ → /kansai/jlist/osaka/clerk/)。
        """
        base_parts = [p for p in urlparse(base).path.split("/") if p]
        # area セグメント = jlist と suffix の間。
        # [{region}, 'jlist', <area...>, {suffix}]
        base_area = base_parts[2:-1]  # 現在の area セグメント列
        region = base_parts[0]
        children: set[str] = set()
        for a in soup.select("a[href*='/jlist/']"):
            href = (a.get("href", "") or "").split("?")[0].split("#")[0]
            full = urljoin(base, href).rstrip("/") + "/"
            p = [x for x in urlparse(full).path.split("/") if x]
            # 形が [{region}, 'jlist', <area...>, {suffix}] で、
            # area がちょうど base_area + 1 セグメント、かつ接頭辞一致。
            if len(p) != len(base_parts) + 1:
                continue
            if p[0] != region or p[1] != "jlist" or p[-1] != suffix:
                continue
            child_area = p[2:-1]
            if child_area[:len(base_area)] == base_area:
                children.add(full)
        return sorted(children)

    # ------------------------------------------------------------------ #
    # 求人詳細ページの解析
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        jp = self._find_jobposting(soup)
        if not jp:
            return None

        # 重複排除キー兼出力URLは求人詳細URL(1求人=1行)。
        item = {Schema.URL: url}

        org = jp.get("hiringOrganization") or {}
        item[Schema.NAME] = _clean(org.get("name"))

        # HP: 企業公式サイト(sameAs)。baitoru/画像CDN は除外。
        same_as = _clean(org.get("sameAs"))
        if same_as and "baitoru.com" not in same_as and "image-cdn" not in same_as:
            item[Schema.HP] = same_as

        # 勤務地(JSON-LD jobLocation)。
        place = jp.get("jobLocation") or {}
        if isinstance(place, list):
            place = place[0] if place else {}
        addr = (place.get("address") or {}) if isinstance(place, dict) else {}
        region = _clean(addr.get("addressRegion"))
        locality = _clean(addr.get("addressLocality"))
        item[Schema.PREF] = region
        item["エリア"] = locality
        item[Schema.ADDR] = _clean(f"{region}{locality}")

        # 職種(求人職種名): JSON-LD title を掲載名としてそのまま取得。
        item["職種"] = _clean(jp.get("title"))

        # 雇用形態: employmentType を日本語ラベルへ。
        item["雇用形態"] = self._map_employment(jp.get("employmentType"))

        # 会社情報ブロック(h3/h4._itemTitle → 次要素が値)。
        fields = self._company_fields(soup)
        # 事業内容・業種。
        item[Schema.LOB] = _clean(fields.get("事業内容"))
        # HP フォールバック(会社情報の URL 欄)。
        if not item.get(Schema.HP):
            hp = _clean(fields.get("URL"))
            if hp and "baitoru.com" not in hp:
                item[Schema.HP] = hp

        # TEL: 代表TEL 表記から番号を取得。
        item[Schema.TEL] = self._extract_tel(soup)

        return item

    @staticmethod
    def _map_employment(value) -> str:
        if not value:
            return ""
        codes = value if isinstance(value, list) else [value]
        labels = []
        for c in codes:
            lab = _EMP_TYPE_MAP.get(str(c).strip().upper(), "")
            if lab and lab not in labels:
                labels.append(lab)
        return " / ".join(labels)

    @staticmethod
    def _find_jobposting(soup) -> dict | None:
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for entry in data if isinstance(data, list) else [data]:
                if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                    return entry
        return None

    @staticmethod
    def _company_fields(soup) -> dict:
        """会社情報ブロックの h3/h4._itemTitle ラベル → 次要素の値 を辞書化する。

        クラス名はビルドごとにハッシュ接尾辞が変わるため部分一致で照合する。
        """
        d: dict = {}
        for h in soup.find_all(re.compile(r"^h[34]$"),
                               class_=re.compile("itemTitle")):
            label = h.get_text(" ", strip=True)
            val = h.find_next_sibling()
            if label and val is not None and label not in d:
                d[label] = val.get_text(" ", strip=True)
        return d

    @staticmethod
    def _extract_tel(soup) -> str:
        for el in soup.find_all(class_=re.compile("itemValue")):
            m = _TEL_RE.search(el.get_text(" ", strip=True))
            if m:
                return m.group(1).strip()
        # フォールバック: ページ全体から "代表TEL:" を探す。
        m = _TEL_RE.search(soup.get_text(" ", strip=True))
        return m.group(1).strip() if m else ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = ClerkScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.baitoru.com/kansai/jlist/clerk/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ウェルミージョブ 沖縄福祉介護 (kaigojob.com) — 沖縄県の福祉・介護求人情報

取得対象:
    - 沖縄県 (p-47) に掲載されている求人のうち、サービス区分が
      「介護」「障がい福祉」のもの (老人福祉・介護 / 障害者福祉 / 児童福祉 /
      訪問看護・訪問リハビリ 等)
    - サービス区分が「医療」(病院・診療所・歯科・薬局) / 「保育」のみの求人は除外

取得フロー:
    1. ルート URL (https://www.kaigojob.com/okinawa) を取得する
       → /{職種スラッグ}/p-{都道府県コード} へ 301 リダイレクトされるので、
         canonical から都道府県セグメント (p-47) とオリジンを取り出す
    2. フッターの職種ナビから全 39 職種のスラッグを収集する
    3. 職種ごとに /{slug}/p-47(?page=N) を巡回し、求人カードの
       「サービス区分」で福祉・介護のみに絞り込む
    4. 各求人の詳細ページ (/{slug}/job-postings-{id}) を 1 件ずつ取得して即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/kaigojob.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kaigojob
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 取得対象とするサービス区分 (福祉・介護系のみ)。
# サイト上のサービス区分は「介護 / 障がい福祉 / 医療 / 保育」の 4 種類のみで、
# 「医療」(病院・診療所等) と「保育」(保育園・こども園等) は備考の指示により除外する。
_TARGET_DIVISIONS = {"介護", "障がい福祉"}

# 詳細ページ URL: /{職種スラッグ}/job-postings-{求人ID}
_DETAIL_RE = re.compile(r"/[a-z0-9\-]+/job-postings-(\d+)")
# 都道府県セグメント: /{職種スラッグ}/p-{都道府県コード}
_PREF_SEG_RE = re.compile(r"/([a-z0-9\-]+)/(p-\d+)")
# 職種スラッグ (フッターナビのリンク)
_OCCUPATION_HREF_RE = re.compile(r"^/[a-z0-9\-]+$")

_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")

# 1 職種あたりのページ巡回上限 (無限ループ防止)
_MAX_PAGES = 100


class KaigoJobOkinawa(StaticCrawler):
    """ウェルミージョブ 沖縄福祉介護 スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "職種",
        "雇用形態",
        "サービス区分",
        "施設形態・サービス種別",
        "求人内容",
        "市区町村",
        "法人名",
        "法人住所",
        "求人広告番号",
        "掲載日",
    ]

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        root_soup = self.get_soup(url)
        if root_soup is None:
            logger.error("ルートページを取得できませんでした: %s", url)
            return

        base_url, pref_seg, first_slug = self._resolve_root(url, root_soup)
        logger.info("ルート解決: base=%s pref=%s 起点職種=%s", base_url, pref_seg, first_slug)

        occupations = self._collect_occupations(root_soup, first_slug)
        logger.info("職種数: %d", len(occupations))

        seen_ids: set[str] = set()
        estimated = 0

        for slug in occupations:
            list_url = f"{base_url}/{slug}/{pref_seg}"

            for page in range(1, _MAX_PAGES + 1):
                page_url = list_url if page == 1 else f"{list_url}?page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                cards = soup.select("li.js-job-posting-list-item")
                if not cards:
                    break

                if page == 1:
                    estimated += self._estimate_count(soup, len(cards))
                    self.total_items = estimated
                    last_page = self._last_page(soup)
                else:
                    last_page = self._last_page(soup)

                for card in cards:
                    try:
                        item = self._build_item(card, base_url, seen_ids)
                    except Exception as exc:  # noqa: BLE001 — 1件の失敗で全体を止めない
                        logger.warning("求人カードの解析に失敗しました (%s): %s", page_url, exc)
                        continue
                    if item:
                        yield item

                if page >= last_page:
                    break

    # ------------------------------------------------------------ root 解決

    def _resolve_root(self, url: str, soup) -> tuple[str, str, str]:
        """ルート URL から オリジン / 都道府県セグメント / 起点職種スラッグ を導出する。

        https://www.kaigojob.com/okinawa は /care-worker/p-47 へ 301 されるため、
        canonical (無ければページ内リンク) から実体のパスを取り出す。
        """
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        candidates = []
        canonical = soup.select_one('link[rel="canonical"][href]')
        if canonical:
            candidates.append(canonical["href"])
        candidates.append(url)
        # 最後の手段: ページ内のページャ / 絞り込みリンクから拾う
        for a in soup.select('a[href*="/p-"]'):
            candidates.append(a.get("href", ""))

        for cand in candidates:
            m = _PREF_SEG_RE.search(urlparse(urljoin(base_url, cand)).path)
            if m:
                return base_url, m.group(2), m.group(1)

        raise RuntimeError(f"都道府県セグメント (p-NN) を特定できませんでした: {url}")

    def _collect_occupations(self, soup, first_slug: str) -> list[str]:
        """フッターの職種ナビから職種スラッグを収集する (起点の職種を先頭に置く)。"""
        slugs: list[str] = [first_slug]
        for ul in soup.select("ul.p-global-footer__sub-actions"):
            hrefs = [a.get("href", "") for a in ul.select("a[href]")]
            if f"/{first_slug}" not in hrefs:
                continue
            for href in hrefs:
                if not _OCCUPATION_HREF_RE.fullmatch(href):
                    continue
                slug = href.lstrip("/")
                if slug not in slugs:
                    slugs.append(slug)
        return slugs

    # -------------------------------------------------------- ページネーション

    @staticmethod
    def _last_page(soup) -> int:
        pages = [
            int(m.group(1))
            for a in soup.select('a[href*="page="]')
            if (m := re.search(r"[?&]page=(\d+)", a.get("href", "")))
        ]
        return max(pages) if pages else 1

    def _estimate_count(self, soup, per_page: int) -> int:
        last = self._last_page(soup)
        return per_page if last <= 1 else last * per_page

    # ---------------------------------------------------------------- item

    def _build_item(self, card, base_url: str, seen_ids: set[str]) -> dict | None:
        division = self._text(card.select_one(".p-job-postings__service-division"))
        if division not in _TARGET_DIVISIONS:
            return None

        link = card.select_one('a[href*="job-postings-"]')
        if link is None:
            return None
        href = link.get("href", "")
        m = _DETAIL_RE.search(href)
        if not m:
            return None
        job_id = m.group(1)
        if job_id in seen_ids:
            return None
        seen_ids.add(job_id)

        detail_url = urljoin(base_url, href)
        service_type = self._text(card.select_one(".p-job-postings__service-type"))

        item = {
            Schema.URL: detail_url,
            Schema.NAME: self._text(
                card.select_one("tr.optimize-job_postings-facility-page td a")
            ),
            Schema.ADDR: self._text(card.select_one("tr.optimize-job_postings-address td")),
            Schema.CAT_LV1: division,
            Schema.CAT_SITE: service_type,
            "サービス区分": division,
            "施設形態・サービス種別": service_type,
            "求人広告番号": job_id,
        }

        self._enrich_from_detail(detail_url, item)

        addr = item.get(Schema.ADDR, "")
        if not item.get(Schema.PREF) and addr:
            pm = _PREF_RE.match(addr)
            if pm:
                item[Schema.PREF] = pm.group(1)

        # 宣言した全カラムを必ず埋める (値が無い場合は空文字)
        for col in self.EXTRA_COLUMNS:
            item.setdefault(col, "")
        for col in (Schema.POST_CODE, Schema.PREF):
            item.setdefault(col, "")

        return item

    def _enrich_from_detail(self, detail_url: str, item: dict) -> None:
        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページを取得できませんでした (一覧の情報のみ使用): %s", detail_url)
            return

        # 定義リスト (職種名 / 雇用形態 / 住所 / 求人広告番号 / 事業所名 /
        #            サービス区分 / サービス種別 / 受動喫煙対策 / 法人名 / 法人住所)
        defs: dict[str, str] = {}
        for dl in soup.select("dl.c-definition__list"):
            for dt in dl.select("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                key = self._text(dt)
                if key and key not in defs:
                    defs[key] = dd.get_text(" ", strip=True)

        if defs.get("事業所名"):
            item[Schema.NAME] = defs["事業所名"]
        if defs.get("住所"):
            item[Schema.ADDR] = defs["住所"]
        if defs.get("サービス区分"):
            item[Schema.CAT_LV1] = defs["サービス区分"]
            item["サービス区分"] = defs["サービス区分"]
        if defs.get("サービス種別"):
            item[Schema.CAT_SITE] = defs["サービス種別"]
            item["施設形態・サービス種別"] = defs["サービス種別"]
        if defs.get("求人広告番号"):
            item["求人広告番号"] = defs["求人広告番号"]
        item["職種"] = defs.get("職種名", "")
        item["雇用形態"] = defs.get("雇用形態", "")
        item["法人名"] = defs.get("法人名", "")
        item["法人住所"] = defs.get("法人住所", "")

        # 募集内容セクション: h3 見出しの直後の div が本文
        item["求人内容"] = self._section_text(soup, "仕事内容")

        # JSON-LD (JobPosting) から郵便番号 / 都道府県 / 市区町村 / 掲載日を補完
        ld = self._job_posting_ld(soup)
        if ld:
            place = (ld.get("jobLocation") or {}).get("address") or {}
            postal = str(place.get("postalCode") or "").strip()
            if re.fullmatch(r"\d{7}", postal):
                postal = f"{postal[:3]}-{postal[3:]}"
            if postal:
                item[Schema.POST_CODE] = postal
            if place.get("addressRegion"):
                item[Schema.PREF] = place["addressRegion"]
            if place.get("addressLocality"):
                item["市区町村"] = place["addressLocality"]
            posted = str(ld.get("datePosted") or "")[:10]
            if posted:
                item["掲載日"] = posted

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _text(node) -> str:
        return node.get_text(" ", strip=True) if node else ""

    @staticmethod
    def _section_text(soup, header: str) -> str:
        for h3 in soup.select("h3.p-offer-details__header"):
            if h3.get_text(strip=True) == header:
                body = h3.find_next_sibling("div")
                if body is not None:
                    return body.get_text("\n", strip=True)
        return ""

    @staticmethod
    def _job_posting_ld(soup) -> dict | None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                    return node
        return None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KaigoJobOkinawa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.kaigojob.com/okinawa")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

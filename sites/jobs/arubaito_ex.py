"""
対象サイト: https://arubaito-ex.jp/

1. MinimumArubaitoExScraper … 求人詳細URL（/jobs/{id}）1ページのみ
2. ArubaitoExScraper … 検索一覧（/search?...&pg=）から求人IDを集め、各詳細を取得
"""
from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Generator

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

if TYPE_CHECKING:
    import bs4

_BASE = "https://arubaito-ex.jp"
# Schema 外のカラム（ItemPipeline の許可リスト用）。build_job_item_from_detail_soup のキーと一致させる。
_EXTRA_FEATURE_TAGS = "求人特徴タグ"
_EXTRA_EMPLOYMENT_TYPE = "雇用形態"
_EXTRA_LISTING_PERIOD = "掲載期間"
_EXTRA_INFO_PROVIDER = "情報提供元"
_JOB_PATH_RE = re.compile(r"^/jobs/(\d+)/?$")
_PREF_ADDR_RE = re.compile(r"^(.+?(?:都|道|府|県))(.*)$")
# 所在地などは div、情報提供元のみ dl などタグが混在するため両方拾う
_WORK_INFO_ROW_SEL = "dl.work-information.row, div.work-information.row"


# =============================================================================
# 1. 最小構成 — 求人詳細1ページのみ（host_para の Minimum と同じ使い方）
# =============================================================================


class MinimumArubaitoExScraper(StaticCrawler):
    """アルバイトEX 最小スクレイパー（求人詳細URLのみ）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        _EXTRA_FEATURE_TAGS,
        _EXTRA_EMPLOYMENT_TYPE,
        _EXTRA_LISTING_PERIOD,
        _EXTRA_INFO_PROVIDER,
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        if not _is_job_detail_url(url):
            self.logger.warning("Minimum版は求人詳細URL（/jobs/数字）のみ対応です: %s", url)
            return
        soup = self.get_soup(url)
        if soup is None:
            return
        item = build_job_item_from_detail_soup(soup, url)
        if item:
            self.total_items = 1
            yield item


# =============================================================================
# 2. 一覧→詳細（検索結果のページネーション付き）
# =============================================================================


class ArubaitoExScraper(MinimumArubaitoExScraper):
    """アルバイトEX 求人スクレイパー（検索一覧→全詳細）"""

    #: 一覧を辿る最大ページ数。None のとき上限なし（運用ではシードURLで条件絞り推奨）。
    MAX_PAGES: int | None = None

    def parse(self, url: str) -> Generator[dict, None, None]:
        if _is_job_detail_url(url):
            yield from super().parse(url)
            return

        if not _is_search_url(url):
            self.logger.warning("検索URLまたは /jobs/{id} 以外は未対応: %s", url)
            return

        job_urls: list[str] = []
        max_page_from_nav: int | None = None
        page = 1
        base_search = _without_pg(url)

        while True:
            if self.MAX_PAGES is not None and page > self.MAX_PAGES:
                break

            page_url = base_search if page == 1 else _merge_query(base_search, {"pg": str(page)})
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧取得失敗 page=%s", page)
                break

            if page == 1:
                total_hits = _parse_total_count(soup)
                max_page_from_nav = _parse_last_page_from_pagination(soup)

                estimated: int | None = total_hits
                if self.MAX_PAGES is not None:
                    cap_jobs = self.MAX_PAGES * 30
                    if estimated is not None:
                        estimated = min(estimated, cap_jobs)
                    else:
                        estimated = cap_jobs
                self.total_items = estimated if estimated is not None else 0

            ids = _collect_job_ids_from_search_soup(soup)
            if not ids:
                self.logger.info("一覧に求人なし page=%s で終了", page)
                break

            for jid in ids:
                job_urls.append(urljoin(_BASE + "/", f"/jobs/{jid}"))

            if max_page_from_nav is not None and page >= max_page_from_nav:
                break
            page += 1

        seen: set[str] = set()
        unique_urls: list[str] = []
        for u in job_urls:
            if u in seen:
                continue
            seen.add(u)
            unique_urls.append(u)

        if unique_urls:
            self.total_items = max(self.total_items or 0, len(unique_urls))

        for job_url in unique_urls:
            try:
                detail_soup = self.get_soup(job_url)
                if detail_soup is None:
                    continue
                item = build_job_item_from_detail_soup(detail_soup, job_url)
                if item:
                    yield item
            except Exception as exc:
                self.logger.warning("スキップ: %s (%s)", job_url, exc)
                continue


def _pick_richer_address_line(addr_html: str, addr_ld: str) -> str:
    """HTML の所在地と JSON-LD の住所のうち、文字数が多い方を採用（番地が LD のみにあるケース向け）。"""
    a = (addr_html or "").strip()
    b = (addr_ld or "").strip()
    if not a:
        return b
    if not b:
        return a
    return b if len(b) > len(a) else a


def _scalar_or_join_street(val: object) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return "".join(str(x).strip() for x in val if x)
    return str(val).strip()


def _format_schema_postal_address(addr_obj: dict) -> str:
    region = (addr_obj.get("addressRegion") or "").strip()
    locality = (addr_obj.get("addressLocality") or "").strip()
    street = _scalar_or_join_street(addr_obj.get("streetAddress"))
    return f"{region}{locality}{street}".strip()


def _first_postal_address_from_job_location(loc: object) -> dict | None:
    if isinstance(loc, dict):
        ad = loc.get("address")
        return ad if isinstance(ad, dict) else None
    if isinstance(loc, list):
        for item in loc:
            if not isinstance(item, dict):
                continue
            ad = item.get("address")
            if isinstance(ad, dict):
                return ad
    return None


def _postal_address_line_from_job_posting(ld: dict | None) -> str:
    if not ld or not isinstance(ld, dict):
        return ""
    addr_obj = _first_postal_address_from_job_location(ld.get("jobLocation"))
    if not addr_obj:
        return ""
    return _format_schema_postal_address(addr_obj)


def _join_site_industry_labels(category: str, job_type: str) -> str:
    """サイト上のカテゴリー・職種を Schema.CAT_SITE（サイト定義業種・ジャンル）用に1本化。"""
    parts = [p.strip() for p in (category or "", job_type or "") if p and p.strip()]
    return " / ".join(parts)


def _normalize_ld_employment_type(et: object) -> str:
    """JSON-LD の employmentType（文字列または schema.org URL）を短い表記にする。"""
    if et is None:
        return ""
    if isinstance(et, list):
        return " / ".join(_normalize_ld_employment_type(x) for x in et if x).strip(" /")
    s = str(et).strip()
    if not s:
        return ""
    low = s.lower()
    if "parttime" in low or "part-time" in low:
        return "パートタイム"
    if "fulltime" in low or "full-time" in low:
        return "フルタイム"
    if "contract" in low:
        return "契約"
    if "temporary" in low:
        return "一時的"
    if "intern" in low:
        return "インターン"
    if s.startswith("http"):
        return s.rsplit("/", maxsplit=1)[-1].replace("_", " ")
    return s


def _extract_info_provider(soup: "bs4.BeautifulSoup") -> str:
    """求人詳細の「情報提供元」（例: シフトワークス）。dl/div の work-information 行を対象に、注釈 p より item1 の名称を優先。"""
    for row in soup.select(_WORK_INFO_ROW_SEL):
        dt = row.select_one("dt.work-information-heading")
        if not dt or "情報提供元" not in dt.get_text(" ", strip=True):
            continue
        dd = row.select_one("dd.work-information-content")
        if not dd:
            return ""
        item1 = dd.select_one("div.item1")
        if item1:
            name = item1.get_text(" ", strip=True)
            if name:
                return name
            for img in item1.select("img[alt]"):
                alt = (img.get("alt") or "").strip()
                if alt:
                    return alt
        chunks: list[str] = []
        for child in dd.children:
            if not getattr(child, "name", None):
                continue
            if child.name == "p" and "work-information-content-note" in (child.get("class") or []):
                break
            if hasattr(child, "get_text"):
                t = child.get_text(" ", strip=True)
                if t:
                    chunks.append(t)
        if chunks:
            return " ".join(chunks).strip()
        return dd.get_text(" ", strip=True)
    return ""


def _extract_job_feature_tags(soup: "bs4.BeautifulSoup") -> str:
    """求人詳細の特徴タグ（ul.list-horizontal-mr5 内の各 featurelabel）を順に拾い、区切りで1セルにまとめる。"""
    texts: list[str] = []
    for span in soup.select("ul.list-horizontal-mr5 li span.featurelabel"):
        t = span.get_text(" ", strip=True)
        if t and t not in texts:
            texts.append(t)
    return " | ".join(texts)


def build_job_item_from_detail_soup(soup: "bs4.BeautifulSoup", page_url: str) -> dict | None:
    """求人詳細ページの soup から1件分の dict を組み立てる（Minimum / 全体で共通）。"""
    name = _dd_text_for_heading(soup, "企業名・店名")
    if not name:
        h1 = soup.select_one("h1.l-work-overview-heading")
        name = h1.get_text(" ", strip=True) if h1 else ""

    addr_html = _dd_text_for_heading(soup, "所在地")
    category = _dd_text_for_heading(soup, "カテゴリー")
    job_type = _dd_text_for_heading(soup, "職種")
    employment = _dd_text_for_heading(soup, "雇用形態")
    listing_period = _dd_text_for_heading(soup, "掲載期間")
    info_provider = _extract_info_provider(soup)
    lob = _dd_text_for_heading(soup, "事業内容")

    ld = _find_job_posting_json(soup)
    addr_ld = _postal_address_line_from_job_posting(ld) if ld else ""
    addr_full = _pick_richer_address_line(addr_html, addr_ld)
    pref, addr_only = _split_prefecture(addr_full)

    if ld:
        if not name:
            org = ld.get("hiringOrganization") or {}
            if isinstance(org, dict):
                name = (org.get("name") or "").strip()
        if not category:
            category = (ld.get("industry") or "").strip()
        et = ld.get("employmentType")
        if not employment and et:
            employment = _normalize_ld_employment_type(et)
        if not listing_period:
            dp = str(ld.get("datePosted") or "").strip()
            vt = str(ld.get("validThrough") or "").strip()
            if dp and vt:
                listing_period = f"{dp} 〜 {vt}"
            elif dp:
                listing_period = dp
            elif vt:
                listing_period = vt

    canonical = soup.select_one('link[rel="canonical"]')
    out_url = canonical.get("href") if canonical and canonical.get("href") else page_url

    if not name and not addr_full:
        return None

    feature_tags = _extract_job_feature_tags(soup)
    cat_site = _join_site_industry_labels(category, job_type)

    return {
        Schema.URL: out_url,
        Schema.NAME: name or "",
        Schema.PREF: pref,
        Schema.ADDR: addr_only or addr_full or "",
        Schema.CAT_SITE: cat_site,
        Schema.LOB: lob or "",
        _EXTRA_FEATURE_TAGS: feature_tags,
        _EXTRA_EMPLOYMENT_TYPE: employment or "",
        _EXTRA_LISTING_PERIOD: listing_period or "",
        _EXTRA_INFO_PROVIDER: info_provider or "",
    }


def _is_job_detail_url(url: str) -> bool:
    p = urlparse(url)
    return bool(_JOB_PATH_RE.match(p.path or ""))


def _is_search_url(url: str) -> bool:
    return urlparse(url).path.rstrip("/") == "/search"


def _without_pg(url: str) -> str:
    p = urlparse(url)
    q = parse_qs(p.query, keep_blank_values=True)
    q.pop("pg", None)
    pairs: list[tuple[str, str]] = []
    for k, vs in q.items():
        for v in vs:
            pairs.append((k, v))
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(pairs), p.fragment))


def _merge_query(url: str, updates: dict[str, str]) -> str:
    p = urlparse(url)
    q = parse_qs(p.query, keep_blank_values=True)
    for k, v in updates.items():
        if v == "":
            q.pop(k, None)
        else:
            q[k] = [v]
    pairs: list[tuple[str, str]] = []
    for k, vs in q.items():
        for v in vs:
            pairs.append((k, v))
    new_query = urlencode(pairs, doseq=True)
    return urlunparse((p.scheme or "https", p.netloc or "arubaito-ex.jp", p.path, p.params, new_query, p.fragment))


def _parse_total_count(soup) -> int | None:
    node = soup.select_one("p.txt_count span.js_count")
    if not node:
        node = soup.select_one("p.resultNum span")
    if not node:
        return None
    digits = "".join(c for c in node.get_text() if c.isdigit())
    return int(digits) if digits else None


def _parse_last_page_from_pagination(soup) -> int | None:
    best = 1
    found = False
    for a in soup.select("nav.pagination a[href]"):
        href = a.get("href", "")
        if "pg=" not in href:
            continue
        qs = parse_qs(urlparse(href).query)
        pg_vals = qs.get("pg", [])
        if not pg_vals:
            continue
        try:
            n = int(pg_vals[0])
        except ValueError:
            continue
        found = True
        if n > best:
            best = n
    return best if found else None


def _collect_job_ids_from_search_soup(soup) -> list[str]:
    joblist = soup.select_one("div.joblist")
    root = joblist or soup
    ids: list[str] = []
    for wrap in root.select("div.job_info_wrapper[data-job-id]"):
        jid = wrap.get("data-job-id", "").strip()
        if jid.isdigit():
            ids.append(jid)
            continue
        p = wrap.select_one("p.job_id")
        if p:
            m = re.search(r"(\d{6,})", p.get_text())
            if m:
                ids.append(m.group(1))
    return ids


def _dd_text_for_heading(soup, heading: str) -> str:
    for row in soup.select(_WORK_INFO_ROW_SEL):
        dt = row.select_one("dt.work-information-heading")
        if not dt:
            continue
        if heading not in dt.get_text(" ", strip=True):
            continue
        dd = row.select_one("dd.work-information-content")
        if dd:
            return dd.get_text(" ", strip=True)
    return ""


def _split_prefecture(location: str) -> tuple[str, str]:
    if not location:
        return "", ""
    m = _PREF_ADDR_RE.match(location.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return "", location.strip()


def _find_job_posting_json(soup) -> dict | None:
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
    return None


# ===== ここから下もコピペするだけ（実行用・host_para と同様）=====
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # 全体（検索→全詳細）。MAX_PAGES を切りたい場合はサブクラスで上書きするか ArubaitoExScraper.MAX_PAGES = 1 など。
    ArubaitoExScraper().execute("https://arubaito-ex.jp/search?form_type=header_search")
    # MinimumArubaitoExScraper().execute("https://arubaito-ex.jp/jobs/376506627")

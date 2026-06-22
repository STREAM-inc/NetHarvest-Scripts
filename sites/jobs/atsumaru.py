"""
あつまるくんの求人案内 (あつナビ / atsumaru.jp) — 求人情報

取得対象:
    - 全エリアの求人票（全件）。掲載企業の企業名・住所・電話番号と、
      求人タイトル・雇用形態/職種・給与・勤務時間・休日・待遇・事業内容を取得する。

取得フロー:
    一覧ページ (/all/list?...&page=N) → 各カード dt の詳細リンク
      → 詳細ページ (/area/detail?kno=...) を 1 件取得するたびに即 yield (Pattern B)。
    詳細ページは schema.org JobPosting の JSON-LD と
    求人情報テーブル (職種/給与/時間/休日/事業内容・他/待遇 等) から構造化データを取得。
    ※長文の自由記述 (仕事内容・求人本文・その他・応募方法) は著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/atsumaru.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id atsumaru
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_KNO_RE = re.compile(r"kno=([A-Za-z0-9]+)")


class AtsumaruCrawler(StaticCrawler):
    """あつまるくんの求人案内 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態・職種",
        "給与",
        "応募資格",
        "待遇",
        "掲載日",
        "掲載期限",
        "求人番号",
    ]

    def parse(self, url: str):
        page = 1
        while True:
            # ページネーションは引数 url を起点に &page=N を派生させる (SSOT = sites.yml の url)
            page_url = url if page == 1 else f"{url}&page={page}"
            soup = self.get_soup(page_url)
            jobs = soup.select("li.jobs")
            if not jobs:
                break

            if page == 1:
                self.total_items = self._estimate_total(soup, len(jobs))

            for job in jobs:
                try:
                    link = job.select_one("dt a[href]")
                    if not link:
                        continue
                    detail_url = urllib.parse.urljoin(page_url, link["href"])
                    # 一覧側でしか取れない / 取りやすいラベルを先に確保
                    cat_el = job.select_one("li.jobtype_category")
                    job_category = cat_el.get_text(" ", strip=True) if cat_el else ""

                    item = self._scrape_detail(detail_url)
                    if item:
                        if not item.get("雇用形態・職種"):
                            item["雇用形態・職種"] = job_category
                        yield item
                except Exception as e:
                    self.logger.warning(f"page {page}: job skip — {e}")
                    continue
            page += 1

    def _estimate_total(self, soup: BeautifulSoup, per_page: int) -> int:
        """ページネーションの最終ページ番号 × 1ページ件数 で総件数を概算"""
        last_page = 1
        for a in soup.select(".pagination a[href]"):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                last_page = max(last_page, int(m.group(1)))
        return last_page * per_page

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        ld = self._parse_jsonld(soup)
        labels = self._parse_tables(soup)

        # --- 企業名 ---
        name = ""
        org = ld.get("hiringOrganization") or {}
        if isinstance(org, dict):
            name = (org.get("name") or "").strip()
        if not name:
            h2 = soup.select_one("h2.bl_card2_ttl")
            name = h2.get_text(" ", strip=True) if h2 else ""

        # --- 住所 (JSON-LD 優先、無ければテーブルから補完) ---
        pref = post_code = addr = ""
        loc = ld.get("jobLocation") or {}
        address = loc.get("address") if isinstance(loc, dict) else None
        if isinstance(address, dict):
            pref = (address.get("addressRegion") or "").strip()
            locality = (address.get("addressLocality") or "").strip()
            street = (address.get("streetAddress") or "").strip()
            addr = (locality + street).strip()
            post_code = (address.get("postalCode") or "").strip()

        addr_raw = labels.get("勤務地") or labels.get("住所") or ""
        if not post_code and addr_raw:
            pc = _POST_CODE_RE.search(addr_raw)
            if pc:
                post_code = pc.group(1)
        if not pref and addr_raw:
            pm = _PREF_PATTERN.search(addr_raw)
            if pm:
                pref = pm.group(1)
                tail = addr_raw[pm.end():]
                tail = _POST_CODE_RE.sub("", tail).strip()
                # 「地図はこちら」等の末尾ノイズを除去
                addr = re.sub(r"\s*地図はこちら.*$", "", tail).strip() or addr

        # --- 電話番号 ---
        tel = ""
        tel_link = soup.select_one('a[href^="tel:"]')
        if tel_link:
            tel = tel_link.get_text(" ", strip=True) or tel_link["href"].replace("tel:", "")

        # --- 求人番号 ---
        kno_m = _KNO_RE.search(url)
        kno = kno_m.group(1) if kno_m else ""

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: labels.get("時間", ""),
            Schema.HOLIDAY: labels.get("休日", ""),
            Schema.LOB: labels.get("事業内容・他", ""),
            "求人タイトル": (ld.get("title") or "").strip(),
            "雇用形態・職種": "",
            "給与": labels.get("給与", ""),
            "応募資格": labels.get("資格", ""),
            "待遇": labels.get("待遇", ""),
            "掲載日": (ld.get("datePosted") or "").strip(),
            "掲載期限": (ld.get("validThrough") or "").strip(),
            "求人番号": kno,
        }

    @staticmethod
    def _parse_jsonld(soup: BeautifulSoup) -> dict:
        """schema.org JobPosting の JSON-LD を返す (制御文字・複数オブジェクト混在に耐性)"""
        decoder = json.JSONDecoder(strict=False)
        for sc in soup.select('script[type="application/ld+json"]'):
            text = sc.string or sc.get_text()
            if not text or "JobPosting" not in text:
                continue
            idx = 0
            n = len(text)
            while idx < n:
                chunk = text[idx:]
                stripped = chunk.lstrip()
                if not stripped:
                    break
                idx += len(chunk) - len(stripped)
                try:
                    obj, end = decoder.raw_decode(text, idx)
                except json.JSONDecodeError:
                    break
                idx = end
                if isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                    return obj
        return {}

    @staticmethod
    def _parse_tables(soup: BeautifulSoup) -> dict:
        """詳細ページ内テーブルの th→td を辞書化"""
        labels: dict[str, str] = {}
        for table in soup.select("table"):
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td:
                    key = th.get_text(" ", strip=True)
                    if key and key not in labels:
                        labels[key] = td.get_text(" ", strip=True)
        return labels


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AtsumaruCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://atsumaru.jp/all/list?utf8=%E2%9C%93&s%5Bkw%5D=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

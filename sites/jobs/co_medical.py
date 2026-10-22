"""
コメディカルドットコム (看護師求人) — https://www.co-medical.com/ns/

取得対象:
    - 全47都道府県の看護師求人 (/ns/{pref}/page/N/) を巡回し、各求人詳細
      (/ns/job{ID}/) から事業所名 / 所在地 / 施設形態 / 雇用形態 / 給与例 等を取得する。

取得フロー:
    ルート (/ns/) の都道府県ナビ (.townArea) から 47 都道府県スラッグを抽出
      → /ns/{pref}/ , /ns/{pref}/page/N/ をページ送り (50件/ページ, 範囲外は 404)
      → 一覧の /ns/job{ID}/ リンクを辿り、詳細を1件ずつ即 yield

備考:
    - 非 www ホスト (co-medical.com) は 403 を返すため、url のホストに www. を補う。
    - 求人ID単位で出力 (同一事業所の複数求人はそれぞれ1行)。都道府県を跨ぐ重複IDは除外。
    - 事業所の電話番号はサイト上に掲載されていない (応募フォームのみ) ため TEL は空。
    - 仕事内容 / 待遇・福利厚生 / 休日・休暇 / 関連施設 / 選考の流れ 等の自由記述は
      著作権リスクのため取得しない (年間休日日数のみ数値として抽出)。
    - sitemap-ns.xml は掲載総数 (約34,000件) の6割程度しか載っていないため使わない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/co_medical.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id co_medical
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_HREF_RE = re.compile(r"^/ns/([a-z]+)/$")
_JOB_HREF_RE = re.compile(r"^(?:https?://[^/]+)?/ns/job(\d+)/$")
_ADDR_RE = re.compile(
    r"^(?:〒\s*(\d{3}-?\d{4}))?\s*(北海道|東京都|京都府|大阪府|.{2,3}県)?\s*(.*)$"
)
_HOLIDAYS_RE = re.compile(r"年間休日\s*(\d+)\s*日")
_PREF_NAME_RE = re.compile(r"^(北海道|.{2,3}[都府県])$")


class CoMedicalScraper(StaticCrawler):
    """コメディカルドットコム 看護師求人 スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "求人職種",
        "募集雇用形態",
        "シフト",
        "休憩時間",
        "給与例",
        "年間休日",
        "雇用期間",
        "試用期間",
        "最寄駅",
        "交通アクセス",
        "診療科目",
        "病床数",
        "看護基準",
        "受動喫煙防止措置",
        "担当者",
        "掲載日",
        "掲載期限",
    ]

    def parse(self, url: str):
        base = self._normalize_base(url)
        root = self.get_soup(base)

        prefs = []
        for a in root.select(".townArea a[href], .prefecturesListArea a[href]"):
            m = _PREF_HREF_RE.match(a.get("href") or "")
            if m and _PREF_NAME_RE.match(a.get_text(strip=True)) and m.group(1) not in prefs:
                prefs.append(m.group(1))
        self.logger.info("都道府県 %d 件", len(prefs))

        seen = set()
        for pref in prefs:
            yield from self._crawl_pref(base, pref, seen)

    @staticmethod
    def _normalize_base(url: str) -> str:
        """co-medical.com は 403 のため https://www.{host}/ns/ に正規化する。"""
        parts = urlsplit(url)
        host = parts.netloc
        if not host.startswith("www."):
            host = "www." + host
        return urlunsplit(("https", host, parts.path or "/", "", ""))

    def _crawl_pref(self, base: str, pref: str, seen: set):
        page = 1
        while True:
            path = f"{pref}/" if page == 1 else f"{pref}/page/{page}/"
            list_url = urljoin(base, path)
            try:
                soup = self.get_soup(list_url)
            except requests.exceptions.HTTPError as e:
                # 最終ページ超過は 404
                self.logger.info("一覧終端 %s: %s", list_url, e)
                break
            if soup is None:
                break

            ids = []
            for a in soup.select("a[href]"):
                m = _JOB_HREF_RE.match(a.get("href") or "")
                if m and m.group(1) not in ids:
                    ids.append(m.group(1))
            if not ids:
                break

            new_ids = [i for i in ids if i not in seen]
            for job_id in new_ids:
                seen.add(job_id)
                detail_url = urljoin(base, f"job{job_id}/")
                try:
                    item = self._scrape_detail(detail_url, job_id)
                except requests.exceptions.RequestException as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, detail_url: str, job_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields = {}
        for tr in soup.select("table tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            if label and label not in fields:
                fields[label] = td

        def text(label: str) -> str:
            td = fields.get(label)
            return td.get_text(" ", strip=True) if td else ""

        name = text("事業所名")
        if not name:
            return None

        # 所在地: "〒020-0056 岩手県 盛岡市 ... 地図を表示"
        raw_addr = re.sub(r"\s*地図を表示\s*$", "", text("所在地"))
        post_code, pref, addr = "", "", raw_addr
        m = _ADDR_RE.match(raw_addr)
        if m:
            post_code = m.group(1) or ""
            if post_code and "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"
            pref = m.group(2) or ""
            # 市区町村と番地の間の区切りスペースのみ除去 (番地内の "2-26 2階" 等は保持)
            addr = re.sub(r"\s+", " ", m.group(3) or "").strip().replace(" ", "", 1)

        job = self._job_posting(soup)
        if not pref:
            pref = ((job.get("jobLocation") or {}).get("address") or {}).get("addressRegion", "")

        salary = ""
        for label, td in fields.items():
            if label.startswith("給料例"):
                dd = td.select_one("dd.salary_model")
                salary = dd.get_text(" ", strip=True) if dd else ""
                break

        holidays = ""
        hm = _HOLIDAYS_RE.search(text("休日・休暇"))
        if hm:
            holidays = f"{hm.group(1)}日"

        h1 = soup.select_one("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: "",  # 事業所TELは非掲載 (応募フォームのみ)
            Schema.EMP_NUM: text("職員数"),
            Schema.CAT_SITE: text("施設形態"),
            Schema.OPEN_DATE: text("設立"),
            "求人ID": job_id,
            "求人タイトル": title,
            "求人職種": text("求人職種"),
            "募集雇用形態": text("募集雇用形態"),
            "シフト": text("シフト"),
            "休憩時間": text("休憩時間"),
            "給与例": salary,
            "年間休日": holidays,
            "雇用期間": text("雇用期間"),
            "試用期間": text("試用期間"),
            "最寄駅": text("最寄駅"),
            "交通アクセス": text("交通アクセス"),
            "診療科目": text("診療科目"),
            "病床数": text("病床数"),
            "看護基準": text("看護基準"),
            "受動喫煙防止措置": text("受動喫煙防止措置"),
            "担当者": text("担当者"),
            "掲載日": job.get("datePosted", ""),
            "掲載期限": job.get("validThrough", ""),
        }

    def _job_posting(self, soup) -> dict:
        """JSON-LD JobPosting (本文に生改行を含むため strict=False で読む)。"""
        for s in soup.select('script[type="application/ld+json"]'):
            raw = s.string or ""
            if "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw, strict=False)
            except json.JSONDecodeError:
                # 本文に不正な文字が混ざる場合は必要項目だけ正規表現で拾う
                data = {"@type": "JobPosting"}
                for key in ("datePosted", "validThrough", "addressRegion"):
                    m = re.search(rf'"{key}"\s*:\s*"([^"]*)"', raw)
                    if m:
                        data[key] = m.group(1)
                if "addressRegion" in data:
                    data["jobLocation"] = {"address": {"addressRegion": data.pop("addressRegion")}}
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CoMedicalScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("http://co-medical.com/ns/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

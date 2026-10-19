"""
対象サイト: https://portal.line-sukimani.me/  (LINEスキマニ / スポットバイト求人)

構造メモ:
- portal.line-sukimani.me は全パスが https://sukimani.line.me/portal-worker へ 301。
  実ホストはリダイレクト先ページの <link rel="canonical"> から導出する。
- 一覧は公開 API GET /api/common/v1/jobs (ログイン不要)
  prefectureIds / startDate / endDate / size / page で取得、slicing.hasNext でページ送り。
  1 件 = 1 シフト (同一求人が日付・時間帯ごとに別 hashId で並ぶ)。
- 詳細 /jobs/{hashId} は SSR。window.__sukimaniSsrPayload 内の
  queryKey ["job","detail",hashId] に companyName / fullAddress / emergencyContact 等がある。

取得方針 (備考):
- 埼玉県(11)・千葉県(12)・東京都(13)・神奈川県(14) のみ。
- LINE ログイン不要の公開範囲のみ (common API + 公開詳細ページ)。
- シフト単位だと同一求人が大量重複するため (店舗ID, 求人タイトル) で名寄せし、
  最初に出現したシフトの勤務日時・給与を出力する。
- 業務内容本文 (workContents) 等の長文は著作権リスクのため取得しない。
"""
import datetime
import json
import re
import urllib.parse
from typing import Generator

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象都道府県 (サイト内 prefectureId = JIS コード)
_TARGET_PREFS = {11: "埼玉県", 12: "千葉県", 13: "東京都", 14: "神奈川県"}
_PAGE_SIZE = 50
_DATE_RANGE_DAYS = 89  # サイトの activeDates と同じ検索可能期間
_MAX_PAGES = 400
_SSR_MARK = "window.__sukimaniSsrPayload ="
_JST = datetime.timezone(datetime.timedelta(hours=9))


class LineScraper(StaticCrawler):
    """LINEスキマニ スクレイパー"""
    DELAY = 3.0  # 待機時間（秒）
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    EXTRA_COLUMNS = [
        "企業名", "市区町村", "求人タイトル", "給与形態", "時給", "給与総額",
        "交通費", "勤務日時", "最寄駅", "特徴",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = self._resolve_base(url)
        today = datetime.datetime.now(_JST).date()
        start = today.isoformat()
        end = (today + datetime.timedelta(days=_DATE_RANGE_DAYS)).isoformat()

        seen = set()
        for pref_id, pref_name in _TARGET_PREFS.items():
            for page in range(1, _MAX_PAGES + 1):
                query = urllib.parse.urlencode({
                    "prefectureIds": pref_id,
                    "startDate": start,
                    "endDate": end,
                    "jobSortTypeId": 1,
                    "size": _PAGE_SIZE,
                    "page": page,
                })
                data = self._get_json(urllib.parse.urljoin(base, f"/api/common/v1/jobs?{query}"))
                if not data:
                    break
                jobs = data.get("data") or []
                for job in jobs:
                    key = (job.get("shopId"), job.get("catchCopy"))
                    if key in seen:
                        continue
                    seen.add(key)
                    item = self._build_item(base, job, pref_name)
                    if item:
                        yield item
                if not jobs or not (data.get("slicing") or {}).get("hasNext"):
                    break

    def _resolve_base(self, url: str) -> str:
        """引数 url (ポータル) のリダイレクト先から実ホストを導出する。"""
        soup = self.get_soup(url)
        if soup:
            link = soup.select_one('link[rel="canonical"]') or soup.select_one('meta[property="og:url"]')
            href = (link.get("href") or link.get("content")) if link else None
            if href:
                return urllib.parse.urljoin(href, "/")
        return urllib.parse.urljoin(url, "/")

    def _get_json(self, api_url: str) -> dict | None:
        soup = self.get_soup(api_url)
        if soup is None:
            return None
        try:
            return json.loads(soup.get_text())
        except ValueError:
            self.logger.warning("JSON 解析失敗: %s", api_url)
            return None

    def _get_detail(self, detail_url: str, hash_id: str) -> dict:
        soup = self.get_soup(detail_url)
        if soup is None:
            return {}
        for script in soup.find_all("script"):
            text = script.string or ""
            if _SSR_MARK not in text:
                continue
            raw = text.split(_SSR_MARK, 1)[1].strip().rstrip(";")
            # JS リテラルの undefined を JSON の null に置換
            raw = re.sub(r"(?<=[:\[,])undefined(?=[,}\]])", "null", raw)
            try:
                payload = json.loads(raw)
            except ValueError:
                self.logger.warning("SSR ペイロード解析失敗: %s", detail_url)
                return {}
            for q in (payload.get("dehydratedTanStackQueryState") or {}).get("queries", []):
                if q.get("queryKey") == ["job", "detail", hash_id]:
                    return (q.get("state") or {}).get("data") or {}
        return {}

    def _build_item(self, base: str, job: dict, pref_name: str) -> dict | None:
        hash_id = job.get("hashId")
        if not hash_id:
            return None
        detail_url = urllib.parse.urljoin(base, f"/jobs/{hash_id}")
        detail = self._get_detail(detail_url, hash_id)

        pref = (job.get("prefecture") or {}).get("name") or pref_name
        if pref not in _TARGET_PREFS.values():
            return None
        city = (job.get("region") or {}).get("name") or ""
        address = (detail.get("fullAddress") or detail.get("shopFullAddress") or "").strip()
        address = re.sub(r"\s+", " ", address)
        if address.startswith(pref):
            address = address[len(pref):].strip()
        elif not address:
            address = city

        tel = re.sub(r"[^\d-]", "", detail.get("emergencyContact") or "")

        salary_type = (job.get("salaryType") or {}).get("name") or ""
        salary_type = {"HOURLY_PAY": "時給", "DAILY_PAY": "日給"}.get(salary_type, salary_type)
        hourly = job.get("baseHourlyPay")
        total = job.get("jobPay")

        start = job.get("startDateTime") or detail.get("workStartDateTime") or ""
        end = job.get("endDateTime") or detail.get("workEndDateTime") or ""
        work_time = f"{start.replace('T', ' ')[:16]}〜{end.replace('T', ' ')[:16]}" if start else ""

        station = job.get("nearestStation") or ""
        minutes = job.get("minutesFromNearestStation") or ""
        if station and minutes:
            station = f"{station} {minutes}"

        return {
            Schema.URL: detail_url,
            Schema.NAME: job.get("shopName") or detail.get("shopName") or "",
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: "/".join(job.get("tags") or detail.get("tags") or []),
            "企業名": detail.get("companyName") or "",
            "市区町村": city,
            "求人タイトル": job.get("catchCopy") or "",
            "給与形態": salary_type,
            "時給": f"{hourly}円" if hourly else "",
            "給与総額": f"{total}円" if total else "",
            "交通費": (job.get("commutationAllowanceType") or {}).get("name") or "",
            "勤務日時": work_time,
            "最寄駅": station,
            "特徴": "/".join(f.get("name", "") for f in detail.get("features") or []),
        }


if __name__ == "__main__":
    scraper = LineScraper()
    scraper.execute("https://portal.line-sukimani.me/")

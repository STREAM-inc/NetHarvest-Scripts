"""
マイナビバイト スクレイパー (baito.mynavi.jp)
---------------------------------------------------------------------------
ver 1.0.0  20260629  新規作成
  - 都道府県別求人一覧をページネーション巡回
  - 求人詳細から店舗名・勤務地・電話番号・給与・アクセス等を取得
  - 企業URL (/cl-{id}/) を重複排除キーとして使用
---------------------------------------------------------------------------
"""

import re
import sys
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://baito.mynavi.jp"

# 都道府県スラッグ一覧（マイナビバイトのURLスラッグ）
PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

PREF_JA: dict[str, str] = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県",
    "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県",
    "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県",
    "oita": "大分県", "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

# 1都道府県あたりの最大巡回ページ数（暴走防止）
MAX_PAGES = 500

# 求人詳細URLパターン: /cl-{companyId}/job-{jobId}/
_JOB_URL_RE = re.compile(r"/cl-\d+/job-\d+/")
# 企業IDを抽出: /cl-{companyId}/
_COMPANY_ID_RE = re.compile(r"/(cl-\d+)/")
# 電話番号パターン
_TEL_RE = re.compile(r"(0\d{1,4}[-‐‒–—―－\s]?\d{1,4}[-‐‒–—―－\s]?\d{3,4})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return ""
    return href if href.startswith("http") else BASE_URL + href


class MyNaviBaitoScraper(DynamicCrawler):
    """マイナビバイト 求人企業情報スクレイパー（baito.mynavi.jp）

    都道府県別の求人一覧をページネーション巡回し、
    各求人詳細ページから店舗・会社情報を抽出する。
    企業URL（/cl-{id}/）を重複排除キーとして全掲載企業を収集。
    """

    DELAY = 1.5  # サーバー負荷軽減のためアクセス間隔を空ける
    EXTRA_COLUMNS = ["求人タイトル", "給与", "雇用形態", "シフト", "最寄り駅"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_jobs: set[str] = set()
        seen_companies: set[str] = set()

        for pref in PREFS:
            pref_ja = PREF_JA.get(pref, pref)
            list_url = f"{BASE_URL}/{pref}/"
            self.logger.info("都道府県巡回開始: %s (%s)", pref, pref_ja)
            yield from self._scrape_pref(list_url, pref_ja, seen_jobs, seen_companies)

        self.logger.info("収集企業数合計: %d社", len(seen_companies))

    # ------------------------------------------------------------------ #
    # 一覧ページのページネーション巡回
    # ------------------------------------------------------------------ #
    def _scrape_pref(
        self,
        base_url: str,
        pref_ja: str,
        seen_jobs: set,
        seen_companies: set,
    ) -> Generator[dict, None, None]:
        for page_no in range(1, MAX_PAGES + 1):
            url = base_url if page_no == 1 else f"{base_url}?p={page_no}"

            try:
                self.page.goto(url, wait_until="domcontentloaded")
                self.page.wait_for_selector("a[href*='/job-']", timeout=10000)
            except Exception:
                break  # ページ無し or タイムアウト → 次の都道府県へ

            soup = BeautifulSoup(self.page.content(), "html.parser")

            # 求人詳細URLを収集
            job_urls: list[str] = []
            for a in soup.find_all("a", href=True):
                href = str(a["href"]).split("?")[0]
                if _JOB_URL_RE.search(href):
                    full = _abs_url(href)
                    if full and full not in seen_jobs:
                        seen_jobs.add(full)
                        job_urls.append(full)

            if not job_urls:
                # このページに新規求人が無い → 最終ページを超えた
                break

            for job_url in job_urls:
                # 企業IDを取得して重複チェック
                m = _COMPANY_ID_RE.search(job_url)
                if not m:
                    continue
                company_url = f"{BASE_URL}/{m.group(1)}/"
                if company_url in seen_companies:
                    continue
                seen_companies.add(company_url)

                item = self._scrape_detail(job_url, pref_ja)
                if item and item.get(Schema.NAME):
                    yield item

    # ------------------------------------------------------------------ #
    # 求人詳細ページから企業・求人情報を抽出
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_selector("h1, h2", timeout=10000)
        except Exception:
            return None

        soup = BeautifulSoup(self.page.content(), "html.parser")
        data: dict = {Schema.URL: url, Schema.PREF: pref_ja}

        # ------------------------------------------------------------------
        # 1. 会社名・店舗名の取得
        #    優先順位:
        #      (a) パンくずリスト内の /cl-{id}/ リンクテキスト（最も正確）
        #      (b) ol/nav 内のリンク
        #      (c) h1/h2 先頭テキスト（フォールバック）
        # ------------------------------------------------------------------
        company_url = url  # デフォルト値
        company_name = ""

        # (a) パンくずリスト
        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            if _COMPANY_ID_RE.search(href) and "/job-" not in href:
                candidate = _clean(a.get_text())
                if candidate:
                    company_name = candidate
                    company_url = _abs_url(href)
                    # 最後に見つかったものを使う（パンくずの末尾が会社名）

        if company_name:
            data[Schema.NAME] = company_name
            data[Schema.URL] = company_url
        else:
            # フォールバック: h1 or h2
            for tag in ("h1", "h2"):
                el = soup.find(tag)
                if el:
                    data[Schema.NAME] = _clean(el.get_text())
                    break

        # ------------------------------------------------------------------
        # 2. 求人タイトル（h2/h3 の最初）
        # ------------------------------------------------------------------
        for tag in ("h2", "h3"):
            el = soup.find(tag)
            if el:
                t = _clean(el.get_text())
                if t and t != data.get(Schema.NAME):
                    data["求人タイトル"] = t
                    break

        # ------------------------------------------------------------------
        # 3. 定義リスト（dt/dd）から各フィールドを抽出
        #    マイナビバイトはラベル-値を dt/dd または li の組み合わせで持つ
        # ------------------------------------------------------------------
        for dl in soup.find_all("dl"):
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                key = _clean(dt.get_text())
                val = _clean(dd.get_text(" "))
                self._assign_field(data, key, val, dd)

        # dt/dd 以外のラベル+値パターン（隣接 li や span など）
        for el in soup.find_all(True):
            txt = _clean(el.get_text())
            if txt in ("給与", "雇用形態", "シフト", "勤務地", "アクセス"):
                nxt = el.find_next_sibling()
                if nxt:
                    self._assign_field(data, txt, _clean(nxt.get_text(" ")), nxt)

        # ------------------------------------------------------------------
        # 4. 電話番号
        #    "電話番号を表示" ボタン後の番号 or ページ内の直接掲載番号を取得
        # ------------------------------------------------------------------
        if not data.get(Schema.TEL):
            page_text = soup.get_text(" ")
            m = _TEL_RE.search(page_text)
            if m:
                data[Schema.TEL] = m.group(1).strip()

        return data if data.get(Schema.NAME) else None

    # ------------------------------------------------------------------ #
    # ラベルに応じて data dict にフィールドを代入
    # ------------------------------------------------------------------ #
    @staticmethod
    def _assign_field(data: dict, key: str, val: str, el) -> None:
        if not val:
            return
        if "給与" in key:
            data.setdefault("給与", val)
        elif "雇用形態" in key:
            data.setdefault("雇用形態", val)
        elif "シフト" in key:
            data.setdefault("シフト", val)
        elif "勤務地" in key:
            data.setdefault(Schema.ADDR, val)
            m = re.match(r"(北海道|東京都|京都府|大阪府|.{2,3}[都道府県])", val)
            if m:
                data[Schema.PREF] = m.group(1)
        elif "アクセス" in key:
            data.setdefault("最寄り駅", val)
        elif "電話番号" in key:
            data.setdefault(Schema.TEL, val)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    # 動作確認：東京都のみ実行
    MyNaviBaitoScraper().execute(f"{BASE_URL}/tokyo/")

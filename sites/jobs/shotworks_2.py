# scripts/sites/jobs/shotworks_2.py
"""
ショットワークス — 短期・単発・スキマバイト求人 (14都府県 × 勤務期間 md_1〜md_8)

対象サイト: https://shotworks.jp/sw/list/md_1/work

取得対象 (詳細ページ /sw/detail/{ID}):
    - 募集企業名: #companyName a
    - 募集企業の【事業内容】【住所】(dt/dd)
    - 勤務先名称, 勤務先住所, 業種, 職種, 雇用形態, 給与, 勤務時間, 応募締切 (dt/dd・th/td)
    - 掲載開始日: p.start_date

    詳細は 2 種類ある:
      W{ID}: 通常求人。dt/dd に勤務先名称・勤務先住所・応募締切がある
      R{ID}: 登録制バイト。勤務先は th「勤務先」(市区町村まで)、勤務時間は「勤務曜日・時間」、
             応募締切は無く div.RtypePostLimit「掲載期間：YYYY/MM/DD(曜)終了予定」で代替

取得フロー:
    起点 url (/sw/list/md_1/work) の md_N を 1〜8 に置換し、各期間 × 14都府県の
    一覧 /sw/list/pr_{JIS}/md_{N}/work?pg=N を 1 ページずつラウンドロビンで巡回
    (「次へ」リンクが無くなるか新規IDゼロで終了) → 詳細を取得即 yield。
    一覧に PR 枠が混ざり他県・他期間と重複するため求人ID で dedupe し、
    詳細の勤務先都道府県で 14都府県に絞る。人材派遣会社の掲載も除外しない。

実行方法:
    python scripts/sites/jobs/shotworks_2.py
    python bin/run_flow.py --site-id shotworks_2
"""

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

# 備考指定の14都府県 (JIS コード)
_TARGET_PREFS = {
    "13": "東京都", "14": "神奈川県", "11": "埼玉県", "12": "千葉県",
    "24": "三重県", "25": "滋賀県", "26": "京都府", "27": "大阪府",
    "28": "兵庫県", "29": "奈良県", "30": "和歌山県", "21": "岐阜県",
    "23": "愛知県", "40": "福岡県",
}
_PERIODS = range(1, 9)  # md_1 (1日だけ) 〜 md_8 (3ヵ月以内)

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_RE = re.compile(r"/sw/detail/([A-Z]\d+)")
_MD_RE = re.compile(r"/md_\d+/")
_PR_RE = re.compile(r"/pr_\d+/")
_START_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
_POST_LIMIT_RE = re.compile(r"(\d{4}/\d{1,2}/\d{1,2})")

# EXTRA カラム名 → 詳細ページのラベル候補 (先に見つかったもの)
_LABEL_COLUMNS = {
    "募集企業住所": ("【住所】",),
    "勤務先名称": ("勤務先名称",),
    "業種": ("業種",),
    "雇用形態": ("雇用形態",),
    "給与": ("給与",),
    "勤務時間": ("勤務時間", "勤務曜日・時間"),
    "応募締切": ("応募締切",),
}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class Shotworks2Scraper(StaticCrawler):
    """ショットワークス スクレイパー (14都府県)"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "募集企業住所", "勤務先名称", "勤務先住所", "業種", "雇用形態", "給与",
        "勤務時間", "掲載開始日", "応募締切", "求人票URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        path = urlparse(url).path
        if _PR_RE.search(path):
            list_urls = [url]
        else:
            base = url if _MD_RE.search(path) else urljoin(url, "md_1/work")
            list_urls = [
                urljoin(base, _MD_RE.sub(f"/pr_{code}/md_{md}/", urlparse(base).path))
                for md in _PERIODS
                for code in _TARGET_PREFS
            ]

        seen: set[str] = set()
        cursors = [{"url": u, "page": 1} for u in list_urls]

        # 一覧を 1 ページずつ交互に進める (時間切れでも地域・期間が偏らない)
        while cursors:
            for cur in list(cursors):
                page_url = cur["url"] if cur["page"] == 1 else f"{cur['url']}?pg={cur['page']}"
                soup = self.get_soup(page_url)
                if soup is None:
                    cursors.remove(cur)
                    continue

                new_ids = []
                for a in soup.select("div.workinfo_wrapper a[href*='/sw/detail/']"):
                    m = _DETAIL_RE.search(a.get("href", ""))
                    if m and m.group(1) not in seen:
                        seen.add(m.group(1))
                        new_ids.append(m.group(1))

                for job_id in new_ids:
                    item = self._parse_detail(urljoin(page_url, f"/sw/detail/{job_id}"))
                    if item:
                        yield item

                has_next = any(_clean(a.get_text()) == "次へ" for a in soup.select("a[href*='pg=']"))
                if not new_ids or not has_next:
                    cursors.remove(cur)
                else:
                    cur["page"] += 1

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # ラベル → 値 (dt/dd を優先。th/td は要約表で R 型の給与・勤務先はこちらにのみある)
        labels: dict[str, str] = {}
        for key_tag, val_tag in (("dt", "dd"), ("th", "td")):
            for k in soup.select(key_tag):
                label = _clean(k.get_text())
                v = k.find_next_sibling(val_tag)
                if label and v is not None and label not in labels:
                    labels[label] = _clean(v.get_text(" "))

        work_addr = labels.get("勤務先住所") or labels.get("勤務先", "")
        m = _PREF_RE.match(work_addr) or _PREF_RE.match(labels.get("勤務先エリア", ""))
        pref = m.group(1) if m else ""
        if pref not in _TARGET_PREFS.values():
            return None

        name = ""
        company = soup.select_one("#companyName a") or soup.select_one("#companyName")
        if company:
            name = _clean(company.get_text())

        posted = ""
        start = soup.select_one("p.start_date")
        m = _START_DATE_RE.search(start.get_text()) if start else None
        if m:
            posted = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.LOB: labels.get("【事業内容】", ""),
            Schema.CAT_SITE: labels.get("職種", ""),
            "勤務先住所": work_addr,
            "掲載開始日": posted,
            "求人票URL": detail_url,
        }
        for col, cands in _LABEL_COLUMNS.items():
            item[col] = next((labels[c] for c in cands if labels.get(c)), "")

        # 登録制 (R 型) は応募締切が無いため掲載終了予定日で代替
        if not item["応募締切"]:
            limit = soup.select_one(".RtypePostLimit")
            m = _POST_LIMIT_RE.search(limit.get_text()) if limit else None
            if m:
                item["応募締切"] = m.group(1)
        return item


if __name__ == "__main__":
    scraper = Shotworks2Scraper()
    scraper.execute("https://shotworks.jp/sw/list/md_1/work")

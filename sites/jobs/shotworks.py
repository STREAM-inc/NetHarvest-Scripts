# scripts/sites/jobs/shotworks.py
"""
ショットワークス — 短期・単発・スキマバイト求人 (埼玉/千葉/東京/神奈川)

対象サイト: https://shotworks.jp/sw/list/

取得対象:
    - 詳細ページ /sw/detail/W{ID} の dt/dd・th/td ラベル
        勤務先名称, 勤務先住所, 勤務先エリア, 勤務先交通, 業種, 職種, 雇用形態,
        給与, 給与支払種別, 交通費, 勤務時間, 応募締切, バイトID,
        募集企業の【事業内容】【住所】
    - 募集企業名: span.companyName (正式社名)。無ければ JSON-LD hiringOrganization.name
    - 市区町村 / 掲載日: JSON-LD JobPosting (jobLocation.addressLocality / datePosted)
      ※ JSON-LD は文字列中に生改行を含み json.loads 不可のため正規表現で抜く

取得フロー:
    起点 url (/sw/list/) 自体は「URLが見つかりません」ページ (HTTP 200)。
    都道府県別一覧は url 配下の pr_{JIS}/work (例: /sw/list/pr_13/work)。
    → 4都県 (11埼玉/12千葉/13東京/14神奈川) を 1 ページずつラウンドロビンで巡回
      (?pg=N、「次へ」リンクが無くなるか新規IDゼロで終了)
    → 一覧カードの詳細リンク /sw/detail/W{ID} を取得即 yield
    ※ 一覧に PR 枠 (list_adv) が混ざり他ページと重複するためバイトID で dedupe。
      PR 枠は他県求人の場合もあるため、詳細の都道府県で 4 都県に絞る。

    起点 url に pr_XX/work が直接渡された場合はその一覧のみを巡回する。

実行方法:
    python scripts/sites/jobs/shotworks.py
    python bin/run_flow.py --site-id shotworks
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

# 備考指定: 埼玉県・千葉県・東京都・神奈川県
_TARGET_PREFS = {"11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県"}

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_RE = re.compile(r"/sw/detail/(W\d+)")
_PREF_LIST_RE = re.compile(r"/pr_\d+/work")

_JOB_LOCATION_RE = re.compile(r'"jobLocation"\s*:\s*\{.*?"addressLocality"\s*:\s*"([^"]*)"', re.S)
_ORG_NAME_RE = re.compile(r'"hiringOrganization"\s*:\s*\{.*?"name"\s*:\s*"([^"]*)"', re.S)
_DATE_POSTED_RE = re.compile(r'"datePosted"\s*:\s*"(\d{4}-\d{2}-\d{2})')

# EXTRA カラム名 → 詳細ページのラベル
_LABEL_COLUMNS = {
    "勤務先名称": "勤務先名称",
    "勤務先エリア": "勤務先エリア",
    "勤務先交通": "勤務先交通",
    "雇用形態": "雇用形態",
    "給与": "給与",
    "給与支払種別": "給与支払種別",
    "交通費": "交通費",
    "勤務時間": "勤務時間",
    "応募締切": "応募締切",
    "業種": "業種",
    "募集企業住所": "【住所】",
    "バイトID": "バイトID",
}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class ShotworksScraper(StaticCrawler):
    """ショットワークス スクレイパー (1都3県)"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["市区町村", "掲載日"] + list(_LABEL_COLUMNS.keys())

    def parse(self, url: str) -> Generator[dict, None, None]:
        if _PREF_LIST_RE.search(urlparse(url).path):
            list_urls = [url]
        else:
            base = url if url.endswith("/") else url + "/"
            list_urls = [urljoin(base, f"pr_{code}/work") for code in _TARGET_PREFS]

        seen: set[str] = set()
        cursors = [{"url": u, "page": 1} for u in list_urls]

        # 都県を 1 ページずつ交互に進める (時間切れでも地域が偏らない)
        while cursors:
            for cur in list(cursors):
                page_url = cur["url"] if cur["page"] == 1 else f"{cur['url']}?pg={cur['page']}"
                soup = self.get_soup(page_url)
                if soup is None:
                    cursors.remove(cur)
                    continue

                new_ids = []
                for a in soup.select("div.workinfo_wrapper a[href*='/sw/detail/W']"):
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

        # ラベル → 値 (dt/dd を優先、th/td は要約表で値が欠けることがあるため後回し)
        labels: dict[str, str] = {}
        for key_tag, val_tag in (("dt", "dd"), ("th", "td")):
            for k in soup.select(key_tag):
                label = _clean(k.get_text())
                v = k.find_next_sibling(val_tag)
                if label and v is not None and label not in labels:
                    labels[label] = _clean(v.get_text(" "))

        addr_full = labels.get("勤務先住所", "")
        m = _PREF_RE.match(addr_full)
        pref = m.group(1) if m else ""
        if not pref:
            area = labels.get("勤務先エリア", "")
            m = _PREF_RE.match(area)
            pref = m.group(1) if m else ""
        if pref not in _TARGET_PREFS.values():
            return None
        addr = addr_full[len(pref):].strip() if addr_full.startswith(pref) else addr_full

        html = soup.decode()
        name_el = soup.select_one(".companyName")
        name = _clean(name_el.get_text()) if name_el else ""
        if not name:
            m = _ORG_NAME_RE.search(html)
            name = _clean(m.group(1)) if m else ""

        m = _JOB_LOCATION_RE.search(html)
        city = _clean(m.group(1)) if m else ""
        m = _DATE_POSTED_RE.search(html)
        posted = m.group(1) if m else ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: labels.get("職種", ""),
            Schema.LOB: labels.get("【事業内容】", ""),
            "市区町村": city,
            "掲載日": posted,
        }
        for col, label in _LABEL_COLUMNS.items():
            item[col] = labels.get(label, "")
        return item


if __name__ == "__main__":
    scraper = ShotworksScraper()
    scraper.execute("https://shotworks.jp/sw/list/")

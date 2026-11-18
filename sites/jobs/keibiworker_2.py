"""
警備ワーカー (keibiworker.jp) — 警備業特化の求人サイト

取得対象:
    - 関東甲信越・東海 13 都県 (PC8〜PC15, PC19〜PC23) の掲載中求人
    - 求人 ID 504 以上のみ (備考指定)
    - 求人1件1行。詳細ページ「企業情報」欄 (掲載企業名・所在地・電話番号・事業内容・HP・従業員数) と更新日

取得フロー:
    /zenkoku/PC{n} (都道府県別一覧、現状ページ送り無しで全件1ページ)
      → 求人カード (div.mod-jobResultBox) から求人ID・タイトル・雇用主名
      → /kyujin/{ID} 詳細 (th/td テーブル) を取得し即 yield
    ※ 同一求人が複数都県に掲載されるため求人IDで重複除外。
    ※ 起点 url の都県を先に処理し、残りの対象都県は url から派生した URL で巡回する。

実行方法:
    python scripts/sites/jobs/keibiworker_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id keibiworker_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象都県コード (備考指定: 茨城〜新潟, 山梨〜愛知)
_TARGET_PCS = [8, 9, 10, 11, 12, 13, 14, 15, 19, 20, 21, 22, 23]
_MIN_JOB_ID = 504
_MAX_LIST_PAGES = 50

_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|"
    "神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|"
    "大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    "福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREFS})")
_ZIP_RE = re.compile(r"〒?\s*(\d{3})\s*[-－ー‐]\s*(\d{4})")
_JOB_HREF_RE = re.compile(r"/kyujin/(\d+)/?$")
_DATE_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})")


def _text(el) -> str:
    if el is None:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()


class Keibiworker2(StaticCrawler):
    """警備ワーカー スクレイパー"""

    DELAY = 1.2  # 備考: アクセス間隔1秒以上
    EXTRA_COLUMNS = [
        "求人タイトル",
        "勤務地",
        "掲載求人URL",
        "掲載確認日(更新日)",
        "求人ID",
    ]

    # ---------- list ----------
    def _list_urls(self, url: str) -> list[str]:
        """起点 url の都県を先頭に、対象13都県の一覧 URL を url から派生させて返す。"""
        urls = [url]
        m = re.search(r"/PC(\d+)", url)
        start_pc = int(m.group(1)) if m else None
        for pc in _TARGET_PCS:
            if pc == start_pc:
                continue
            urls.append(urljoin(url, f"PC{pc}"))
        return urls

    def _iter_cards(self, list_url: str):
        """一覧ページ (ページ送りがあれば辿る) から (job_id, title, employer) を返す。"""
        page_url = list_url
        visited = set()
        for _ in range(_MAX_LIST_PAGES):
            if not page_url or page_url in visited:
                return
            visited.add(page_url)
            soup = self.get_soup(page_url)
            if soup is None:
                return
            for card in soup.select("div.mod-jobResultBox"):
                a = card.select_one(".mod-jobResultBox__header h2 a[href]")
                if not a:
                    continue
                m = _JOB_HREF_RE.search(a["href"])
                if not m:
                    continue
                employer = ""
                for tr in card.select("table tr"):
                    if _text(tr.find("th")) == "雇用主名":
                        employer = _text(tr.find("td"))
                        break
                yield int(m.group(1)), _text(a), employer, urljoin(page_url, a["href"])
            nxt = soup.select_one(
                ".mod-pagination-wrap a[rel=next], .mod-pagination-wrap li.next a[href], a[rel=next]"
            )
            page_url = urljoin(page_url, nxt["href"]) if nxt and nxt.get("href") else None

    # ---------- detail ----------
    @staticmethod
    def _detail_fields(soup) -> dict:
        fields: dict[str, object] = {}
        for tr in soup.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = _text(th)
            if label and label not in fields:
                fields[label] = td
        return fields

    def parse(self, url: str):
        seen: set[int] = set()
        for list_url in self._list_urls(url):
            for job_id, title, employer, detail_url in self._iter_cards(list_url):
                if job_id < _MIN_JOB_ID or job_id in seen:
                    continue
                seen.add(job_id)
                soup = self.get_soup(detail_url)
                if soup is None:
                    continue  # 掲載終了 (404) 等
                f = self._detail_fields(soup)
                if not f:
                    continue

                def val(label: str) -> str:
                    return _text(f.get(label))

                name = val("掲載企業名") or val("雇用主名") or employer
                addr_raw = val("所在地")
                zip_code = ""
                m = _ZIP_RE.search(addr_raw)
                if m:
                    zip_code = f"{m.group(1)}-{m.group(2)}"
                    addr_raw = addr_raw[m.end():]
                addr = addr_raw.strip(" 　")
                pref = ""
                pm = _PREF_RE.match(addr)
                if pm:
                    pref = pm.group(1)
                    addr = addr[pm.end():].strip(" 　")

                hp = ""
                hp_td = f.get("HP") or f.get("ホームページ") or f.get("URL")
                if hp_td is not None:
                    a = hp_td.find("a", href=True)
                    hp = a["href"].strip() if a else _text(hp_td)

                updated = ""
                dm = _DATE_RE.search(val("更新日"))
                if dm:
                    updated = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"

                yield {
                    Schema.NAME: name,
                    Schema.TEL: val("電話番号"),
                    Schema.PREF: pref,
                    Schema.POST_CODE: zip_code,
                    Schema.ADDR: addr,
                    Schema.LOB: val("事業内容"),
                    Schema.HP: hp,
                    Schema.EMP_NUM: val("従業員数"),
                    Schema.URL: detail_url,
                    "求人タイトル": title,
                    "勤務地": val("勤務地"),
                    "掲載求人URL": detail_url,
                    "掲載確認日(更新日)": updated,
                    "求人ID": str(job_id),
                }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Keibiworker2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://keibiworker.jp/zenkoku/PC13")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

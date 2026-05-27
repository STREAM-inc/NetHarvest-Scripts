"""
ゲッツ — 男性向け高収入求人サイト（全国クロール）

取得対象:
    - 全26エリアの求人一覧から店舗情報を取得
    - 詳細ページ + 公式HP で TEL/住所を補完

取得フロー:
    1. 全26エリアをループ（JA0001〜JA0042）
    2. 各エリアの type_search.php をページネーション
    3. 各求人の詳細ページ（info.php）を取得
    4. HP URL が取れた場合は TEL/都道府県を追加補完

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/getswork.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id getswork
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


_BASE = "https://www.getswork.com/"

_AREAS = [
    "JA0001", "JA0002", "JA0003", "JA0004", "JA0005", "JA0006", "JA0007",
    "JA0008", "JA0033",
    "JA0011", "JA0012", "JA0013", "JA0042",
    "JA0016", "JA0017", "JA0018", "JA0019", "JA0020", "JA0021",
    "JA0022", "JA0023", "JA0025",
    "JA0027", "JA0028", "JA0029", "JA0030",
]

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(r"0\d{1,4}[\-－]\d{1,4}[\-－]\d{3,4}")
_TEL_PLAIN_RE = re.compile(r"0\d{9,11}")
_JOB_ID_RE = re.compile(r"id=(J\d+)")


class GetsworkCrawler(StaticCrawler):
    """ゲッツ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "給与", "待遇", "アクセス"]

    def parse(self, url: str):
        seen_ids: set[str] = set()

        for area in _AREAS:
            page = 0
            while True:
                list_url = (
                    f"{_BASE}type_search.php?area={area}&type=job&run=true"
                    f"&area_PAL[]=match+comp&job_form_PAL[]=match+or&page={page}"
                )
                try:
                    soup = self.get_soup(list_url)
                except Exception as e:
                    self.logger.warning(f"List page failed {list_url}: {e}")
                    break

                items = soup.select("article.block-job")
                if not items:
                    break

                for item in items:
                    a_tag = item.select_one("h1.rl-title a[href]")
                    if not a_tag:
                        continue
                    detail_path = a_tag.get("href", "")
                    m = _JOB_ID_RE.search(detail_path)
                    if not m:
                        continue
                    job_id = m.group(1)
                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    # 職種 from listing table
                    shoku = ""
                    for row in item.select("table.search_tbl tr"):
                        th = row.find("th")
                        td = row.find("td")
                        if th and td and "職種" in th.get_text():
                            shoku = td.get_text(strip=True)
                            break

                    detail_url = urljoin(_BASE, detail_path)
                    record = self._scrape_detail(detail_url, shoku)
                    if record:
                        yield record

                # ページネーション: 次ページリンクの存在確認
                pager = soup.select_one("ul.block-pager")
                has_next = False
                if pager:
                    for a in pager.find_all("a", href=True):
                        if f"page={page + 1}" in a.get("href", ""):
                            has_next = True
                            break
                if not has_next:
                    break
                page += 1

    def _scrape_detail(self, url: str, shoku: str):
        try:
            soup = self.get_soup(url)
            article = soup.find("article", class_="track-job")
            if not article:
                return None

            # NAME: div.rl-headline > span.rl-text
            hl = article.find("div", class_="rl-headline")
            name_span = hl.find("span", class_="rl-text") if hl else None
            name = name_span.get_text(strip=True) if name_span else ""

            # block-lookingfor の th→td マップ
            lf_rows: dict = {}
            for section in article.find_all("section", class_="block-lookingfor"):
                tbl = section.find("table", class_="rl-table")
                if tbl:
                    for row in tbl.find_all("tr"):
                        th = row.find("th")
                        td = row.find("td")
                        if th and td:
                            lf_rows[th.get_text(strip=True)] = td

            # block-company-info の th→td マップ
            ci_rows: dict = {}
            for section in article.find_all("section", class_="block-company-info"):
                tbl = section.find("table", class_="rl-table")
                if tbl:
                    for row in tbl.find_all("tr"):
                        th = row.find("th")
                        td = row.find("td")
                        if th and td:
                            ci_rows[th.get_text(strip=True)] = td

            # ADDR / PREF
            addr_raw = lf_rows.get("勤務地", None)
            addr_full = addr_raw.get_text(strip=True) if addr_raw else ""
            pref, addr = self._split_pref(addr_full)

            # TEL（休日フィールド内の「■電話：」から抽出）
            tel = ""
            kyujitsu_td = ci_rows.get("休日", None)
            if kyujitsu_td:
                tm = re.search(r"電話[：:]\s*([\d\-－]+)", kyujitsu_td.get_text(strip=True))
                if tm:
                    tel = tm.group(1)

            # LINE（"ID: xxxx" 形式）
            line_id = ""
            line_td = lf_rows.get("LINE QRコード", None)
            if line_td:
                lm = re.search(r"ID\s*[：:]\s*(\S+)", line_td.get_text(separator=" ", strip=True))
                if lm:
                    line_id = lm.group(1)

            # REP_NM（求人用の説明文・役職のみは除外）
            rep_td = ci_rows.get("担当者", None)
            rep_nm = rep_td.get_text(strip=True) if rep_td else ""
            if re.search(r"求人|採用担当|対応いたします", rep_nm) or re.fullmatch(r"@[\w.]+", rep_nm):
                rep_nm = ""

            # HP
            hp_td = ci_rows.get("ホームページ", None)
            hp = ""
            if hp_td:
                hp_a = hp_td.find("a", href=True)
                hp = hp_a.get("href", "") if hp_a else hp_td.get_text(strip=True)

            # EXTRA
            kyuyo = lf_rows["給与・時給"].get_text(strip=True)[:200] if "給与・時給" in lf_rows else ""
            taiguu = lf_rows["待遇"].get_text(strip=True)[:200] if "待遇" in lf_rows else ""
            access = lf_rows["アクセス"].get_text(strip=True)[:200] if "アクセス" in lf_rows else ""

            # HP フェッチで TEL / PREF を補完
            if hp and (not tel or not pref):
                tel, pref, addr = self._hp_supplement(hp, tel, pref, addr)

            return {
                Schema.NAME: name,
                Schema.URL: url,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.REP_NM: rep_nm,
                Schema.HP: hp,
                Schema.LINE: line_id,
                "職種": shoku,
                "給与": kyuyo,
                "待遇": taiguu,
                "アクセス": access,
            }
        except Exception as e:
            self.logger.error(f"Detail scrape failed {url}: {e}")
            return None

    def _hp_supplement(self, hp_url: str, tel: str, pref: str, addr: str):
        try:
            hp_soup = self.get_soup(hp_url)
            text = hp_soup.get_text(separator=" ", strip=True)

            if not tel:
                tm = _TEL_RE.search(text) or _TEL_PLAIN_RE.search(text)
                if tm:
                    tel = tm.group(0)

            if not pref:
                pm = _PREF_RE.search(text)
                if pm:
                    pref = pm.group(1)
                    if not addr:
                        snippet = text[pm.start(): pm.start() + 60]
                        p2, a2 = self._split_pref(snippet)
                        if a2:
                            addr = a2
        except Exception as e:
            self.logger.debug(f"HP fetch skipped ({hp_url}): {e}")
        return tel, pref, addr

    def _split_pref(self, address: str):
        m = _PREF_RE.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        return "", address


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GetsworkCrawler()
    scraper.execute(_BASE)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

# scripts/sites/jobs/navi.py
"""
ジョブ・ナビ愛知 (navi.j-kin.com) — 愛知県中途求人ポータル

取得対象:
    - 中途求人 (約11,399件 / 約2,280ページ, 5件/ページ)
    - 求人ごとに「求人詳細ページ」+「掲載企業の企業詳細ページ」をマージして1レコードにする

取得フロー:
    /index.php?app_controller=search&type=mid&run=true&hw=on&page=N (N=0..最終)
      → 各求人 /index.php?app_controller=info&type=mid&id=J0000XXX
      → 紐づく企業 /index.php?app_controller=info&type=cUser&id=C0000XXX (企業単位でキャッシュ)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/navi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id navi
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE = "https://www.navi.j-kin.com"
LIST_PATH = "/index.php?app_controller=search&type=mid&run=true&hw=on"
JOB_DETAIL = BASE + "/index.php?app_controller=info&type=mid&id={id}"
COMPANY_DETAIL = BASE + "/index.php?app_controller=info&type=cUser&id={id}"

_TOTAL_RE = re.compile(r"(\d{2,})\s*件")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_ZIP_RE = re.compile(r"〒\s*(\d{3})\s*-?\s*(\d{4})")
_JOB_ID_RE = re.compile(r"id=(J\d+)")
_COMPANY_ID_RE = re.compile(r"id=(C\d+)")
_TITLE_RE = re.compile(r"^(.+?)の求人情報\s*(.+?)\s*-\s*ジョブ")
_EMPTY_DAY_RE = re.compile(r"【午前】\s*～\s*【午後】\s*～")
_PLACEHOLDERS = {"", "未登録", "未設定", "－", "-"}


class NaviScraper(StaticCrawler):
    """ジョブ・ナビ愛知 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "募集職種",
        "勤務形態",
        "給与",
        "勤務地",
        "最寄駅",
        "待遇",
        "応募資格",
        "応募方法詳細",
        "採用担当者",
        "担当者名",
        "担当者役職",
        "FAX",
        "平均年齢",
        "取り扱い内容",
    ]

    def parse(self, url: str):
        company_cache: dict[str, dict] = {}
        page = 0
        max_page: int | None = None

        while True:
            list_url = f"{BASE}{LIST_PATH}&page={page}"
            soup = self.get_soup(list_url)

            if page == 0:
                # 「11399 件」表記から総件数を取得
                text = soup.get_text(" ", strip=True)
                # ページャ末尾 ">>|" のリンク先 page=N が最終ページ index
                last_href = ""
                for a in soup.select('a[href*="page="]'):
                    label = a.get_text(strip=True)
                    if label in (">>|", "»", "最後"):
                        last_href = a.get("href", "")
                        break
                last_page_match = re.search(r"page=(\d+)", last_href)
                if last_page_match:
                    max_page = int(last_page_match.group(1)) + 1
                m = _TOTAL_RE.search(text)
                if m:
                    total = int(m.group(1))
                    self.total_items = total
                    if max_page is None:
                        max_page = (total + 4) // 5
                    self.logger.info(
                        "全 %d 件 / 想定ページ数 %s", total, max_page
                    )

            job_ids: list[str] = []
            seen_on_page: set[str] = set()
            for a in soup.select('a[href*="app_controller=info"][href*="type=mid"]'):
                href = a.get("href", "")
                m = _JOB_ID_RE.search(href)
                if not m:
                    continue
                jid = m.group(1)
                if jid in seen_on_page:
                    continue
                seen_on_page.add(jid)
                job_ids.append(jid)

            if not job_ids:
                self.logger.info("ページ %d: 求人リンクなし、終了", page)
                break

            for jid in job_ids:
                try:
                    item = self._scrape_job(jid, company_cache)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("求人取得失敗: %s (%s)", jid, e)

            if max_page is not None and page + 1 >= max_page:
                self.logger.info("最終ページに到達 (page=%d)", page)
                break
            page += 1

    def _scrape_job(self, job_id: str, company_cache: dict) -> dict | None:
        url = JOB_DETAIL.format(id=job_id)
        soup = self.get_soup(url)

        title = soup.title.get_text(strip=True) if soup.title else ""
        page_title = ""
        m = _TITLE_RE.match(title)
        if m:
            page_title = re.sub(r"\s*\[.+?\]\s*$", "", m.group(2)).strip()

        item: dict = {
            Schema.URL: url,
            "求人ID": job_id,
            "求人タイトル": page_title,
        }

        job_fields = self._collect_th_td(soup)

        company_name = job_fields.get("企業", "").strip()
        if not company_name:
            return None
        item[Schema.NAME] = company_name

        if v := job_fields.get("募集職種"):
            item["募集職種"] = v
            item[Schema.CAT_SITE] = v
        if v := job_fields.get("仕事内容"):
            item[Schema.LOB] = v
        if v := job_fields.get("勤務形態"):
            item["勤務形態"] = v
        if v := job_fields.get("給与"):
            item["給与"] = v
        if v := job_fields.get("勤務地"):
            item["勤務地"] = v
        if v := job_fields.get("最寄駅"):
            item["最寄駅"] = v
        if v := job_fields.get("待遇"):
            item["待遇"] = v
        if v := job_fields.get("休日・休暇"):
            item[Schema.HOLIDAY] = v
        if v := job_fields.get("勤務時間"):
            item[Schema.TIME] = v
        if v := job_fields.get("応募資格"):
            item["応募資格"] = v
        if v := job_fields.get("応募方法詳細"):
            item["応募方法詳細"] = v
        if v := job_fields.get("採用担当者"):
            item["採用担当者"] = v

        # 紐づく企業の cUser ページをキャッシュ経由で取得
        company_a = soup.select_one('a[href*="type=cUser"]')
        if company_a:
            href = company_a.get("href", "")
            cm = _COMPANY_ID_RE.search(href)
            if cm:
                cid = cm.group(1)
                cdata = company_cache.get(cid)
                if cdata is None:
                    cdata = self._scrape_company(cid) or {}
                    company_cache[cid] = cdata
                for k, v in cdata.items():
                    if not item.get(k):
                        item[k] = v

        return item

    def _scrape_company(self, company_id: str) -> dict | None:
        url = COMPANY_DETAIL.format(id=company_id)
        try:
            soup = self.get_soup(url)
        except Exception as e:
            self.logger.warning("企業ページ取得失敗: %s (%s)", company_id, e)
            return None

        fields = self._collect_th_td(soup)
        out: dict = {}

        # 所在地: "〒460-0008 愛知県名古屋市中区栄 2-4-1 （地図を見る）"
        addr_raw = fields.get("所在地", "")
        if addr_raw:
            addr_raw = re.sub(r"（地図を見る）|地図を見る", "", addr_raw).strip()
            zm = _ZIP_RE.search(addr_raw)
            if zm:
                out[Schema.POST_CODE] = f"{zm.group(1)}-{zm.group(2)}"
                addr_raw = (addr_raw[: zm.start()] + addr_raw[zm.end():]).strip()
            pm = _PREF_RE.search(addr_raw)
            if pm:
                out[Schema.PREF] = pm.group(1)
                out[Schema.ADDR] = addr_raw[pm.end():].strip()
            elif addr_raw:
                out[Schema.ADDR] = addr_raw

        def keep(label: str) -> str:
            v = fields.get(label, "").strip()
            return "" if v in _PLACEHOLDERS else v

        if v := keep("代表者名"):
            out[Schema.REP_NM] = v
        if v := keep("担当者名"):
            out["担当者名"] = v
        if v := keep("担当者役職"):
            out[Schema.POS_NM] = v
            out["担当者役職"] = v
        if v := keep("会社設立日"):
            out[Schema.OPEN_DATE] = v
        if v := keep("従業員数"):
            out[Schema.EMP_NUM] = v
        if v := keep("平均年齢"):
            out["平均年齢"] = v
        if v := keep("電話番号"):
            out[Schema.TEL] = v
        if v := keep("FAX番号"):
            out["FAX"] = v
        v_hp = fields.get("ホームページ", "").strip()
        if v_hp.startswith("http"):
            out[Schema.HP] = v_hp
        if v := keep("取り扱い内容"):
            out["取り扱い内容"] = v

        # 曜日別営業時間: 値が "【午前】 ～ 【午後】 ～" だけなら未設定として除外
        day_map = {
            "月曜日": Schema.TIME_MON,
            "火曜日": Schema.TIME_TUE,
            "水曜日": Schema.TIME_WED,
            "木曜日": Schema.TIME_THU,
            "金曜日": Schema.TIME_FRI,
            "土曜日": Schema.TIME_SAT,
            "日曜日": Schema.TIME_SUN,
        }
        for label, schema_key in day_map.items():
            v = fields.get(label, "")
            cleaned = _EMPTY_DAY_RE.sub("", v).strip()
            if cleaned:
                out[schema_key] = v

        return out

    @staticmethod
    def _collect_th_td(soup) -> dict[str, str]:
        fields: dict[str, str] = {}
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = " ".join(td.get_text(" ", strip=True).split())
            if label and label not in fields:
                fields[label] = value
        return fields


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NaviScraper()
    scraper.execute(f"{BASE}{LIST_PATH}")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

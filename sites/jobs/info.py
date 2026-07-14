"""
ガテン職(info) — 建設業の求人・転職サイト GATEN職 (https://gaten.info)

取得対象:
    - 求人一覧 (/list) に掲載された全求人の企業/募集情報
    - 会社名・所在地・電話番号・従業員数・HP・担当者・勤務時間・休日・業種 等

取得フロー:
    1. 一覧ページ /list?page=N を巡回 (1ページ25件, 総 約7,800件)
    2. 各カードから業種 (.company-industory-type) と詳細リンク /job/{id} を取得
    3. 詳細ページ /job/{id} を開き、構造化フィールドを抽出して即 yield
    4. カードが無くなるページで終了

備考:
    - Cloudflare の managed challenge が有効なため requests では 403。
      Playwright (DynamicCrawler) はチャレンジを通過できるため Dynamic を採用。
    - 求人紹介文・仕事内容・企業理念など自由記述(プロース)は著作権リスク回避のため取得しない。

実行方法:
    python scripts/sites/jobs/info.py
    docker compose exec worker python /app/bin/run_flow.py --site-id info
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from urllib.parse import urljoin

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 都道府県 (所在地文字列の先頭から抽出)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,3}[-(]?\d{1,4}[-)]?\d{3,4}")


class GatenInfo(DynamicCrawler):
    """ガテン職(info) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種タグ",
        "契約形態",
        "給与報酬",
        "勤務地",
        "募集業種",
        "対象となる方",
        "担当者カナ",
    ]

    def get_soup(self, url: str, wait_until: str = "domcontentloaded"):
        return super().get_soup(url, wait_until=wait_until)

    def parse(self, url: str):
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cards = soup.select(".companylist.d-none.d-xl-block .companylist_outer")
            if not cards:
                # フォールバック: レイアウト差異に備え全カードを拾う
                cards = soup.select(".companylist_outer")
            if not cards:
                break

            if page == 1:
                self.total_items = self._read_total(soup)

            for card in cards:
                link = card.select_one('a[href*="/job/"]')
                if not link or not link.get("href"):
                    continue
                detail_href = re.sub(r"#.*$", "", link["href"])
                detail_url = urljoin(page_url, detail_href)
                industries = [
                    x.get_text(strip=True)
                    for x in card.select(".company-industory-type")
                    if x.get_text(strip=True)
                ]
                list_name = card.select_one("h3.companyname")
                list_name = list_name.get_text(strip=True) if list_name else ""

                try:
                    item = self._scrape_detail(detail_url, industries, list_name)
                except Exception as e:  # noqa: BLE001 — 個別詳細の失敗は握って継続
                    self.error_count += 1
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, industries: list, list_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {
            Schema.URL: url,
            Schema.NAME: "",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMP_NUM: "",
            Schema.REP_NM: "",
            Schema.HP: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.CAT_SITE: " / ".join(industries),
            "職種タグ": "",
            "契約形態": "",
            "給与報酬": "",
            "勤務地": "",
            "募集業種": "",
            "対象となる方": "",
            "担当者カナ": "",
        }

        # 会社名
        name_el = soup.select_one("h2.detail-title")
        item[Schema.NAME] = name_el.get_text(strip=True) if name_el else list_name

        # 企業概要 (従業員数 / 所在地 / ウェブサイト)
        company = self._pairs(
            soup, "li.detail-company-info-item",
            ".detail-company-info-label", ".detail-company-info-text",
        )
        item[Schema.EMP_NUM] = company.get("従業員数", "")

        addr_raw = company.get("所在地", "")
        if addr_raw:
            m = _POST_PATTERN.search(addr_raw)
            if m:
                item[Schema.POST_CODE] = m.group(1)
                addr_raw = addr_raw[m.end():].strip()
            addr_raw = addr_raw.lstrip("〒 　").strip()
            pm = _PREF_PATTERN.search(addr_raw)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr_raw[pm.end():].strip() or addr_raw
            else:
                item[Schema.ADDR] = addr_raw

        # ウェブサイト (href 優先)
        for li in soup.select("li.detail-company-info-item"):
            lab = li.select_one(".detail-company-info-label")
            if lab and "ウェブサイト" in lab.get_text(strip=True):
                a = li.select_one("a[href]")
                item[Schema.HP] = a["href"].strip() if a and a.get("href") else \
                    company.get("ウェブサイト", "")
                break

        # 応募・選考 (連絡先 / 担当者)
        recruit = self._pairs(
            soup, "li.detail-recruitment-info-item",
            ".detail-recruitment-info-label", ".detail-recruitment-info-text",
        )
        tel_src = recruit.get("連絡先", "")
        if tel_src:
            tels = []
            for t in _TEL_PATTERN.findall(tel_src):
                if t not in tels:
                    tels.append(t)
            item[Schema.TEL] = " / ".join(tels)

        rep = recruit.get("担当者", "")
        if rep:
            km = re.search(r"[（(]([^）)]+)[）)]", rep)
            if km:
                item["担当者カナ"] = km.group(1).strip()
                rep = re.sub(r"[（(][^）)]+[）)]", "", rep)
            item[Schema.REP_NM] = rep.strip()

        # 募集要項 (guideline)
        guide = self._guideline(soup)
        item[Schema.TIME] = guide.get("勤務時間", "")
        item[Schema.HOLIDAY] = guide.get("休日休暇", "")
        item["契約形態"] = guide.get("契約形態", "")
        item["給与報酬"] = guide.get("給与/報酬", "")
        item["勤務地"] = guide.get("勤務地", "")
        item["募集業種"] = guide.get("募集業種", "")
        item["対象となる方"] = guide.get("対象となる方", "")

        # 職種タグ
        tags = [
            li.get_text(strip=True)
            for li in soup.select(".detail-inner-tag .tag-list li")
            if li.get_text(strip=True)
        ]
        item["職種タグ"] = " / ".join(dict.fromkeys(tags))

        return item

    @staticmethod
    def _pairs(soup, item_sel: str, label_sel: str, text_sel: str) -> dict:
        out = {}
        for li in soup.select(item_sel):
            lab = li.select_one(label_sel)
            val = li.select_one(text_sel)
            if not lab:
                continue
            key = lab.get_text(strip=True)
            out[key] = re.sub(r"\s+", " ", val.get_text(" ", strip=True)).strip() if val else ""
        return out

    @staticmethod
    def _guideline(soup) -> dict:
        out = {}
        for title in soup.select(".detail-recruitment-guideline-title"):
            li = title.find_parent("li") or title.parent
            val = li.select_one(".detail-recruitment-guideline-text") if li else None
            key = title.get_text(strip=True)
            out[key] = re.sub(r"\s+", " ", val.get_text(" ", strip=True)).strip() if val else ""
        return out

    @staticmethod
    def _read_total(soup) -> int:
        el = soup.select_one(".search-detail") or soup.select_one(".search-title")
        if el:
            m = re.search(r"([\d,]+)\s*件", el.get_text())
            if m:
                return int(m.group(1).replace(",", ""))
        return 0


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GatenInfo()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://gaten.info/list")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

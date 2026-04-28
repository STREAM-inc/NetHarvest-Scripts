"""
人材サービス総合サイト — 職業紹介事業所スクレイパー

取得対象:
    - 職業紹介事業の許可・届出事業所（全国 38,232件）
    - 運営: 厚生労働省

取得フロー:
    1. prepare(): セッション初期化 → GICB101010.do → 職業紹介事業ページへ遷移
    2. parse(): 全国検索 POST → 一覧ページを巡回 (20件/ページ, 約1,912ページ)

補足:
    - 既存の jinzai_hellowork は「労働者派遣事業」を取得済み
    - 本スクレイパーは「職業紹介事業」（別事業区分）を対象とする
    - 一覧ページに就職者数・離職者数(最新年度)・手数料区分を含む

実行方法:
    python scripts/sites/government/jinzai_service.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id jinzai_service
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb"
TOP_URL = f"{BASE_URL}/GICB101010.do"
SEARCH_URL = f"{BASE_URL}/GICB102030.do"

_PREF_RE = re.compile(
    r"^(北海道|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県|東京都|(?:大阪|京都)府)"
)

_ALL_PREFS = {
    "cbZenkoku": "1", "cbHokkaido_Tohoku": "1",
    "cbHokkaido": "1", "cbAomori": "1", "cbIwate": "1", "cbMiyagi": "1",
    "cbAkita": "1", "cbYamagata": "1", "cbFukushima": "1",
    "cbKanto": "1", "cbIbaragi": "1", "cbTochigi": "1", "cbGunma": "1",
    "cbSaitama": "1", "cbChiba": "1", "cbTokyo": "1", "cbKanagawa": "1",
    "cbKoshinetsu_Hokuriku": "1", "cbNigata": "1", "cbToyama": "1",
    "cbIshikawa": "1", "cbFukui": "1", "cbYamanashi": "1", "cbNagano": "1",
    "cbTokai": "1", "cbGifu": "1", "cbShizuoka": "1", "cbAichi": "1", "cbMie": "1",
    "cbKinki": "1", "cbShiga": "1", "cbKyoto": "1", "cbOsaka": "1",
    "cbHyogo": "1", "cbNara": "1", "cbWakayama": "1",
    "cbChugoku": "1", "cbTottori": "1", "cbShimane": "1",
    "cbOkayama": "1", "cbHiroshima": "1", "cbYamaguchi": "1",
    "cbShikoku": "1", "cbTokushima": "1", "cbKagawa": "1", "cbEhime": "1", "cbKochi": "1",
    "cbKyushu_Okinawa": "1", "cbFukuoka": "1", "cbSaga": "1", "cbNagasaki": "1",
    "cbKumamoto": "1", "cbOita": "1", "cbMiyazaki": "1", "cbKagoshima": "1", "cbOkinawa": "1",
}


def _clean(el) -> str:
    if el is None:
        return ""
    text = el.get_text(" ", strip=True) if hasattr(el, "get_text") else str(el)
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


class JinzaiServiceScraper(StaticCrawler):
    """人材サービス総合サイト 職業紹介事業所スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "事業所名称", "許可受理番号", "許可届出年月日",
        "就職者_4ヶ月以上", "就職者_うち無期", "就職者_4ヶ月未満",
        "離職者数", "手数料", "返戻金制度",
    ]

    def prepare(self):
        self.session.get(
            f"{TOP_URL}?action=initDisp&screenId=GICB101010",
            timeout=self.TIMEOUT,
        )
        self.session.post(
            TOP_URL,
            data={
                "action": "transition",
                "screenId": "GICB101010",
                "params": "1",
                "maba_vrbs": "",
            },
            timeout=self.TIMEOUT,
        )

    def _post_soup(self, data: dict) -> BeautifulSoup | None:
        try:
            resp = self.session.post(SEARCH_URL, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.warning("POST 失敗: %s", e)
            return None

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self._post_soup({
            "action": "search",
            "screenId": "GICB102030",
            "params": "none",
            **_ALL_PREFS,
            "txtJigyonushiName": "", "cbJigyonushiName": "1",
            "txtJigyoshoName": "", "cbJigyoshoName": "1",
            "ucKyokatodokedeNo1": "", "txtKyokatodokedeNo2": "", "txtKyokatodokedeNo3": "",
            "hfScrollTop": "0", "maba_vrbs": "",
        })
        if soup is None:
            return

        count_el = soup.select_one("#ID_lbSearchCount")
        if count_el:
            self.total_items = int(count_el.get_text(strip=True).replace(",", ""))
            self.logger.info("総件数: %d 件", self.total_items)

        page = 1
        while True:
            if page > 1:
                time.sleep(self.DELAY)
                soup = self._post_soup({
                    "action": "page",
                    "screenId": "GICB102030",
                    "params": str(page),
                    "maba_vrbs": "",
                })
                if soup is None:
                    break

            rows = soup.select("#search tr[onmouseover]")
            if not rows:
                self.logger.info("ページ %d: 行なし → 終了", page)
                break

            self.logger.info("ページ %d: %d 行取得", page, len(rows))
            for row in rows:
                try:
                    item = self._parse_row(row)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("行スキップ: %s", e)

            page += 1

    def _parse_row(self, row) -> dict | None:
        def t(sel: str) -> str:
            el = row.select_one(sel)
            return _clean(el)

        kyoka_no = t("#ID_lbKyokatodokedeNo")
        if not kyoka_no:
            return None

        kyoka_date = t("#ID_lbKyokatodokedeDate")
        name = t("#ID_lbJigyonushiName")
        jigyosho = t("#ID_lbJigyoshoName")

        address = t("#ID_lbJigyoshoAddress")
        pref, addr_short = "", address
        m = _PREF_RE.match(address)
        if m:
            pref = m.group(1)
            addr_short = address[m.end():].strip()

        tel = t("#ID_lbTel")

        hp = ""
        for sel in ("#ID_linkJigyonushiURL", "#ID_linkJigyoshoURL"):
            a = row.select_one(sel)
            if a and a.get("href", "").startswith("http"):
                hp = a["href"]
                break

        detail_a = row.select_one('a[href*="action=detail"]')
        detail_url = ""
        if detail_a:
            href = detail_a.get("href", "")
            detail_url = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('./')}"

        shushoku_yuki = _clean(row.select_one("#ID_lbShushokusha1yuki3"))
        shushoku_muki = _clean(row.select_one("#ID_lbShushokusha1muki3"))
        shushoku_4m = _clean(row.select_one("#ID_lbShushokusha2yuki3"))
        rishoku = _clean(row.select_one("#ID_lbRishokusha3"))

        # 手数料・返戻金はtd内テキスト（セレクタIDなし）
        tds = row.select("td")
        tesuryo = _clean(tds[-3]) if len(tds) >= 3 else ""
        henikin = _clean(tds[-2]) if len(tds) >= 2 else ""

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.PREF: pref,
            Schema.POST_CODE: "",
            Schema.ADDR: addr_short,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CO_NUM: "",
            Schema.REP_NM: "",
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            Schema.CAT_SITE: "職業紹介事業",
            "事業所名称": jigyosho,
            "許可受理番号": kyoka_no,
            "許可届出年月日": kyoka_date,
            "就職者_4ヶ月以上": shushoku_yuki,
            "就職者_うち無期": shushoku_muki,
            "就職者_4ヶ月未満": shushoku_4m,
            "離職者数": rishoku,
            "手数料": tesuryo,
            "返戻金制度": henikin,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JinzaiServiceScraper()
    scraper.execute(f"{TOP_URL}?action=initDisp&screenId=GICB101010")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

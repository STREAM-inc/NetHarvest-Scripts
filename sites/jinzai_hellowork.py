"""
人材サービス総合サイト (厚生労働省) — 労働者派遣事業所スクレイパー

取得対象:
    - 労働者派遣事業の許可・届出事業所（全国 45,587件）

取得フロー:
    1. prepare(): セッション初期化 → 検索フォームページへ遷移
    2. parse(): 全国検索 POST → 一覧ページを巡回 → 各詳細ページ取得

実行方法:
    # ローカルテスト
    python scripts/sites/government/jinzai_hellowork.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id jinzai_hellowork
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb"
SEARCH_URL = f"{BASE_URL}/GICB102010.do"

_PREF_PATTERN = re.compile(
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


class JinzaiHelloworkScraper(StaticCrawler):
    """人材サービス総合サイト 労働者派遣事業所スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "事業所名称", "許可受理番号", "許可届出年月日",
        "得意とする職種", "派遣労働者数", "派遣先件数",
        "派遣料金平均額", "派遣労働者賃金平均額", "マージン率",
        "労使協定の締結", "キャリア形成支援制度", "その他の情報",
        "特定地域づくり事業認定年月日", "備考",
    ]

    def prepare(self):
        self.session.get(
            f"{BASE_URL}/GICB101010.do?action=initDisp&screenId=GICB101010",
            timeout=self.TIMEOUT,
        )
        self.session.post(
            f"{BASE_URL}/GICB101010.do",
            data={"screenId": "GICB101010", "action": "transition", "params": "0", "maba_vrbs": ""},
            timeout=self.TIMEOUT,
        )

    def _post_soup(self, data: dict) -> BeautifulSoup:
        resp = self.session.post(SEARCH_URL, data=data, timeout=self.TIMEOUT)
        resp.encoding = "utf-8"
        return BeautifulSoup(resp.text, "html.parser")

    def parse(self, url: str):
        soup = self._post_soup({
            "screenId": "GICB102010",
            "action": "search",
            "params": "none",
            **_ALL_PREFS,
            "txtJigyonushiName": "", "cbJigyonushiName": "1",
            "txtJigyoshoName": "", "cbJigyoshoName": "1",
            "ucKyokatodokedeNo1": "", "txtKyokatodokedeNo2": "", "txtKyokatodokedeNo3": "",
            "hfScrollTop": "0", "maba_vrbs": "",
        })

        count_el = soup.select_one("#ID_lbSearchCount")
        if count_el:
            self.total_items = int(count_el.get_text(strip=True).replace(",", ""))

        page = 1
        while True:
            if page > 1:
                soup = self._post_soup({
                    "screenId": "GICB102010",
                    "action": "page",
                    "params": str(page),
                    "maba_vrbs": "",
                })

            rows = soup.select("#search tr[onmouseover]")
            if not rows:
                break

            for row in rows:
                try:
                    detail_a = row.select_one('a[href*="action=detail"]')
                    if not detail_a:
                        continue
                    detail_url = urljoin(f"{BASE_URL}/GICB102010.do", detail_a["href"])
                    detail_resp = self.session.get(detail_url, timeout=self.TIMEOUT)
                    detail_resp.encoding = "utf-8"
                    detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                    item = self._parse_detail(detail_soup, detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("行スキップ: %s", e)

            page += 1

    def _parse_detail(self, soup: BeautifulSoup, url: str) -> dict | None:
        def _text(sel: str) -> str:
            el = soup.select_one(sel)
            return el.get_text(strip=True) if el else ""

        def _field(title: str) -> str:
            for row in soup.select("#searchDet tr"):
                th = row.select_one("td.searchDet_title")
                if th and title in th.get_text():
                    td = row.select_one("td.searchDet_data")
                    return td.get_text(" ", strip=True) if td else ""
            return ""

        address = _text("#ID_lbJigyoshoAddress")
        pref, addr_short = "", address
        m = _PREF_PATTERN.match(address)
        if m:
            pref = m.group(1)
            addr_short = address[m.end():].strip()

        hp = ""
        for sel in ("#ID_linkJigyonushiURL", "#ID_linkJigyoshoURL"):
            a = soup.select_one(sel)
            if a and a.get("href", "").startswith("http"):
                hp = a["href"]
                break

        career_parts = [_text(f"#ID_lbCareer0{i}") for i in range(1, 5)]
        career = "、".join(c for c in career_parts if c.strip() and c.strip() != "\xa0")

        sonota_parts = [_text(f"#ID_lbSonota0{i}") for i in range(1, 5)]
        sonota = "、".join(s for s in sonota_parts if s.strip() and s.strip() != "\xa0")

        return {
            Schema.URL: url,
            Schema.NAME: _text("#ID_lbJigyonushiName"),
            Schema.PREF: pref,
            Schema.ADDR: addr_short,
            Schema.TEL: _text("#ID_lbTel"),
            Schema.HP: hp,
            "事業所名称": _text("#ID_lbJigyoshoName"),
            "許可受理番号": _text("#ID_lbKyokatodokedeNo"),
            "許可届出年月日": _text("#ID_lbKyokatodokedeDate"),
            "得意とする職種": _text("#ID_lbTokui"),
            "派遣労働者数": _text("#ID_lbRoudousu"),
            "派遣先件数": _text("#ID_lbHakenkensu"),
            "派遣料金平均額": _text("#ID_lbRyokinave"),
            "派遣労働者賃金平均額": _text("#ID_lbChinginave"),
            "マージン率": _text("#ID_lbMargen"),
            "労使協定の締結": _field("労使協定の締結"),
            "キャリア形成支援制度": career,
            "その他の情報": sonota,
            "特定地域づくり事業認定年月日": _text("#ID_lbNinteiDate"),
            "備考": _field("備考"),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JinzaiHelloworkScraper()
    scraper.execute("https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb/GICB101010.do?action=initDisp&screenId=GICB101010")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

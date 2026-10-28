"""
対象サイト: https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do?action=initDisp&screenId=GECA110010
ハローワークインターネットサービス 求人情報 (清掃・洗浄の職業 / 1都3県)

取得フロー (すべて requests で取得可能。Playwright 不要):
    1. 引数 url (かんたん検索フォーム GECA110010) を GET してセッション Cookie とフォーム値を取得
    2. 「もっと詳しい条件を入力する」(searchShosaiBtn) を POST → 詳細検索フォーム (GECA110030)
    3. 詳細検索フォームに以下をセットして searchBtn を POST
         - 求人区分: 一般求人 (kjKbnRadioBtn=1) のフルタイム + パート (ippanCKBox=1,2)
         - 就業場所: todohukenHidden = 13 / 14 / 12 / 11 (都道府県ごとに 1 回ずつ検索)
         - 職業分類: sKGYBRUIJo1 = 096 (中分類「清掃・洗浄作業員」= 096-01 ビル・建物清掃員,
           096-02 ハウスクリーニング作業員, 096-03 旅館・ホテル客室清掃整備員, 096-04 道路・公園清掃員,
           096-05 ごみ・資源収集作業員, 096-99 その他 …)
         - 表示件数: 50 件/ページ
    4. 一覧の各求人の詳細ページ (action=dispDetailBtn の GET URL) を get_soup() で取得し即 yield
    5. 「次へ＞」(fwListNaviBtnNext) を POST して最終ページ (次へ が disabled) まで繰り返す

補足:
    - 検索結果の件数上限は無い (東京都×096 = 6,254 件で最終ページ 126 まで到達を確認) ため
      市区町村分割は行わない。
    - ハローワークの職業分類検索は求人に登録された職業分類 (最大 3 つ) のいずれかに一致すれば
      ヒットする。そのため主職種が「マンション管理人 (057-01)」等で、副職種に清掃を含む求人も
      含まれる (サイトの検索結果そのまま)。主職種コードは EXTRA「職業分類コード」で判別可能。
    - 「仕事内容」は自由記述だが、依頼の取得カラムとして明示指定されているため取得する。
"""

import math
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Generator

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 就業場所 (検索順): 東京都・神奈川県・千葉県・埼玉県
_TARGET_PREFS: list[tuple[str, str]] = [
    ("13", "東京都"),
    ("14", "神奈川県"),
    ("12", "千葉県"),
    ("11", "埼玉県"),
]

# 職業分類 中分類 096「清掃・洗浄作業員」
_SHOKUGYO_BUNRUI = "096"

_PAGE_SIZE = "50"
_MAX_ATTEMPTS = 3

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_COUNT_PATTERN = re.compile(r"([\d,]+)\s*件中")


class Hellowork2Scraper(StaticCrawler):
    """ハローワーク 求人情報 (清掃・洗浄の職業 / 東京都・神奈川県・千葉県・埼玉県) スクレイパー"""

    DELAY = 2.0
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "求人番号",
        "受付年月日",
        "紹介期限日",
        "受理安定所",
        "求人区分",
        "職種",
        "職業分類コード",
        "仕事内容",
        "雇用形態",
        "雇用期間",
        "就業形態",
        "就業場所",
        "就業場所_都道府県",
        "事業所番号",
        "担当者",
        "FAX",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref_code, pref_name in _TARGET_PREFS:
            try:
                soup = self._search(url, pref_code)
            except RuntimeError as e:
                self.logger.warning("[%s] 検索に失敗したためスキップ: %s", pref_name, e)
                continue

            total = self._read_count(soup)
            max_pages = math.ceil(total / int(_PAGE_SIZE)) + 2 if total else 1000
            self.logger.info("[%s] 検索結果 %s 件", pref_name, total)

            page = 1
            while True:
                for detail_url in self._detail_urls(url, soup):
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    item = self._scrape_detail(detail_url, pref_name)
                    if item:
                        yield item

                next_btn = soup.select_one('input[name="fwListNaviBtnNext"]')
                if next_btn is None or next_btn.has_attr("disabled") or page >= max_pages:
                    self.logger.info("[%s] 最終ページ %d に到達", pref_name, page)
                    break
                try:
                    soup = self._submit(url, soup, [("fwListNaviBtnNext", "次へ＞")])
                except RuntimeError as e:
                    self.logger.warning("[%s] ページ %d → 次ページ遷移失敗: %s", pref_name, page, e)
                    break
                page += 1

    # ------------------------------------------------------------------
    # 一覧 (フォーム POST)
    # ------------------------------------------------------------------
    def _search(self, url: str, pref_code: str) -> bs4.BeautifulSoup:
        """かんたん検索 → 詳細検索フォーム → 条件指定検索 を行い、一覧 1 ページ目を返す。"""
        last_error = None
        for attempt in range(_MAX_ATTEMPTS):
            if attempt:
                time.sleep(min(2 ** attempt * 5, 60))
            try:
                time.sleep(self.DELAY)
                resp = self.session.get(url, timeout=self.TIMEOUT)
                resp.raise_for_status()
                soup = bs4.BeautifulSoup(resp.text, "html.parser")
                self._check_page(soup)
                soup = self._submit(url, soup, [("searchShosaiBtn", "もっと詳しい条件を入力する")], retry=False)
                overrides = {
                    "kjKbnRadioBtn": ["1"],
                    "ippanCKBox": ["1", "2"],
                    "todohukenHidden": [pref_code],
                    "sKGYBRUIJo1": [_SHOKUGYO_BUNRUI],
                    "fwListNaviDisp": [_PAGE_SIZE],
                    "fwListNaviDispTop": [_PAGE_SIZE],
                    "fwListNaviDispBtm": [_PAGE_SIZE],
                }
                soup = self._submit(url, soup, [("searchBtn", "検索する")], overrides=overrides, retry=False)
                if soup.select_one('input[name="fwListNowPage"]') is None and not soup.select('a[href*="dispDetailBtn"]'):
                    raise RuntimeError("一覧ページが返らなかった")
                return soup
            except (requests.RequestException, RuntimeError) as e:
                last_error = e
                self.logger.warning("検索失敗 (pref=%s, %d/%d): %s", pref_code, attempt + 1, _MAX_ATTEMPTS, e)
        raise RuntimeError(f"検索が {_MAX_ATTEMPTS} 回失敗: {last_error}")

    def _submit(
        self,
        url: str,
        soup: bs4.BeautifulSoup,
        buttons: list[tuple[str, str]],
        overrides: dict[str, list[str]] | None = None,
        retry: bool = True,
    ) -> bs4.BeautifulSoup:
        """現在ページの form_1 を (overrides で上書きして) POST し、結果ページを返す。"""
        form = soup.select_one('form[name="form_1"]')
        if form is None:
            raise RuntimeError("form_1 が見つからない")
        action = urllib.parse.urljoin(url, form.get("action", ""))
        overrides = overrides or {}

        data: list[tuple[str, str]] = []
        for el in form.select("input, select"):
            name = el.get("name")
            typ = (el.get("type") or "").lower()
            if not name or name in overrides or typ in ("button", "submit", "image"):
                continue
            if typ in ("checkbox", "radio") and not el.has_attr("checked"):
                continue
            if el.name == "select":
                opt = el.select_one("option[selected]")
                data.append((name, opt.get("value", "") if opt else ""))
                continue
            data.append((name, el.get("value", "")))
        for name, values in overrides.items():
            data.extend((name, v) for v in values)
        data.extend(buttons)

        attempts = _MAX_ATTEMPTS if retry else 1
        last_error = None
        for attempt in range(attempts):
            if attempt:
                time.sleep(min(2 ** attempt * 5, 60))
            time.sleep(self.DELAY)
            try:
                resp = self.session.post(action, data=data, timeout=60)
                resp.raise_for_status()
                result = bs4.BeautifulSoup(resp.text, "html.parser")
                self._check_page(result)
                return result
            except (requests.RequestException, RuntimeError) as e:
                last_error = e
                self.logger.warning("POST 失敗 (%d/%d): %s", attempt + 1, attempts, e)
        raise RuntimeError(f"POST が {attempts} 回失敗: {last_error}")

    @staticmethod
    def _check_page(soup: bs4.BeautifulSoup) -> None:
        title = soup.title.get_text(strip=True) if soup.title else ""
        if "エラー" in title:
            raise RuntimeError(f"エラー画面: {title}")

    @staticmethod
    def _read_count(soup: bs4.BeautifulSoup) -> int:
        hidden = soup.select_one('input[name="kyujinkensu"]')
        if hidden and hidden.get("value", "").isdigit():
            return int(hidden["value"])
        m = _COUNT_PATTERN.search(soup.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else 0

    @staticmethod
    def _detail_urls(url: str, soup: bs4.BeautifulSoup) -> list[str]:
        urls: list[str] = []
        for a in soup.select('a[href*="action=dispDetailBtn"]'):
            full = urllib.parse.urljoin(url, a.get("href", "").strip())
            if full not in urls:
                urls.append(full)
        return urls

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url: str, pref_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        def text(css: str) -> str:
            el = soup.select_one(css)
            return el.get_text(" ", strip=True) if el else ""

        name = text("#ID_jgshMei")
        if not name:
            self.logger.warning("事業所名が取得できない: %s", detail_url)
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: text("#ID_jgshMeiKana"),
            Schema.CO_NUM: text("#ID_hoNinNo"),
            Schema.POS_NM: text("#ID_yshk"),
            Schema.REP_NM: text("#ID_dhshaMei"),
            Schema.EMP_NUM: text("#ID_jgisKigyoZentai"),
            Schema.CAP: text("#ID_shkn"),
            Schema.OPEN_DATE: text("#ID_setsuritsuNen"),
            Schema.LOB: text("#ID_jigyoNy"),
            Schema.CAT_SITE: text("#ID_sngBrui"),
            Schema.HP: self._labeled(soup, "ホームページ"),
            Schema.TEL: text("#ID_ttsTel"),
        }

        # 事業所所在地
        addr = text("#ID_szci")
        post = text("#ID_szciYbn")
        m = _POST_CODE_PATTERN.search(post)
        item[Schema.POST_CODE] = m.group(1) if m else ""
        item[Schema.ADDR] = addr
        pm = _PREF_PATTERN.match(addr)
        item[Schema.PREF] = pm.group(1) if pm else ""

        # 就業場所
        shg_post = text("#ID_shgBsYubinNo")
        shg_addr = text("#ID_shgBsJusho")
        item["就業場所"] = f"{shg_post} {shg_addr}".strip()
        spm = _PREF_PATTERN.match(shg_addr)
        item["就業場所_都道府県"] = spm.group(1) if spm else pref_name

        # 職種 / 職業分類コード (job tag リンクの code=XXX-XX)
        item["職種"] = text("#ID_sksu")
        code = ""
        link = soup.select_one("#ID_shokugyojohou")
        if link is not None:
            qs = urllib.parse.parse_qs(urllib.parse.urlsplit(link.get("href", "")).query)
            code = (qs.get("code") or [""])[0]
        item["職業分類コード"] = code

        item["求人番号"] = text("#ID_kjNo")
        item["受付年月日"] = text("#ID_uktkYmd")
        item["紹介期限日"] = text("#ID_shkiKigenHi")
        item["受理安定所"] = text("#ID_juriAtsh")
        item["求人区分"] = text("#ID_kjKbn")
        item["仕事内容"] = text("#ID_shigotoNy")
        # td 全体は「パート労働者 正社員登用の有無 なし」等。付記ラベル以降を落とす
        koyo = self._labeled(soup, "雇用形態", as_text=True) or text("#ID_koyoKeitai")
        item["雇用形態"] = re.split(r"\s*(?:正社員以外の名称|正社員登用の有無)", koyo)[0].strip()
        item["雇用期間"] = text("#ID_koyoKikan")
        # 「派遣・請負等」欄は派遣・請負求人のみ表示 (例: "就業形態 請負")。無ければ空
        haken = text("#ID_hakenUkeoiToShgKeitai")
        item["就業形態"] = re.sub(r"^就業形態\s*", "", haken)
        item["事業所番号"] = text("#ID_jgshNo")
        item["担当者"] = text("#ID_ttsTts")
        item["FAX"] = text("#ID_ttsFax")
        return item

    @staticmethod
    def _labeled(soup: bs4.BeautifulSoup, label: str, as_text: bool = False) -> str:
        for th in soup.find_all("th"):
            if th.get_text(" ", strip=True).startswith(label):
                td = th.find_next_sibling("td")
                if td:
                    a = None if as_text else td.find("a", href=True)
                    return a["href"].strip() if a else td.get_text(" ", strip=True)
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Hellowork2Scraper()
    scraper.execute("https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do?action=initDisp&screenId=GECA110010")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

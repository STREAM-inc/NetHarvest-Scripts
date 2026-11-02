"""
対象サイト: https://www.jobkita.jp/shukatsu/2027/job/search/
ジョブキタ就活2027 (北海道の新卒向け就職情報サイト) — 掲載企業 スクレイパー

取得フロー:
    1. 起点 url (検索条件設定ページ) から市区町村チェックボックスのラベルを読み、
       都道府県が省略された本社所在地 (例: "富良野市八幡丘") を北海道と判定する材料にする
    2. 検索結果一覧 (url の兄弟パス ../list/?page=N, 20求人/ページ・約19ページ) を巡回
    3. 求人カード /job/detail/{企業ID}/{求人番号}/ から企業IDを抽出 (企業単位で重複除去)
    4. 企業詳細 /job/detail/{企業ID}/company/ の会社データ表 (th/td) を取得
    5. 電話番号は企業ページに無く求人詳細の #jobContact にのみあるため、
       その企業の最初の求人ページから取得し、1社ごとに即 yield

備考:
    - 北海道絞り込みはしない (全件取得)。
    - 代表者は掲載ラベル (「代表者」等) がある企業のみ。サンプル13社では掲載無し。
    - 利用規約 (https://www.haj.co.jp/rules/?mediaid=95) は「転載・販売・営利目的利用」を
      禁止しているが、スクレイピング/クローリングの明示的禁止条項は無い。
"""
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_COMPANY_ID_PATTERN = re.compile(r"/job/detail/(\d+)/(\d{4})/")
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川"
    r"|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島"
    r"|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3})[-－‐ー]?(\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,4}[-－‐(（]\d{1,4}[-－‐)）]\d{3,4}|0\d{9,10}")

# 北海道の全市 (検索ページの市区町村ラベルを補完する)
_HOKKAIDO_CITIES = (
    "札幌市", "函館市", "小樽市", "旭川市", "室蘭市", "釧路市", "帯広市", "北見市", "夕張市",
    "岩見沢市", "網走市", "留萌市", "苫小牧市", "稚内市", "美唄市", "芦別市", "江別市",
    "赤平市", "紋別市", "士別市", "名寄市", "三笠市", "根室市", "千歳市", "滝川市", "砂川市",
    "歌志内市", "深川市", "富良野市", "登別市", "恵庭市", "伊達市", "北広島市", "石狩市",
    "北斗市",
)

# 会社データ表の th ラベル → Schema
_LABEL_MAP = {
    "事業内容": Schema.LOB,
    "資本金": Schema.CAP,
    "社員数": Schema.EMP_NUM,
    "従業員数": Schema.EMP_NUM,
    "売上高": Schema.SALES,
    "ホームページ": Schema.HP,
}

# 短い数値系の補足データ (括弧内の補足文は除去して出力)
_EXTRA_LABELS = ("平均年齢", "平均勤続年数", "月平均所定外労働時間", "有給休暇の消化率")


def _clean(s) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _strip_note(s: str) -> str:
    """'29.3歳 （数年前から…）' → '29.3歳' のように補足の括弧書きを除去する。"""
    return re.split(r"\s*[（(※]", s, maxsplit=1)[0].strip()


def _normalize_date(s: str) -> str:
    """'1963年6月' → '1963-06'、'1958年' → '1958'。年が取れなければ原文。"""
    m = re.search(r"(\d{4})年(?:\s*(\d{1,2})月(?:\s*(\d{1,2})日)?)?", s)
    if not m:
        return s
    y, mo, d = m.groups()
    if d:
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    if mo:
        return f"{y}-{int(mo):02d}"
    return y


class Jobkita2Scraper(StaticCrawler):
    """ジョブキタ就活2027 掲載企業 スクレイパー"""
    DELAY = 3.0  # 低頻度巡回 (1社あたり 企業ページ+求人ページの2リクエスト)
    EXTRA_COLUMNS = list(_EXTRA_LABELS)

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_soup = self.get_soup(url)
        hokkaido_places = set(_HOKKAIDO_CITIES)
        if search_soup:
            for label in search_soup.select("label"):
                text = _clean(label.get_text())
                if re.fullmatch(r"\S+?[市町村]", text):
                    hokkaido_places.add(text)
        self._hokkaido_places = tuple(sorted(hokkaido_places, key=len, reverse=True))

        list_url = urljoin(url, "../list/")
        seen: set[str] = set()
        page = 1
        while True:
            soup = self.get_soup(f"{list_url}?page={page}")
            if soup is None:
                break
            cards = soup.select(".jobItem")
            if not cards:
                break

            for card in cards:
                a = card.select_one('a[href*="/job/detail/"]')
                if not a:
                    continue
                m = _COMPANY_ID_PATTERN.search(a.get("href", ""))
                if not m or m.group(1) in seen:
                    continue
                company_id = m.group(1)
                seen.add(company_id)

                job_url = urljoin(list_url, a["href"])
                company_url = urljoin(list_url, f"../detail/{company_id}/company/")
                item = self._scrape_company(company_url, job_url)
                if item:
                    yield item

            if not soup.select_one(f'a[href*="page={page + 1}"]'):
                break
            page += 1

    def _scrape_company(self, company_url: str, job_url: str) -> dict | None:
        soup = self.get_soup(company_url)
        if soup is None:
            return None

        name_el = soup.select_one("h1.jobDetailHeader_title") or soup.select_one("h1.accountName")
        name = _clean(name_el.get_text()) if name_el else ""
        if not name:
            return None

        item = {
            Schema.URL: company_url,
            Schema.NAME: name,
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.REP_NM: "",
            Schema.EMP_NUM: "",
            Schema.LOB: "",
            Schema.CAP: "",
            Schema.SALES: "",
            Schema.HP: "",
            Schema.OPEN_DATE: "",
        }
        for col in _EXTRA_LABELS:
            item[col] = ""

        for tr in soup.select(".jobSection table tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text()).strip("[]［］")
            value = _clean(td.get_text(" "))
            if not value:
                continue

            if label in _LABEL_MAP:
                key = _LABEL_MAP[label]
                if key == Schema.HP:
                    a = td.find("a", href=True)
                    value = a["href"] if a and a["href"].startswith("http") else value
                if not item[key]:
                    item[key] = value
            elif label == "本社所在地":
                self._fill_address(item, value)
            elif label in ("創立", "設立", "創業"):
                if not item[Schema.OPEN_DATE]:
                    item[Schema.OPEN_DATE] = _normalize_date(value)
            elif "代表" in label:
                item[Schema.REP_NM] = value
            elif label in _EXTRA_LABELS:
                item[label] = _strip_note(value)

        item[Schema.TEL] = self._fetch_tel(job_url)
        return item

    def _fill_address(self, item: dict, value: str) -> None:
        # 「(登記上の所在地。…)」などの補足は除去
        addr = re.split(r"\s*[（(]", value, maxsplit=1)[0].strip()
        m = _POST_CODE_PATTERN.match(addr)
        if m:
            item[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            addr = addr[m.end():].strip()
        pm = _PREF_PATTERN.match(addr)
        if pm:
            item[Schema.PREF] = pm.group(1)
            addr = addr[pm.end():].strip()
        elif addr.startswith(self._hokkaido_places):
            item[Schema.PREF] = "北海道"
        item[Schema.ADDR] = addr

    def _fetch_tel(self, job_url: str) -> str:
        soup = self.get_soup(job_url)
        if soup is None:
            return ""
        contact = soup.select_one("#jobContact")
        if not contact:
            return ""
        m = _TEL_PATTERN.search(contact.get_text(" "))
        return m.group(0) if m else ""


if __name__ == "__main__":
    scraper = Jobkita2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jobkita.jp/shukatsu/2027/job/search/")

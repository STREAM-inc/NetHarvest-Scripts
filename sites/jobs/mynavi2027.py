import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin
from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://job.mynavi.jp"

PREF_CODES = {
    "1": "北海道", "2": "青森県", "3": "岩手県", "4": "宮城県", "5": "秋田県",
    "6": "山形県", "7": "福島県", "8": "茨城県", "9": "栃木県", "10": "群馬県",
    "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県",
    "16": "富山県", "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県", "25": "滋賀県",
    "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県",
    "31": "鳥取県", "32": "島根県", "33": "岡山県", "34": "広島県", "35": "山口県",
    "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県", "45": "宮崎県",
    "46": "鹿児島県", "47": "沖縄県",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class Mynavi2027Scraper(DynamicCrawler):
    """マイナビ2027 新卒採用企業情報スクレイパー（job.mynavi.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["平均年齢", "平均勤続年数", "売上高_詳細"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_corps: set[str] = set()
        detail_urls: list[tuple[str, str]] = []

        # Step 1: 全都道府県の企業URLを全件収集
        for code, pref_ja in PREF_CODES.items():
            list_url = f"{BASE_URL}/27/pc/search/hq{code}.html"
            self.logger.info("都道府県取得: %s (%s)", pref_ja, list_url)
            try:
                urls = self._collect_corp_urls(list_url)
                for u in urls:
                    if u not in seen_corps:
                        seen_corps.add(u)
                        detail_urls.append((u, pref_ja))
            except Exception as exc:
                self.logger.error("都道府県 %s の一覧取得エラー: %s", pref_ja, exc)

        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        # Step 2: 収集した全URLから詳細データを取得
        for detail_url, pref_ja in detail_urls:
            try:
                item = self._scrape_detail(detail_url, pref_ja)
                if item and item.get(Schema.NAME):
                    yield item
            except Exception as exc:
                self.logger.error("詳細取得エラー %s: %s", detail_url, exc)
                continue

    def _collect_corp_urls(self, list_url: str) -> list[str]:
        """一覧ページから企業詳細URLを収集する（ページネーションのクリック対応）"""
        urls: list[str] = []
        seen: set[str] = set()
        
        # 最初のページを開く
        soup = self.get_soup(list_url, wait_until="domcontentloaded")
        if soup is None:
            return urls

        while True:
            # 現在表示されているページの企業リンクを収集
            for a in soup.select("a[href*='/27/pc/search/corp'][href*='/outline.html']"):
                href = a.get("href", "").strip()
                full = href if href.startswith("http") else BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

            # ページネーション: 「次の100社」リンクを探す
            next_a = (
                soup.find("a", string=lambda t: t and "次の100社" in t)
                or soup.find("a", string=lambda t: t and "次へ" in t)
                or soup.select_one("a[rel='next']")
                or soup.select_one(".pager a.next, .boxpager a.next, [class*='pager'] a.next")
            )

            # 次へボタンがなければ終了
            if not next_a:
                break

            href = next_a.get("href", "")
            
            # --- 修正箇所: JavaScriptリンクの場合はクリック操作を行う ---
            if href.startswith("javascript:"):
                try:
                    text_to_click = next_a.get_text(strip=True)
                    self.logger.info(f"ページ切り替え: 「{text_to_click}」をクリックします")
                    
                    # リンクテキストを指定してクリック
                    self.page.locator(f"a:has-text('{text_to_click}')").first.click()
                    
                    # 次のページが読み込まれるまで待機
                    self.page.wait_for_timeout(3000)
                    
                    # 新しくなった画面のHTMLを取得
                    soup = BeautifulSoup(self.page.content(), "html.parser")
                except Exception as e:
                    self.logger.warning(f"次へボタンのクリックに失敗しました: {e}")
                    break
            else:
                # 通常のURL遷移の場合
                next_url = urljoin(BASE_URL, href)
                soup = self.get_soup(next_url, wait_until="domcontentloaded")
                if soup is None:
                    break

        return urls

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url, wait_until="domcontentloaded")
        if soup is None:
            return None

        # --- 動的読み込みの待機 ---
        try:
            if hasattr(self, 'page') and self.page:
                self.page.wait_for_selector("th, dt", timeout=10000)
                # 念のため3秒待機して確実にDOMを完成させる
                self.page.wait_for_timeout(3000)
                soup = BeautifulSoup(self.page.content(), "html.parser")
        except Exception:
            pass 

        # CSV出力用データの初期化（事業内容を除外）
        data: dict = {
            Schema.URL: url,
            Schema.NAME: "",
            Schema.PREF: pref_ja,
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.REP_NM: "",
            Schema.EMP_NUM: "",
            Schema.CAP: "",
            Schema.CAT_SITE: "",
            Schema.OPEN_DATE: "",
            Schema.SALES: "",
            Schema.HP: "",
            "平均年齢": "",
            "平均勤続年数": "",
            "売上高_詳細": ""
        }
        
        # 1. dl/dt/dd 形式の場合 (ページ全体から)
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for i, dt in enumerate(dts):
                key = re.sub(r"\s+", "", dt.get_text())
                val = _clean(dds[i].get_text(" ")) if i < len(dds) else ""
                if not key or not val:
                    continue
                self._map_field(data, key, val, dds[i])

        # 2. テーブル形式の場合 (ページ全体から)
        for tr in soup.find_all("tr"):
            th_element = tr.find("th") or tr.find("td", class_="heading")
            td_elements = tr.find_all("td")
            if not td_elements:
                continue
                
            td_element = td_elements[-1]
            if th_element and td_element and th_element != td_element:
                key = re.sub(r"\s+", "", th_element.get_text())
                val = _clean(td_element.get_text(" "))
                self._map_field(data, key, val, td_element)

        # 企業名の補完 (取得できなければh1から)
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        # 都道府県の補完
        if data.get(Schema.ADDR) and not data.get(Schema.PREF):
            m = re.match(r"^(東京都|北海道|(?:大阪|京都|神奈川|和歌山|鹿児島|埼玉|千葉|愛知|静岡|福島|茨城|栃木|群馬|兵庫|新潟|長野|岐阜|三重|滋賀|奈良|岡山|広島|山口|愛媛|高知|福岡|長崎|熊本|宮崎|大分|沖縄)府|.+?[都道府県])", data[Schema.ADDR])
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = data[Schema.ADDR][m.end():].strip()

        if not data.get(Schema.NAME):
            return None
            
        return data

    def _map_field(self, data: dict, key: str, val: str, tag) -> None:
        if ("商号" in key or "会社名" in key or "社名" in key) and not data.get(Schema.NAME):
            data[Schema.NAME] = val

        elif ("本社郵便番号" in key or "郵便番号" in key) and not data.get(Schema.POST_CODE):
            m = re.search(r"(\d{3})-?(\d{4})", val)
            if m:
                data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"

        elif ("本社所在地" in key or "所在地" in key or "住所" in key) and not data.get(Schema.ADDR):
            cleaned = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", val).strip()
            m = re.match(r"^(東京都|北海道|(?:大阪|京都|神奈川|和歌山|鹿児島|埼玉|千葉|愛知|静岡|福島|茨城|栃木|群馬|兵庫|新潟|長野|岐阜|三重|滋賀|奈良|岡山|広島|山口|愛媛|高知|福岡|長崎|熊本|宮崎|大分|沖縄)府|.+?[都道府県])", cleaned)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = cleaned[m.end():].strip()
            else:
                data[Schema.ADDR] = cleaned

        elif ("本社電話番号" in key or "代表電話番号" in key or "電話番号" in key) and not data.get(Schema.TEL):
            data[Schema.TEL] = val

        elif ("代表者" in key or "代表取締役" in key or "社長" in key) and not any(x in key for x in ["メッセージ", "挨拶", "あいさつ", "言葉", "語る"]) and not data.get(Schema.REP_NM):
            if len(val) < 100:
                name = re.sub(r"代表取締役(?:社長|会長)?|取締役(?:社長|会長)|社長|会長|代表|役職\S+", "", val).strip()
                name = re.sub(r"^[:：\s]+", "", name).strip()
                data[Schema.REP_NM] = name or val

        elif "従業員" in key and not data.get(Schema.EMP_NUM):
            data[Schema.EMP_NUM] = val

        elif "資本金" in key and not data.get(Schema.CAP):
            data[Schema.CAP] = val

        elif "業種" in key and not data.get(Schema.CAT_SITE):
            data[Schema.CAT_SITE] = val

        elif ("設立" in key or "法人化" in key) and not data.get(Schema.OPEN_DATE):
            m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", val)
            if m:
                data[Schema.OPEN_DATE] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            else:
                m2 = re.search(r"(\d{4})年(\d{1,2})月", val)
                if m2:
                    data[Schema.OPEN_DATE] = f"{m2.group(1)}-{int(m2.group(2)):02d}"
                else:
                    m3 = re.search(r"(\d{4})年", val)
                    if m3:
                        data[Schema.OPEN_DATE] = m3.group(1)

        elif ("売上高" in key or "売上" in key) and not data.get(Schema.SALES):
            data[Schema.SALES] = val

        elif "平均年齢" in key and not data.get("平均年齢"):
            data["平均年齢"] = val

        elif "平均勤続" in key and not data.get("平均勤続年数"):
            data["平均勤続年数"] = val

        elif ("ホームページ" in key or "HP" in key or "URL" in key) and not data.get(Schema.HP):
            a = tag.find("a", href=True) if tag else None
            hp = a["href"] if a else val
            if hp and not hp.startswith("/") and "mynavi" not in hp:
                data[Schema.HP] = hp


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    Mynavi2027Scraper().execute("https://job.mynavi.jp/2027/")



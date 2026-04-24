"""
対象サイト: https://www.town-life.jp/reform/
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.town-life.jp"
LIST_URL = f"{BASE_URL}/reform/sp_shoplist.html"
DETAIL_RE = re.compile(r"/reform/(?:sp_)?shopdetail(\d+)\.html", re.IGNORECASE)
POST_CODE_RE = re.compile(r"(?:〒\s*)?(\d{3})-?(\d{4})")
PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
TARGET_PREFS = {"東京都", "神奈川県", "千葉県", "埼玉県", "茨城県", "栃木県", "群馬県"}


class TownLifeReformScraper(StaticCrawler):
    """タウンライフリフォーム 企業情報スクレイパー"""

    DELAY = 4.0
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = [
        "detail_url",
        "エリア",
        "業種",
        "代表者役職",
        "売り上げ",
        "設立日",
        "FAX",
        "メール",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        def clean(text: str | None) -> str:
            if text is None:
                return ""
            return re.sub(r"\s+", " ", str(text)).strip()

        def stop_if_blocked(status_code: int, target_url: str) -> None:
            if status_code in (403, 429):
                raise RuntimeError(f"アクセス制限を検知したため停止します: {status_code} {target_url}")

        def fetch_soup(target_url: str) -> bs4.BeautifulSoup | None:
            try:
                response = self.session.get(target_url, timeout=self.TIMEOUT)
                stop_if_blocked(response.status_code, target_url)
                if response.status_code != 200:
                    self.logger.warning("ページ取得失敗 (%s): %s", response.status_code, target_url)
                    return None
                if "charset=" not in response.headers.get("Content-Type", "").lower():
                    response.encoding = response.apparent_encoding
                return bs4.BeautifulSoup(response.text, "html.parser")
            except Exception as exc:
                if self.CONTINUE_ON_ERROR:
                    self.error_count += 1
                    self.logger.warning("通信エラーをスキップ: %s (%s)", target_url, exc)
                    return None
                raise

        def normalize_detail_url(href: str) -> str:
            m = DETAIL_RE.search(href or "")
            if not m:
                return ""
            return f"{BASE_URL}/reform/shopdetail{m.group(1)}.html"

        def parse_town_life_table(soup: bs4.BeautifulSoup) -> tuple[dict[str, str], dict[str, str]]:
            values: dict[str, str] = {}
            links: dict[str, str] = {}
            for tr in soup.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = clean(th.get_text(" ", strip=True)).rstrip("：:")
                val = clean(td.get_text(" ", strip=True))
                if key and key not in values:
                    values[key] = val
                if key:
                    a = td.find("a", href=True)
                    if a:
                        links[key] = clean(a.get("href", ""))
            return values, links

        def split_pref_addr(addr: str) -> tuple[str, str]:
            addr = clean(addr)
            if not addr:
                return "", ""
            m = PREF_RE.search(addr)
            if not m:
                return "", addr
            pref = m.group(1)
            tail = clean(addr[m.end() :])
            return pref, tail

        def extract_post_code(text: str) -> str:
            m = POST_CODE_RE.search(text or "")
            if not m:
                return ""
            return f"{m.group(1)}-{m.group(2)}"

        def extract_tel_fax(contact: str) -> tuple[str, str]:
            contact = clean(contact)
            tel = ""
            fax = ""
            m_tel = re.search(r"TEL[:：]\s*([0-9\-]+)", contact, flags=re.IGNORECASE)
            if m_tel:
                tel = clean(m_tel.group(1))
            m_fax = re.search(r"FAX[:：]\s*([0-9\-]+)", contact, flags=re.IGNORECASE)
            if m_fax:
                fax = clean(m_fax.group(1))
            return tel, fax

        def pick_first_pref(*texts: str) -> str:
            for text in texts:
                m = PREF_RE.search(text or "")
                if m:
                    return m.group(1)
            return ""

        list_soup = fetch_soup(url or LIST_URL)
        if list_soup is None:
            return

        seen: set[str] = set()
        entries: list[tuple[str, str, str]] = []
        for pref_block in list_soup.select("div.area__prefecture"):
            pref_label = clean((pref_block.select_one("label.sitemap-pref") or pref_block.select_one("label")).get_text(" ", strip=True) if (pref_block.select_one("label.sitemap-pref") or pref_block.select_one("label")) else "")
            for a in pref_block.select("li.shop a[href]"):
                detail_url = normalize_detail_url(a.get("href", ""))
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)
                name = clean(a.get_text(" ", strip=True))
                entries.append((detail_url, pref_label, name))

        self.total_items = len(entries)
        self.logger.info("詳細URL収集: %d 件", len(entries))

        for detail_url, pref_from_list, name_from_list in entries:
            soup = fetch_soup(detail_url)
            if soup is None:
                continue

            label_values, label_links = parse_town_life_table(soup)

            name = clean(
                label_values.get("企業名")
                or label_values.get("会社名")
                or name_from_list
            )
            location_raw = clean(label_values.get("所在地"))
            post_code = extract_post_code(location_raw)
            pref_by_addr, addr_tail = split_pref_addr(location_raw)
            pref = pref_by_addr or pref_from_list

            area = clean(label_values.get("対応可能エリア"))
            if not pref:
                pref = pick_first_pref(area, location_raw)
            if pref not in TARGET_PREFS:
                continue

            contact_raw = clean(label_values.get("問い合わせ先"))
            tel, fax = extract_tel_fax(contact_raw)

            hp = clean(label_links.get("会社URL") or label_values.get("会社URL"))
            rep = clean(label_values.get("代表者氏名") or label_values.get("代表者名"))
            cap = clean(label_values.get("資本金"))
            emp_num = clean(label_values.get("従業員数"))
            open_date = clean(label_values.get("設立"))
            reform_types = clean(label_values.get("対応可能リフォーム"))

            if not name or not detail_url:
                continue

            yield {
                Schema.URL: detail_url,
                Schema.NAME: name,
                Schema.TEL: tel,
                Schema.PREF: pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR: addr_tail or location_raw,
                Schema.CO_NUM: "",
                Schema.POS_NM: "",
                Schema.REP_NM: rep,
                Schema.CAP: cap,
                Schema.SALES: "",
                Schema.EMP_NUM: emp_num,
                Schema.OPEN_DATE: open_date,
                Schema.LOB: reform_types,
                Schema.HP: hp,
                Schema.INSTA: "",
                Schema.FB: "",
                Schema.X: "",
                Schema.LINE: "",
                "detail_url": detail_url,
                "エリア": area,
                "業種": "",
                "代表者役職": "",
                "売り上げ": "",
                "設立日": open_date,
                "FAX": fax,
                "メール": "",
            }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    TownLifeReformScraper().execute(LIST_URL)

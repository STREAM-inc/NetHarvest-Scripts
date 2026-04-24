"""
対象サイト: https://www.nuri-kae.jp/
"""

import gzip
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

BASE_URL = "https://www.nuri-kae.jp"
LIST_PATH_TEMPLATE = "/area/{pref}/part/exterior_outer-wall"
COMPANY_PATH_RE = re.compile(r"/company/(\d+)")
POST_CODE_RE = re.compile(r"(?:〒\s*)?(\d{3}-\d{4}|\d{7})")
PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

TARGET_PREFS = ["東京都", "神奈川県", "千葉県", "埼玉県", "茨城県", "栃木県", "群馬県"]


class NuriKaeScraper(StaticCrawler):
    """ヌリカエ 会社情報スクレイパー"""

    DELAY = 4.0
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
    CONTINUE_ON_ERROR = True

    def parse(self, url: str) -> Generator[dict, None, None]:
        def clean(text: str | None) -> str:
            if text is None:
                return ""
            value = re.sub(r"\s+", " ", str(text)).strip()
            value = re.sub(r"(?:\s*閉じる)+$", "", value).strip()
            parts = value.split(" ")
            if len(parts) >= 2 and len(parts) % 2 == 0:
                half_n = len(parts) // 2
                if parts[:half_n] == parts[half_n:]:
                    value = " ".join(parts[:half_n]).strip()
            if len(value) >= 4 and len(value) % 2 == 0:
                half = len(value) // 2
                if value[:half] == value[half:]:
                    value = value[:half].strip()
            return value

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

        def extract_company_urls_from_list_page(soup: bs4.BeautifulSoup) -> list[str]:
            found: list[str] = []
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                m = COMPANY_PATH_RE.search(href)
                if not m:
                    continue
                found.append(f"{BASE_URL}/company/{m.group(1)}")
            return found

        def has_next_page(soup: bs4.BeautifulSoup, page_num: int) -> bool:
            next_num = page_num + 1
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if f"page={next_num}" in href:
                    return True
                rel = a.get("rel") or []
                if isinstance(rel, list) and "next" in rel:
                    return True
            return False

        def fetch_sitemap_company_urls() -> list[str]:
            sitemap_url = f"{BASE_URL}/sitemap/company"
            try:
                response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
                stop_if_blocked(response.status_code, sitemap_url)
                if response.status_code != 200:
                    self.logger.warning("sitemap取得失敗 (%s): %s", response.status_code, sitemap_url)
                    return []
                raw = response.content
                try:
                    text = gzip.decompress(raw).decode("utf-8", "ignore")
                except Exception:
                    text = raw.decode("utf-8", "ignore")
                urls = re.findall(r"<loc>(https://www\.nuri-kae\.jp/company/\d+)</loc>", text)
                return urls
            except Exception as exc:
                if self.CONTINUE_ON_ERROR:
                    self.error_count += 1
                    self.logger.warning("sitemap処理エラー: %s", exc)
                    return []
                raise

        def build_label_maps(soup: bs4.BeautifulSoup) -> tuple[dict[str, str], dict[str, str]]:
            label_to_value: dict[str, str] = {}
            label_to_href: dict[str, str] = {}

            for item in soup.select(".fv-company-info__item"):
                label_el = item.select_one(".fv-company-info__label")
                value_el = item.select_one(".fv-company-info__value")
                label = clean(label_el.get_text(" ", strip=True) if label_el else "")
                value = clean(value_el.get_text(" ", strip=True) if value_el else "")
                if label and value and label not in label_to_value:
                    label_to_value[label] = value

            for label_el in soup.select('[class*="label"], dt, th'):
                label = clean(label_el.get_text(" ", strip=True)).rstrip(":：")
                if not label:
                    continue

                value = ""
                href = ""

                sibling = label_el.find_next_sibling()
                if sibling:
                    value = clean(sibling.get_text(" ", strip=True))
                    link = sibling.find("a", href=True)
                    if link:
                        href = clean(link.get("href", ""))

                if not value:
                    parent = label_el.parent
                    if parent:
                        value_el = parent.select_one('[class*="value"], dd, td')
                        if value_el and value_el is not label_el:
                            value = clean(value_el.get_text(" ", strip=True))
                            link = value_el.find("a", href=True)
                            if link:
                                href = clean(link.get("href", ""))

                if value and label not in label_to_value:
                    label_to_value[label] = value
                if href and label not in label_to_href:
                    label_to_href[label] = href

            return label_to_value, label_to_href

        def pick_first_value(label_to_value: dict[str, str], labels: list[str]) -> str:
            for label in labels:
                value = clean(label_to_value.get(label, ""))
                if value:
                    return value
            return ""

        def pick_first_link(label_to_href: dict[str, str], labels: list[str]) -> str:
            for label in labels:
                href = clean(label_to_href.get(label, ""))
                if href:
                    return href
            return ""

        def pick_company_hp(soup: bs4.BeautifulSoup, label_to_href: dict[str, str]) -> str:
            hp = pick_first_link(label_to_href, ["会社HP", "ホームページ", "公式サイト"])
            if hp:
                return hp

            for a in soup.select("a[href]"):
                href = clean(a.get("href", ""))
                if not href.startswith("http"):
                    continue
                netloc = urlparse(href).netloc.lower()
                if "nuri-kae.jp" in netloc:
                    continue
                if any(
                    s in netloc
                    for s in ["line.me", "instagram.com", "facebook.com", "x.com", "twitter.com", "speee.jp"]
                ):
                    continue
                return href
            return ""

        def pick_tel(soup: bs4.BeautifulSoup, label_to_value: dict[str, str]) -> str:
            for key, value in label_to_value.items():
                if "TEL" in key.upper() and clean(value):
                    return clean(value)
            tel_link = soup.select_one('a[href^="tel:"]')
            if tel_link:
                return clean(tel_link.get("href", "").replace("tel:", ""))
            return ""

        def extract_sns_and_mail(soup: bs4.BeautifulSoup) -> dict[str, str]:
            values = {
                Schema.INSTA: "",
                Schema.FB: "",
                Schema.X: "",
                Schema.LINE: "",
                "メール": "",
            }
            for a in soup.select("a[href]"):
                href = clean(a.get("href", ""))
                if not href:
                    continue

                lower = href.lower()
                if lower.startswith("mailto:") and not values["メール"]:
                    values["メール"] = href.replace("mailto:", "", 1)
                elif "instagram.com" in lower and not values[Schema.INSTA]:
                    values[Schema.INSTA] = href
                elif "facebook.com" in lower and not values[Schema.FB]:
                    values[Schema.FB] = href
                elif (("x.com" in lower) or ("twitter.com" in lower)) and not values[Schema.X]:
                    values[Schema.X] = href
                elif "line.me" in lower and not values[Schema.LINE]:
                    values[Schema.LINE] = href
            return values

        def split_addr(addr: str) -> tuple[str, str]:
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
            code = m.group(1)
            if len(code) == 7 and "-" not in code:
                code = f"{code[:3]}-{code[3:]}"
            return code

        # 1) 一覧ページから収集（指定URL形式 + ページング）
        candidate_urls: list[str] = []
        seen_detail_urls: set[str] = set()

        start_urls = []
        if url and url.startswith("http"):
            start_urls.append(url)
        for pref in TARGET_PREFS:
            start_urls.append(urljoin(BASE_URL, LIST_PATH_TEMPLATE.format(pref=pref)))

        for list_url in start_urls:
            page = 1
            empty_streak = 0
            while True:
                page_url = f"{list_url}?page={page}" if page > 1 else list_url
                soup = fetch_soup(page_url)
                if soup is None:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
                    page += 1
                    continue

                extracted = extract_company_urls_from_list_page(soup)
                new_count = 0
                for detail_url in extracted:
                    if detail_url in seen_detail_urls:
                        continue
                    seen_detail_urls.add(detail_url)
                    candidate_urls.append(detail_url)
                    new_count += 1

                if new_count == 0:
                    empty_streak += 1
                else:
                    empty_streak = 0

                if empty_streak >= 2:
                    break
                if not has_next_page(soup, page) and new_count == 0:
                    break

                page += 1
                if page > 200:
                    break

        # 2) フォールバック: sitemap から company URL 補完
        if not candidate_urls:
            self.logger.warning("一覧ページからURL収集できなかったため、sitemap/companyにフォールバックします。")
            sitemap_urls = fetch_sitemap_company_urls()
            for detail_url in sitemap_urls:
                if detail_url in seen_detail_urls:
                    continue
                seen_detail_urls.add(detail_url)
                candidate_urls.append(detail_url)

        self.total_items = len(candidate_urls)
        self.logger.info("詳細URL収集: %d 件", len(candidate_urls))

        # 3) 詳細ページ抽出
        for detail_url in candidate_urls:
            soup = fetch_soup(detail_url)
            if soup is None:
                continue

            label_to_value, label_to_href = build_label_maps(soup)

            name = clean(
                (soup.select_one("h1.company-show__heading-type-1") or soup.select_one("h1")).get_text(" ", strip=True)
                if (soup.select_one("h1.company-show__heading-type-1") or soup.select_one("h1"))
                else ""
            )
            if not name:
                name = pick_first_value(label_to_value, ["会社名", "名称"])

            addr_raw = pick_first_value(label_to_value, ["会社所在地", "所在地", "住所"])
            pref, addr_wo_pref = split_addr(addr_raw)
            post_code = extract_post_code(addr_raw)
            tel = pick_tel(soup, label_to_value)
            hp = pick_company_hp(soup, label_to_href)
            area = pick_first_value(label_to_value, ["対応エリア", "エリア"])
            emp_num = pick_first_value(label_to_value, ["従業員数"])
            open_date = pick_first_value(label_to_value, ["設立日", "設立年月日", "創業"])
            co_num = pick_first_value(label_to_value, ["法人番号"])
            rep = pick_first_value(label_to_value, ["代表者", "代表者名"])
            pos = pick_first_value(label_to_value, ["代表者役職", "役職"])
            cap = pick_first_value(label_to_value, ["資本金"])
            sales = pick_first_value(label_to_value, ["売り上げ", "売上高", "売上"])
            lob = pick_first_value(label_to_value, ["事業内容"])
            fax = pick_first_value(label_to_value, ["FAX", "FAX番号"])
            industry = pick_first_value(label_to_value, ["業種"])
            sns = extract_sns_and_mail(soup)

            # 対象エリアで絞り込み（一都6県）
            pref_for_filter = pref or pick_first_value(label_to_value, ["都道府県"])
            if not pref_for_filter and area:
                m = PREF_RE.search(area)
                if m:
                    pref_for_filter = m.group(1)
            if pref_for_filter and pref_for_filter not in TARGET_PREFS:
                continue
            if not pref_for_filter:
                continue
            if not pref:
                pref = pref_for_filter

            if not name or not detail_url:
                continue

            yield {
                Schema.URL: detail_url,
                Schema.NAME: name,
                Schema.TEL: tel,
                Schema.PREF: pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR: addr_wo_pref or addr_raw,
                Schema.CO_NUM: co_num,
                Schema.POS_NM: pos,
                Schema.REP_NM: rep,
                Schema.CAP: cap,
                Schema.SALES: sales,
                Schema.EMP_NUM: emp_num,
                Schema.LOB: lob,
                Schema.HP: hp,
                Schema.INSTA: sns[Schema.INSTA],
                Schema.FB: sns[Schema.FB],
                Schema.X: sns[Schema.X],
                Schema.LINE: sns[Schema.LINE],
                "detail_url": detail_url,
                "エリア": area,
                "業種": industry,
                "代表者役職": pos,
                "売り上げ": sales,
                "設立日": open_date,
                "FAX": fax,
                "メール": sns["メール"],
            }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    NuriKaeScraper().execute(urljoin(BASE_URL, LIST_PATH_TEMPLATE.format(pref="東京都")))

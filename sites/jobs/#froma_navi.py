import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL_TEMPLATE = (
    "https://www.froma.com/prefectures/{pref}/job_search/"
    "?emp=08&emp=04&emp=03&emp=02&sc=2"
)

PREFECTURES = [
    ("hokkaido", "北海道"),
    ("aomori", "青森県"),
    ("iwate", "岩手県"),
    ("miyagi", "宮城県"),
    ("akita", "秋田県"),
    ("yamagata", "山形県"),
    ("fukushima", "福島県"),
    ("ibaraki", "茨城県"),
    ("tochigi", "栃木県"),
    ("gunma", "群馬県"),
    ("saitama", "埼玉県"),
    ("chiba", "千葉県"),
    ("tokyo", "東京都"),
    ("kanagawa", "神奈川県"),
    ("niigata", "新潟県"),
    ("toyama", "富山県"),
    ("ishikawa", "石川県"),
    ("fukui", "福井県"),
    ("yamanashi", "山梨県"),
    ("nagano", "長野県"),
    ("gifu", "岐阜県"),
    ("shizuoka", "静岡県"),
    ("aichi", "愛知県"),
    ("mie", "三重県"),
    ("shiga", "滋賀県"),
    ("kyoto", "京都府"),
    ("osaka", "大阪府"),
    ("hyogo", "兵庫県"),
    ("nara", "奈良県"),
    ("wakayama", "和歌山県"),
    ("tottori", "鳥取県"),
    ("shimane", "島根県"),
    ("okayama", "岡山県"),
    ("hiroshima", "広島県"),
    ("yamaguchi", "山口県"),
    ("tokushima", "徳島県"),
    ("kagawa", "香川県"),
    ("ehime", "愛媛県"),
    ("kochi", "高知県"),
    ("fukuoka", "福岡県"),
    ("saga", "佐賀県"),
    ("nagasaki", "長崎県"),
    ("kumamoto", "熊本県"),
    ("oita", "大分県"),
    ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"),
    ("okinawa", "沖縄県"),
]

ZIP_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")
PHONE_RE = re.compile(
    r"(?:tel:)?(?:\+?81[-\s()]*\d{1,4}[-\s()]*\d{1,4}[-\s()]*\d{3,4}|0\d{9,10})"
)


class FromaNaviScraper(StaticCrawler):
    """フロム・エー ナビの求人詳細ページから企業情報を取得するクローラー。"""

    DELAY = 0.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()

        for pref_id, pref_name in self._resolve_prefectures(url):
            list_url = BASE_URL_TEMPLATE.format(pref=pref_id)
            yield from self._scrape_prefecture(pref_name, list_url, seen_urls)

    def _resolve_prefectures(self, url: str) -> list[tuple[str, str]]:
        match = re.search(r"/prefectures/([^/]+)/job_search/", url)
        if match:
            pref_id = match.group(1)
            for current_id, pref_name in PREFECTURES:
                if current_id == pref_id:
                    return [(current_id, pref_name)]
        return PREFECTURES

    def _scrape_prefecture(
        self,
        pref_name: str,
        list_url: str,
        seen_urls: set[str],
    ) -> Generator[dict, None, None]:
        current_url = list_url

        while current_url:
            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)

            detail_urls: list[str] = []
            for link in soup.select("a.styles_bigCard__pKdMA.styles_resultBgCard__5ZYNQ"):
                href = link.get("href")
                if not href:
                    continue
                detail_url = urljoin("https://www.froma.com", href)
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                detail_urls.append(detail_url)

            for detail_url in detail_urls:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)

                item = self._scrape_detail(pref_name, detail_url)
                if item:
                    yield item

            next_link = soup.select_one("a.styles_arrowButton__K2TR1.styles_next__5kh7k[href]")
            if next_link and next_link.get("aria-disabled") != "true":
                current_url = urljoin("https://www.froma.com", next_link["href"])
            else:
                current_url = None

    def _scrape_detail(self, pref_name: str, url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup = self.get_soup(url)

        item: dict[str, str] = {
            Schema.URL: url,
            Schema.PREF: pref_name,
        }

        info_root = soup.select_one("div[class*='styles_listCompanyInfo']")
        if not info_root:
            return None

        tel_candidates: dict[str, str] = {
            "お問い合わせ": "",
            "代表電話番号": "",
            "電話番号": "",
            "TEL": "",
        }

        for block in info_root.select("div[class*='styles_companyInfo']"):
            label = self._extract_label(block)
            if not label:
                continue

            text_value, href_value = self._extract_value(block)

            if any(key in label for key in ["会社名", "企業名", "社名"]):
                item[Schema.NAME] = text_value
            elif "代表者" in label:
                item[Schema.REP_NM] = text_value
            elif any(key in label for key in ["所在住所", "所在地", "本社所在地", "住所"]):
                zip_code, address = self._split_zip_address(text_value)
                if zip_code:
                    item[Schema.POST_CODE] = zip_code
                if address:
                    item[Schema.ADDR] = address
            elif any(key in label for key in ["事業内容", "仕事内容"]):
                item[Schema.LOB] = text_value
            elif ("ホームページ" in label) or (label.strip().upper() == "HP") or ("ＨＰ" in label):
                item[Schema.HP] = href_value or text_value
            elif any(key in label for key in tel_candidates):
                phone = self._normalize_phone(href_value or text_value)
                if not phone:
                    continue
                for key in tel_candidates:
                    if key in label and not tel_candidates[key]:
                        tel_candidates[key] = phone
                        break

        for key in ["お問い合わせ", "代表電話番号", "電話番号", "TEL"]:
            if tel_candidates[key]:
                item[Schema.TEL] = tel_candidates[key]
                break

        if Schema.NAME not in item:
            return None

        return item

    @staticmethod
    def _extract_label(block) -> str:
        elem = (
            block.select_one("h3 p")
            or block.select_one("p[class*='title']")
            or block.find("p")
        )
        if not elem:
            return ""
        return " ".join(elem.get_text(" ", strip=True).split())

    @staticmethod
    def _extract_value(block) -> tuple[str, str]:
        elem = (
            block.select_one("p[class*='content']")
            or block.find("a")
            or block.find("p")
        )
        if not elem:
            return "", ""

        text_value = " ".join(elem.get_text(" ", strip=True).split())
        href_value = ""
        if elem.name == "a" and elem.has_attr("href"):
            href_value = elem["href"].strip()

        return text_value, href_value

    @staticmethod
    def _split_zip_address(value: str) -> tuple[str, str]:
        text = " ".join((value or "").split())
        match = ZIP_RE.match(text)
        if not match:
            return "", text

        zip_code = f"{match.group(1)}-{match.group(2)}"
        address = text[match.end():].strip(" ,，、：:　")
        address = re.split(r"(→大きな地図で見る|MAP|地図|アクセス|最寄駅)", address)[0].strip()
        return zip_code, address

    @staticmethod
    def _normalize_phone(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""

        match = PHONE_RE.search(raw)
        if not match:
            return ""

        phone = match.group(0)
        if phone.startswith("tel:"):
            phone = phone[4:]

        phone = (
            phone.replace("（", "(")
            .replace("）", ")")
            .replace("－", "-")
            .replace("ー", "-")
            .replace("―", "-")
            .replace("–", "-")
            .replace("—", "-")
            .replace("‐", "-")
        )
        phone = re.sub(r"\s+", "", phone)

        if phone.startswith("+81"):
            phone = "0" + phone[3:]

        digits = re.sub(r"\D", "", phone)
        if digits.startswith("81"):
            digits = "0" + digits[2:]
        if digits.startswith("0"):
            return digits
        return phone.strip("-")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    FromaNaviScraper().execute(
        "https://www.froma.com/prefectures/tokyo/job_search/?emp=08&emp=04&emp=03&emp=02&sc=2"
    )

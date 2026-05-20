"""
対象サイト: https://nightly.jp/job/
"""

from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if (_project_root / "src").exists() and str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
elif (_project_root / "NetHarvest" / "src").exists():
    net_harvest_root = _project_root / "NetHarvest"
    if str(net_harvest_root) not in sys.path:
        sys.path.insert(0, str(net_harvest_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://nightly.jp"
JOB_SITEMAP_URL = f"{BASE_URL}/main/shops-job-sitemap.xml"
SHOP_JOB_RE = re.compile(r"^https://nightly\.jp/shops/[^/]+/job/$")

PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
POST_RE = re.compile(r"〒?\s*([0-9０-９]{3}[-－]?[0-9０-９]{4})")
NAME_KANA_RE = re.compile(r"^(.+?)[（(]([^）)]+)[）)]$")
SHOP_ID_RE = re.compile(r"/shops/([^/]+)/")
DATE_RE = re.compile(
    r"公開\s*([0-9]{4}\.[0-9]{1,2}\.[0-9]{1,2})\s*更新\s*([0-9]{4}\.[0-9]{1,2}\.[0-9]{1,2})"
)
ADULT_SERVICE_KEYWORDS = (
    "デリヘル",
    "ホテヘル",
    "ソープ",
    "ヘルス",
    "ファッションヘルス",
    "ピンサロ",
    "性感",
    "回春",
    "SMクラブ",
    "イメクラ",
    "人妻デリ",
    "風俗エステ",
)


def _clean(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _join_values(values: list[str]) -> str:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = _clean(value)
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return " / ".join(result)


def _split_name_kana(value: str) -> tuple[str, str]:
    value = _clean(value)
    match = NAME_KANA_RE.match(value)
    if not match:
        return value, ""
    return _clean(match.group(1)), _clean(match.group(2))


def _extract_city(address_without_pref: str) -> str:
    text = _clean(address_without_pref)
    if not text:
        return ""
    first = text.split(" ")[0]
    patterns = [
        r"^(.+?郡.+?[町村])",
        r"^(.+?市.+?区)",
        r"^(.+?[市区町村])",
    ]
    for pattern in patterns:
        match = re.match(pattern, first)
        if match:
            return match.group(1)
    return first


def _split_address(raw_address: str) -> tuple[str, str, str, str]:
    """住所から (郵便番号, 都道府県, 都道府県以降住所, 市区町村) を返す。"""
    raw_address = _clean(raw_address)
    post_code = ""
    match = POST_RE.search(raw_address)
    if match:
        post_code = match.group(1).translate(str.maketrans("０１２３４５６７８９－", "0123456789-"))
    address = POST_RE.sub("", raw_address).strip()

    pref = ""
    addr = address
    pref_match = PREF_RE.match(address.replace(" ", ""))
    if pref_match:
        pref = pref_match.group(1)
        if address.startswith(pref):
            addr = address[len(pref) :].strip()
        else:
            addr = address.replace(" ", "", 1)[len(pref) :].strip()
    return post_code, pref, addr, _extract_city(addr)


def _section_name_for_table(table) -> str:
    heading = table.find_previous("h3")
    return _clean(heading.get_text(" ", strip=True) if heading else "")


def _cell_text(cell) -> str:
    for tag in cell.select("script, style, iframe"):
        tag.decompose()
    return _clean(cell.get_text(" ", strip=True))


def _clean_tel(value: str) -> str:
    value = re.sub(r"「ナイトリーを見た」とお伝え下さい。?", "", _clean(value)).strip()
    if not re.search(r"\d|[０-９]", value):
        return ""
    return value


def _table_rows_by_section(soup) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    for table in soup.select("table"):
        section = _section_name_for_table(table) or "その他"
        rows = sections.setdefault(section, {})
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = _cell_text(cells[0])
            value = _cell_text(cells[1])
            if key:
                rows[key] = value
    return sections


def _row_link(table_rows, label: str) -> str:
    for table in table_rows:
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2 or _cell_text(cells[0]) != label:
                continue
            link = cells[1].select_one("a[href]")
            if link:
                return _clean(link.get("href"))
    return ""


def _classify_links(soup) -> dict[str, str]:
    links = {
        "tel": "",
        "line": "",
        "instagram": "",
        "facebook": "",
        "x": "",
        "tiktok": "",
        "web_apply": "",
        "external": "",
    }
    for a in soup.select("a[href]"):
        href = _clean(a.get("href"))
        text = _clean(a.get_text(" ", strip=True))
        low = href.lower()
        if href.startswith("tel:") and not links["tel"]:
            links["tel"] = href.replace("tel:", "").strip()
        elif "web応募" in text and not links["web_apply"]:
            links["web_apply"] = urljoin(BASE_URL, href)
        elif "line.me" in low and "social-plugin/share" not in low and not links["line"]:
            links["line"] = href
        elif "instagram.com" in low and not links["instagram"]:
            links["instagram"] = href
        elif "facebook.com" in low and "sharer" not in low and not links["facebook"]:
            links["facebook"] = href
        elif ("twitter.com/" in low or "x.com/" in low) and "intent/tweet" not in low and not links["x"]:
            links["x"] = href
        elif "tiktok.com" in low and not links["tiktok"]:
            links["tiktok"] = href
        elif (
            href.startswith("http")
            and "nightly.jp" not in low
            and "facebook.com/sharer" not in low
            and "twitter.com/intent" not in low
            and "timeline.line.me" not in low
            and not any(domain in low for domain in ["line.me", "instagram.com", "facebook.com", "tiktok.com", "x.com"])
            and not links["external"]
        ):
            links["external"] = href
    return links


class NightlyScraper(StaticCrawler):
    """ナイトリー 求人掲載店舗スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "店舗ID",
        "店舗URL",
        "求人URL",
        "店舗表示名",
        "住所_フル",
        "エリア",
        "市区町村",
        "営業許可証",
        "最寄駅",
        "特徴",
        "座席数",
        "在籍人数",
        "在籍年齢",
        "公開日",
        "更新日",
        "求人タイトル",
        "求人本文",
        "募集職種",
        "仕事内容",
        "応募条件",
        "時給（本入店）",
        "時給（体験入店）",
        "日給（本入店）",
        "日給（体験入店）",
        "給料について",
        "バック等",
        "最低出勤日数",
        "最低出勤時間",
        "出勤について",
        "給与支払方法",
        "服装・髪型",
        "面接持ち物",
        "求人特徴_お給料",
        "求人特徴_待遇・サポート",
        "求人特徴_働き方",
        "求人特徴_服装・髪型",
        "即日体験入店_JSON",
        "詳細項目_JSON",
        "WEB応募URL",
        "LINE応募URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        job_urls = self._collect_job_urls(url)
        self.total_items = len(job_urls)
        self.logger.info("ナイトリー求人URL収集完了: %d 件", self.total_items)

        for job_url in job_urls:
            try:
                item = self._scrape_detail(job_url)
            except Exception as e:
                self.error_count += 1
                self.logger.warning("詳細ページ解析失敗: %s (%s)", job_url, e)
                continue
            if item:
                yield item

    def _collect_job_urls(self, url: str) -> list[str]:
        if SHOP_JOB_RE.match(url):
            return [url]

        sitemap_url = url if url.endswith(".xml") else JOB_SITEMAP_URL
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("求人sitemap取得失敗: %s (%s)", sitemap_url, e)
            return []

        urls: list[str] = []
        seen: set[str] = set()
        for elem in root.iter():
            if not elem.tag.endswith("loc") or not elem.text:
                continue
            loc = elem.text.strip()
            if SHOP_JOB_RE.match(loc) and loc not in seen:
                seen.add(loc)
                urls.append(loc)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        sections = _table_rows_by_section(soup)
        store_info = sections.get("店舗情報", {})
        job_info = sections.get("求人概要（女の子）", {})
        feature_info = sections.get("求人特徴（女の子）", {})
        daywork_info = sections.get("即日体験入店（女の子）", {})
        detail_json = {
            section: rows
            for section, rows in sections.items()
            if section not in ("店舗情報", "求人概要（女の子）", "求人特徴（女の子）", "即日体験入店（女の子）")
        }

        display_name = store_info.get("店名") or _clean(soup.select_one("h2").get_text(" ", strip=True) if soup.select_one("h2") else "")
        name, kana = _split_name_kana(display_name)
        if not name:
            return None

        links = _classify_links(soup)
        table_tags = soup.select("table")

        raw_address = store_info.get("住所", "")
        post_code, pref, addr, city = _split_address(raw_address)
        tel = _clean_tel(links["tel"] or store_info.get("電話番号", ""))

        shop_url = url.rsplit("/job/", 1)[0] + "/"
        shop_id_match = SHOP_ID_RE.search(url)
        shop_id = shop_id_match.group(1) if shop_id_match else ""

        page_text = _clean(soup.get_text(" ", strip=True))
        date_match = DATE_RE.search(page_text)
        published = date_match.group(1) if date_match else ""
        updated = date_match.group(2) if date_match else ""

        pr_section = soup.select_one("section.shop_pr")
        job_title = ""
        job_body = ""
        if pr_section:
            h3 = pr_section.select_one("h3")
            job_title = _clean(h3.get_text(" ", strip=True) if h3 else "")
            if h3:
                h3.decompose()
            job_body = _clean(pr_section.get_text(" ", strip=True))

        hp = _row_link(table_tags, "ホームページ") or links["external"]

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: store_info.get("業種", ""),
            Schema.TIME: store_info.get("営業時間", ""),
            Schema.HOLIDAY: store_info.get("定休日", ""),
            Schema.HP: hp,
            Schema.LINE: links["line"],
            Schema.INSTA: links["instagram"],
            Schema.FB: links["facebook"],
            Schema.X: links["x"],
            Schema.TIKTOK: links["tiktok"],
            "店舗ID": shop_id,
            "店舗URL": shop_url,
            "求人URL": url,
            "店舗表示名": display_name,
            "住所_フル": raw_address,
            "エリア": city,
            "市区町村": city,
            "営業許可証": self._license_status(store_info.get("営業許可証", "")),
            "最寄駅": store_info.get("最寄駅", ""),
            "特徴": store_info.get("特徴", ""),
            "座席数": store_info.get("座席数", ""),
            "在籍人数": store_info.get("在籍人数", ""),
            "在籍年齢": store_info.get("在籍年齢", ""),
            "公開日": published,
            "更新日": updated,
            "求人タイトル": job_title,
            "求人本文": job_body,
            "募集職種": job_info.get("募集職種", ""),
            "仕事内容": job_info.get("仕事内容", ""),
            "応募条件": job_info.get("応募条件", ""),
            "時給（本入店）": job_info.get("時給（本入店）", ""),
            "時給（体験入店）": job_info.get("時給（体験入店）", ""),
            "日給（本入店）": job_info.get("日給（本入店）", ""),
            "日給（体験入店）": job_info.get("日給（体験入店）", ""),
            "給料について": job_info.get("給料について", ""),
            "バック等": job_info.get("バック等", ""),
            "最低出勤日数": job_info.get("最低出勤日数", ""),
            "最低出勤時間": job_info.get("最低出勤時間", ""),
            "出勤について": job_info.get("出勤について", ""),
            "給与支払方法": job_info.get("給与支払方法", ""),
            "服装・髪型": job_info.get("服装・髪型", ""),
            "面接持ち物": job_info.get("面接持ち物", ""),
            "求人特徴_お給料": feature_info.get("お給料", ""),
            "求人特徴_待遇・サポート": feature_info.get("待遇・サポート", ""),
            "求人特徴_働き方": feature_info.get("働き方", ""),
            "求人特徴_服装・髪型": feature_info.get("服装・髪型", ""),
            "即日体験入店_JSON": json.dumps(daywork_info, ensure_ascii=False) if daywork_info else "",
            "詳細項目_JSON": json.dumps(detail_json, ensure_ascii=False) if detail_json else "",
            "WEB応募URL": links["web_apply"],
            "LINE応募URL": links["line"],
        }

        if post_code:
            item[Schema.POST_CODE] = post_code

        if self._is_excluded_adult_service(item):
            return None

        return item

    def _license_status(self, value: str) -> str:
        value = _clean(value)
        if value.startswith("確認済み"):
            return "確認済み"
        if value.startswith("未確認"):
            return "未確認"
        return value

    def _is_excluded_adult_service(self, item: dict) -> bool:
        text = " ".join(
            [
                item.get(Schema.NAME, ""),
                item.get(Schema.CAT_SITE, ""),
                item.get("求人タイトル", ""),
            ]
        )
        return any(keyword in text for keyword in ADULT_SERVICE_KEYWORDS)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    NightlyScraper().execute("https://nightly.jp/job/")

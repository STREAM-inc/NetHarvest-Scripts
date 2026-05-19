"""
対象サイト: https://clients.itszai.jp/sitemap.xml
"""

import html
import json
import os
import re
from typing import Generator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler


SITEMAP_URL = "https://clients.itszai.jp/sitemap.xml"
CLIENTS_HOST = "clients.itszai.jp"

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)
PREF_RE = re.compile("|".join(map(re.escape, PREFECTURES)))
POST_RE = re.compile(r"〒?\s*(\d{3})[-ー−]?\s*(\d{4})")
TEL_RE = re.compile(
    r"(?:TEL(?:番号)?|電話番号|電話|お電話)[\s:：]*([0-9０-９][0-9０-９\-\u30fc\u2212()\s]{8,})",
    re.IGNORECASE,
)

SECTION_LABELS = {
    "募集職種", "職種", "会社名", "店舗名", "業務内容", "仕事内容", "事業内容",
    "勤務時間", "就業時間", "雇用区分", "雇用形態", "休日・休暇", "休日休暇",
    "勤務地", "本社所在地", "給与", "待遇・福利厚生", "福利厚生", "加入保険",
    "受動喫煙防止措置", "応募について", "TEL番号", "受付時間", "代表者", "従業員数",
    "必要資格・経験", "応募資格", "アピールポイント", "試用期間", "契約期間",
    "最寄駅", "最寄り駅", "アクセス", "交通アクセス", "勤務地所在地",
    "勤務形態", "求める人材", "備考", "選考プロセス", "応募する",
}


def _clean(value: object) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value)).replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _one_line(value: object) -> str:
    return re.sub(r"\s+", " ", _clean(value)).strip()


def _norm_tel(value: str) -> str:
    value = value.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    value = value.replace("ー", "-").replace("−", "-")
    value = re.sub(r"\s+", "", value)
    return value.strip("：:,.、。")


def _extract_locs(xml_text: str) -> list[str]:
    return [html.unescape(m.group(1).strip()) for m in re.finditer(r"<loc>\s*(.*?)\s*</loc>", xml_text)]


def _first_section(sections: dict[str, str], *labels: str) -> str:
    for label in labels:
        if sections.get(label):
            return sections[label]
    return ""


def _format_salary(base_salary) -> str:
    if not isinstance(base_salary, dict):
        return ""
    value = base_salary.get("value")
    currency = base_salary.get("currency") or ""
    if not isinstance(value, dict):
        return ""
    min_value = value.get("minValue")
    max_value = value.get("maxValue")
    unit = value.get("unitText") or ""
    unit_label = {
        "HOUR": "時給",
        "DAY": "日給",
        "DAI": "日給",
        "MONTH": "月給",
        "YEAR": "年収",
    }.get(str(unit).upper(), str(unit))
    if min_value and max_value:
        try:
            amount = f"{float(min_value):,.0f} - {float(max_value):,.0f}"
        except (TypeError, ValueError):
            amount = f"{min_value} - {max_value}"
    else:
        amount = f"{min_value or max_value or ''}"
    suffix = "円" if currency == "JPY" else currency
    return _one_line(f"{unit_label} {amount}{suffix}")


def _split_address(value: str) -> tuple[str, str, str]:
    text = _one_line(value)
    for marker in (
        "最寄駅", "最寄り駅", "アクセス", "交通アクセス", "求める人材", "勤務時間",
        "給与", "休日", "待遇", "備考", "応募", "仕事内容", "必要資格", "加入保険",
    ):
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx].strip()
    post_code = ""
    m_post = POST_RE.search(text)
    if m_post:
        post_code = f"{m_post.group(1)}-{m_post.group(2)}"
        text = (text[:m_post.start()] + text[m_post.end():]).strip()
    pref = ""
    m_pref = PREF_RE.search(text)
    if m_pref:
        pref = m_pref.group(0)
        text = text[m_pref.start():].strip()
    text = re.split(
        r"\s+(?:JR|東京メトロ|都営|東急|小田急|京王|西武|近鉄|阪急|阪神|名鉄|地下鉄|バス|徒歩)",
        text,
        maxsplit=1,
    )[0].strip()
    text = re.sub(r"^(所在地|住所|本社所在地|勤務地)\s*[:：]?", "", text).strip()
    return post_code, pref, text


def _section_lines(soup: BeautifulSoup) -> list[str]:
    lines = []
    for line in soup.get_text("\n", strip=True).splitlines():
        line = _one_line(line)
        if line:
            lines.append(line)
    return lines


def _split_sections(lines: list[str]) -> dict[str, str]:
    sections: dict[str, str] = {}
    current = ""
    buffer: list[str] = []

    def flush() -> None:
        if current and buffer:
            value = "\n".join(buffer).strip()
            if value:
                sections[current] = f"{sections.get(current, '')}\n{value}".strip()

    for line in lines:
        label = line.strip(" :：")
        if label in SECTION_LABELS:
            flush()
            current = label
            buffer = []
            continue
        if current:
            buffer.append(line)
    flush()
    return sections


def _json_ld_objects(soup: BeautifulSoup) -> list[dict]:
    objects: list[dict] = []
    for script in soup.select('script[type="application/ld+json"]'):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            graph = parsed.get("@graph")
            if isinstance(graph, list):
                objects.extend(obj for obj in graph if isinstance(obj, dict))
            else:
                objects.append(parsed)
        elif isinstance(parsed, list):
            objects.extend(obj for obj in parsed if isinstance(obj, dict))
    return objects


class ItszaiScraper(StaticCrawler):
    """イツザイ公開採用サイト スクレイパー"""

    DELAY = 0.25
    EXTRA_COLUMNS = [
        "勤務地",
        "本社所在地",
        "募集職種",
        "雇用区分",
        "給与",
        "勤務時間",
        "休日休暇",
        "待遇/福利厚生",
        "加入保険",
        "受動喫煙防止措置",
        "応募受付時間",
        "採用サイトURL",
        "求人URL",
        "業種",
        "代表者",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        target_urls = self._collect_target_urls(url or SITEMAP_URL)
        max_details = self._env_int("NH_ITSZAI_MAX_DETAILS")
        if max_details:
            target_urls = target_urls[:max_details]
        self.total_items = len(target_urls)
        self.logger.info("イツザイ取得対象URL: %d件", len(target_urls))

        for detail_url in target_urls:
            try:
                soup = self.get_soup(detail_url)
                if soup is None:
                    continue
                item = self._parse_detail(soup, detail_url)
                if item.get(Schema.NAME):
                    yield item
            except Exception as exc:
                self.logger.warning("イツザイ詳細取得スキップ: %s (%s)", detail_url, exc)

    def _collect_target_urls(self, url: str) -> list[str]:
        if "/job/" in url or "/recruitments/" in url:
            return [url]

        if "sungrove.co.jp/itszai" in url:
            return self._collect_from_sungrove(url)

        sitemap_urls = [url]
        first_xml = self._get_text(url)
        if first_xml and "<sitemapindex" in first_xml:
            sitemap_urls = _extract_locs(first_xml)

        max_sitemaps = self._env_int("NH_ITSZAI_MAX_SITEMAPS")
        if max_sitemaps:
            sitemap_urls = sitemap_urls[:max_sitemaps]

        seen: set[str] = set()
        job_urls: list[str] = []
        recruitment_urls: list[str] = []
        for sitemap_url in sitemap_urls:
            xml_text = first_xml if sitemap_url == url and first_xml else self._get_text(sitemap_url)
            for loc in _extract_locs(xml_text or ""):
                if loc.endswith("/entry-forms"):
                    continue
                if "/job/" not in loc and "/recruitments/" not in loc:
                    continue
                normalized = loc.strip()
                if normalized in seen:
                    continue
                seen.add(normalized)
                if "/job/" in normalized:
                    job_urls.append(normalized)
                else:
                    recruitment_urls.append(normalized)

        # 旧型 job ページのほうが会社情報・求人情報が多いため先に処理する
        return job_urls + recruitment_urls

    def _collect_from_sungrove(self, url: str) -> list[str]:
        seen: set[str] = set()
        urls: list[str] = []
        for page in range(1, 100):
            page_url = url.rstrip("/") + "/" if page == 1 else f"{url.rstrip('/')}/page/{page}/"
            html_text = self._get_text(page_url)
            if not html_text or "ページが見つかりません" in html_text:
                break
            found = re.findall(r'href=["\'](https://clients\.itszai\.jp/[^"\']+)["\']', html_text)
            if not found:
                break
            for found_url in found:
                if found_url not in seen:
                    seen.add(found_url)
                    urls.append(found_url)
        return urls

    def _get_text(self, url: str) -> str:
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        if "charset=" not in (response.headers.get("Content-Type") or "").lower():
            response.encoding = response.apparent_encoding
        return response.text

    def _parse_detail(self, soup: BeautifulSoup, url: str) -> dict:
        lines = _section_lines(soup)
        sections = _split_sections(lines)
        item: dict = {
            Schema.URL: url,
            "求人URL": url,
            "採用サイトURL": self._extract_recruit_site_url(soup, url),
        }

        self._apply_json_ld(item, soup)
        self._apply_sections(item, sections, soup)

        if not item.get("業種"):
            item["業種"] = self._infer_industry(item, "\n".join(lines))

        if not item.get(Schema.HP):
            item[Schema.HP] = self._extract_external_hp(soup)

        if not item.get(Schema.NAME):
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            item[Schema.NAME] = re.sub(r"\s*採用サイト.*$", "", title).strip()

        return {key: value for key, value in item.items() if value not in ("", None)}

    def _apply_json_ld(self, item: dict, soup: BeautifulSoup) -> None:
        for obj in _json_ld_objects(soup):
            if obj.get("@type") != "JobPosting":
                continue
            item.setdefault("募集職種", _one_line(obj.get("title")))
            item.setdefault("雇用区分", _one_line(obj.get("employmentType")))
            item.setdefault("給与", _format_salary(obj.get("baseSalary")))

            org = obj.get("hiringOrganization")
            if isinstance(org, dict):
                item.setdefault(Schema.NAME, _one_line(org.get("name")))

            desc = obj.get("description")
            if desc and not item.get(Schema.LOB):
                item[Schema.LOB] = _one_line(BeautifulSoup(desc, "html.parser").get_text(" "))

            location = obj.get("jobLocation")
            if isinstance(location, list):
                location = location[0] if location else {}
            if isinstance(location, dict):
                addr = location.get("address")
                if isinstance(addr, dict):
                    post = _one_line(addr.get("postalCode"))
                    pref = _one_line(addr.get("addressRegion"))
                    city = _one_line(addr.get("addressLocality"))
                    street = _one_line(addr.get("streetAddress"))
                    full_addr = _one_line(f"{pref}{city}{street}")
                    if post:
                        item.setdefault(Schema.POST_CODE, post)
                    if pref:
                        item.setdefault(Schema.PREF, pref)
                    if full_addr:
                        item.setdefault(Schema.ADDR, full_addr)
                        item.setdefault("勤務地", f"〒{post} {full_addr}".strip())

    def _apply_sections(self, item: dict, sections: dict[str, str], soup: BeautifulSoup) -> None:
        name = _first_section(sections, "会社名", "店舗名")
        if name:
            item[Schema.NAME] = _one_line(name.splitlines()[0])

        title = _first_section(sections, "募集職種", "職種")
        if title:
            item["募集職種"] = _one_line(title)

        employment = _first_section(sections, "雇用区分", "雇用形態")
        if employment:
            item["雇用区分"] = _one_line(employment)

        salary = _first_section(sections, "給与")
        if salary:
            item["給与"] = _clean(salary)

        work_time = _first_section(sections, "勤務時間", "就業時間")
        if work_time:
            item["勤務時間"] = _clean(work_time)

        holidays = _first_section(sections, "休日・休暇", "休日休暇")
        if holidays:
            item["休日休暇"] = _clean(holidays)

        benefits = _first_section(sections, "待遇・福利厚生", "福利厚生")
        if benefits:
            item["待遇/福利厚生"] = _clean(benefits)

        insurance = _first_section(sections, "加入保険")
        if insurance:
            item["加入保険"] = _clean(insurance)

        smoking = _first_section(sections, "受動喫煙防止措置")
        if smoking:
            item["受動喫煙防止措置"] = _clean(smoking)

        location = _first_section(sections, "勤務地")
        if location:
            item["勤務地"] = _clean(location)

        head_office = _first_section(sections, "本社所在地")
        if head_office:
            item["本社所在地"] = _clean(head_office)

        address_source = head_office or location
        if address_source:
            post_code, pref, address = _split_address(address_source)
            if post_code:
                item[Schema.POST_CODE] = post_code
            if pref:
                item[Schema.PREF] = pref
            if address:
                item[Schema.ADDR] = address

        business = _first_section(sections, "事業内容", "業務内容", "仕事内容")
        if business:
            item[Schema.LOB] = _clean(business)

        rep = _first_section(sections, "代表者")
        if rep:
            rep_text = _one_line(rep)
            item[Schema.REP_NM] = rep_text
            item["代表者"] = rep_text

        employees = _first_section(sections, "従業員数")
        if employees:
            item[Schema.EMP_NUM] = _one_line(employees)

        all_text = "\n".join(_section_lines(soup))
        tel = self._extract_tel(all_text)
        if tel:
            item[Schema.TEL] = tel

        receipt_time = _first_section(sections, "受付時間")
        if not receipt_time:
            m = re.search(r"受付時間\s*[:：]?\s*([^\n]+)", all_text)
            receipt_time = m.group(1).strip() if m else ""
        if receipt_time:
            item["応募受付時間"] = _one_line(receipt_time)

    def _extract_tel(self, text: str) -> str:
        m = TEL_RE.search(text)
        if m:
            return _norm_tel(m.group(1))
        return ""

    def _extract_recruit_site_url(self, soup: BeautifulSoup, current_url: str) -> str:
        for tag in soup.select("[href], [content]"):
            value = tag.get("href") or tag.get("content") or ""
            if not value.startswith("http"):
                continue
            parsed = urlparse(value)
            host = parsed.netloc.lower()
            if host == CLIENTS_HOST:
                continue
            if host.endswith(".itszai.jp") or host.endswith(".itszai.net"):
                return f"{parsed.scheme}://{parsed.netloc}/"

        parsed_current = urlparse(current_url)
        parts = [p for p in parsed_current.path.split("/") if p]
        if parts:
            return f"{parsed_current.scheme}://{parsed_current.netloc}/{parts[0]}/"
        return ""

    def _extract_external_hp(self, soup: BeautifulSoup) -> str:
        skip_hosts = {
            CLIENTS_HOST,
            "fonts.googleapis.com",
            "fonts.gstatic.com",
            "use.fontawesome.com",
            "cdnjs.cloudflare.com",
        }
        for a in soup.select("a[href]"):
            href = urljoin("https://clients.itszai.jp/", a.get("href", "").strip())
            parsed = urlparse(href)
            host = parsed.netloc.lower()
            if not parsed.scheme.startswith("http"):
                continue
            if host in skip_hosts or host.endswith(".cloudfront.net"):
                continue
            if host.endswith(".itszai.jp") or host.endswith(".itszai.net"):
                continue
            if "/wp-json/" in parsed.path or "/oembed/" in parsed.path:
                continue
            return href
        return ""

    def _infer_industry(self, item: dict, text: str) -> str:
        haystack = "\n".join(
            [
                item.get("募集職種", ""),
                item.get(Schema.LOB, ""),
                text[:3000],
            ]
        )
        patterns = [
            ("建設業", ("建築", "建設", "施工", "土木", "工事", "リフォーム", "設備", "電気工事")),
            ("医療・福祉", ("介護", "看護", "福祉", "医療", "歯科", "整体", "訪問看護")),
            ("保育・教育", ("保育", "幼稚園", "塾", "講師", "学校")),
            ("飲食業", ("飲食", "居酒屋", "カフェ", "レストラン", "調理", "キッチン", "ホール")),
            ("美容", ("美容", "サロン", "理容", "ネイル", "エステ", "アイリスト")),
            ("運輸・物流", ("運送", "配送", "物流", "ドライバー", "倉庫", "タクシー")),
            ("製造業", ("製造", "工場", "加工", "組立", "溶接")),
            ("小売・販売", ("販売", "接客", "店舗", "アパレル", "携帯")),
            ("IT・通信", ("IT", "システム", "エンジニア", "通信", "Web")),
            ("不動産", ("不動産", "賃貸", "売買", "宅建")),
        ]
        for industry, keywords in patterns:
            if any(keyword in haystack for keyword in keywords):
                return industry
        return ""

    def _env_int(self, name: str) -> int:
        value = os.getenv(name, "").strip()
        if not value:
            return 0
        try:
            parsed = int(value)
        except ValueError:
            self.logger.warning("%s が数値ではありません: %s", name, value)
            return 0
        return max(parsed, 0)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = ItszaiScraper()
    scraper.site_name = "イツザイ"
    scraper.site_id = "itszai"
    scraper.execute(SITEMAP_URL)
    print(f"CSV保存先: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

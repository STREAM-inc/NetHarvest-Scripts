"""
トリムトリム【サロン】 — トリミングサロン・ペットサロン検索

取得対象:
    - 全国のトリミングサロン約 15,792 件
    - 一覧ページ (https://trimtrim.jp/salons?page=N) → 詳細ページ (/salon-detail/{id})

実行方法:
    python scripts/sites/beauty/trimtrim_salons.py
    python bin/run_flow.py --site-id trimtrim_salons
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


_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DAY_TO_SCHEMA = {
    "月曜日": Schema.TIME_MON,
    "火曜日": Schema.TIME_TUE,
    "水曜日": Schema.TIME_WED,
    "木曜日": Schema.TIME_THU,
    "金曜日": Schema.TIME_FRI,
    "土曜日": Schema.TIME_SAT,
    "日曜日": Schema.TIME_SUN,
}


class TrimtrimSalonsScraper(StaticCrawler):
    """トリムトリム【サロン】 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "アクセス",
        "駐車場",
        "祝日営業時間",
        "サービス内容",
        "施設の特徴",
        "利用条件・備考",
        "対応できない品種_犬",
        "対応できない品種_猫",
        "動物取扱業登録",
    ]

    LIST_URL_TMPL = "https://trimtrim.jp/salons?page={page}"
    BASE_URL = "https://trimtrim.jp"

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全ページを巡回して詳細URLを集め、各詳細ページをスクレイプ"""
        first_soup = self.get_soup(self.LIST_URL_TMPL.format(page=1))
        if first_soup is None:
            return

        last_page = self._detect_last_page(first_soup)
        total = self._detect_total_count(first_soup)
        if total:
            self.total_items = total
        self.logger.info("総件数=%s 総ページ=%d", total or "不明", last_page)

        for page in range(1, last_page + 1):
            soup = first_soup if page == 1 else self.get_soup(self.LIST_URL_TMPL.format(page=page))
            if soup is None:
                continue
            detail_urls = self._collect_detail_urls(soup)
            self.logger.info("page=%d/%d urls=%d", page, last_page, len(detail_urls))
            for d_url in detail_urls:
                try:
                    item = self._scrape_detail(d_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細エラー: %s — %s", d_url, e)
                    continue

    def _detect_last_page(self, soup) -> int:
        max_page = 1
        for a in soup.select('a[href*="page="]'):
            href = a.get("href", "")
            m = re.search(r"[?&]page=(\d+)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _detect_total_count(self, soup) -> int | None:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"全\s*([\d,]+)", text)
        if m:
            return int(m.group(1).replace(",", ""))
        return None

    def _collect_detail_urls(self, soup) -> list[str]:
        urls = []
        seen = set()
        for a in soup.select('a[href*="/salon-detail/"]'):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(self.BASE_URL, href)
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {
            Schema.URL: url,
            Schema.CAT_SITE: "トリミングサロン",
        }

        h1 = soup.find("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(strip=True)
            kana_el = h1.find_next_sibling()
            if kana_el:
                kana_text = kana_el.get_text(strip=True)
                if kana_text and not kana_text.startswith(data.get(Schema.NAME, "")):
                    data[Schema.NAME_KANA] = kana_text

        for h2 in soup.find_all("h2"):
            label = h2.get_text(strip=True)
            block = h2.parent.find_next_sibling() if h2.parent else None
            if block is None:
                continue

            if label.startswith("店舗情報"):
                self._parse_store_info(block, data)
            elif label.startswith("利用条件"):
                p = block.find("p")
                if p:
                    data["利用条件・備考"] = p.get_text("\n", strip=True)
            elif label.startswith("営業時間"):
                self._parse_business_hours(block, data)
            elif label.startswith("サービス内容"):
                data["サービス内容"] = self._collect_tags(block)
            elif label.startswith("施設の特徴"):
                data["施設の特徴"] = self._collect_tags(block)
            elif label.startswith("対応できない品種"):
                self._parse_unsupported(block, data)
            elif label.startswith("動物取扱業"):
                data["動物取扱業登録"] = self._collect_dt_dd(block)
            elif label.endswith("の評価"):
                m = re.match(r"^([\d.]+)の評価", label)
                if m:
                    data[Schema.SCORES] = m.group(1)

        if not data.get(Schema.NAME):
            return None
        return data

    def _parse_store_info(self, block, data: dict) -> None:
        access_parts = []
        parking_parts = []
        for row in block.select("div.flex.gap-6"):
            label_el = row.select_one("span")
            if not label_el:
                continue
            label = label_el.get_text(strip=True)
            value_box = row.select_one("div.flex-1")
            if value_box is None:
                continue

            if label == "所在地":
                lines = [
                    s.strip()
                    for s in value_box.stripped_strings
                    if s.strip()
                ]
                for line in lines:
                    if line.startswith("〒"):
                        zip_text = line.lstrip("〒").strip()
                        digits = re.sub(r"[^\d]", "", zip_text)
                        if len(digits) == 7:
                            data[Schema.POST_CODE] = f"{digits[:3]}-{digits[3:]}"
                        else:
                            data[Schema.POST_CODE] = zip_text
                    else:
                        addr = data.get(Schema.ADDR, "")
                        data[Schema.ADDR] = (addr + " " + line).strip() if addr else line
                addr_full = data.get(Schema.ADDR, "")
                # 住所先頭の "都道府県 " を分離
                m = _PREF_PATTERN.match(addr_full.replace(" ", "").replace("　", ""))
                if m:
                    pref = m.group(1)
                    data[Schema.PREF] = pref
                    rest = addr_full
                    # remove leading pref (with possible spaces)
                    rest = re.sub(r"^\s*" + re.escape(pref) + r"\s*", "", rest)
                    data[Schema.ADDR] = rest.strip()
            elif label == "アクセス":
                txt = value_box.get_text("\n", strip=True)
                if txt:
                    access_parts.append(txt)
            elif label == "駐車場":
                txt = value_box.get_text("\n", strip=True)
                if txt:
                    parking_parts.append(txt)
            elif label == "電話番号":
                a = value_box.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if href.startswith("tel:"):
                        data[Schema.TEL] = href[len("tel:") :].strip()
                    else:
                        data[Schema.TEL] = a.get_text(strip=True)
                else:
                    data[Schema.TEL] = value_box.get_text(strip=True)

        if access_parts:
            data["アクセス"] = " / ".join(access_parts)
        if parking_parts:
            data["駐車場"] = " / ".join(parking_parts)

    def _parse_business_hours(self, block, data: dict) -> None:
        for row in block.select("div.flex.gap-6"):
            label_el = row.select_one("span")
            value_el = row.select("span")
            if not label_el or len(value_el) < 2:
                continue
            day = label_el.get_text(strip=True)
            value = value_el[1].get_text(strip=True)
            if day in _DAY_TO_SCHEMA:
                data[_DAY_TO_SCHEMA[day]] = value
            elif day == "祝日":
                data["祝日営業時間"] = value

    def _collect_tags(self, block) -> str:
        tags = []
        for span in block.select("span"):
            t = span.get_text(strip=True)
            if t:
                tags.append(t)
        return "; ".join(dict.fromkeys(tags))

    def _parse_unsupported(self, block, data: dict) -> None:
        for h3 in block.find_all("h3"):
            kind = h3.get_text(strip=True)
            container = h3.find_next_sibling()
            if container is None:
                continue
            tags = [s.get_text(strip=True) for s in container.select("span") if s.get_text(strip=True)]
            joined = "; ".join(dict.fromkeys(tags))
            if kind == "犬":
                data["対応できない品種_犬"] = joined
            elif kind == "猫":
                data["対応できない品種_猫"] = joined

    def _collect_dt_dd(self, block) -> str:
        pairs = []
        for row in block.find_all(["div"]):
            dt = row.find("dt")
            dd = row.find("dd")
            if dt and dd:
                k = dt.get_text(strip=True)
                v = dd.get_text(strip=True)
                if k:
                    pairs.append(f"{k}:{v}")
        return "; ".join(pairs)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TrimtrimSalonsScraper()
    scraper.execute("https://trimtrim.jp/salons")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

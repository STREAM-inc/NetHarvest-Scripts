# scripts/sites/jobs/ishikawa_jobnavi.py
"""
いしかわ就活スマートナビ (jobnavi-i.jp) — 石川県若者就職情報総合ポータル企業スクレイパー

取得対象:
    - 掲載企業 (約522件, 53ページ)

取得フロー:
    /search?list=N&type=location (N=1..53) → 各企業の /detail_company?id=ID を取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/ishikawa_jobnavi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id ishikawa_jobnavi
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE = "https://jobnavi-i.jp"

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_ZIP_RE = re.compile(r"〒\s*(\d{3})\s*-\s*(\d{4})")
_TEL_RE = re.compile(r"TEL\s*[：:]\s*([0-9\-()]+)")
_FAX_RE = re.compile(r"FAX\s*[：:]\s*([0-9\-()]+)")
_EMAIL_RE = re.compile(r"Email\s*[：:]\s*([^\s]+@[^\s]+)")
_TOTAL_RE = re.compile(r"(\d+)件中")


class IshikawaJobnaviScraper(StaticCrawler):
    """いしかわ就活スマートナビ 企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["FAX", "Email", "年商", "県内事業所", "各種認定"]
    _test_limit: int | None = None

    def parse(self, url: str):
        seen: set[str] = set()
        detail_urls: list[str] = []
        page = 1
        max_page: int | None = None

        while True:
            list_url = f"{BASE}/search?list={page}&type=location"
            soup = self.get_soup(list_url)

            if page == 1:
                text = soup.get_text(" ", strip=True)
                m = _TOTAL_RE.search(text)
                if m:
                    total = int(m.group(1))
                    self.total_items = total
                    max_page = (total + 9) // 10
                    self.logger.info("全 %d 件 / 最終ページ %d", total, max_page)

            page_links = [
                a["href"]
                for a in soup.select('a[href*="detail_company?id="]')
                if a.get("href")
            ]
            new_links = [u for u in page_links if u not in seen]
            for u in new_links:
                seen.add(u)
                detail_urls.append(u if u.startswith("http") else BASE + u)

            if not new_links:
                self.logger.info("ページ %d: 新規リンクなし、終了", page)
                break
            if max_page and page >= max_page:
                break
            page += 1

        self.logger.info("詳細ページ %d 件取得予定", len(detail_urls))

        if self._test_limit:
            detail_urls = detail_urls[: self._test_limit]

        for du in detail_urls:
            try:
                item = self._scrape_detail(du)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", du, e)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        name_el = soup.select_one("h2.company-info__ttl")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        item: dict = {Schema.URL: url, Schema.NAME: name}

        # 全ての dt.listTtl と dd.listText のペアをパース
        for dt in soup.select("dt.listTtl"):
            dd = dt.find_next_sibling("dd", class_="listText")
            if not dd:
                continue
            label = dt.get_text(strip=True)
            value = " ".join(dd.stripped_strings)
            value = re.sub(r"\s+", " ", value).strip()
            self._apply_field(label, value, dd, item)

        if Schema.NAME not in item:
            return None
        return item

    def _apply_field(self, label: str, value: str, dd, item: dict) -> None:
        if label == "代表者名":
            item[Schema.REP_NM] = value

        elif label == "所在地":
            # 〒XXX - XXXX 住所 TEL：... FAX：... Email：...
            zm = _ZIP_RE.search(value)
            if zm:
                item[Schema.POST_CODE] = f"{zm.group(1)}-{zm.group(2)}"
                addr_text = value[zm.end():]
            else:
                addr_text = value

            tm = _TEL_RE.search(addr_text)
            if tm:
                item[Schema.TEL] = tm.group(1)
                addr_text = addr_text[: tm.start()]

            fm = _FAX_RE.search(value)
            if fm:
                item["FAX"] = fm.group(1)

            em = _EMAIL_RE.search(value)
            if em:
                item["Email"] = em.group(1)

            addr_text = addr_text.strip()
            pm = _PREF_RE.search(addr_text)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr_text[pm.end():].strip()
            else:
                item[Schema.ADDR] = addr_text

        elif label == "設立":
            item[Schema.OPEN_DATE] = value

        elif label == "年商":
            item["年商"] = value

        elif label == "従業員数":
            item[Schema.EMP_NUM] = value

        elif label == "県内事業所":
            item["県内事業所"] = value

        elif label == "業種":
            item[Schema.CAT_SITE] = value

        elif label == "業種内容":
            item[Schema.LOB] = value

        elif label == "ホームページ":
            a = dd.select_one("a[href]")
            if a:
                href = a.get("href", "").strip()
                if href and href.startswith("http"):
                    item[Schema.HP] = href
            if Schema.HP not in item and value.startswith("http"):
                item[Schema.HP] = value

        elif label == "各種認定":
            if value and value != "－":
                item["各種認定"] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IshikawaJobnaviScraper()
    scraper.execute(f"{BASE}/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

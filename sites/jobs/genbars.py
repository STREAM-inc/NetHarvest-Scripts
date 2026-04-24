"""
ゲンバーズ — 現場仕事の求人サイト (genbars.jp)

取得対象:
    全国の現場系求人 (建設・土木・建築・警備・運送) 約2,182件

取得フロー:
    /kw/all/all/?page=N で全件一覧 → 各 jobdetail 詳細ページから企業情報を抽出

実行方法:
    python scripts/sites/jobs/genbars.py
    python bin/run_flow.py --site-id genbars
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

BASE_URL = "https://genbars.jp"
START_URL = "https://genbars.jp/kw/all/all/"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTAL_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_TOTAL_PATTERN = re.compile(r"/\s*([\d,]+)\s*件")
_H1_AREA_PATTERN = re.compile(r"^(.+?)の現場求人(?:\s*\(([^)]+)\))?\s*$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


def _multi_clean(s) -> str:
    if s is None:
        return ""
    lines = [re.sub(r"[ \t　]+", " ", ln).strip() for ln in str(s).splitlines()]
    return "\n".join([ln for ln in lines if ln])


class GenbarsScraper(StaticCrawler):
    """ゲンバーズ (genbars.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "キャッチコピー",
        "職種",
        "給与",
        "勤務地_原文",
        "最寄駅",
        "雇用形態",
        "特長",
        "許可番号",
        "加入保険",
        "受動喫煙対策",
        "採用担当者メッセージ",
        "応募先",
        "応募方法",
        "面接地",
        "情報最終更新日",
        "掲載終了予定日",
        "求人ID",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)

        # 総件数の取得 (例: "(1～20)件 / 2182件")
        total = None
        body_text = soup.get_text(" ", strip=True)
        m = _TOTAL_PATTERN.search(body_text)
        if m:
            try:
                total = int(m.group(1).replace(",", ""))
                self.total_items = total
                self.logger.info("総件数: %d 件", total)
            except ValueError:
                pass

        seen: set[str] = set()
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}?page={page}"
            self.logger.info("一覧ページ取得: %s", page_url)
            list_soup = self.get_soup(page_url) if page > 1 else soup

            # 一覧アイテム
            container = list_soup.select_one("ul.tab-body.active")
            items = container.select(":scope > li.item") if container else []
            if not items:
                self.logger.info("アイテム無し → 終了 (page=%d)", page)
                break

            for li in items:
                a = li.select_one("h3 a[href]")
                if not a:
                    continue
                detail_url = urljoin(BASE_URL, a.get("href"))
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                # 一覧アイテムから補完用に取得
                listing_data = self._extract_listing(li)

                try:
                    item = self._scrape_detail(detail_url, listing_data)
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗 %s: %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            # 次へリンクの有無
            next_link = list_soup.select_one('a[href*="page="]')
            has_next = False
            for a in list_soup.select("a"):
                if a.get_text(strip=True) == "次へ":
                    has_next = True
                    break
            if not has_next:
                self.logger.info("次へ無し → 終了 (page=%d)", page)
                break
            page += 1

    def _extract_listing(self, li) -> dict:
        """一覧 li.item から company name / catch / job_id をフォールバックとして取得"""
        out = {}
        a = li.select_one("h3 a[href]")
        if a:
            out["name_listing"] = _clean(a.get_text())
            href = a.get("href", "")
            id_m = re.search(r"id=(\d+)", href)
            if id_m:
                out["job_id"] = id_m.group(1)
        catch = li.select_one(".content h4")
        if catch:
            out["catch"] = _clean(catch.get_text())
        return out

    def _scrape_detail(self, url: str, listing_data: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        if listing_data.get("job_id"):
            data["求人ID"] = listing_data["job_id"]
        if listing_data.get("catch"):
            data["キャッチコピー"] = listing_data["catch"]

        # h1 から会社名 + エリア
        h1 = soup.select_one("h1")
        if h1:
            h1_text = _clean(h1.get_text())
            m = _H1_AREA_PATTERN.match(h1_text)
            if m:
                if not data.get(Schema.NAME):
                    data[Schema.NAME] = m.group(1).strip()
                if m.group(2):
                    data["エリア"] = m.group(2).strip()

        # TEL: a[href^="tel:"] の最初
        tel_a = soup.select_one('a[href^="tel:"]')
        if tel_a:
            tel_raw = tel_a.get("href", "").replace("tel:", "").strip()
            if tel_raw:
                data[Schema.TEL] = tel_raw

        # dl の dt → dd を全て収集 (重複キーは最初の出現を採用)
        dl_data: dict[str, str] = {}
        dl_html: dict[str, object] = {}
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt", recursive=False) or dl.find_all("dt")
            for dt in dts:
                key = _clean(dt.get_text())
                if not key:
                    continue
                dd = dt.find_next_sibling()
                if not dd or dd.name != "dd":
                    continue
                val = _multi_clean(dd.get_text("\n"))
                if key not in dl_data and val:
                    dl_data[key] = val
                    dl_html[key] = dd

        # マッピング: Schema 列
        if "会社名" in dl_data and not data.get(Schema.NAME):
            data[Schema.NAME] = dl_data["会社名"].splitlines()[0].strip()

        if "代表者" in dl_data:
            data[Schema.REP_NM] = dl_data["代表者"]
        if "設立年月日" in dl_data:
            data[Schema.OPEN_DATE] = dl_data["設立年月日"]
        if "資本金" in dl_data:
            data[Schema.CAP] = dl_data["資本金"]
        if "事業内容" in dl_data:
            data[Schema.LOB] = dl_data["事業内容"]
        if "業種" in dl_data:
            data[Schema.CAT_SITE] = dl_data["業種"]

        # 住所/郵便番号: 本店所在地 を最優先、なければ 連絡先 dd の2行目
        addr_text = dl_data.get("本店所在地", "")
        if not addr_text and "連絡先" in dl_data:
            lines = [ln.strip() for ln in dl_data["連絡先"].splitlines() if ln.strip()]
            if len(lines) >= 2:
                addr_text = lines[1]
            elif lines:
                addr_text = lines[0]
        if addr_text:
            postal_m = _POSTAL_PATTERN.search(addr_text)
            if postal_m:
                data[Schema.POST_CODE] = postal_m.group(1)
                addr_text = _POSTAL_PATTERN.sub("", addr_text).strip()
            addr_text = _clean(addr_text)
            data[Schema.ADDR] = addr_text
            pref_m = _PREF_PATTERN.search(addr_text)
            if pref_m:
                data[Schema.PREF] = pref_m.group(1)

        # 連絡先 dd から会社名フォールバック
        if not data.get(Schema.NAME) and "連絡先" in dl_data:
            data[Schema.NAME] = dl_data["連絡先"].splitlines()[0].strip()

        # HP: 会社URL dd の a[href]
        if "会社URL" in dl_html:
            a = dl_html["会社URL"].find("a", href=True)
            if a and a["href"].startswith("http"):
                data[Schema.HP] = a["href"]

        # EXTRA カラム
        extra_map = {
            "職種": "職種",
            "給与": "給与",
            "勤務地": "勤務地_原文",
            "最寄駅": "最寄駅",
            "雇用形態": "雇用形態",
            "特長": "特長",
            "許可番号等": "許可番号",
            "加入保険": "加入保険",
            "受動喫煙対策": "受動喫煙対策",
            "採用担当者から": "採用担当者メッセージ",
            "応募先": "応募先",
            "応募方法": "応募方法",
            "面接地": "面接地",
            "情報最終更新日": "情報最終更新日",
            "掲載終了予定日": "掲載終了予定日",
        }
        for src_key, col in extra_map.items():
            if src_key in dl_data:
                data[col] = dl_data[src_key]

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GenbarsScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

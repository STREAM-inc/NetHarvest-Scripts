"""
POLA 店舗検索スクレイパー

取得対象:
    全国 約2,500店舗の POLA 店舗情報 (店舗名・住所・都道府県・TEL・営業時間・定休日・
    店舗カテゴリ・サービス・アクセス)

取得フロー:
    - https://www.pola.net/ の都道府県 select から pref_code=01〜47 と件数を取得
    - 各 pref_code について ?pref_code=NN&page=N を末尾ページまで巡回
    - 各ページの section.resultList から一覧表示だけで全フィールドを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/pola.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id pola
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_COUNT_PATTERN = re.compile(r"（(\d+)件）")
_SEARCH_URL = "https://www.pola.net/search"


class PolaScraper(StaticCrawler):
    """POLA 店舗検索スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["サービス", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        pref_entries = self._collect_prefectures()
        self.total_items = sum(count for _, count in pref_entries)
        self.logger.info(
            "都道府県 %d 件、総店舗数 %d 件を対象に巡回開始",
            len(pref_entries),
            self.total_items,
        )

        for pref_code, _count in pref_entries:
            page = 1
            while True:
                page_url = f"{_SEARCH_URL}?pref_code={pref_code}&page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                sections = soup.select("section.resultList")
                if not sections:
                    break

                for section in sections:
                    try:
                        item = self._parse_section(section)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("セクション解析エラー: %s", e)
                        continue
                    if item:
                        yield item

                page += 1

    def _collect_prefectures(self) -> list[tuple[str, int]]:
        """トップページから都道府県コードと件数を抽出"""
        soup = self.get_soup("https://www.pola.net/")
        if soup is None:
            return []

        entries: list[tuple[str, int]] = []
        select = soup.find("select", attrs={"name": "pref_code"})
        if not select:
            self.logger.warning("都道府県 select が見つからない")
            return entries

        for opt in select.find_all("option"):
            code = (opt.get("value") or "").strip()
            if not code:
                continue
            text = opt.get_text(strip=True)
            m = _COUNT_PATTERN.search(text)
            count = int(m.group(1)) if m else 0
            entries.append((code, count))
        return entries

    def _parse_section(self, section) -> dict | None:
        """一覧ページの 1 件分 (section.resultList) を dict に変換"""
        title_a = section.select_one(".resultList__title__name")
        if not title_a:
            return None

        name = title_a.get_text(strip=True)
        shop_url = title_a.get("href", "").strip()

        addr_a = section.select_one(".resultList__address--main")
        address = addr_a.get_text(" ", strip=True) if addr_a else ""

        access_sup = section.select_one(".resultList__address--sup")
        access = access_sup.get_text(" ", strip=True) if access_sup else ""

        cat_span = section.select_one(".resultList__title .c-txt--small")
        cat_site = cat_span.get_text(strip=True) if cat_span else ""

        # サービス (ラベル先頭のテキストノードのみ: ヘルプ説明を除外)
        services: list[str] = []
        for li in section.select(".resultList__labels > li"):
            label = ""
            for node in li.contents:
                if isinstance(node, str):
                    s = node.strip()
                    if s:
                        label = s
                        break
            if label:
                services.append(label)
        services_str = ",".join(services)

        # 営業時間・定休日・電話番号 — dl dt/dd から抽出
        info = self._parse_info_dl(section)
        tel = info.get("電話番号", "")
        time_val = info.get("営業時間", "")
        holiday = info.get("定休日", "")

        # 電話番号は tel: リンクから直接取ってフォールバック
        if not tel:
            tel_a = section.select_one('a[href^="tel:"]')
            if tel_a:
                tel = tel_a.get_text(strip=True) or tel_a.get("href", "").replace("tel:", "")

        pref = ""
        addr_clean = address
        if address:
            m = _PREF_PATTERN.match(address)
            if m:
                pref = m.group(1)
                addr_clean = address[m.end():].strip()

        item = {
            Schema.NAME: name,
            Schema.URL: shop_url,
            Schema.PREF: pref,
            Schema.ADDR: addr_clean,
            Schema.TEL: tel,
            Schema.TIME: time_val,
            Schema.HOLIDAY: holiday,
            Schema.CAT_SITE: cat_site,
            "サービス": services_str,
            "アクセス": access,
        }
        return item

    @staticmethod
    def _parse_info_dl(section) -> dict[str, str]:
        """resultList__info 配下の dl から dt→dd を抽出"""
        result: dict[str, str] = {}
        for div in section.select(".resultList__info > div, .resultList__info dl > div"):
            dt = div.find("dt")
            dd = div.find("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True)
            # dd 内の改行を維持しつつ空白を整理
            for br in dd.find_all("br"):
                br.replace_with("\n")
            value = re.sub(r"[ \t]+", " ", dd.get_text("\n")).strip()
            value = re.sub(r"\n\s*\n+", "\n", value)
            result[key] = value
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PolaScraper()
    scraper.execute("https://www.pola.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

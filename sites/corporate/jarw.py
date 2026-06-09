"""
日本冷蔵倉庫協会 (jarw.or.jp) — 地域別事業所会員名簿スクレイパー

取得対象:
    - 全国 10 地域の冷蔵倉庫事業所会員（F級・C級の別）
    - 企業名・事業所名・電話番号・HP・F級・C級・住所

取得フロー:
    10 地域ページ（/find/memberlist/{slug}）を順次巡回し、
    各ページのテーブルから事業所情報を抽出する

実行方法:
    python scripts/sites/corporate/jarw.py
    python bin/run_flow.py --site-id jarw
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.jarw.or.jp"
INDEX_URL = f"{BASE_URL}/find/memberlist"

# 地域スラッグと表示名のリスト（東海のみ URL エンコードされた特殊スラッグ）
REGIONS = [
    ("hokkaido",           "北海道"),
    ("touhoku",            "東北"),
    ("shutoken",           "首都圏"),
    ("kantoukoushinetsu",  "関東甲信越"),
    ("hokuriku",           "北陸"),
    (
        "%E5%9C%B0%E5%9F%9F%E5%88%A5%E4%BA%8B%E6%A5%AD%E6%89%80%E4%BC%9A%E5%93%A1%E5%90%8D%E7%B0%BF%E6%9D%B1%E6%B5%B7%E5%9C%B0%E6%96%B9",
        "東海",
    ),
    ("kinki",              "近畿"),
    ("chugoku",            "中国"),
    ("shikoku",            "四国"),
    ("kyushuokinawa",      "九州沖縄"),
]

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _normalize(text: str) -> str:
    """全角英数字→半角に変換してマッチしやすくする。"""
    result = []
    for c in text:
        code = ord(c)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(c)
    return "".join(result)


def _split_pref(addr: str) -> tuple[str, str]:
    """住所から (都道府県, 市区町村以降) を返す。"""
    addr = _clean(addr)
    pm = _PREF_RE.match(addr)
    if pm:
        return pm.group(1), addr[pm.end() :].strip()
    return "", addr



class JarwScraper(StaticCrawler):
    """日本冷蔵倉庫協会 地域別事業所会員名簿スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["事業所名", "F級", "C級", "地域"]

    def parse(self, url: str):
        for slug, region_name in REGIONS:
            region_url = f"{INDEX_URL}/{slug}"
            self.logger.info("地域巡回: %s (%s)", region_name, region_url)
            soup = self.get_soup(region_url)
            if soup is None:
                self.logger.warning("取得失敗: %s", region_url)
                continue

            for item in self._parse_region_page(soup, region_url, region_name):
                yield item

    def _parse_region_page(self, soup, page_url: str, region_name: str):
        # 実際の列構造（<th>ヘッダーなし、td[0]=番号）:
        # td[0]=番号 td[1]=企業名 td[2]=事業所名 td[3]=電話番号
        # td[4]=HP(○リンク) td[5]=F級 td[6]=C級 td[7]=住所
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            # 番号列(td[0])が数字の行が含まれるテーブルのみ対象
            data_rows = [
                tr for tr in rows
                if len(tr.find_all("td")) >= 7
                and re.match(r"^\d+$", _clean(tr.find_all("td")[0].get_text(strip=True)))
            ]
            if not data_rows:
                continue

            for tr in data_rows:
                tds = tr.find_all("td")

                def _get(idx: int) -> str:
                    if idx < len(tds):
                        return _clean(tds[idx].get_text(" ", strip=True))
                    return ""

                name = _get(1)
                if not name:
                    continue

                # td[3]: 電話番号（<a href="tel:..."> または plain text）
                tel = _get(3)

                # td[4]: HP（<a href="http...">○</a> の場合に URL 取得）
                hp = ""
                if len(tds) > 4:
                    a = tds[4].find("a", href=True)
                    if a:
                        href = a.get("href", "")
                        if href.startswith("http"):
                            hp = href.strip()

                addr_raw = _get(7)
                pref, addr = _split_pref(addr_raw)

                try:
                    yield {
                        Schema.URL: page_url,
                        Schema.NAME: name,
                        Schema.PREF: pref,
                        Schema.ADDR: addr,
                        Schema.TEL: tel,
                        Schema.HP: hp,
                        "事業所名": _get(2),
                        "F級": _get(5),
                        "C級": _get(6),
                        "地域": region_name,
                    }
                except Exception:
                    self.logger.exception("行解析失敗: %s", page_url)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JarwScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

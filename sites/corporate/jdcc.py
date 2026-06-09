"""
日本データセンター協会 (jdcc.or.jp) — データセンター一覧スクレイパー

取得対象:
    - 約 300 件のデータセンター情報（単一ページ）
    - DC名・所在地・会社名・設置年度・FSレベル・サーバー室面積・TEL・HP

注記:
    - メールアドレスは @ を画像で難読化しているため取得不可（除外）

取得フロー:
    /dclist/ の単一ページからテーブルを全行パース

実行方法:
    python scripts/sites/corporate/jdcc.py
    python bin/run_flow.py --site-id jdcc
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.jdcc.or.jp"
LIST_URL = f"{BASE_URL}/dclist/"

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
_TEL_RE = re.compile(r"(\d{2,4}-\d{2,4}-\d{3,4})")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _normalize(text: str) -> str:
    result = []
    for c in text:
        code = ord(c)
        if 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        else:
            result.append(c)
    return "".join(result)


def _split_pref(location: str) -> tuple[str, str]:
    """所在地から (都道府県, 市区町村以降) を返す。"""
    loc = _clean(location)
    pm = _PREF_RE.match(loc)
    if pm:
        return pm.group(1), loc[pm.end() :].strip()
    return "", loc


def _col_index(headers: list[str], *keywords: str) -> int:
    for kw in keywords:
        for i, h in enumerate(headers):
            if kw in _normalize(h):
                return i
    return -1


class JdccScraper(StaticCrawler):
    """日本データセンター協会 データセンター一覧スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["設置年度", "FSレベル", "サーバー室面積"]

    def parse(self, url: str):
        soup = self.get_soup(LIST_URL)
        if soup is None:
            return

        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if not header_row:
                continue
            headers = [_normalize(_clean(th.get_text(strip=True))) for th in header_row.find_all(["th", "td"])]
            if not any(kw in " ".join(headers) for kw in ["データセンター", "DC", "会社"]):
                continue

            dc_idx      = _col_index(headers, "データセンター名", "DC名")
            location_idx = _col_index(headers, "所在地")
            company_idx = _col_index(headers, "会社名", "会社")
            year_idx    = _col_index(headers, "設置年度", "年度")
            fs_idx      = _col_index(headers, "FSレベル", "FS")
            area_idx    = _col_index(headers, "面積", "サーバー室")
            contact_idx = _col_index(headers, "問い合わせ", "連絡先", "お問い合わせ")

            data_rows = table.find_all("tr")[1:]
            self.total_items = len(data_rows)

            for tr in data_rows:
                tds = tr.find_all("td")
                if not tds:
                    continue

                def _get(idx: int) -> str:
                    if 0 <= idx < len(tds):
                        return _clean(tds[idx].get_text(" ", strip=True))
                    return ""

                dc_name  = _get(dc_idx) if dc_idx >= 0 else ""
                company  = _get(company_idx) if company_idx >= 0 else ""
                if not dc_name and not company:
                    continue

                location = _get(location_idx) if location_idx >= 0 else ""
                pref, addr = _split_pref(location)

                # 問い合わせ先セル: TEL と HP を抽出
                tel, hp = "", ""
                if contact_idx >= 0 and contact_idx < len(tds):
                    contact_td = tds[contact_idx]
                    contact_text = _clean(contact_td.get_text(" ", strip=True))
                    tm = _TEL_RE.search(contact_text)
                    if tm:
                        tel = tm.group(1)
                    for a in contact_td.find_all("a", href=True):
                        href = a.get("href", "")
                        if href.startswith("http") and not href.startswith("mailto"):
                            hp = href.strip()
                            break

                try:
                    yield {
                        Schema.URL: LIST_URL,
                        Schema.FAC_NAME: dc_name,
                        Schema.NAME: company,
                        Schema.PREF: pref,
                        Schema.ADDR: addr,
                        Schema.TEL: tel,
                        Schema.HP: hp,
                        "設置年度": _get(year_idx) if year_idx >= 0 else "",
                        "FSレベル": _get(fs_idx) if fs_idx >= 0 else "",
                        "サーバー室面積": _get(area_idx) if area_idx >= 0 else "",
                    }
                except Exception:
                    self.logger.exception("行解析失敗: %s", dc_name)

            break  # 最初に見つかった有効テーブルのみ処理


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JdccScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

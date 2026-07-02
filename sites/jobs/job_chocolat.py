import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

PREF_MAP = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県",
    "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県",
    "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県",
    "oita": "大分県", "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

# 詳細ページのURL形態: /{pref}/a_NNN/shop/{id}/ （末尾 /shop/数字/）
_DETAIL_RE = re.compile(r"/shop/\d+/?$")
# 住所欄に混入する地図リンク文言を除去
_MAP_NOISE_RE = re.compile(r"\s*Google\s*MAP.*$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s{2,}", " ", re.sub(r"[\r\n\t]+", " ", str(s))).strip()


def _norm_key(s: str) -> str:
    """ラベルの内部空白を除去して正規化（例: 「住 所」→「住所」「業 種」→「業種」）"""
    return re.sub(r"\s+", "", s or "")


class JobChocolatScraper(DynamicCrawler):
    """ジョブショコラ ナイト求人スクレイパー（job-chocolat.jp）"""

    DELAY = 0.6
    EXTRA_COLUMNS = ["業種", "LINE公式", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして扱い、都道府県一覧URLを派生させる
        seen: set[str] = set()
        for pref_code, pref_ja in PREF_MAP.items():
            list_url = urljoin(url, f"{pref_code}/shoplist/")
            self.logger.info("都道府県: %s (%s)", pref_ja, list_url)
            yield from self._scrape_pref(list_url, pref_ja, seen)

    def _scrape_pref(self, list_url: str, pref_ja: str, seen: set) -> Generator[dict, None, None]:
        current = list_url
        while current:
            # 静的SSRページなので domcontentloaded で十分（networkidle はタイムアウト要因）
            soup = self.get_soup(current, wait_until="domcontentloaded")
            if soup is None:
                break

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href or not _DETAIL_RE.search(href):
                    continue
                full = urljoin(current, href)
                if full in seen:
                    continue
                seen.add(full)
                item = self._scrape_detail(full, pref_ja)
                if item:
                    yield item

            # ページ送り: 「次へ」テキストリンク、無ければ終了
            next_url = None
            for a in soup.select("a[href]"):
                if a.get_text(strip=True) == "次へ" and a.get("href"):
                    next_url = urljoin(current, a["href"])
                    break
            current = next_url if next_url and next_url != current else None

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url, wait_until="domcontentloaded")
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        # 店舗情報は2テーブルに分かれる:
        #   基本情報table(店鋪名/業種/営業時間/定休日/アクセス) と 応募情報table(応募方法/電話で応募/LINE/住所/採用担当)
        # 一方 応募フォームtable(氏名/メールアドレス/電話番号…) は混入させたくないので、
        # th に「店鋪名/店舗名」か「住所/所在地」を含むテーブルのみを情報テーブルとして採用する。
        info_tables = []
        for table in soup.select("table"):
            keys = {_norm_key(th.get_text()) for th in table.select("th")}
            if keys & {"店鋪名", "店舗名"} or any("住所" in k or "所在地" in k for k in keys):
                info_tables.append(table)

        for info_table in info_tables:
            for tr in info_table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _norm_key(th.get_text())
                val = _clean(td.get_text(" "))
                if not val:
                    continue
                if key in ("店鋪名", "店舗名"):
                    data.setdefault(Schema.NAME, val)
                elif "住所" in key or "所在地" in key:
                    data[Schema.ADDR] = _clean(_MAP_NOISE_RE.sub("", val))
                elif "電話" in key or key == "TEL":
                    if val not in ("--", "-", "―"):
                        data[Schema.TEL] = val
                elif "業種" in key:
                    data["業種"] = val
                elif "営業時間" in key:
                    data[Schema.TIME] = val
                elif "定休日" in key:
                    data[Schema.HOLIDAY] = val
                elif "LINE" in key:
                    data["LINE公式"] = val
                elif "アクセス" in key:
                    data["アクセス"] = val

        # フォールバック: 店鋪名が取れなければ h1 から
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JobChocolatScraper().execute("https://job-chocolat.jp/")

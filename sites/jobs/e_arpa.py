"""
アルパ (e-arpa) — 求人サイト「アルパ」クローラー

取得対象:
    - 全国 47 都道府県の求人情報 (企業名・勤務地・給与・電話番号・事業内容 等)

取得フロー:
    トップ (https://www.e-arpa.jp/) を起点に、47 都道府県それぞれの一覧
        /{pref}/Companies/index?page=N
    を巡回し、各求人の詳細ページ
        /{pref}/detail.html?jid=...
    を 1 件取得するごとに即 yield する (Pattern B / 早期 yield)。

備考対応:
    - 「一覧ではなく各都道府県ごとに求人情報がある」点に対応し、都道府県単位で
      一覧→詳細を巡回する。地域・期間等の明示的なフィルター指示は無いため全件取得。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/e_arpa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id e_arpa
"""

import logging
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# トップページの都道府県リンクと同じ slug。url を起点に
# urljoin(url, f"{slug}/Companies/index") として一覧 URL を派生させる。
PREFECTURES = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "nigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県", "tokushima": "徳島県",
    "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県", "fukuoka": "福岡県",
    "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県", "oita": "大分県",
    "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

_PREF_RE = re.compile(
    r"(北海道|東京都|(?:京都|大阪)府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|"
    r"埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)
_POST_RE = re.compile(r"(\d{3}-\d{4})")
_TEL_RE = re.compile(r"0[\d\-]{8,}")


def _clean(text: str) -> str:
    """改行・連続空白を 1 個の半角スペースに畳む。"""
    return re.sub(r"\s+", " ", text or "").strip()


class EArpa(StaticCrawler):
    """アルパ (e-arpa) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "雇用形態",       # 一覧の区分タグ (正社員 / 一般派遣 等)
        "募集職種",       # 一覧の求人タイトル
        "給与",          # 詳細: 給与 (構造的・数値情報)
        "勤務地",         # 詳細: 勤務地 (市区町村)
        "アクセス",       # 詳細: 勤務地アクセス方法
        "勤務期間",       # 詳細: 勤務期間
        "特徴",          # 詳細: 特徴タグ列
        "待遇・福利厚生",  # 詳細: 待遇・福利厚生 (条件タグ列)
    ]

    def parse(self, url: str):
        base = url.rstrip("/")
        first = True

        for slug, pref_name in PREFECTURES.items():
            page = 1
            while True:
                list_url = f"{base}/{slug}/Companies/index?page={page}"
                try:
                    soup = self.get_soup(list_url)
                except Exception as e:
                    logger.warning("一覧取得失敗 %s: %s", list_url, e)
                    break

                if first:
                    # 進捗表示用に全国総件数を 1 度だけ設定 (取得できなければ未設定のまま)
                    total_el = soup.select_one(".total")
                    if total_el:
                        m = re.search(r"([\d,]+)\s*件", total_el.get_text())
                        if m:
                            self.total_items = int(m.group(1).replace(",", ""))
                    first = False

                articles = soup.select("article.block-pr")
                if not articles:
                    break

                for art in articles:
                    a = art.select_one('a[href*="detail.html"]')
                    if not a or not a.get("href"):
                        continue
                    detail_url = urljoin(url, a.get("href"))

                    cate = art.select_one(".cate")
                    title = art.select_one("h3 a")
                    listing = {
                        "雇用形態": cate.get_text(strip=True) if cate else "",
                        "募集職種": title.get_text(strip=True) if title else "",
                    }

                    try:
                        item = self._scrape_detail(detail_url, listing, pref_name)
                    except Exception as e:
                        logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                        continue
                    if item:
                        yield item

                page += 1

    def _scrape_detail(self, url: str, listing: dict, pref_fallback: str) -> dict | None:
        soup = self.get_soup(url)

        # 詳細ページは <tr><td>(ラベル)</td><td>(値)</td></tr> 形式。
        # ラベルは NFKC 正規化して全角/半角の揺れ (例: ＵＲＬ→URL) を吸収する。
        # 同じラベルが上部サマリ表と下部スペック表に重複する場合があるため、
        # より構造化された下部スペック表 (後勝ち) の値を採用する。
        data: dict[str, str] = {}
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 2:
                continue
            label = unicodedata.normalize("NFKC", _clean(tds[0].get_text(" ", strip=True)))
            value = _clean(tds[1].get_text(" ", strip=True))
            if label and value:
                data[label] = value

        name = data.get("企業名") or data.get("応募会社", "")
        if not name:
            return None

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref_fallback,
            Schema.ADDR: "",
            Schema.LOB: data.get("事業内容", ""),
            Schema.TIME: data.get("勤務時間", ""),
            Schema.HOLIDAY: data.get("休日・休暇", ""),
            Schema.CAT_SITE: data.get("職種", ""),
            # EXTRA
            "雇用形態": listing.get("雇用形態", ""),
            "募集職種": listing.get("募集職種", ""),
            "給与": data.get("給与", ""),
            "勤務地": data.get("勤務地", ""),
            "アクセス": data.get("勤務地アクセス方法", ""),
            "勤務期間": data.get("勤務期間", ""),
            "特徴": data.get("特徴", ""),
            "待遇・福利厚生": data.get("待遇・福利厚生", ""),
        }

        # 住所 → 郵便番号 / 都道府県 / 住所(以降) を分解
        addr = data.get("住所", "")
        if addr:
            pc = _POST_RE.search(addr)
            if pc:
                item[Schema.POST_CODE] = pc.group(1)
            m = _PREF_RE.search(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = _clean(addr[m.end():])
            else:
                item[Schema.ADDR] = _clean(_POST_RE.sub("", addr))

        # 電話番号 (先頭の番号のみ。担当者名等は除去。全角→半角は Pipeline が処理)
        tel_raw = data.get("電話番号", "")
        if tel_raw:
            tm = _TEL_RE.search(tel_raw)
            item[Schema.TEL] = tm.group(0) if tm else tel_raw

        # 会社 HP (ＵＲＬ / URL ラベル)
        hp = data.get("URL", "")
        if hp:
            item[Schema.HP] = hp

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EArpa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.e-arpa.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

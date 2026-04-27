"""
バイトル — 全国市区町村×事業所単位 求人企業情報スクレイパー（baitoru.com）

取得対象:
    - 47 都道府県の各 /area/{pref}/ から市区町村URLを動的取得（全国 約1,330市区町村）
    - 各市区町村ページを link[rel=next] でページ巡回（サイト側500ページ上限まで）
    - 求人詳細の企業情報から、事業所単位 (社名, 住所) で重複排除しつつレコード化

カラム:
    Schema (9): URL, NAME, PREF, ADDR, TEL, REP_NM, LOB, HP, CAT_SITE
    EXTRA  (3): サービス地域, 初出時の市区町村コード, 仕事内容

取得フロー:
    for (region, pref_roman, pref_ja) in PREFECTURES:
        /area/{pref_roman}/ を取得 → 市区町村パスを抽出
        for city_path in city_paths:
            for page in 1..N (link[rel=next] で進む):
                求人カード収集 → 詳細ページにアクセス
                  → (社名, 正規化住所) が初出なら yield

実行方法:
    python scripts/sites/jobs/baitoru.py
    python bin/run_flow.py --site-id baitoru
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

BASE_URL = "https://www.baitoru.com"

# (region, pref_roman, pref_ja)
PREFECTURES: list[tuple[str, str, str]] = [
    ("tohoku", "hokkaido", "北海道"),
    ("tohoku", "aomori", "青森県"),
    ("tohoku", "iwate", "岩手県"),
    ("tohoku", "miyagi", "宮城県"),
    ("tohoku", "akita", "秋田県"),
    ("tohoku", "yamagata", "山形県"),
    ("tohoku", "fukushima", "福島県"),
    ("kanto", "ibaraki", "茨城県"),
    ("kanto", "tochigi", "栃木県"),
    ("kanto", "gumma", "群馬県"),
    ("kanto", "saitama", "埼玉県"),
    ("kanto", "chiba", "千葉県"),
    ("kanto", "tokyo", "東京都"),
    ("kanto", "kanagawa", "神奈川県"),
    ("koshinetsu", "nigata", "新潟県"),
    ("koshinetsu", "toyama", "富山県"),
    ("koshinetsu", "ishikawa", "石川県"),
    ("koshinetsu", "fukui", "福井県"),
    ("koshinetsu", "yamanashi", "山梨県"),
    ("koshinetsu", "nagano", "長野県"),
    ("tokai", "gifu", "岐阜県"),
    ("tokai", "shizuoka", "静岡県"),
    ("tokai", "aichi", "愛知県"),
    ("tokai", "mie", "三重県"),
    ("kansai", "shiga", "滋賀県"),
    ("kansai", "kyoto", "京都府"),
    ("kansai", "osaka", "大阪府"),
    ("kansai", "hyogo", "兵庫県"),
    ("kansai", "nara", "奈良県"),
    ("kansai", "wakayama", "和歌山県"),
    ("chushikoku", "tottori", "鳥取県"),
    ("chushikoku", "shimane", "島根県"),
    ("chushikoku", "okayama", "岡山県"),
    ("chushikoku", "hiroshima", "広島県"),
    ("chushikoku", "yamaguchi", "山口県"),
    ("chushikoku", "tokushima", "徳島県"),
    ("chushikoku", "kagawa", "香川県"),
    ("chushikoku", "ehime", "愛媛県"),
    ("chushikoku", "kochi", "高知県"),
    ("kyushu", "fukuoka", "福岡県"),
    ("kyushu", "saga", "佐賀県"),
    ("kyushu", "nagasaki", "長崎県"),
    ("kyushu", "kumamoto", "熊本県"),
    ("kyushu", "oita", "大分県"),
    ("kyushu", "miyazaki", "宮崎県"),
    ("kyushu", "kagoshima", "鹿児島県"),
    ("kyushu", "okinawa", "沖縄県"),
]

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_TEL_RE = re.compile(r"TEL[:：]\s*([0-9０-９()\-－\s]+)")

# 市区町村末尾パターン（ku/shi/cho/mura/gun で終わる）
_CITY_END_RE = re.compile(r"(ku|shi|cho|mura|gun)$")

# 職種カテゴリ等のノイズパス（市区町村として誤検出しないため）
_NOISE_PATH_RE = re.compile(
    r"(food|sales|service|hallstaff|kitchen|conveni|trm|mrt|haken|next|"
    r"shain|saler|salesetc|foodetc|serviceetc|kosateniseki|driver|"
    r"office|education|nightwork|wear|delivery|cleaning|factory|warehouse|"
    r"medical|nursing|childcare|beauty|reception|callcenter|teacher)"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class BaitoruScraper(StaticCrawler):
    """バイトル 全国市区町村×事業所単位 スクレイパー"""

    DELAY = 0.7
    EXTRA_COLUMNS = ["サービス地域", "初出時の市区町村コード", "仕事内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_facilities: set[tuple[str, str]] = set()
        seen_details: set[str] = set()

        for region, pref_roman, pref_ja in PREFECTURES:
            city_paths = self._collect_city_paths(region, pref_roman)
            self.logger.info(
                "[%s] 市区町村 %d 件収集", pref_ja, len(city_paths)
            )
            for city_path in city_paths:
                city_url = urljoin(BASE_URL, city_path)
                yield from self._scrape_city(
                    city_url,
                    pref_ja,
                    city_path,
                    seen_details,
                    seen_facilities,
                )

        self.logger.info(
            "全体完了: 求人 %d 件アクセス → 事業所 %d 件",
            len(seen_details), len(seen_facilities),
        )

    # ------------------------------------------------------------------
    # 市区町村URL の動的収集
    # ------------------------------------------------------------------

    def _collect_city_paths(self, region: str, pref_roman: str) -> list[str]:
        """都道府県の /area/{pref}/ ページから市区町村URLパスを動的に収集する。

        都道府県によって URL 階層が異なる:
          - 政令市等: /{region}/jlist/{pref}/{大都市階層}/{市区町村}/
          - それ以外: /{region}/jlist/{pref}/{市区町村}/
        両方を採取し、件数の多い方を採用する。
        """
        area_url = f"{BASE_URL}/area/{pref_roman}/"
        soup = self.get_soup(area_url)
        if soup is None:
            self.logger.warning("/area/%s/ の取得失敗", pref_roman)
            return []

        prefix = f"/{region}/jlist/{pref_roman}/"
        cities2: set[str] = set()
        cities1: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "") or ""
            if not href.startswith(prefix):
                continue
            tail = href[len(prefix):]
            tail = tail.rstrip("/")
            if not tail:
                continue
            parts = tail.split("/")
            # ノイズパス除外
            if any(_NOISE_PATH_RE.match(p) for p in parts):
                continue
            # 末尾が ku/shi/cho/mura/gun で終わるもののみ採用
            if not _CITY_END_RE.search(parts[-1]):
                continue
            full = prefix + tail + "/"
            if len(parts) == 2:
                cities2.add(full)
            elif len(parts) == 1:
                cities1.add(full)

        if len(cities2) >= len(cities1):
            return sorted(cities2)
        return sorted(cities1)

    # ------------------------------------------------------------------
    # 市区町村単位の巡回
    # ------------------------------------------------------------------

    def _scrape_city(
        self,
        city_url: str,
        pref_ja: str,
        city_code: str,
        seen_details: set[str],
        seen_facilities: set[tuple[str, str]],
    ) -> Generator[dict, None, None]:
        current = city_url
        page_no = 1
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            articles = soup.select("article.list-jobListDetail")
            if not articles:
                break

            new_on_page = 0
            for article in articles:
                a = article.select_one("h3 a[href]") or article.select_one(
                    "a[href*='/job']"
                )
                if not a:
                    continue
                href = a.get("href", "").strip()
                detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                detail_url = detail_url.split("?")[0]
                if detail_url in seen_details:
                    continue
                seen_details.add(detail_url)
                new_on_page += 1

                try:
                    item = self._scrape_detail(detail_url, pref_ja, city_code)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                    continue
                if item is None:
                    continue

                key = self._facility_key(item)
                if key is None or key in seen_facilities:
                    continue
                seen_facilities.add(key)
                yield item

            self.logger.info(
                "[%s] %s page=%d: %d cards, %d new (累計 求人%d / 事業所%d)",
                pref_ja, city_code, page_no, len(articles),
                new_on_page, len(seen_details), len(seen_facilities),
            )

            next_link = soup.find("link", rel="next")
            if next_link and next_link.get("href"):
                href = next_link["href"]
                next_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                if next_url == current:
                    break
                current = next_url
                page_no += 1
            else:
                break

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, pref_ja: str, city_code: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {
            Schema.URL: url,
            Schema.PREF: pref_ja,
            "初出時の市区町村コード": city_code,
        }

        company = soup.select_one("div.detail-companyInfo")
        if not company:
            return None

        # 会社名: .pt02 > 最初のdd > 最初のp
        pt02 = company.select_one(".pt02")
        if pt02:
            first_dd = pt02.select_one("dd")
            if first_dd:
                first_p = first_dd.select_one("p")
                if first_p:
                    data[Schema.NAME] = _clean(first_p.get_text())
                else:
                    raw = first_dd.get_text(" ", strip=True)
                    raw = re.sub(r"この会社の[^\s]+", "", raw)
                    data[Schema.NAME] = _clean(raw)

        # ラベル–値ペア
        for dl in company.select(".pt03 > dl"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)

            if label == "所在地":
                p = dd.select_one("p") or dd
                text = p.get_text("\n").strip()
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                addr_parts = []
                for line in lines:
                    if "TEL" in line:
                        m = _TEL_RE.search(line)
                        if m:
                            tel = m.group(1).strip()
                            tel = re.split(r"FAX|ＦＡＸ", tel)[0].strip()
                            if tel:
                                data[Schema.TEL] = tel
                        addr_part = re.split(r"TEL[:：]", line)[0].strip()
                        if addr_part:
                            addr_parts.append(addr_part)
                    elif "FAX" in line or "ＦＡＸ" in line:
                        continue
                    else:
                        addr_parts.append(line)
                if addr_parts:
                    addr = " ".join(addr_parts)
                    m = _PREF_RE.match(addr)
                    if m:
                        data[Schema.PREF] = m.group(1)
                        data[Schema.ADDR] = addr[m.end():].strip()
                    else:
                        data[Schema.ADDR] = addr

            elif label == "代表者名" or label == "代表者":
                data[Schema.REP_NM] = _clean(dd.get_text())

            elif label == "事業内容":
                data[Schema.LOB] = _clean(dd.get_text())

            elif label == "URL" or label == "ホームページ":
                a = dd.select_one("a[href]")
                if a:
                    data[Schema.HP] = a["href"].strip()
                else:
                    data[Schema.HP] = _clean(dd.get_text())

            elif label == "サービス地域":
                data["サービス地域"] = _clean(dd.get_text())

            elif label == "業種":
                data[Schema.CAT_SITE] = _clean(dd.get_text())

        # 仕事内容（事業所単位では初出時の値）
        recruit = soup.select_one("div.detail-recruitInfo")
        if recruit:
            for dl in recruit.select("dl"):
                dt = dl.select_one("dt")
                dd = dl.select_one("dd")
                if not dt or not dd:
                    continue
                if dt.get_text(strip=True) == "仕事内容":
                    data["仕事内容"] = _clean(dd.get_text(" "))[:500]
                    break

        # 会社名フォールバック
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                name = _clean(h1.get_text())
                name = re.sub(r"のアルバイト・パートの求人情報.*$", "", name)
                data[Schema.NAME] = name

        if not data.get(Schema.NAME):
            return None
        return data

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------

    @staticmethod
    def _facility_key(item: dict) -> tuple[str, str] | None:
        """事業所識別キー: (社名, 住所) を空白除去で正規化したタプル"""
        name = item.get(Schema.NAME, "") or ""
        addr = item.get(Schema.ADDR, "") or ""
        if not name:
            return None
        norm_name = re.sub(r"\s+", "", name)
        norm_addr = re.sub(r"\s+", "", addr)
        return (norm_name, norm_addr)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruScraper()
    scraper.execute(BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

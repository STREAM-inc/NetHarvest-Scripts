"""
バイトル — 全国求人企業情報スクレイパー（baitoru.com）

取得対象:
    - 詳細ページの企業情報セクション (div.detail-companyInfo)
        社名, 所在地(住所+TEL), 代表者名, 事業内容, HP URL, サービス地域
    - 詳細ページの基本情報セクション (div.detail-basicInfo)
        職種, 給与, 勤務時間
    - 詳細ページの募集情報セクション (div.detail-recruitInfo)
        仕事内容

取得フロー:
    7地方 × 47都道府県ループ
    → 一覧ページ /{region}/jlist/{pref}/ を link[rel="next"] でページ巡回
    → 各求人詳細ページから企業・求人情報を取得
    → 詳細URLで重複除外

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

# (region, pref_roman, pref_ja) — バイトル独自表記: gumma(群馬), nigata(新潟)
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


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class BaitoruScraper(StaticCrawler):
    """バイトル 求人企業情報スクレイパー（全国47都道府県巡回）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["サービス地域", "職種", "給与", "勤務時間", "仕事内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_details: set[str] = set()
        for region, pref_roman, pref_ja in PREFECTURES:
            list_url = f"{BASE_URL}/{region}/jlist/{pref_roman}/"
            self.logger.info("都道府県: %s (%s)", pref_ja, list_url)
            yield from self._scrape_pref(list_url, pref_ja, seen_details)

    def _scrape_pref(
        self, list_url: str, pref_ja: str, seen: set
    ) -> Generator[dict, None, None]:
        current = list_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            articles = soup.select("article.list-jobListDetail")
            if not articles:
                break

            for article in articles:
                a = article.select_one("h3 a[href]") or article.select_one(
                    "a[href*='/job']"
                )
                if not a:
                    continue
                href = a.get("href", "").strip()
                detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                # クエリ除去（?pname=... など）で重複判定を正規化
                detail_url = detail_url.split("?")[0]
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                item = self._scrape_detail(detail_url, pref_ja)
                if item and item.get(Schema.NAME):
                    yield item

            next_link = soup.find("link", rel="next")
            if next_link and next_link.get("href"):
                href = next_link["href"]
                next_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                current = next_url if next_url != current else None
            else:
                current = None

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_ja}

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
                    # フォールバック: dd全テキストから「この会社の…」を除外
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
                            # FAX部分を除去
                            tel = re.split(r"FAX|ＦＡＸ", tel)[0].strip()
                            if tel:
                                data[Schema.TEL] = tel
                        # TEL 行にも住所が混ざっている場合に備え、TEL: より前を住所候補に
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

        # 基本情報セクション
        basic = soup.select_one("div.detail-basicInfo")
        if basic:
            dl01 = basic.select_one("dl.dl01")
            if dl01:
                dd = dl01.select_one("dd")
                if dd:
                    data["職種"] = _clean(dd.get_text(" "))[:500]

            dl02 = basic.select_one("dl.dl02")
            if dl02:
                dd = dl02.select_one("dd")
                if dd:
                    data["給与"] = _clean(dd.get_text(" "))[:500]

            dl03 = basic.select_one("dl.dl03")
            if dl03:
                dd = dl03.select_one("dd")
                if dd:
                    data["勤務時間"] = _clean(dd.get_text(" "))[:500]

        # 募集情報セクション — 仕事内容
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

        # 会社名フォールバック: h1 から「〜の求人情報」を削除
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                name = _clean(h1.get_text())
                name = re.sub(r"のアルバイト・パートの求人情報.*$", "", name)
                data[Schema.NAME] = name

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruScraper()
    scraper.execute(f"{BASE_URL}/kanto/jlist/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

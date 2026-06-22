"""
KeePer PROSHOP (洗車関連) — KeePerコーティング技術認定店スクレイパー

取得対象:
    - 全国の KeePer PROSHOP (洗車・コーティング提供店) の店舗情報
    - 名称 / 運営会社 / 住所 / 都道府県 / TEL / 営業時間 / 定休日 /
      洗車・コーティング営業時間 / 取扱いサービス(対応メニュー)
    - 営業時間を曜日ごとに分解したカラム (営業時間[月] … 営業時間[日])。
      「平日」表記は月火水木金に展開する。
    - 資格者の人数 (EXキーパー資格者 / 1級資格者 / 2級資格者)

取得フロー (3階層):
    /proshop                      … 都道府県インデックス (都道府県リンク)
      → /proshop/{pref}           … 市区町村インデックス (cityN リンク)
        → /proshop/{pref}/cityN   … 店舗一覧 (店舗詳細リンク)
          → /proshop/{pref}/cityN/{id}  … 店舗詳細ページ (ここで各フィールド取得)

    各店舗詳細を取得するたびに即 yield する (Pattern B / 早期 yield)。
    備考の「中部地方を優先取得」に従い、都道府県の巡回順を中部地方→その他の順に並べ替える。
    フィルタではなく優先順位付けのため、全国 (約6,658店舗) を取得対象とする。

実行方法:
    # ローカルテスト
    python scripts/sites/auto/keeperproshop.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id keeperproshop
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

# 中部地方の都道府県スラッグ (この順で優先的に巡回する)
# nigata は当サイトの新潟県スラッグ表記 (niigata ではない)
_CHUBU_SLUGS = [
    "nigata", "toyama", "ishikawa", "fukui",
    "yamanashi", "nagano", "gifu", "shizuoka", "aichi",
]

# 都道府県以外のリンク (検索・位置情報・言語切替) を除外する
_NON_PREF_SLUGS = {"search", "geolocation", "global"}

# 都道府県インデックスの都道府県リンク: /proshop/{slug} (単一セグメント, 英小文字)
_PREF_HREF = re.compile(r"^/proshop/([a-z]+)/?$")
# 市区町村リンク: /proshop/{slug}/cityN
_CITY_HREF = re.compile(r"^/proshop/[a-z]+/city\d+/?$")
# 店舗詳細リンク: /proshop/{slug}/cityN/{数字ID}
_SHOP_HREF = re.compile(r"^/proshop/[a-z]+/city\d+/\d+/?$")

# 住所先頭から都道府県を切り出す
_PREF_PATTERN = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")

# 営業時間を曜日ごとに分解するための定義 -------------------------------------
# 出力する曜日カラムの順序 (営業時間[月] … 営業時間[日])
_WEEKDAYS = ["月", "火", "水", "木", "金", "土", "日"]


def _time_col(day: str) -> str:
    return f"営業時間[{day}]"


# 営業時間テキスト中の時間表記 (例: 8：00～20：00 / 24時間) を抜き出す
_TIME_SPAN = re.compile(
    r"\d{1,2}\s*[:：]\s*\d{2}\s*[~〜～\-－]\s*\d{1,2}\s*[:：]\s*\d{2}"
    r"|24\s*時間"
)


def _normalize_time(text: str) -> str:
    """全角コロン/チルダ等を半角に寄せ、空白を除いて読みやすくする。"""
    return (
        text.replace("：", ":")
        .replace("〜", "~")
        .replace("～", "~")
        .replace("－", "-")
        .replace(" ", "")
        .replace("　", "")
    )


def _label_to_days(label: str) -> list[str]:
    """時間表記の直前ラベルから対象曜日を判定する。

    「平日」=月火水木金。「祝(日)」は曜日カラムを持たないため除外する
    (「日祝」「日曜・祝日」のように日曜とまとめられている場合は日曜として扱う)。
    """
    days: list[str] = []
    has_heijitsu = "平日" in label
    # 「平日」「祝日」「祝」は曜日文字 (日) を含むため、個別判定前に取り除く
    rest = label.replace("平日", "").replace("祝日", "").replace("祝", "")
    if has_heijitsu:
        days += ["月", "火", "水", "木", "金"]
    if "土" in rest:
        days.append("土")
    if "日" in rest:
        days.append("日")
    for ch in ["月", "火", "水", "木", "金"]:
        if ch in rest:
            days.append(ch)
    # 重複除去 (順序保持)
    out: list[str] = []
    for d in days:
        if d not in out:
            out.append(d)
    return out


def _parse_day_hours(text: str) -> dict[str, str]:
    """営業時間テキストを {曜日: 時間} に分解する。

    例: "平日8：00～20：00 日祝8：00～19：00"
        → {月..金: "8:00~20:00", 日: "8:00~19:00"}
        "＜平日・土曜＞8:00～18:30 ＜日曜・祝日＞8:00～18：00"
        → {月..土: "8:00~18:30", 日: "8:00~18:00"}
    曜日ラベルの無い時間表記は (未設定の) 全曜日に適用する (例: "8:00~18:00" / "24時間")。
    """
    result: dict[str, str] = {}
    if not text:
        return result
    prev_end = 0
    for m in _TIME_SPAN.finditer(text):
        label = text[prev_end:m.start()]
        prev_end = m.end()
        value = _normalize_time(m.group(0))
        days = _label_to_days(label) or _WEEKDAYS
        for d in days:
            result.setdefault(d, value)
    return result


# 「資格者・施工車写真投稿数」セクションで人数を取得する資格区分
_QUAL_COLUMNS = ["EXキーパー資格者", "1級資格者", "2級資格者"]


class KeeperProshopScraper(StaticCrawler):
    """KeePer PROSHOP (洗車関連) スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = (
        ["運営会社"]
        + [_time_col(d) for d in _WEEKDAYS]
        + ["洗車・コーティング営業時間", "取扱いサービス"]
        + _QUAL_COLUMNS
    )

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 1. 都道府県インデックス → 都道府県URLを収集 (中部地方を優先)
        pref_urls = self._collect_pref_urls(url)
        self.logger.info("都道府県URL収集完了: %d 件", len(pref_urls))

        for pref_url in pref_urls:
            # 2. 都道府県ページ → 市区町村URLを収集
            for city_url in self._collect_links(pref_url, _CITY_HREF):
                # 3. 市区町村ページ → 店舗詳細URLを収集
                for shop_url in self._collect_links(city_url, _SHOP_HREF):
                    # 4. 店舗詳細を取得して即 yield (早期 yield)
                    item = self._scrape_detail(shop_url)
                    if item:
                        yield item

    def _collect_pref_urls(self, index_url: str) -> list[str]:
        """都道府県インデックスから都道府県URLを収集し、中部地方を先頭に並べ替える。"""
        soup = self.get_soup(index_url)
        if soup is None:
            return []

        slug_to_url: dict[str, str] = {}
        for a in soup.select("a[href]"):
            m = _PREF_HREF.match(a.get("href", ""))
            if not m:
                continue
            slug = m.group(1)
            if slug in _NON_PREF_SLUGS or slug in slug_to_url:
                continue
            slug_to_url[slug] = urljoin(index_url, a["href"])

        chubu = [slug_to_url[s] for s in _CHUBU_SLUGS if s in slug_to_url]
        others = [u for s, u in slug_to_url.items() if s not in _CHUBU_SLUGS]
        return chubu + others

    def _collect_links(self, list_url: str, pattern: re.Pattern) -> list[str]:
        """一覧ページから pattern に一致するリンクを収集する (重複除去)。"""
        soup = self.get_soup(list_url)
        if soup is None:
            return []
        seen: dict[str, None] = {}
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if pattern.match(href):
                seen.setdefault(urljoin(list_url, href), None)
        return list(seen.keys())

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        info = soup.select_one(".detailTitleInfo")
        if info:
            h2 = info.select_one("h2")
            if h2:
                name_b = h2.select_one("b")
                if name_b:
                    data[Schema.NAME] = name_b.get_text(strip=True)
                company = h2.select_one("span")
                if company:
                    data["運営会社"] = company.get_text(strip=True)
            tel = info.select_one(".detailTitleTel")
            if tel:
                data[Schema.TEL] = tel.get_text(strip=True)

        # 基本情報の dt/dd ペア
        for dt in soup.find_all("dt"):
            label = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd")
            if dd is None:
                continue
            value = dd.get_text(" ", strip=True)
            if label == "所在地":
                data[Schema.ADDR] = value
                pm = _PREF_PATTERN.match(value)
                if pm:
                    data[Schema.PREF] = pm.group(1)
            elif label == "電話番号":
                data.setdefault(Schema.TEL, value)
            elif label == "営業時間":
                data[Schema.TIME] = value
                # 営業時間を曜日ごとのカラムに分解 (平日=月火水木金)
                for day, hours in _parse_day_hours(value).items():
                    data[_time_col(day)] = hours
            elif label == "定休日":
                data[Schema.HOLIDAY] = value
            elif label == "洗車・コーティング営業時間":
                data["洗車・コーティング営業時間"] = value

        # 取扱いサービス (対応メニュー: 洗車・コーティング等) — アイコンの alt から取得
        services = self._extract_services(soup)
        if services:
            data["取扱いサービス"] = " / ".join(services)

        # 資格者の人数 (資格区分ごと)
        for qual, count in self._extract_qualifications(soup).items():
            data[qual] = count

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _extract_services(soup) -> list[str]:
        """「取扱いサービス」見出し直後のアイコン alt 文言を順序保持で収集する。"""
        heading = None
        for h in soup.find_all("h3"):
            if h.get_text(strip=True) == "取扱いサービス":
                heading = h
                break
        if heading is None:
            return []
        box = heading.find_next_sibling()
        if box is None:
            return []
        services = []
        for img in box.select("ul li img[alt]"):
            alt = img.get("alt", "").strip()
            if alt and alt not in services:
                services.append(alt)
        return services

    @staticmethod
    def _extract_qualifications(soup) -> dict[str, str]:
        """「資格者・施工車写真投稿数」セクションから資格区分ごとの人数を取得する。

        各 dl は dt の img[alt] に資格区分名 (例: EXキーパー資格者 / 1級資格者)、
        dd に人数 (例: 2名) を持つ。人数が無い区分 (有り/- 等) は対象外とする。
        """
        heading = None
        for h in soup.find_all("h3"):
            if h.get_text(strip=True) == "資格者・施工車写真投稿数":
                heading = h
                break
        if heading is None:
            return {}
        section = heading.find_parent("section") or heading.parent
        if section is None:
            return {}

        result: dict[str, str] = {}
        for dl in section.select("dl"):
            img = dl.select_one("dt img[alt]")
            dd = dl.select_one("dd")
            if img is None or dd is None:
                continue
            alt = img.get("alt", "").strip()
            if alt not in _QUAL_COLUMNS:
                continue
            value = dd.get_text(" ", strip=True)
            # 「N名」形式のみ採用 (「-」「有り」等は人数ではないので除外)
            if "名" in value:
                result[alt] = value
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KeeperProshopScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.keepercoating.jp/proshop")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

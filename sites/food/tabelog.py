"""
食べログ — 新着オープン店舗の全国巡回スクレイパー

用途:
    食べログの都道府県別「新規オープン(ニューオープン順)」一覧を全国47都道府県について
    巡回し、直近で開店した飲食店(全ジャンル)を優先的に収集する。

取得フロー:
    エントリ URL (https://tabelog.com/) を起点に、各都道府県の新着オープン一覧
        {pref}/rstLst/cond16-00-00/{page}/   (cond16 = ニューオープン順, 20件/ページ)
    を「ページ番号でラウンドロビン」してクロールする。
    page=1 は各都道府県で最も新しい開店店舗のため、全県の page=1 → 全県の page=2 …
    の順に回ることで、直近オープン店舗を全国横断で優先的に取得できる。
    各一覧の .list-rst → 詳細ページ URL を得て、詳細ページから全カラムを抽出し、
    fetch 直後に 1 件ずつ yield する (全件収集後 yield は時間切れで 0 件になるため)。

実行方法:
    # ローカルテスト
    python scripts/sites/food/tabelog.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id tabelog
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


_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 食べログの都道府県 URL スラッグ (47 件)。エントリ URL からの相対で使用する。
_PREF_SLUGS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie",
    "shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi",
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki",
    "kagoshima", "okinawa",
]

# 各都道府県あたりの巡回ページ上限 (食べログは全国/エリア検索を最大60ページでキャップ)。
_MAX_PAGES = 60
# 一覧アイテム内の「YYYY年M月D日オープン」/「YYYY年M月オープン」表記からオープン日を拾う
_LIST_OPEN_PATTERN = re.compile(r"(\d{4}\s*年\s*\d{1,2}\s*月(?:\s*\d{1,2}\s*日)?)\s*オープン")
_HOLIDAY_PATTERN = re.compile(r"定休日[ :：]*([^■]+)")
_BUDGET_PATTERN = re.compile(r"￥[\d,]+[～〜](?:￥[\d,]+)?")
# 詳細ページ URL 判定 (関連店舗抽出でチェーン店リンクを拾うため)
_DETAIL_URL_PATTERN = re.compile(r"https?://tabelog\.com/[a-z]+/[^/]+/[^/]+/\d+/?$")


class TabelogScraper(StaticCrawler):
    """食べログ 新着オープン店舗スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "席数",
        "評価点",
        "口コミ数",
        "予算_夜",
        "予算_昼",
        "予算_口コミ集計",
        "サービス",
        "関連店舗情報",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして、新着オープン一覧 URL を派生させる。
        # ページ番号を外側ループにしたラウンドロビン (全県 page=1 → 全県 page=2 …) で
        # 各県の最新オープン店舗を全国横断で優先取得する。
        seen: set[str] = set()
        count = 0
        for page in range(1, _MAX_PAGES + 1):
            any_items = False
            for slug in _PREF_SLUGS:
                list_url = urljoin(
                    url, "{slug}/rstLst/cond16-00-00/{page}/".format(slug=slug, page=page)
                )
                soup = self.get_soup(list_url)
                if soup is None:
                    continue
                items = soup.select(".list-rst")
                if not items:
                    continue
                any_items = True

                for item in items:
                    detail_url = item.get("data-detail-url") or ""
                    if not detail_url:
                        a = item.select_one("h3.list-rst__rst-name a")
                        if a and a.get("href"):
                            detail_url = a["href"].strip()
                    if not detail_url:
                        continue
                    detail_url = urljoin(url, detail_url)
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    # 一覧に出ているオープン日を fallback として拾う
                    om = _LIST_OPEN_PATTERN.search(item.get_text(" ", strip=True))
                    open_hint = re.sub(r"\s+", "", om.group(1)) if om else ""

                    try:
                        data = self._scrape_detail(detail_url, open_hint)
                    except Exception as e:
                        self.logger.warning("詳細取得エラー: %s (%s)", detail_url, e)
                        continue
                    if data:
                        count += 1
                        yield data

            if not any_items:
                self.logger.info("page=%d: 全県 0件 → 終了", page)
                break
        self.logger.info("取得完了: 累計 %d 件", count)

    def _scrape_detail(self, url: str, open_hint: str = "") -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        name_el = soup.select_one("h2.display-name")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)

        # 全 th-td ペアを辞書化
        fields: dict = {}
        for table in soup.select("table.rstinfo-table__table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    key = re.sub(r"\s+", "", th.get_text())
                    if key not in fields:
                        fields[key] = td

        def _text(key: str) -> str:
            td = fields.get(key)
            if td is None:
                return ""
            txt = td.get_text(" ", strip=True)
            txt = txt.replace("大きな地図を見る", "").replace("周辺のお店を探す", "").strip()
            return re.sub(r"\s+", " ", txt)

        # 店名カナ (fields["店名"] の "（...）" 内)
        shop_name_td = fields.get("店名")
        if shop_name_td:
            inner = shop_name_td.get_text(" ", strip=True)
            m = re.search(r"（([^）]+)）", inner)
            if m:
                kana = m.group(1).strip()
                # 「【旧店名】xxx」は除外
                if not kana.startswith("【"):
                    data[Schema.NAME_KANA] = kana

        # 住所 → 都道府県抽出
        addr_p = soup.select_one(".rstinfo-table__address")
        if addr_p:
            addr_text = addr_p.get_text(" ", strip=True)
            addr_text = addr_text.replace("大きな地図を見る", "").replace("周辺のお店を探す", "").strip()
            addr_text = re.sub(r"\s+", " ", addr_text)
        else:
            addr_text = _text("住所")
        if addr_text:
            m = _PREF_PATTERN.match(addr_text)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = addr_text[m.end():].strip()
            else:
                data[Schema.ADDR] = addr_text

        tel = _text("電話番号")
        if tel:
            data[Schema.TEL] = tel

        genre = _text("ジャンル")
        if genre:
            data[Schema.CAT_SITE] = genre

        hp_td = fields.get("ホームページ")
        if hp_td:
            a = hp_td.find("a", href=True)
            if a:
                data[Schema.HP] = a["href"].strip()
            else:
                hp_txt = hp_td.get_text(strip=True)
                if hp_txt:
                    data[Schema.HP] = hp_txt

        hours = _text("営業時間")
        if hours:
            data[Schema.TIME] = hours
            hm = _HOLIDAY_PATTERN.search(hours)
            if hm:
                data[Schema.HOLIDAY] = hm.group(1).strip()

        # オープン日 (= 設立年月日)。詳細に無ければ一覧の表記を fallback に使う。
        open_date = _text("オープン日")
        if not open_date and open_hint:
            open_date = open_hint
        if open_date:
            data[Schema.OPEN_DATE] = open_date

        # EXTRA_COLUMNS (単純な th→td マッピング)
        for key in ("席数", "サービス"):
            val = _text(key)
            if val:
                data[key] = val

        budget_td = fields.get("予算")
        if budget_td:
            btxt = budget_td.get_text(" ", strip=True)
            parts = _BUDGET_PATTERN.findall(btxt)
            if parts:
                data["予算_夜"] = parts[0]
                if len(parts) > 1:
                    data["予算_昼"] = parts[1]

        budget_review_td = fields.get("予算（口コミ集計）")
        if budget_review_td:
            btxt = budget_review_td.get_text(" ", strip=True).replace("利用金額分布を見る", "").strip()
            data["予算_口コミ集計"] = re.sub(r"\s+", " ", btxt)

        rating_el = soup.select_one(
            ".rdheader-rating__score-val, .rdheader-rating__score-val-text, .rating-val"
        )
        if rating_el:
            rv = rating_el.get_text(strip=True)
            if re.match(r"^\d+\.\d+$", rv):
                data["評価点"] = rv

        review_el = soup.select_one(".rdheader-rating__review-target em, .rvw-count-num")
        if review_el:
            rc = re.sub(r"[^0-9]", "", review_el.get_text())
            if rc:
                data["口コミ数"] = rc

        # 関連店舗情報 (系列店 / 関連店舗): 同一運営の他店舗名を収集 (無ければ空欄)
        related = self._extract_related(soup, url, data.get(Schema.NAME, ""))
        if related:
            data["関連店舗情報"] = related

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _extract_related(soup, self_url: str, self_name: str) -> str:
        """「系列店」「関連店舗」見出しの近傍から他店舗名を集めて " / " 連結で返す。"""
        names: list[str] = []
        seen_names: set[str] = set()
        for el in soup.find_all(string=re.compile(r"系列店|関連店舗")):
            heading = getattr(el, "parent", None)
            if heading is None:
                continue
            # 見出し自体が短いテキストであることを確認 (本文中の言及を誤検出しない)
            heading_txt = heading.get_text(" ", strip=True)
            if len(heading_txt) > 30:
                continue
            # 見出しの親コンテナ内から店舗詳細リンクを収集する
            container = heading.parent or heading
            for a in container.find_all("a", href=True):
                href = urljoin(self_url, a["href"].strip())
                if not _DETAIL_URL_PATTERN.match(href):
                    continue
                if href.rstrip("/") == self_url.rstrip("/"):
                    continue
                nm = a.get_text(strip=True)
                if nm and nm != self_name and nm not in seen_names:
                    seen_names.add(nm)
                    names.append(nm)
        return " / ".join(names)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TabelogScraper()
    scraper.execute("https://tabelog.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

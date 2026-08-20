"""
エステラブワーク (job.eslove.jp) — 全国のメンズエステ求人ポータル

取得フロー (備考「全リストがない。各都道府県から調べてリストを検索する必要がある」を尊重):
    1. ルート URL (引数 url) を取得し、フッター/エリアナビから 47 都道府県の
       `/{region}/{pref}/` リンクを抽出する (全国横断の一覧ページは存在しない)
    2. 各都道府県の求人一覧 `/{region}/{pref}/search/` を `?page=N` で巡回
       (1 ページ 40 件。ページ上の「全 N 件」から最終ページを算出する)
       ※ 範囲外の page を指定すると 1 ページ目が返る仕様のため、
         算出した最終ページ + 新規 ID ゼロで打ち切る二重ガードを入れている
    3. 一覧カード (div.listFrame) から都道府県・エリア・掲載プランを取得し、
       求人詳細 `/detail/{id}` を 1 件ずつ取得して即 yield する (Pattern B)
    4. 詳細ページの店舗基本情報テーブル (店舗名/業種/電話番号/住所/営業時間/定休日/
       ホームページ/公式アカウント) と募集要項テーブルのタグ類を抽出する

アクセス上の注意:
    Google Cloud Armor により通常のブラウザ UA (Chrome/Safari) は全パス 403 で拒否される
    (データセンター IP 起因)。クローラー用 UA を指定した場合のみ 200 が返るため
    USER_AGENT を上書きしている。robots.txt は Disallow を持たない (sitemap 宣言のみ)。

利用規約 (https://job.eslove.jp/terms):
    スクレイピング・クローリング・自動取得を明示的に禁止する条項は無い。
    ただし「本サイトを利用した、営利を目的とした行為」「商業目的で利用(複製・複写等)する行為」
    が禁止行為に挙げられているため、取得データの利用範囲には注意すること。

取得しないフィールド (著作権リスク: 自由記述プロース):
    仕事内容 / 福利厚生・待遇 / 勤務時間・勤務日・応募資格の本文 / お店からのメッセージ /
    キャッチコピー / お店紹介文 / アクセス (道順の説明文) / 求人タイトル
    → いずれも店舗が書いた宣伝文のため、構造化タグ (shopDataTag) のみ取得する

実行方法:
    python scripts/sites/jobs/job_7.py
    docker compose exec worker python /app/bin/run_flow.py --site-id job_7
"""

import logging
import math
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# ルートページ上の都道府県リンク `/{region}/{pref}/` (例: /kanto/tokyo/)
_PREF_LINK_PATTERN = re.compile(r"^/([a-z][a-z\-]+)/([a-z][a-z\-]+)/$")
# 都道府県リンクと同じ形をした非エリアページを除外する
_NOT_AREA_SEGMENTS = {"user", "campaign", "detail", "shop", "cliplist", "search", "sitemap"}

# 一覧ページの「1～40 件を表示 ／ 全 972 件」から総件数を取得する
_TOTAL_PATTERN = re.compile(r"全\s*([\d,]+)\s*件")
# 詳細ページ URL から求人 ID を取得する
_DETAIL_ID_PATTERN = re.compile(r"/detail/(\d+)")
# LINE ID (例: LINE ID検索 : @394rmemc)
_LINE_ID_PATTERN = re.compile(r"@[\w.\-]+")

# 1 ページあたりの掲載件数 (一覧ページ実測値)
_PER_PAGE = 40
# 万一「全 N 件」が取得できなかった場合の保険 (新規 ID ゼロで打ち切るため通常は到達しない)
_MAX_PAGES = 300

# 都道府県名の正規化 (パンくず「東京メンズエステ求人」→「東京都」用のフォールバック)
_PREF_SUFFIX = {
    "北海道": "北海道",
    "東京": "東京都",
    "大阪": "大阪府",
    "京都": "京都府",
}
# 住所文字列から都道府県を拾うためのパターン
_ADDR_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class EsloveWork(StaticCrawler):
    """エステラブワーク スクレイパー"""

    # Google Cloud Armor がブラウザ UA を 403 で拒否するため、クローラー UA を使用する
    USER_AGENT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    DELAY = 1.0

    EXTRA_COLUMNS = [
        "エリア",                    # 一覧カード data-local-area (例: 渋谷)
        "掲載プラン",                # 一覧カードの掲載ランク (例: プラチナ)
        "募集職種",                  # 募集要項見出し (例: セラピスト)
        "雇用形態",                  # 業務委託 / アルバイト 等
        "給与レンジ",                # span.pay (例: 日給3万円～7万円可能)
        "給与条件",                  # 給与欄のタグ (例: 日払いOK / 歩合あり)
        "勤務日条件",                # 勤務日欄のタグ (例: 自由出勤 / 週3以下OK)
        "応募資格条件",              # 応募資格欄のタグ (例: 未経験歓迎 / 短期OK)
        "お店の特徴",                # タグ (例: 新店オープン / 20代活躍中)
        "職場環境の特徴",            # タグ (例: 研修あり / 送迎あり)
        "応募時のメリット",          # タグ (例: 面接交通費支給)
        "最寄り駅",                  # 勤務地欄の駅タグ
        "出張エリア",                # 派遣・出張型の対応エリア
        "求人ホームページ",          # ホームページ（求人）の URL
        "店舗情報ページURL",         # eslove.jp 側の店舗ページ URL
        "系列店",                    # 系列店の店舗名
        "在籍セラピストの年齢層",
        "在籍セラピストの人数",
        "在籍セラピストの未経験者率",
        "1日の平均接客人数",
        "ルーム数",
        "お客さんの年齢層",
        "お客さんのタイプ",
        "お客さんが多い時間帯",
    ]

    # atmosphereChart (お店の環境・雰囲気) で拾うラベル
    _ATMOSPHERE_KEYS = (
        "在籍セラピストの年齢層",
        "在籍セラピストの人数",
        "在籍セラピストの未経験者率",
        "1日の平均接客人数",
        "ルーム数",
        "お客さんの年齢層",
        "お客さんのタイプ",
        "お客さんが多い時間帯",
    )

    def prepare(self):
        """HTML を明示的に要求するヘッダを付与する。"""
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )
        self._seen_ids: set[str] = set()
        self._total_estimate = 0

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        # 1. ルートページから都道府県の一覧 URL を組み立てる (引数 url が唯一のルート)
        pref_urls = self._collect_pref_search_urls(url)
        if not pref_urls:
            logger.error("都道府県リンクを抽出できませんでした: %s", url)
            return
        logger.info("都道府県: %d 件", len(pref_urls))

        # 2. 都道府県ごとに一覧を巡回 → 詳細を 1 件ずつ取得して即 yield
        for pref_search_url in pref_urls:
            yield from self._crawl_prefecture(pref_search_url)

    # ------------------------------------------------------------------
    # 都道府県リンクの抽出
    # ------------------------------------------------------------------
    def _collect_pref_search_urls(self, root_url: str) -> list[str]:
        soup = self.get_soup(root_url)
        if soup is None:
            return []

        search_urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            path = urlsplit(href).path if href.startswith("http") else href
            m = _PREF_LINK_PATTERN.match(path)
            if not m:
                continue
            region, pref = m.group(1), m.group(2)
            if region in _NOT_AREA_SEGMENTS or pref in _NOT_AREA_SEGMENTS:
                continue
            search_url = urljoin(root_url, f"{region}/{pref}/search/")
            if search_url in seen:
                continue
            seen.add(search_url)
            search_urls.append(search_url)
        return search_urls

    # ------------------------------------------------------------------
    # 都道府県単位の一覧巡回
    # ------------------------------------------------------------------
    def _crawl_prefecture(self, search_url: str):
        max_page = _MAX_PAGES
        for page in range(1, _MAX_PAGES + 1):
            page_url = search_url if page == 1 else f"{search_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                return

            if page == 1:
                total = self._read_total(soup)
                if total:
                    # 総件数から最終ページを算出 (範囲外 page は 1 ページ目が返る仕様の対策)
                    max_page = max(1, math.ceil(total / _PER_PAGE))
                    self._total_estimate += total
                    self.total_items = self._total_estimate
                    logger.info("%s: 全 %d 件 (%d ページ)", search_url, total, max_page)
                else:
                    logger.warning("総件数を取得できませんでした: %s", page_url)

            cards = soup.select("div.listFrame")
            if not cards:
                return

            new_on_page = 0
            for card in cards:
                try:
                    item = self._scrape_card(card, page_url)
                except Exception as e:  # noqa: BLE001 — 1 件の失敗で全体を止めない
                    logger.warning("カード処理でエラー (スキップ): %s — %s", page_url, e)
                    continue
                if item is None:
                    continue
                new_on_page += 1
                yield item

            # 新規 ID ゼロ = 範囲外 page で 1 ページ目が再表示されている → 打ち切り
            if new_on_page == 0 or page >= max_page:
                return

    def _scrape_card(self, card, page_url: str) -> dict | None:
        """一覧カードの情報を取得し、詳細ページとマージした 1 レコードを返す。"""
        link = card.select_one('a[href*="/detail/"]')
        if not link:
            return None
        href = link.get("href") or ""
        m = _DETAIL_ID_PATTERN.search(href)
        if not m:
            return None
        detail_id = m.group(1)
        if detail_id in self._seen_ids:  # 複数エリアに重複掲載される店舗があるため
            return None
        self._seen_ids.add(detail_id)

        # 詳細 URL は一覧ページ URL から派生させる (ルート URL のハードコード禁止)
        detail_url = urljoin(page_url, f"/detail/{detail_id}")
        list_info = {
            "pref": (card.get("data-prefecture") or "").strip(),
            "area": (card.get("data-local-area") or "").strip(),
            "plan": self._plan_label(card),
        }
        return self._scrape_detail(detail_url, list_info)

    @staticmethod
    def _plan_label(card) -> str:
        """掲載プラン (おすすめ優良店アイコン / class 修飾子) を短いラベルで返す。"""
        icon = card.select_one("img.recommendIcon")
        if icon and (icon.get("alt") or "").strip():
            return icon["alt"].strip()
        for cls in card.get("class") or []:
            if cls.startswith("listFrame--"):
                return cls.replace("listFrame--", "")
        return ""

    @staticmethod
    def _read_total(soup) -> int | None:
        m = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
        if not m:
            return None
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url: str, list_info: dict) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        rows = self._table_rows(soup)
        name = self._row_text(rows, "店舗名") or self._row_text(rows, "応募先店舗名")
        if not name:
            logger.warning("店舗名を取得できませんでした: %s", detail_url)

        addr = self._row_text(rows, "住所")
        pref = list_info.get("pref") or self._pref_from_addr(addr) or self._pref_from_breadcrumb(soup)
        atmosphere = self._atmosphere(soup)
        sns = self._sns_links(rows.get("公式アカウント"))

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: self._row_text(rows, "電話番号"),
            Schema.CAT_SITE: self._row_text(rows, "業種"),
            Schema.TIME: self._row_text(rows, "営業時間"),
            Schema.HOLIDAY: self._row_text(rows, "定休日"),
            Schema.HP: self._row_href(rows, "ホームページ"),
            Schema.LINE: sns.get("line") or self._line_id(rows.get("LINE問い合わせ")),
            Schema.X: sns.get("x", ""),
            Schema.INSTA: sns.get("instagram", ""),
            Schema.FB: sns.get("facebook", ""),
            Schema.TIKTOK: sns.get("tiktok", ""),
            "エリア": list_info.get("area", ""),
            "掲載プラン": list_info.get("plan", ""),
            "募集職種": self._job_title(soup),
            "雇用形態": self._row_text(rows, "雇用形態"),
            "給与レンジ": self._row_span(rows, "給与", "span.pay"),
            "給与条件": self._row_tags(rows, "給与"),
            "勤務日条件": self._row_tags(rows, "勤務日"),
            "応募資格条件": self._row_tags(rows, "応募資格・条件"),
            "お店の特徴": self._row_tags(rows, "お店の特徴"),
            "職場環境の特徴": self._row_tags(rows, "職場環境の特徴"),
            "応募時のメリット": self._row_tags(rows, "応募時のメリット"),
            "最寄り駅": self._stations(rows.get("勤務地")),
            "出張エリア": self._row_text(rows, "出張エリア"),
            "求人ホームページ": self._row_href(rows, "ホームページ（求人）"),
            "店舗情報ページURL": self._row_href(rows, "お店紹介"),
            "系列店": self._affiliates(rows.get("系列店")),
        }
        for key in self._ATMOSPHERE_KEYS:
            item[key] = atmosphere.get(key, "")
        return item

    # ------------------------------------------------------------------
    # 詳細ページのパーツ抽出
    # ------------------------------------------------------------------
    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    @classmethod
    def _text(cls, el) -> str:
        return cls._clean(el.get_text(" ", strip=True)) if el else ""

    @classmethod
    def _table_rows(cls, soup) -> dict:
        """詳細ページの全テーブルを {ラベル: tr} で引けるようにする (先勝ち)。"""
        rows: dict = {}
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = cls._text(th)
            if label and label not in rows:
                rows[label] = tr
        return rows

    @classmethod
    def _row_text(cls, rows: dict, label: str) -> str:
        tr = rows.get(label)
        return cls._text(tr.find("td")) if tr else ""

    @classmethod
    def _row_tags(cls, rows: dict, label: str) -> str:
        """自由記述本文を除き、構造化タグ (div.shopDataTag span) のみを連結する。"""
        tr = rows.get(label)
        if not tr:
            return ""
        tags = [cls._text(s) for s in tr.select("div.shopDataTag span")]
        if not tags:
            # table.opt (お店の特徴 等) はタグのみで構成されるため td 全体を採用する
            td_text = cls._text(tr.find("td"))
            return td_text if label.endswith("特徴") or label == "応募時のメリット" else ""
        return " / ".join(t for t in tags if t)

    @classmethod
    def _row_span(cls, rows: dict, label: str, selector: str) -> str:
        tr = rows.get(label)
        return cls._text(tr.select_one(selector)) if tr else ""

    @classmethod
    def _row_href(cls, rows: dict, label: str) -> str:
        tr = rows.get(label)
        if not tr:
            return ""
        a = tr.select_one("td a[href]")
        return (a.get("href") or "").strip() if a else ""

    @classmethod
    def _stations(cls, tr) -> str:
        if not tr:
            return ""
        stations = [cls._text(p) for p in tr.select("p.areaBlock__tag")]
        return " / ".join(s for s in stations if s)

    @classmethod
    def _affiliates(cls, tr) -> str:
        if not tr:
            return ""
        names = [cls._text(a) for a in tr.select("td a")]
        return " / ".join(n for n in names if n)

    @classmethod
    def _line_id(cls, tr) -> str:
        if not tr:
            return ""
        m = _LINE_ID_PATTERN.search(cls._text(tr.find("td")))
        return m.group(0) if m else ""

    @classmethod
    def _sns_links(cls, tr) -> dict:
        """公式アカウント欄のリンクをドメインで振り分ける。"""
        result: dict = {}
        if not tr:
            return result
        for a in tr.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            low = href.lower()
            if "instagram.com" in low:
                result.setdefault("instagram", href)
            elif "twitter.com" in low or "//x.com" in low:
                result.setdefault("x", href)
            elif "facebook.com" in low:
                result.setdefault("facebook", href)
            elif "tiktok.com" in low:
                result.setdefault("tiktok", href)
            elif "line.me" in low:
                result.setdefault("line", href)
        return result

    @classmethod
    def _job_title(cls, soup) -> str:
        """募集要項見出し「セラピストの募集要項」から職種名のみを取り出す。"""
        h3 = soup.select_one("#recruit_condition h3 span.em")
        return cls._text(h3)

    @classmethod
    def _atmosphere(cls, soup) -> dict:
        """お店の環境・雰囲気 (選択済みの目盛ラベル) を取得する。"""
        result: dict = {}
        for li in soup.select("ul.atmosphereChart li.atmosphereChart__list"):
            label = cls._text(li.select_one(".atmosphereChart__ttl"))
            if not label:
                continue
            values = [
                cls._text(s)
                for s in li.select(".atmosphereChart__graphItem.-select .atmosphereChart__graplabel")
            ]
            result[label] = " / ".join(v for v in values if v)
        return result

    @classmethod
    def _pref_from_addr(cls, addr: str) -> str:
        m = _ADDR_PREF_PATTERN.search(addr or "")
        return m.group(1) if m else ""

    @classmethod
    def _pref_from_breadcrumb(cls, soup) -> str:
        """パンくず「東京メンズエステ求人」→「東京都」に正規化する。"""
        for a in soup.select('[class*="bread"] a'):
            text = cls._text(a)
            if not text.endswith("メンズエステ求人"):
                continue
            base = text.replace("メンズエステ求人", "").strip()
            if not base or base == "メンズエステ求人TOP":
                continue
            if base in _PREF_SUFFIX:
                return _PREF_SUFFIX[base]
            candidate = f"{base}県"
            if _ADDR_PREF_PATTERN.fullmatch(candidate):
                return candidate
        return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = EsloveWork()
    scraper.execute("https://job.eslove.jp/")

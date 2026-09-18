"""
買取大吉 (kaitori_daikichi) — 新店舗オープン情報スクレイパー

取得対象:
    買取大吉 (kaitori-daikichi.jp) の「新店舗オープン」一覧に掲載された新規
    オープン店舗 (調査時点で 131 ページ / 約 1,301 件)。
    店舗名 / 都道府県 / 市区町村 / 住所 / TEL / 営業時間 / 定休日 /
    メールアドレス / オープン日 / 買取品目 / 設備・サービス / 駐車場 /
    店長ブログ URL を取得する。

取得フロー:
    1. ルート URL (新店舗オープン一覧) の article.st-NewsUnit から記事 URL と
       記事タイトルを列挙する。ページ送りは `page/{N}/` (新しい順・10件/ページ)。
    2. 各オープン記事 (/open/open-XXXXXXX/) から本文のラベル値
       (オープン日 / 住所 / 営業時間 / 定休日 / 電話番号) と、
       店舗詳細ページ (/store/<スラッグ>/) の URL を取得する。
       ※ 本文中のアンカー href が別店舗を指している記事があるため、
          店舗 URL は本文テキスト中の URL 文字列を優先する。
    3. 店舗詳細ページから店舗名 (h1) / 店舗情報テーブル (営業時間・定休日・
       TEL・FAX・MAIL・住所) / 店長ブログ URL / 買取品目 / 設備・サービス /
       駐車場情報 / パンくず (地区) を取得し、記事本文より優先して採用する。
    4. 1 件組み立てるたびに即 yield する (Pattern B / 早期 yield)。

備考 (依頼元の指示):
    - 記事タイトル「【都道府県 市区町村】○月○日『買取大吉 ○○店』OPEN!!」から
      都道府県・市区町村・オープン日・店舗名をパースする。
    - 本サイトはオープン当日〜直前に記事が公開される運用のため、10月オープン分は
      10月末、11月オープン分は11月末〜12月頭に実行し、2回分を UNION する
      (コード側のフィルタではなく実行タイミングで担保する)。

注意:
    - 店舗個別 LP URL および店舗固有 SNS アカウントは本サイトに存在しない。
      店舗詳細ページの SNS リンクは全店共通の公式アカウント (フッター) のみ
      なので取得しない。店舗別の外部リンクは店長ブログのみ。
    - 郵便番号は店舗情報テーブルに掲載が無いため取得しない。
    - 自由記述の文章 (店長のご挨拶 / アクセス案内 / お客様の声 / FAQ 等) は
      著作権リスク回避のため取得しない。
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。

実行方法:
    # ローカルテスト
    python scripts/sites/service/kaitori_daikichi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kaitori_daikichi
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


# 都道府県 (記事タイトルの【】内・住所先頭の切り出しに使用)
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_PATTERN = re.compile("(" + "|".join(_PREFS) + ")")

# 記事タイトル: 【愛知県豊川市】\t9月18日\t『買取大吉 ドミー小坂井店』OPEN!!
_TITLE_AREA_PATTERN = re.compile(r"【([^】]+)】")
_TITLE_NAME_PATTERN = re.compile(r"『([^』]+)』")
_TITLE_DATE_PATTERN = re.compile(r"(\d{1,2})月(\d{1,2})日")

# 記事本文: オープン日：2026年9月18日
_FULL_DATE_PATTERN = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
# 記事本文中の店舗詳細ページ URL
_STORE_URL_PATTERN = re.compile(r"https?://[\w.\-]*kaitori-daikichi\.jp/store/([\w\-]+)/?")
# ページ送りの最終ページ番号
_PAGE_NUM_PATTERN = re.compile(r"/page/(\d+)/")

# 記事本文のラベル → 値 (全角/半角コロン混在)
_BODY_LABELS = {
    "オープン日": "open_date",
    "住所": "addr",
    "営業時間": "time",
    "定休日": "holiday",
    "電話番号": "tel",
    "TEL": "tel",
}

# サイト固有の構造化カラム (いずれも短いラベル/URL。自由記述は含めない)
_COL_CITY = "市区町村"
_COL_AREA = "地区"
_COL_SLUG = "店舗スラッグ"
_COL_FAX = "FAX"
_COL_BLOG = "店長ブログURL"
_COL_SERVICE = "設備・サービス"
_COL_PARKING = "駐車場"
_COL_PARKING_FEE = "駐車場料金"

# 安全弁: ページ送りの上限 (無限ループ防止)
_MAX_PAGES = 400
# 1記事あたりに試す店舗詳細ページ候補の上限 (無駄な通信を防ぐ)
_MAX_STORE_CANDIDATES = 2


class KaitoriDaikichi(StaticCrawler):
    """買取大吉 新店舗オープン情報スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        _COL_CITY,
        _COL_AREA,
        _COL_SLUG,
        _COL_FAX,
        _COL_BLOG,
        _COL_SERVICE,
        _COL_PARKING,
        _COL_PARKING_FEE,
    ]

    def parse(self, url: str):
        last_page = None
        seen_articles = set()

        page = 1
        while page <= _MAX_PAGES:
            list_url = url if page == 1 else urljoin(url, f"page/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                last_page = self._detect_last_page(soup)
                if last_page:
                    # 1ページ10件 (最終ページのみ端数) → 概算件数を ETA 用に設定
                    self.total_items = last_page * 10

            articles = self._extract_articles(soup, list_url)
            if not articles:
                break

            for article_url, title in articles:
                if article_url in seen_articles:
                    continue
                seen_articles.add(article_url)
                try:
                    item = self._scrape_article(article_url, title)
                except Exception as e:  # 個別記事の失敗は握りつぶして継続
                    self.logger.warning("記事取得失敗 %s — %s", article_url, e)
                    continue
                if item:
                    yield item

            if last_page and page >= last_page:
                break
            page += 1

    # ------------------------------------------------------------------ 一覧
    def _extract_articles(self, soup, list_url: str) -> list[tuple[str, str]]:
        """一覧ページから (記事URL, 記事タイトル) を抽出する。"""
        results = []
        for unit in soup.select("article.st-NewsUnit"):
            link = unit.select_one("h3.st-NewsUnit_Title a[href]")
            if not link:
                continue
            title = re.sub(r"\s+", " ", link.get_text(" ", strip=True)).strip()
            results.append((urljoin(list_url, link["href"]), title))
        return results

    def _detect_last_page(self, soup) -> int | None:
        """ページネーションから最終ページ番号を取得する。"""
        nums = []
        for a in soup.select("nav.pagination a.page-numbers[href]"):
            m = _PAGE_NUM_PATTERN.search(a["href"])
            if m:
                nums.append(int(m.group(1)))
        return max(nums) if nums else None

    # ------------------------------------------------------------ オープン記事
    def _scrape_article(self, article_url: str, list_title: str) -> dict | None:
        soup = self.get_soup(article_url)
        if soup is None:
            return None

        item = {Schema.URL: article_url}

        # 記事タイトル (詳細ページの h1 を優先。無ければ一覧のタイトル)
        h1 = soup.select_one("h1.nsin-Article_Title")
        title = re.sub(r"\s+", " ", h1.get_text(" ", strip=True)).strip() if h1 else list_title

        # --- タイトルから 都道府県 / 市区町村 / 店舗名 / オープン日(月日) ---
        pref, city = self._split_area(title)
        if pref:
            item[Schema.PREF] = pref
        if city:
            item[_COL_CITY] = city

        m = _TITLE_NAME_PATTERN.search(title)
        if m:
            item[Schema.NAME] = m.group(1).strip()

        # 記事の掲載年 (オープン日の年が本文に無い場合のフォールバック用)
        article_year = None
        time_tag = soup.select_one("time.nsin-Article_Date[datetime]")
        if time_tag:
            ym = re.match(r"(\d{4})", time_tag["datetime"].strip())
            if ym:
                article_year = ym.group(1)

        # --- 記事本文のラベル値 ---
        body = soup.select_one("div.nsin-Article_Conteiner")
        body_text = body.get_text("\n", strip=True) if body else ""
        body_vals = self._parse_body(body_text)

        open_date = self._to_iso_date(body_vals.get("open_date", ""), title, article_year)
        if open_date:
            item[Schema.OPEN_DATE] = open_date

        # --- 店舗詳細ページ ---
        # 本文のリンクが別店舗を指す記事があるため、タイトルの都道府県と
        # 店舗ページの住所が矛盾しない候補を採用する
        store_url, store = self._resolve_store(body, article_url, pref)
        if store_url:
            item[Schema.HP] = store_url
            slug = _STORE_URL_PATTERN.search(store_url)
            if slug:
                item[_COL_SLUG] = slug.group(1)

        # 店舗詳細 → 記事本文 の優先順で値を採用
        name = store.get("name") or item.get(Schema.NAME, "")
        if name:
            item[Schema.NAME] = name

        addr = store.get("addr") or body_vals.get("addr", "")
        if addr:
            pref2, rest = self._split_addr(addr)
            item[Schema.PREF] = item.get(Schema.PREF) or pref2
            item[Schema.ADDR] = rest or addr

        for key, col in (
            ("tel", Schema.TEL),
            ("time", Schema.TIME),
            ("holiday", Schema.HOLIDAY),
        ):
            val = store.get(key) or body_vals.get(key, "")
            if val:
                item[col] = val

        for key, col in (
            ("email", Schema.EMAIL),
            ("items", Schema.CAT_SITE),
        ):
            if store.get(key):
                item[col] = store[key]

        for key, col in (
            ("fax", _COL_FAX),
            ("blog", _COL_BLOG),
            ("service", _COL_SERVICE),
            ("parking", _COL_PARKING),
            ("parking_fee", _COL_PARKING_FEE),
            ("area", _COL_AREA),
        ):
            if store.get(key):
                item[col] = store[key]

        # 市区町村がタイトルから取れない場合はパンくずから補完
        if not item.get(_COL_CITY) and store.get("city"):
            item[_COL_CITY] = store["city"]

        return item if item.get(Schema.NAME) else None

    def _split_area(self, title: str) -> tuple[str, str]:
        """【愛知県豊川市】→ ("愛知県", "豊川市")"""
        m = _TITLE_AREA_PATTERN.search(title)
        if not m:
            return "", ""
        area = m.group(1).strip()
        pm = _PREF_PATTERN.match(area)
        if not pm:
            return "", area
        return pm.group(1), area[pm.end():].strip()

    def _split_addr(self, addr: str) -> tuple[str, str]:
        """住所文字列を (都道府県, 市区町村以降) に分割する。"""
        pm = _PREF_PATTERN.match(addr)
        if not pm:
            return "", addr
        return pm.group(1), addr[pm.end():].strip()

    def _parse_body(self, body_text: str) -> dict:
        """記事本文の「ラベル：値」行を辞書化する。"""
        vals = {}
        for line in body_text.splitlines():
            line = line.strip()
            m = re.match(r"^([^：:]{2,8})\s*[：:]\s*(.+)$", line)
            if not m:
                continue
            key = _BODY_LABELS.get(m.group(1).strip())
            if key and key not in vals:
                vals[key] = m.group(2).strip()
        return vals

    def _to_iso_date(self, raw: str, title: str, article_year: str | None) -> str:
        """「2026年9月18日」→ 2026-09-18。年が無ければ記事掲載年で補完。"""
        m = _FULL_DATE_PATTERN.search(raw)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        md = _TITLE_DATE_PATTERN.search(raw) or _TITLE_DATE_PATTERN.search(title)
        if md and article_year:
            return f"{article_year}-{int(md.group(1)):02d}-{int(md.group(2)):02d}"
        return ""

    def _store_url_candidates(self, body, article_url: str) -> list[str]:
        """記事本文から店舗詳細ページ URL の候補を優先順に列挙する。

        アンカーの href が別店舗を指している記事 (URL 文字列が複数の a タグに
        分断され、途中に mailto リンクが挟まる) が存在するため、
        区切り文字なしで連結した本文テキスト中の URL 文字列を最優先し、
        次に href を候補とする。
        """
        candidates = []
        if body is None:
            return candidates

        def _add(slug: str):
            cand = urljoin(article_url, f"/store/{slug}/")
            if cand not in candidates:
                candidates.append(cand)

        # 区切り無しで連結 (分断された URL 文字列を復元する)
        for slug in _STORE_URL_PATTERN.findall(body.get_text("")):
            _add(slug)
        for a in body.select("a[href]"):
            m = _STORE_URL_PATTERN.search(a["href"])
            if m:
                _add(m.group(1))
        return candidates

    def _resolve_store(self, body, article_url: str, title_pref: str) -> tuple[str, dict]:
        """店舗詳細ページ候補を順に取得し、都道府県が矛盾しない店舗を採用する。"""
        fallback = ("", {})
        for cand in self._store_url_candidates(body, article_url)[:_MAX_STORE_CANDIDATES]:
            store = self._scrape_store(cand)
            if not store:
                continue
            store_pref, _ = self._split_addr(store.get("addr", ""))
            if not title_pref or not store_pref or store_pref == title_pref:
                return cand, store
            self.logger.warning(
                "店舗ページの都道府県が記事と不一致 (候補をスキップ): %s (%s != %s)",
                cand, store_pref, title_pref,
            )
            if not fallback[0]:
                fallback = (cand, store)
        # 全候補が不一致の場合は記事本文の値のみを使う (誤った店舗情報を混ぜない)
        if fallback[0]:
            self.logger.warning("店舗詳細を採用せず記事本文のみ使用: %s", article_url)
        return "", {}

    # ------------------------------------------------------------ 店舗詳細
    def _scrape_store(self, store_url: str) -> dict:
        soup = self.get_soup(store_url)
        if soup is None:
            return {}

        store = {}

        h1 = soup.select_one("h1.ssin-Header_Title")
        if h1:
            store["name"] = re.sub(r"\s+", " ", h1.get_text(" ", strip=True)).strip()

        # 店舗情報テーブル (営業時間 / 定休日 / TEL / FAX / MAIL / 住所)
        info = soup.select_one("section.ssin-Information div.ssin-Information_Content")
        if info:
            label_map = {
                "営業時間": "time",
                "定休日": "holiday",
                "TEL": "tel",
                "FAX": "fax",
                "MAIL": "email",
                "住所": "addr",
            }
            for row in info.select("table tr"):
                cells = row.find_all(["td", "th"], recursive=False)
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                value = re.sub(r"\s+", " ", cells[1].get_text(" ", strip=True)).strip()
                key = label_map.get(label)
                if key and value and value != "-":
                    store[key] = value

            # 店長ブログ (href に前後空白が入る店舗がある)
            blog = info.select_one("a.ssin-Information_Btn[href]")
            if blog:
                store["blog"] = urljoin(store_url, blog["href"].strip())

        # パンくず: [トップ, 買取 店舗紹介, ○○地区の店舗紹介, ○○県の店舗紹介, ○○市の店舗紹介, 店舗名]
        crumbs = [
            li.get_text(strip=True)
            for li in soup.select("nav.page-HeaderNav ul.page-HeaderNav_List li")
        ]
        for crumb in crumbs:
            if crumb.endswith("地区の店舗紹介"):
                store["area"] = crumb[: -len("の店舗紹介")]
            elif crumb.endswith("の店舗紹介") and not _PREF_PATTERN.match(crumb):
                store.setdefault("city", crumb[: -len("の店舗紹介")])

        # 買取品目 (短いカテゴリラベル)
        items = [
            re.sub(r"\s+", "", li.get_text(" ", strip=True))
            for li in soup.select("ul.st-Items_List li")
        ]
        items = [i for i in items if i]
        if items:
            store["items"] = "/".join(dict.fromkeys(items))

        # 設備・サービス (短いラベル)
        services = [
            re.sub(r"\s+", "", li.get_text(" ", strip=True))
            for li in soup.select("ul.ssin-Service_List li")
        ]
        services = [s for s in services if s]
        if services:
            store["service"] = "/".join(dict.fromkeys(services))

        # 駐車場情報 (駐車場名と料金のみ。アクセス案内の文章は取得しない)
        parking = soup.select_one("section.ssin-Parking, section[id='駐車場情報']")
        if parking:
            names = [
                s.get_text(strip=True)
                for s in parking.select("summary.ssin-DetailsUnit_Summary")
            ]
            names = [n for n in names if n]
            if names:
                store["parking"] = "/".join(dict.fromkeys(names))
            fees = []
            for row in parking.select("table.ssin-DetailsUnit_Table tr"):
                cells = row.find_all(["td", "th"], recursive=False)
                if len(cells) >= 2 and cells[0].get_text(strip=True) == "料金":
                    fee = cells[1].get_text(" ", strip=True)
                    if fee and fee != "-":
                        fees.append(fee)
            if fees:
                store["parking_fee"] = "/".join(dict.fromkeys(fees))

        return store


if __name__ == "__main__":
    scraper = KaitoriDaikichi()
    scraper.execute("https://www.kaitori-daikichi.jp/open/category/new/")

"""
R30ナイトバイト (キャバジョブ / caba-job.com) — ナイトワーク求人 掲載店舗スクレイパー

取得対象:
    - 東京都 / 神奈川県 / 埼玉県 / 千葉県 の掲載店舗 (実測 248件: 東京149・神奈川46・埼玉49・千葉4)
    - 掲載カテゴリ全種 (キャバクラ / 熟女キャバクラ / 姉キャバクラ / パブ / スナック /
      クラブ / ラウンジ / ガールズバー / 朝キャバ・昼キャバ)
    - 店舗名 / サイト内業種 / 都道府県 / 住所 / 店舗TEL / 応募用番号(携帯) /
      募集職種 / 給与 / 掲載期間(開始日・終了日) / 担当 / 店舗説明 / HP / SNS / 営業時間 / 定休日

取得フロー:
    1. 引数 url (/area/?pref=tokyo) から pref を読み取り、備考指定の対象4県を巡回順に決める
    2. 各県のエリア分類サイトマップ (/sitemap-tax-{pref}.xml) からエリア一覧ページを収集
    3. 各エリアページの店舗カード (article.tmp_shop_list > a) から詳細URL (/shop/{id}/) を取得
    4. 詳細1件を取得するごとに即 yield (店舗IDで重複除外)

⚠ 一覧ページ /area/ のページネーション (/area/page/N/?pref=xxx) はサーバ側の不具合で機能しない。
   page/2・page/3 を取得しても 1ページ目と同一の20件が返り (wp-pagenavi の current が 1 のまま)、
   ?paged=N ?page=N も同じ。そのため 1県あたり20件で頭打ちになり全件取得できない。
   代わりにエリア分類ページ (/{pref}/{area}/) を列挙経路として採用している。
   実測でエリアページ経由の集合は /area/ 1ページ目の20件を完全に包含し、
   件数も /area/ の総ページ表示 (東京8・神奈川3・埼玉3・千葉1ページ × 20件) と整合する。
   エリアページ側は全55エリアが1ページ表示だが、将来の増加に備えて次ページリンクを追従する。

⚠ 求人セクション (section.tmp_shop_detail_tmp02) には「女の子にインタビュー」等の
   インタビュー記事も同じクラスで混在するため、見出しが「〜求人情報」のものだけを求人として扱う。

⚠ 掲載期間は空欄の店舗がある (`掲載期間：` のみ)。備考の指示どおり推測補完せず空欄で出力する。
   開始日のみ欠落する `掲載期間：～YYYY/MM/DD` 形式にも対応。

利用規約 (https://www.caba-job.com/company/) は運営会社情報とプライバシーポリシーのみで、
スクレイピングを禁止する条項は無い。robots.txt の Disallow は /columntest/ のみ。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/r30_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id r30_2
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path
from typing import Generator, Iterable
from urllib.parse import parse_qs, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

# sitemap.xml を html.parser で読む際の警告を抑制 (loc 抽出のみで十分なため)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 対象4県 (備考指定) — pref パラメータ値 → 都道府県名
_TARGET_PREFS: dict[str, str] = {
    "tokyo": "東京都",
    "kanagawa": "神奈川県",
    "saitama": "埼玉県",
    "chiba": "千葉県",
}

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 電話番号 (0ABJ / 携帯 いずれも)。掲載値は "03-1234-5678(21時～翌2時)" のように注記付き
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
# 掲載期間: "掲載期間：2026/07/22～2027/01/22" / 開始日欠落の "掲載期間：～2026/10/28" もあり
_PERIOD_PATTERN = re.compile(
    r"(\d{4}[/.-]\d{1,2}[/.-]\d{1,2})?\s*[～~〜]\s*(\d{4}[/.-]\d{1,2}[/.-]\d{1,2})?"
)
_DATE_PATTERN = re.compile(r"(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})")
# 詳細URL: https://www.caba-job.com/shop/{id}/
_SHOP_ID_PATTERN = re.compile(r"/shop/(\d+)")
# 求人セクションの見出し "フロアレディ求人情報" → 職種名 "フロアレディ"
_JOB_TITLE_SUFFIX = re.compile(r"求人情報\s*$")

_SNS_HOSTS = {
    Schema.INSTA: ("instagram.com",),
    Schema.X: ("twitter.com", "x.com"),
    Schema.FB: ("facebook.com", "fb.com"),
    Schema.TIKTOK: ("tiktok.com",),
    Schema.LINE: ("line.me", "lin.ee"),
}

# 求人セクションで給与が入るラベル (フロアレディ=時給 / 男子スタッフ=勤務形態給与)
_SALARY_LABELS = ("時給", "勤務形態給与", "給与")
# 求人セクションで勤務時間が入るラベル
_TIME_LABELS = ("時間", "勤務時間")

_EXTRA_JOB_TYPE = "募集職種"
_EXTRA_SALARY = "給与"
_EXTRA_PERIOD_FROM = "掲載開始日"
_EXTRA_PERIOD_TO = "掲載終了日"
_EXTRA_STAFF = "担当"
_EXTRA_AREA = "掲載エリア"


class R302Scraper(StaticCrawler):
    """R30ナイトバイト (caba-job.com) スクレイパー"""

    DELAY = 1.0
    # 一覧/詳細ともに素の requests で 200 (WAF なし)
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # 1エリアページあたりの追従上限 (現状すべて1ページだが暴走防止のガード)
    MAX_PAGES_PER_AREA = 30

    EXTRA_COLUMNS: list[str] = [
        _EXTRA_JOB_TYPE,
        _EXTRA_SALARY,
        _EXTRA_PERIOD_FROM,
        _EXTRA_PERIOD_TO,
        _EXTRA_STAFF,
        _EXTRA_AREA,
    ]

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_shops: set[str] = set()

        for pref_slug in self._target_pref_slugs(url):
            pref_name = _TARGET_PREFS[pref_slug]
            area_urls = self._area_urls(url, pref_slug)
            self.logger.info("%s: エリアページ %d件", pref_name, len(area_urls))

            for area_url in area_urls:
                for shop_url in self._shop_urls_in_area(area_url):
                    shop_id = self._shop_id(shop_url)
                    if shop_id in seen_shops:
                        continue
                    seen_shops.add(shop_id)
                    # 進捗表示(ETA)用。列挙しながら増えるため発見済み件数を都度反映する
                    self.total_items = len(seen_shops)

                    try:
                        record = self._scrape_detail(shop_url, pref_name)
                    except Exception as e:  # 個別店舗の失敗は握りつぶして続行
                        self.logger.warning("詳細取得失敗: %s (%s)", shop_url, e)
                        continue
                    if record:
                        yield record

    # ------------------------------------------------------------------ #
    # 列挙: pref → エリア分類ページ → 店舗詳細URL
    # ------------------------------------------------------------------ #

    def _target_pref_slugs(self, root_url: str) -> list[str]:
        """引数 url の pref を先頭に、備考指定の対象4県を巡回順に並べて返す。"""
        query = parse_qs(urlparse(root_url).query)
        first = (query.get("pref") or [""])[0].strip().lower()
        order = [p for p in _TARGET_PREFS if p == first]
        order += [p for p in _TARGET_PREFS if p != first]
        return order

    def _area_urls(self, root_url: str, pref_slug: str) -> list[str]:
        """/sitemap-tax-{pref}.xml からエリア分類ページURLを収集する。

        取得できない場合は引数 url の pref を差し替えた一覧ページにフォールバックする
        (ページネーションが壊れているため20件分のみになる点に注意)。
        """
        sitemap_url = urljoin(root_url, f"/sitemap-tax-{pref_slug}.xml")
        prefix = urljoin(root_url, f"/{pref_slug}/")
        locs: list[str] = []

        soup = self.get_soup(sitemap_url)
        if soup is not None:
            for loc in soup.find_all("loc"):
                text = loc.get_text(strip=True)
                if text.startswith(prefix) and text not in locs:
                    locs.append(text)

        if not locs:
            parsed = urlparse(root_url)
            fallback = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?pref={pref_slug}"
            self.logger.warning(
                "エリアサイトマップを取得できず一覧ページにフォールバック: %s", fallback
            )
            locs = [fallback]
        return locs

    def _shop_urls_in_area(self, area_url: str) -> Iterable[str]:
        """エリアページの店舗カードから詳細URLを yield する。次ページがあれば追従する。"""
        page_url = area_url
        for _ in range(self.MAX_PAGES_PER_AREA):
            soup = self.get_soup(page_url)
            if soup is None:
                return

            for anchor in soup.select("article.tmp_shop_list > a[href]"):
                href = (anchor.get("href") or "").split("#")[0]
                m = _SHOP_ID_PATTERN.search(href)
                if m:
                    # 一覧のリンクは ?post_id=NNN 付きなので正規形 /shop/{id}/ に揃える
                    yield urljoin(page_url, f"/shop/{m.group(1)}/")

            next_link = soup.select_one(".wp-pagenavi a.nextpostslink[href]")
            next_url = urljoin(page_url, next_link["href"]) if next_link else ""
            # ページネーション不具合で同一URLが返る場合は打ち切る (無限ループ防止)
            if not next_url or next_url == page_url:
                return
            page_url = next_url

    @staticmethod
    def _shop_id(shop_url: str) -> str:
        m = _SHOP_ID_PATTERN.search(shop_url)
        return m.group(1) if m else shop_url

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str, pref_hint: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        shop_info = self._shop_info_map(soup)

        name = shop_info.get("店名", "")
        if not name:
            title = soup.select_one("h2.tit")
            name = self._clean(title.get_text(" ")) if title else ""
        if not name:
            self.logger.warning("店名を取得できませんでした: %s", url)
            return None

        addr = shop_info.get("住所", "")
        # 住所から都道府県を取り、取れなければ巡回中の pref から補う
        pref = self._pref(addr) or pref_hint

        meta = self._shop_meta(soup)
        period_from, period_to = self._period(meta.get("period", ""))
        job_types, salary, work_time = self._job_sections(soup)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            # 店舗TEL(固定電話) と 応募用番号(携帯) は別掲載のため分けて格納する
            Schema.TEL: self._tel(shop_info.get("店舗TEL", "")),
            Schema.PHONE: self._tel(shop_info.get("応募用TEL", "")),
            Schema.CAT_SITE: shop_info.get("業種", "") or meta.get("job", ""),
            Schema.HOLIDAY: shop_info.get("休日", ""),
            Schema.TIME: work_time,
            Schema.HP: self._homepage(shop_info),
            # 店舗説明: 備考で明示的に指示された取得項目 (説明文セクション全文)
            Schema.DESCRIPTION: self._description(soup),
            _EXTRA_JOB_TYPE: job_types,
            _EXTRA_SALARY: salary,
            # 掲載期間は空欄の店舗がある。推測補完せず空文字のままにする
            _EXTRA_PERIOD_FROM: period_from,
            _EXTRA_PERIOD_TO: period_to,
            # 担当: 「採用係」「ママ」等の呼称・担当者名をサイト記載どおりに格納する
            _EXTRA_STAFF: shop_info.get("担当", ""),
            _EXTRA_AREA: meta.get("area", ""),
        }
        record.update(self._sns(shop_info))
        return record

    def _shop_info_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """「店舗情報・応募方法」テーブルの th/td をラベル辞書化する。

        th は "店 \\t 名" のように空白混じりなので空白を除去して正規化する。
        応募フォーム内のテーブルは入力欄なので除外する。
        """
        info: dict[str, str] = {}
        section = soup.select_one("section.shop_detail_shop")
        if section is None:
            return info
        for row in section.select("tr"):
            if row.find_parent("form") is not None:
                continue
            th, td = row.find("th"), row.find("td")
            if not th or not td:
                continue
            label = re.sub(r"\s+", "", th.get_text())
            if label and label not in info:
                info[label] = self._clean(td.get_text("\n"))
        return info

    def _shop_meta(self, soup: BeautifulSoup) -> dict[str, str]:
        """div.shop_meta の p.job / p.area / p.period を取り出す。"""
        meta: dict[str, str] = {}
        block = soup.select_one("div.shop_meta")
        if block is None:
            return meta
        for key in ("job", "area", "period"):
            el = block.select_one(f"p.{key}")
            if el is not None:
                meta[key] = self._clean(el.get_text(" "))
        return meta

    def _description(self, soup: BeautifulSoup) -> str:
        """説明文セクション (キャッチコピー見出し + 本文) のテキストを返す。"""
        section = soup.select_one("section.tmp_shop_detail_desc")
        if section is not None:
            return self._clean(section.get_text(" "))
        catch = soup.select_one("p.catch")
        return self._clean(catch.get_text(" ")) if catch else ""

    def _job_sections(self, soup: BeautifulSoup) -> tuple[str, str, str]:
        """求人セクションから 募集職種・給与・勤務時間を取り出す。

        section.tmp_shop_detail_tmp02 は店舗情報テーブルやインタビュー記事とクラスを
        共有しているため、見出しが「〜求人情報」のセクションだけを求人として扱う。

        Returns:
            (募集職種, 給与, 勤務時間) — 求人が複数ある場合は職種名を添えて連結する。
        """
        titles: list[str] = []
        salaries: list[tuple[str, str]] = []
        times: list[tuple[str, str]] = []

        for section in soup.select("section.tmp_shop_detail_tmp02"):
            classes = section.get("class") or []
            if "shop_detail_shop" in classes:
                continue  # 店舗情報テーブルは対象外
            heading = section.select_one("h3")
            if heading is None:
                continue
            raw_title = self._clean(heading.get_text(" "))
            if not _JOB_TITLE_SUFFIX.search(raw_title):
                continue  # インタビュー記事など求人以外のセクションを除外
            title = _JOB_TITLE_SUFFIX.sub("", raw_title).strip()
            if not title:
                continue
            titles.append(title)

            for row in section.select("tr"):
                th, td = row.find("th"), row.find("td")
                if not th or not td:
                    continue
                label = re.sub(r"\s+", "", th.get_text())
                value = self._clean(td.get_text("\n"))
                if not value:
                    continue
                if label in _SALARY_LABELS:
                    salaries.append((title, value))
                elif label in _TIME_LABELS:
                    times.append((title, value))

        unique_titles = list(dict.fromkeys(titles))
        return (
            " / ".join(unique_titles),
            self._join_by_job(salaries, len(unique_titles)),
            self._join_by_job(times, len(unique_titles)),
        )

    @staticmethod
    def _join_by_job(pairs: list[tuple[str, str]], section_count: int) -> str:
        """求人が複数ある場合のみ職種名を接頭辞として付けて連結する。"""
        if not pairs:
            return ""
        if section_count <= 1:
            return pairs[0][1]
        return " / ".join(f"{title}: {value}" for title, value in pairs)

    # ------------------------------------------------------------------ #
    # 値の正規化
    # ------------------------------------------------------------------ #

    def _period(self, text: str) -> tuple[str, str]:
        """"掲載期間：2026/07/22～2027/01/22" → ("2026-07-22", "2027-01-22")。

        空欄・開始日のみ欠落の場合は該当側を空文字で返す (推測補完はしない)。
        """
        if not text:
            return "", ""
        m = _PERIOD_PATTERN.search(text)
        if not m:
            single = _DATE_PATTERN.search(text)
            return (self._date(single.group(0)) if single else "", "")
        return self._date(m.group(1) or ""), self._date(m.group(2) or "")

    @staticmethod
    def _date(text: str) -> str:
        m = _DATE_PATTERN.search(text or "")
        if not m:
            return ""
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    @staticmethod
    def _tel(text: str) -> str:
        """"03-5647-8087(21時～翌2時)" → "03-5647-8087"。"""
        m = _TEL_PATTERN.search(text or "")
        return m.group(0) if m else ""

    @staticmethod
    def _pref(addr: str) -> str:
        m = _PREF_PATTERN.search(addr or "")
        return m.group(1) if m else ""

    @staticmethod
    def _homepage(info: dict[str, str]) -> str:
        """「HP」行の URL。SNS の URL しか無い場合は HP としては扱わない。"""
        value = info.get("HP", "")
        m = re.search(r"https?://\S+", value)
        if not m:
            return ""
        url = m.group(0)
        for hosts in _SNS_HOSTS.values():
            if any(host in url for host in hosts):
                return ""
        return url

    @staticmethod
    def _sns(info: dict[str, str]) -> dict[str, str]:
        """「SNS」「HP」行に載る URL をドメインで各SNSカラムに振り分ける。"""
        result = {col: "" for col in _SNS_HOSTS}
        for key in ("SNS", "HP"):
            for url in re.findall(r"https?://\S+", info.get(key, "")):
                for col, hosts in _SNS_HOSTS.items():
                    if not result[col] and any(host in url for host in hosts):
                        result[col] = url
        return result

    @staticmethod
    def _clean(text: str) -> str:
        """改行・タブ・連続空白を1つの半角スペースに畳む。"""
        if not text:
            return ""
        text = text.replace("　", " ")
        return re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = R302Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.caba-job.com/area/?pref=tokyo")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

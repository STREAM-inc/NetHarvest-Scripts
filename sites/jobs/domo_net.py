"""
DOMO NET (ドモネット) / 関東エリア (domonet.jp/kanto) — アルバイト・パート求人サイト スクレイパー

取得対象:
    - 一覧ページ (.searchList_Box h4 a[href] を主セレクタとし、h3/h2/任意アンカー、
      さらにページ全体走査の段階的フォールバックで詳細URLを収集) から詳細ページURLを収集
    - 各詳細ページ (table.contents_single_table の th/td ペア) から:
        社名 / 事業内容 / 所在地 / URL / お問い合わせ先(TEL) /
        求人タイトル / 給与 / 勤務地 / 時間・勤務日 / 休日・休暇 /
        最寄駅 / 資格 / 待遇 / 期間
      ※ お仕事内容・応募方法・応募後のプロセス・その他 などの自由記述プロースは
        著作権リスクのため取得しない (EXTRA_COLUMNS に含めない)

取得フロー (URL 一貫性 / SSOT = sites.yml の url):
    引数 url (= https://domonet.jp/kanto) を唯一のルートとする。
    一覧ページは url から派生 (末尾に /list が無ければ付与)。
    ?page=N(&rs_start=...) でページネーション。各ページで詳細URLを集め、
    1件取得するごとに即 yield する (早期 yield / Pattern B)。
    別地方 (shizuoka / nagoya / kansai / pado) は別 site_id として登録するため、
    このスクリプトでは引数 url の地方 (関東) のみを対象とし、他ルートはハードコードしない。
    例: 静岡は jobs/domo_2.py (url=https://domonet.jp/shizuoka/list) として別登録済み。
    残りの地方も「地方ごとの url を sites.yml に登録 → 同一構造の別 site_id」で増設する。

実行方法:
    python scripts/sites/jobs/domo_net.py
    python bin/run_flow.py --site-id domo_net

注意:
    domonet.jp は Cloudflare の JS チャレンジ (cf-mitigated: challenge) を返すため、
    一部の egress IP からは 403 となる。prepare()/parse() でトップページを踏んで
    セッションを温め、ブラウザ相当のヘッダーを付与してリトライする。
    HTML 構造は同一プラットフォームの実績クローラー jobs/domonet.py と同一。
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"(?:TEL|電話|Tel)[\s:]*([\d\-()\s]{8,20})")

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
}


class DomoNetScraper(StaticCrawler):
    """DOMO NET (関東) アルバイト・パート求人 スクレイパー"""

    DELAY = 3.0
    # 自由記述プロース (お仕事内容/応募方法/応募後のプロセス/その他) は著作権リスクのため除外
    EXTRA_COLUMNS = [
        "求人タイトル",
        "給与",
        "勤務地",
        "勤務時間",
        "休日・休暇",
        "最寄駅",
        "資格",
        "待遇",
        "期間",
    ]

    def prepare(self):
        """Cloudflare/Bot 判定回避のためブラウザ相当のヘッダーを付与する。"""
        self.session.headers.update(_BROWSER_HEADERS)

    def parse(self, url: str):
        """引数 url を唯一のルートとし、ページごとに詳細を取得即 yield する。"""
        # ルート url からドメイン (origin) と一覧ページ (list_base) を派生する。
        # 別ルート URL のハードコードはしない。
        origin = "{0.scheme}://{0.netloc}".format(urlsplit(url))
        list_base = self._derive_list_base(url)

        self.session.headers.update(_BROWSER_HEADERS)
        # トップページでセッションを温める (Cloudflare/Bot 判定回避)。
        # ※ get_soup は test_runner が中断可能なラップ呼び出し。直後に最初の一覧へ進む。
        if origin:
            self.get_soup(origin)
            time.sleep(self.DELAY)

        page = 1
        rs_start: str | None = None
        referer = origin
        max_pages_safety = 2000  # 暴走防止

        while page <= max_pages_safety:
            list_url = self._build_list_url(list_base, page, rs_start)
            self.logger.info("一覧ページ取得: %s", list_url)
            soup = self._fetch_list_soup(list_url, referer)
            if soup is None:
                break

            if rs_start is None:
                rs_start = self._extract_rs_start(soup)

            detail_urls = self._extract_detail_urls(soup, origin)
            if not detail_urls:
                break

            for detail_url in detail_urls:
                try:
                    self.session.headers["Referer"] = list_url
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                    continue

            # 次ページが無ければ終了
            if not soup.select_one(f'a[href*="page={page + 1}"]'):
                break

            referer = list_url
            page += 1

    @staticmethod
    def _derive_list_base(url: str) -> str:
        """引数 url から一覧ページ URL を派生する。

        url が地方ランディング (例: /kanto) の場合は /list を付与し、
        既に /list を含む場合はそのまま使う (クエリ・フラグメントは除去)。
        """
        parts = urlsplit(url)
        path = parts.path.rstrip("/")
        if not path.endswith("/list"):
            path = f"{path}/list"
        return f"{parts.scheme}://{parts.netloc}{path}"

    # 詳細ページURLらしさの判定 (一覧/検索/ページング/トップ等を除外)
    _DETAIL_HREF_RE = re.compile(r"/(?:detail|job|recruit|entry|jinzai|info)\b|\d{3,}")
    _NON_DETAIL_RE = re.compile(
        r"/(?:list|search|category|area|login|mypage|company_top|guide|contact)\b"
        r"|[?&](?:page|rs_start)="
    )

    def _extract_detail_urls(self, soup: bs4.BeautifulSoup, origin: str) -> list[str]:
        """一覧ページから詳細ページURLを抽出する。

        主セレクタ (.searchList_Box h4 a) を最優先しつつ、マークアップの揺れに
        備えて段階的にフォールバックする (h3/h2/任意のアンカー → ページ全体走査)。
        """
        boxes = soup.select(".searchList_Box") or soup.select(
            "[class*='searchList'], [class*='resultList'], [class*='jobList'], li.result"
        )

        urls: list[str] = []
        seen: set[str] = set()

        def _add(href: str) -> None:
            href = (href or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                return
            full = urljoin(origin, href).split("#")[0]
            if full in seen:
                return
            seen.add(full)
            urls.append(full)

        for box in boxes:
            a = (
                box.select_one("h4 a[href]")
                or box.select_one("h3 a[href]")
                or box.select_one("h2 a[href]")
                or box.select_one("a[href]")
            )
            if a:
                _add(a.get("href", ""))

        if urls:
            return urls

        # フォールバック: ボックスが取れない/リンクが無い場合はページ全体から
        # 詳細ページらしいアンカーを拾う。
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            if self._NON_DETAIL_RE.search(href):
                continue
            if self._DETAIL_HREF_RE.search(href):
                _add(href)

        return urls

    def _build_list_url(self, list_base: str, page: int, rs_start: str | None) -> str:
        if page <= 1:
            return list_base
        params = [f"page={page}"]
        if rs_start:
            params.append(f"rs_start={rs_start}")
        return f"{list_base}?{'&'.join(params)}"

    def _extract_rs_start(self, soup: bs4.BeautifulSoup) -> str | None:
        for a in soup.select('a[href*="rs_start="]'):
            m = re.search(r"rs_start=(\d+)", a.get("href", ""))
            if m:
                return m.group(1)
        return None

    def _fetch_list_soup(self, list_url: str, referer: str) -> bs4.BeautifulSoup | None:
        """一覧ページ取得。403 時は待機してリトライする。"""
        for attempt in range(3):
            time.sleep(self.DELAY)
            self.session.headers["Referer"] = referer
            soup = self.get_soup(list_url)
            if soup is not None:
                return soup
            if attempt < 2:
                wait = self.DELAY * (attempt + 2)
                self.logger.warning(
                    "一覧ページ再試行 (%d/3): %s (%d秒待機)", attempt + 2, list_url, wait
                )
                time.sleep(wait)
        return None

    def _fetch_detail_soup(self, detail_url: str) -> bs4.BeautifulSoup | None:
        """詳細ページ取得。一覧と同じく Cloudflare 403 を踏むため待機リトライする。"""
        for attempt in range(3):
            soup = self.get_soup(detail_url)
            if soup is not None:
                return soup
            if attempt < 2:
                wait = self.DELAY * (attempt + 2)
                self.logger.warning(
                    "詳細ページ再試行 (%d/3): %s (%d秒待機)", attempt + 2, detail_url, wait
                )
                time.sleep(wait)
        return None

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから th/td ペアを抽出して Schema + EXTRA にマッピング"""
        soup = self._fetch_detail_soup(detail_url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: "アルバイト・パート求人",
        }

        # 求人タイトル (本文エリアの h2 を優先)
        title_el = soup.select_one("#contentsBox h2") or soup.select_one("h2")
        if title_el:
            item["求人タイトル"] = title_el.get_text(strip=True)

        # 詳細テーブル: table.contents_single_table の th/td を収集
        pairs: dict[str, str] = {}
        tables = soup.select("table.contents_single_table")
        for table in tables:
            for row in table.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text("\n", strip=True)
                val = re.sub(r"\n{2,}", "\n", val).strip()
                if key and key not in pairs:
                    pairs[key] = val

        # --- Schema マッピング ---
        if "社名" in pairs:
            item[Schema.NAME] = pairs["社名"]

        if "事業内容" in pairs:
            item[Schema.LOB] = pairs["事業内容"]

        if "所在地" in pairs:
            addr_raw = pairs["所在地"].replace("\n", " ")
            m_post = _POST_RE.search(addr_raw)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
                addr_raw = _POST_RE.sub("", addr_raw, count=1).strip()
            m_pref = _PREF_RE.search(addr_raw)
            if m_pref:
                item[Schema.PREF] = m_pref.group(1)
                item[Schema.ADDR] = addr_raw[m_pref.end():].strip() or addr_raw
            else:
                item[Schema.ADDR] = addr_raw

        if "URL" in pairs:
            url_td = None
            for table in tables:
                for row in table.select("tr"):
                    th = row.select_one("th")
                    if th and th.get_text(strip=True) == "URL":
                        url_td = row.select_one("td")
                        break
                if url_td:
                    break
            if url_td:
                a = url_td.select_one("a[href]")
                hp = a.get("href", "").strip() if a else pairs["URL"].strip()
                if hp:
                    item[Schema.HP] = hp

        if "お問い合わせ先" in pairs:
            m_tel = _TEL_RE.search(pairs["お問い合わせ先"])
            if m_tel:
                tel = re.sub(r"[\s()]", "", m_tel.group(1)).strip("-")
                item[Schema.TEL] = tel

        # --- EXTRA_COLUMNS マッピング (構造化された短いラベルのみ) ---
        extra_map = {
            "給与": "給与",
            "勤務地": "勤務地",
            "時間・勤務日": "勤務時間",
            "休日・休暇": "休日・休暇",
            "最寄駅": "最寄駅",
            "資格": "資格",
            "待遇": "待遇",
            "期間": "期間",
        }
        for src_key, dst_col in extra_map.items():
            if src_key in pairs:
                item[dst_col] = pairs[src_key]

        # NAME フォールバック (一覧の社名 h3)
        if Schema.NAME not in item:
            company_el = soup.select_one("#contentsBox h3") or soup.select_one("h3")
            if company_el:
                name = company_el.get_text(strip=True)
                if name:
                    item[Schema.NAME] = name

        if Schema.NAME not in item:
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DomoNetScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://domonet.jp/kanto")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

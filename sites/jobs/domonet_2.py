"""
DOMONET (【ドーモ】DOMO / domonet.jp) — アルバイト・パート・正社員 求人サイト スクレイパー

運営: 株式会社アルバイトタイムス

取得フロー (URL 一貫性 / SSOT = sites.yml の url):
    引数 url (= https://domonet.jp/) を唯一のルートとする。
    トップページの `/{region}/list` リンクから地域スラッグ (kanto / shizuoka /
    nagoya / kansai / pado) を動的に導出し、各地域の一覧ページを
    `?page=N` でページネーションしながら巡回する。
    一覧の各 .searchList_Box から詳細 URL (/{region}/ad/{type}/{id}) を取り、
    詳細ページを 1 件取得するごとに即 yield する (早期 yield / Pattern B)。

取得データ:
    一覧 (.searchList_Box)
        h3                              … 社名・店舗名
        h4 a[href]                      … 求人タイトル + 詳細 URL
        img.condition_icon[alt]         … 雇用形態 (アルバイト/パート/正社員…)
        .searchList_categoryArea span   … 条件タグ (学生歓迎・交通費支給…)
        table.searchList_contentsTable  … 給与 / 勤務地 / 勤務時間
    詳細 (table.contents_single_table の th/td, 掲載終了時は table.searchTable)
        社名 / 事業内容 / 所在地 / URL / お問い合わせ先(TEL・メール) /
        給与 / 年収例 / 勤務地 / 時間・勤務日 / 休日・休暇 / 最寄駅 /
        資格 / 待遇 / 期間
    詳細 JSON-LD (JobPosting)
        title / datePosted / validThrough / hiringOrganization / jobLocation

    ※ お仕事内容・職場情報・キャッチコピー・応募方法・応募後のプロセス・その他 などの
      自由記述プロースは著作権リスクのため取得しない (EXTRA_COLUMNS に含めない)。

注意 (Cloudflare):
    domonet.jp は Cloudflare の JS チャレンジ (cf-mitigated: challenge) を返すため、
    egress IP によっては全パス 403 になる。ブラウザ相当のヘッダーを付与したうえで、
    live 取得が失敗した場合のみ Wayback Machine の生スナップショット (…id_/…) に
    フォールバックする (live 優先・取得不能時のみ代替)。

実行方法:
    python scripts/sites/jobs/domonet_2.py
    python bin/run_flow.py --site-id domonet_2
"""

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(r"(?:TEL|Tel|ＴＥＬ|電話)[\s:：]*([0-9０-９\-－()（）\s]{9,20})")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_COUNT_RE = re.compile(r"条件にマッチする求人\s*([\d,]+)\s*件")
_LIST_PATH_RE = re.compile(r"^/([a-z][a-z0-9_-]*)/list(?:[/?]|$)")
_REGION_FROM_DETAIL_RE = re.compile(r"^/([a-z][a-z0-9_-]*)/ad/")

# トップページからリンクを導出できなかった場合のみ使うフォールバック
_FALLBACK_REGIONS = ["kanto", "shizuoka", "nagoya", "kansai", "pado"]

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
}

# 詳細 th ラベル → EXTRA カラム名 (自由記述プロースのラベルは意図的に含めない)
_DETAIL_EXTRA_MAP = {
    "給与": "給与",
    "年収例": "年収例",
    "勤務地": "勤務地",
    "時間・勤務日": "勤務時間",
    "勤務時間": "勤務時間",
    "休日・休暇": "休日・休暇",
    "最寄駅": "最寄駅",
    "資格": "資格",
    "待遇": "待遇",
    "期間": "期間",
}


class Domonet2Scraper(StaticCrawler):
    """【ドーモ】DOMO 求人情報 スクレイパー (全地域)"""

    DELAY = 1.5
    MAX_PAGES_PER_REGION = 3000   # 暴走防止 (1ページ20件)
    MAX_FETCH_ATTEMPTS = 2        # live 取得のリトライ上限 (超過で Wayback へ)

    EXTRA_COLUMNS = [
        "求人タイトル",
        "給与",
        "年収例",
        "勤務地",
        "勤務時間",
        "休日・休暇",
        "最寄駅",
        "資格",
        "待遇",
        "期間",
        "条件タグ",
        "掲載開始日",
        "掲載終了日",
        "掲載状態",
        "掲載エリア",
    ]

    def prepare(self):
        """Cloudflare / Bot 判定対策としてブラウザ相当のヘッダーを付与する。"""
        self.session.headers.update(_BROWSER_HEADERS)

    # ------------------------------------------------------------------ #
    # 取得 (live 優先 → Cloudflare 等で取得不能なら Wayback 生スナップショット)
    # ------------------------------------------------------------------ #
    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        for attempt in range(self.MAX_FETCH_ATTEMPTS):
            soup = super().get_soup(url)
            if soup is not None and not self._is_challenge(soup):
                return soup
            if attempt < self.MAX_FETCH_ATTEMPTS - 1:
                time.sleep(min(2 ** attempt, 4))
        self.logger.warning("live 取得不可 (Wayback へフォールバック): %s", url)
        return self._fetch_wayback(url)

    @staticmethod
    def _is_challenge(soup: bs4.BeautifulSoup) -> bool:
        """Cloudflare のチャレンジ画面 (200 で返ることがある) を検出する。"""
        title = soup.title.get_text(strip=True) if soup.title else ""
        return "Just a moment" in title or "しばらくお待ちください" in title

    def _fetch_wayback(self, url: str) -> bs4.BeautifulSoup | None:
        """Wayback Machine の生スナップショット (…id_/…) を取得する。

        クエリ付き URL はアーカイブされていないことが多いため、可用性判定は
        クエリを落とした URL で行う (= 1 ページ目相当が返ることがある。
        呼び出し側は取得済み URL の重複排除で空振りを検出する)。
        """
        lookup = urlunsplit(urlsplit(url)._replace(query="", fragment=""))
        try:
            api = "http://archive.org/wayback/available?url=" + quote(lookup, safe="")
            meta = self.session.get(api, timeout=self.TIMEOUT).json()
            snap = (meta.get("archived_snapshots") or {}).get("closest")
            if not snap or not snap.get("available"):
                self.logger.warning("Wayback スナップショット無し: %s", lookup)
                return None
            raw = f"https://web.archive.org/web/{snap['timestamp']}id_/{lookup}"
            resp = self.session.get(raw, headers=_BROWSER_HEADERS, timeout=self.TIMEOUT)
            if resp.status_code != 200:
                self.logger.warning("Wayback HTTP %s: %s", resp.status_code, raw)
                return None
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except (requests.exceptions.RequestException, ValueError) as e:
            self.logger.warning("Wayback 取得エラー: %s — %s", lookup, e)
            return None

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """引数 url を唯一のルートとし、地域一覧 → 詳細を取得即 yield する。"""
        origin = "{0.scheme}://{0.netloc}".format(urlsplit(url))
        top_soup = self.get_soup(url)
        regions = self._extract_regions(top_soup)
        self.logger.info("対象地域: %s", ", ".join(regions))

        seen_details: set[str] = set()
        total = 0

        for region in regions:
            list_base = f"{origin}/{region}/list"
            page = 1
            next_url = list_base

            while next_url and page <= self.MAX_PAGES_PER_REGION:
                self.logger.info("一覧ページ取得: %s", next_url)
                list_url = next_url
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                if page == 1:
                    count = self._extract_total_count(soup)
                    if count:
                        total += count
                        self.total_items = total
                        self.logger.info("地域 %s: %d 件", region, count)

                boxes = soup.select(".searchList_Box")
                if not boxes:
                    break

                new_on_page = 0
                for box in boxes:
                    listing = self._parse_list_box(box, origin)
                    detail_url = listing.pop("_detail_url", "")
                    if not detail_url or detail_url in seen_details:
                        continue
                    seen_details.add(detail_url)
                    new_on_page += 1
                    try:
                        item = self._build_item(detail_url, listing, region)
                    except Exception as e:  # 1件の失敗で全体を止めない
                        self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                        continue
                    if item:
                        yield item

                if new_on_page == 0:
                    # 同一ページが返り続けている (Wayback フォールバック等) → 打ち切り
                    self.logger.info("新規求人なし: %s", list_url)
                    break

                next_url = self._find_next_page(soup, list_url, page)
                page += 1
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    def _extract_regions(self, soup: bs4.BeautifulSoup | None) -> list[str]:
        """トップページの `/{region}/list` リンクから地域スラッグを導出する。"""
        regions: list[str] = []
        if soup is not None:
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                path = urlsplit(href).path if href.startswith("http") else href
                m = _LIST_PATH_RE.match(path)
                if m and m.group(1) not in regions:
                    regions.append(m.group(1))
        if not regions:
            self.logger.warning("トップから地域リンクを取得できず、既定の地域を使用します")
            return list(_FALLBACK_REGIONS)
        return regions

    @staticmethod
    def _extract_total_count(soup: bs4.BeautifulSoup) -> int | None:
        m = _COUNT_RE.search(soup.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else None

    @staticmethod
    def _find_next_page(soup: bs4.BeautifulSoup, list_url: str, page: int) -> str | None:
        """ページャの「次へ」リンク (なければ ?page=N+1 リンク) を返す。"""
        for a in soup.select("a[href]"):
            if a.get_text(strip=True) == "次へ":
                return urljoin(list_url, a["href"])
        nxt = soup.select_one(f'a[href*="page={page + 1}"]')
        return urljoin(list_url, nxt["href"]) if nxt else None

    def _parse_list_box(self, box: bs4.Tag, origin: str) -> dict:
        """一覧カードから取得できる値を集める (詳細が取れない場合の土台にもなる)。"""
        data: dict = {}

        a = box.select_one("h4 a[href]")
        if a:
            data["_detail_url"] = urljoin(origin, a["href"].strip())
            title = a.get_text(" ", strip=True)
            if title:
                data["求人タイトル"] = title

        h3 = box.select_one("h3")
        if h3 and h3.get_text(strip=True):
            data[Schema.NAME] = h3.get_text(" ", strip=True)

        employ = [
            i.get("alt", "").strip()
            for i in box.select("img.condition_icon")
            if i.get("alt", "").strip()
        ]
        if employ:
            data[Schema.CAT_SITE] = " / ".join(dict.fromkeys(employ))

        tags = [
            s.get_text(strip=True)
            for s in box.select(".searchList_categoryArea span")
            if s.get_text(strip=True)
        ]
        if tags:
            data["条件タグ"] = " / ".join(dict.fromkeys(tags))

        for tr in box.select("table.searchList_contentsTable tr"):
            th, td = tr.select_one("th"), tr.select_one("td")
            if not th or not td:
                continue
            col = _DETAIL_EXTRA_MAP.get(th.get_text(strip=True))
            if col:
                data[col] = self._text(td)

        return data

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _build_item(self, detail_url: str, listing: dict, region: str) -> dict | None:
        """一覧カードの値を土台に、詳細ページの値で上書きした 1 レコードを作る。"""
        item: dict = {Schema.URL: detail_url, "掲載エリア": region}
        item.update(listing)

        soup = self.get_soup(detail_url)
        if soup is not None:
            self._merge_detail(item, soup)

        if not str(item.get(Schema.NAME) or "").strip():
            return None
        return item

    def _merge_detail(self, item: dict, soup: bs4.BeautifulSoup) -> None:
        pairs = self._detail_pairs(soup)
        ld = self._json_ld(soup)

        item["掲載状態"] = "掲載終了" if soup.select_one(".detailend_Txt") else "掲載中"

        # --- 名称 (社名 → JSON-LD → h1) ---
        name = (
            pairs.get("社名")
            or (ld.get("hiringOrganization") or {}).get("name")
            or self._first_text(soup, "h1")
        )
        if name:
            item[Schema.NAME] = re.sub(r"\s+", " ", name).strip()

        # --- 求人タイトル (h2 → JSON-LD title) ---
        title = self._first_text(soup, "#contentsBox h2") or (ld.get("title") or "")
        if title.strip():
            item["求人タイトル"] = title.strip()

        # --- 事業内容 ---
        if pairs.get("事業内容"):
            item[Schema.LOB] = pairs["事業内容"]

        # --- 所在地 (〒 は Pipeline が住所から分離するのでそのまま渡す) ---
        addr_raw = re.sub(r"\s+", " ", pairs.get("所在地", "")).strip()
        pref = ""
        if not addr_raw:
            addr_raw, pref = self._address_from_ld(ld)
        if addr_raw:
            # 「本社：〒160-0022 …」のような接頭ラベルは落とす
            # (Pipeline は住所先頭の〒付き郵便番号のみを郵便番号カラムへ分離するため)
            mark = addr_raw.find("〒")
            if 0 < mark <= 12:
                addr_raw = addr_raw[mark:]
            m = _PREF_RE.search(addr_raw)
            if m:
                pref = pref or m.group(1)
                addr_raw = (addr_raw[: m.start()] + addr_raw[m.end():]).strip()
            item[Schema.ADDR] = addr_raw
        if not pref:
            pref = self._pref_from_breadcrumb(soup)
        if pref:
            item[Schema.PREF] = pref

        # --- 企業 URL ---
        hp = ""
        url_td = self._td_for(soup, "URL")
        if url_td is not None:
            a = url_td.select_one("a[href]")
            hp = (a.get("href") if a else url_td.get_text(strip=True)) or ""
        hp = hp.strip() or (ld.get("hiringOrganization") or {}).get("sameAs", "")
        if hp.startswith("http"):
            item[Schema.HP] = hp

        # --- お問い合わせ先 (TEL / メール) ---
        contact = pairs.get("お問い合わせ先", "")
        if contact:
            m_tel = _TEL_RE.search(contact)
            if m_tel:
                item[Schema.TEL] = re.sub(r"[\s()（）]", "", m_tel.group(1)).strip("-－")
            m_mail = _EMAIL_RE.search(contact)
            if m_mail:
                item[Schema.EMAIL] = m_mail.group(0)

        # --- 雇用形態 (サイト定義の区分) ---
        employ = [
            i.get("alt", "").strip()
            for i in soup.select("img.condition_icon")
            if i.get("alt", "").strip()
        ]
        if employ:
            item[Schema.CAT_SITE] = " / ".join(dict.fromkeys(employ))

        # --- 掲載期間 ---
        if ld.get("datePosted"):
            item["掲載開始日"] = ld["datePosted"]
        if ld.get("validThrough"):
            item["掲載終了日"] = ld["validThrough"]

        # --- 条件系 EXTRA ---
        for label, col in _DETAIL_EXTRA_MAP.items():
            if pairs.get(label):
                item[col] = pairs[label]

    def _detail_pairs(self, soup: bs4.BeautifulSoup) -> dict:
        """詳細テーブルの th/td ペアを集める (掲載終了ページは searchTable)。"""
        pairs: dict[str, str] = {}
        for table in soup.select("table.contents_single_table, table.searchTable"):
            for tr in table.select("tr"):
                th, td = tr.select_one("th"), tr.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                if key and key not in pairs:
                    pairs[key] = self._text(td)
        return pairs

    @staticmethod
    def _td_for(soup: bs4.BeautifulSoup, label: str) -> bs4.Tag | None:
        for tr in soup.select("table.contents_single_table tr, table.searchTable tr"):
            th = tr.select_one("th")
            if th and th.get_text(strip=True) == label:
                return tr.select_one("td")
        return None

    @staticmethod
    def _json_ld(soup: bs4.BeautifulSoup) -> dict:
        for sc in soup.select('script[type="application/ld+json"]'):
            if not sc.string:
                continue
            try:
                data = json.loads(sc.string)
            except ValueError:
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                    return node
        return {}

    @staticmethod
    def _address_from_ld(ld: dict) -> tuple[str, str]:
        """JSON-LD jobLocation から (住所, 都道府県) を取り出す。"""
        locations = ld.get("jobLocation") or []
        for place in locations if isinstance(locations, list) else [locations]:
            if not isinstance(place, dict):
                continue
            addr = place.get("address")
            if isinstance(addr, str) and addr.strip():
                return re.sub(r"\s+", " ", addr).strip(), ""
            if isinstance(addr, dict):
                region = (addr.get("addressRegion") or "").strip()
                joined = " ".join(
                    part
                    for part in (
                        region,
                        (addr.get("addressLocality") or "").strip(),
                        (addr.get("streetAddress") or "").strip(),
                    )
                    if part
                )
                if joined.strip():
                    return joined.strip(), region
        return "", ""

    @staticmethod
    def _pref_from_breadcrumb(soup: bs4.BeautifulSoup) -> str:
        for a in soup.select('[class*="bread"] a[href], [class*="topic"] a[href]'):
            m = _PREF_RE.fullmatch(a.get_text(strip=True))
            if m:
                return m.group(1)
        return ""

    @staticmethod
    def _first_text(soup: bs4.BeautifulSoup, selector: str) -> str:
        el = soup.select_one(selector)
        return el.get_text(" ", strip=True) if el else ""

    @staticmethod
    def _text(el: bs4.Tag) -> str:
        return re.sub(r"\n{2,}", "\n", el.get_text("\n", strip=True)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Domonet2Scraper()
    scraper.execute("https://domonet.jp/")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

"""
ぐるなび — 飲食店情報スクレイパー

取得対象:
    - ぐるなび掲載の飲食店（既定は TARGET_PREFS の4都県。定数を変えれば他県にも広げられる）
    - 店名 / 郵便番号 / 住所 / 都道府県 / TEL / ジャンル / 取得URL

取得フロー（逐次ストリーミング）:
    1. 都道府県一覧 https://r.gnavi.co.jp/area/{slug}/rs/ を起点にする
    2. 一覧は「1ページ30件 / p=334 が上限」＝1URLから約10,020件しか辿れないため、
       ページ内の「該当件数」を読み、上限を超える一覧はページ内のジャンル別・
       サブエリア別リンクへ自動的に細分化する（和集合＋店舗URLで重複排除）
    3. 一覧を1ページ読むごとに、その場で各店舗の**店舗トップ**
       https://r.gnavi.co.jp/{shop_id}/ の JSON-LD (@type="Restaurant") を取得して
       逐次 yield する（全URLを溜め込まないのでテスト実行でもすぐ結果が出る）

⚠️ 重要な実装上の注意（過去の不具合と対策）:
    - 巡回起点をトップページ（https://www.gnavi.co.jp/）にすると一覧要素が無く
      0件で終わる。必ず /area/{slug}/rs/ 形式の一覧を起点にすること。
    - **店舗の `/map/` ページからTELを取ってはいけない。** `/map/` の電話番号表示は
      ぐるなびのネット予約用050転送番号であり店舗の実番号ではない
      （実測: 同一店舗で JSON-LD `telephone`=03-6261-2725 に対し /map/ は 050-5484-8670）。
      TELは店舗トップの JSON-LD `telephone` のみを使用する。
    - robots.txt (User-agent: *) 遵守: 都道府県インデックス /{pref}/ は Disallow
      （例 /tokyo/。Allow: /tokyo/$ は完全一致のみ）なのでエリア収集に使わない。
      /area/*/entertainment|karaoke|entertainmentrest/kods00118|00119|00239/ も Disallow。
      一覧 /area/... と店舗詳細 /{shop_id}/ は許可されている。
    - 基底クラスの DELAY は「yield 1件ごと」に効くため、一覧ページ巡回には効かない。
      一覧の連続取得には LIST_DELAY で明示的に間隔を空けている。

実行方法:
    python scripts/sites/food/gnavi.py
    python bin/run_flow.py --site-id gnavi
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator, Iterator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# --- 対象エリア（ここを変えれば他県にも広げられる） ---------------------------
TARGET_PREFS: dict[str, str] = {
    "埼玉県": "saitama",
    "千葉県": "chiba",
    "東京都": "tokyo",
    "神奈川県": "kanagawa",
}

_ALL_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    "茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    "新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    "三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    "福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_HEAD = re.compile(rf"^\s*({_ALL_PREFS})")
_PREF_ANY = re.compile(rf"({_ALL_PREFS})")

# 店舗URL: https://r.gnavi.co.jp/{shop_id}/ （1セグメント・英数・数字を必ず含む）
_SHOP_URL = re.compile(r"^https://r\.gnavi\.co\.jp/([0-9a-z]{4,14})/?$")
# 1セグメントだが店舗ではない予約語
_NOT_SHOP = {
    "area", "eki", "sitemap", "search", "rs", "special", "feature",
    "plan", "coupon", "news", "ranking", "help", "guide", "about",
}

# 細分化に使える一覧URL（ジャンル別 / サブエリア別）
_LIST_GENRE = re.compile(r"^https://r\.gnavi\.co\.jp/area/[0-9a-z_]+/[0-9a-z_]+/rs/$")
_LIST_AREAM = re.compile(r"^https://r\.gnavi\.co\.jp/area/aream\d+/rs/$")

# robots.txt (User-agent: *) の Disallow に該当する一覧は巡回しない
_ROBOTS_DENY = re.compile(
    r"/area/[^/]+/(entertainment|karaoke|entertainmentrest)/kods00(118|119|239)/"
)

_HIT_LABELLED = re.compile(r"(?:該当|検索結果|全)[^0-9]{0,12}([\d,]{1,12})\s*件")
_HIT_BARE = re.compile(r"([\d,]{1,12})\s*件")
_ZIP = re.compile(r"(\d{3}-?\d{4})")
_WS = re.compile(r"\s+")


class GnaviScraper(StaticCrawler):
    """ぐるなび 飲食店情報スクレイパー"""

    # 実ブラウザに近い新しめの UA。古い Chrome94 だと bot 判定でスロットルされ
    # 店舗ページが read timeout しやすい（実測: 同一URLで full ヘッダは 200、素の
    # crawler ヘッダは timeout になる瞬間があった）。
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    DELAY = 0.5             # 基底クラスが yield 1件ごとに待つ間隔
    LIST_DELAY = 0.5        # 一覧ページを連続取得するときの間隔
    # ぐるなびの店舗ページは一定割合で read timeout する（同一URLでも別時刻には正常に
    # 応答する断続的な遅延で、特定エリアに固まって出ることがある）。既定の TIMEOUT=20 ×
    # リトライ3回 だと遅延ページ数本で 60 秒以上を浪費し、最初の1件を yield する前に
    # 制限時間を使い切る。短いタイムアウトで遅延ページを素早く見切り、次の店舗へ進む
    # （実測 2026-08: 6 秒あれば健全なページは確実に応答する）。
    TIMEOUT = 6
    MAX_PAGE = 334          # 一覧のページング上限 (実測 2026-07: p=334まで / p=400 は404)
    PER_PAGE = 30           # 一覧1ページの件数 (実測)
    SPLIT_LIMIT = 10_020    # MAX_PAGE * PER_PAGE = 1一覧URLから辿れる上限
    MAX_SPLIT_DEPTH = 3     # 都道府県 → ジャンル → サブエリア まで
    EXTRA_COLUMNS = ["アクセス", "駐車場", "エリア", "業態"]

    # 一覧起点の候補（先頭から試し、店舗URLが取れたものを採用する）
    LIST_TEMPLATES = (
        "https://r.gnavi.co.jp/area/{slug}/rs/",
        "https://r.gnavi.co.jp/{slug}/rs/",
    )

    def _setup(self):
        """基底のセッション設定に加え、読み取りタイムアウトのリトライを抑制する。

        基底の Retry(total=3) は read timeout も 3 回リトライするため、遅延ページ
        1 本で TIMEOUT×3 ＋ バックオフを消費してしまう。read は即あきらめて次の店舗へ
        進み（read=0）、接続失敗とサーバ一時エラー(5xx)のみ従来通り粘る。
        """
        super()._setup()
        retries = Retry(
            total=None, connect=2, read=0, status=3, backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        # 実ブラウザ相当のヘッダを付けて bot 判定によるスロットルを避ける
        self.session.headers.update({
            "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,*/*;q=0.8"),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

    def prepare(self):
        """一覧1ページ目の soup をキャッシュする（件数判定とページ巡回で再利用）。"""
        self._page1: dict[str, object] = {}
        self._seen_shops: set[str] = set()

    # ---------------- メイン（逐次ストリーミング） ----------------

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 起点を都県ごとに解決し、詳細取得は「全都県を1件ずつラウンドロビン」で回す。
        # ぐるなびの店舗ページは特定エリアに固まって断続的に read timeout することがあり
        # （実測 2026-08: 埼玉の1ページ目が全滅している間、東京/千葉は正常）、県を順番に
        # 処理すると先頭県の不調だけで制限時間を使い切り 0 件になる。ラウンドロビンなら
        # 健全な県から即座に結果が出る。上限超過エリアの細分化は各県ジェネレータ内で遅延実行。
        streams: list[tuple[str, Iterator[str]]] = []
        for pref_jp, slug in TARGET_PREFS.items():
            root = self._resolve_root(slug)
            if root:
                streams.append((pref_jp, self._iter_pref_shops(pref_jp, root)))
            else:
                self.logger.error(
                    "%s (%s): 一覧の起点URLを解決できませんでした（候補: %s）",
                    pref_jp, slug,
                    " / ".join(t.format(slug=slug) for t in self.LIST_TEMPLATES),
                )

        if not streams:
            # 全県で一覧が使えない＝サイト構造が変わった可能性。公式サイトマップで代替する
            self.logger.error(
                "全都県で一覧起点を解決できないため、公式サイトマップにフォールバックします"
            )
            yield from self._parse_via_sitemap()
            return

        kept = 0
        pref_kept: dict[str, int] = {p: 0 for p, _ in streams}
        active = list(streams)
        while active:
            still: list[tuple[str, Iterator[str]]] = []
            for pref_jp, gen in active:
                try:
                    shop_url = next(gen)
                except StopIteration:
                    continue
                still.append((pref_jp, gen))
                if shop_url in self._seen_shops:
                    continue
                self._seen_shops.add(shop_url)
                item = self._scrape_detail(shop_url)
                if item:
                    kept += 1
                    pref_kept[pref_jp] += 1
                    yield item
            active = still

        for pref_jp, cnt in pref_kept.items():
            self.logger.info("%s: 採用 %d 件", pref_jp, cnt)
        self.logger.info("全県完了: 採用 %d 件", kept)

    def _iter_pref_shops(self, pref_jp: str, root: str) -> Iterator[str]:
        """1都県ぶんの店舗URLを遅延生成する（本体一覧→必要なら細分化子一覧の順）。

        ジェネレータにすることで、ラウンドロビン側は「次の店舗URLが必要になった時」だけ
        一覧ページを取得する。細分化（件数上限超過エリアの子一覧展開）も本体一覧を
        流し終えて初めて評価されるため、テスト実行では実行されず最初の1件が速く出る。
        """
        listed = self._hit_count(root)
        self.logger.info(
            "=== %s: 掲載件数 %s ===",
            pref_jp, f"{listed:,}" if listed else "不明",
        )
        # まず都道府県一覧そのもの（最初の約10,020件）を流す
        yield from self._iter_shop_urls(root)
        # 上限を超える残りは、細分化した子一覧で回収する（重複は呼び出し側がURLで排除）
        if listed and listed > self.SPLIT_LIMIT:
            children = self._split_until_under_limit(root, depth=0)
            extra = [c for c in children if c != root]
            self.logger.info(
                "%s: 掲載 %d 件 > 上限 %d のため子一覧 %d 本を追加",
                pref_jp, listed, self.SPLIT_LIMIT, len(extra),
            )
            for child in extra:
                yield from self._iter_shop_urls(child)

    # ---------------- 一覧の起点解決 / サイトマップ代替 ----------------

    def _resolve_root(self, slug: str) -> str | None:
        """一覧の起点URLを候補から解決する（店舗URLが1件でも取れた候補を採用）。"""
        for tmpl in self.LIST_TEMPLATES:
            cand = tmpl.format(slug=slug)
            soup = self._list_first_page(cand)
            if soup is None:
                continue
            for a in soup.find_all("a", href=True):
                if self._is_shop_url(self._abs(a["href"])):
                    self.logger.info("一覧起点を解決: %s", cand)
                    return cand
            self.logger.warning("候補に店舗URLが無し: %s", cand)
        return None

    def _parse_via_sitemap(self) -> Generator[dict, None, None]:
        """最後の手段: robots.txt 記載の公式サイトマップから店舗URLを集めて判定する。

        全国分を辿るため件数が多い。対象4都県の判定は詳細ページの JSON-LD で行う。
        """
        import gzip
        import xml.etree.ElementTree as ET

        entries: list[str] = []
        try:
            r = self.session.get("https://r.gnavi.co.jp/robots.txt", timeout=20)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        entries.append(line.split(":", 1)[1].strip())
        except Exception as e:
            self.logger.warning("robots.txt からサイトマップを取得できません: %s", e)
        if not entries:
            entries = ["https://r.gnavi.co.jp/sitemap/sitemap_index_rs_all.xml.gz"]

        queue, visited = list(entries), set()
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        kept = 0
        while queue:
            sm = queue.pop(0)
            if sm in visited:
                continue
            visited.add(sm)
            try:
                r = self.session.get(sm, timeout=30)
                time.sleep(self.LIST_DELAY)
                if r.status_code != 200:
                    continue
                raw = r.content
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                root = ET.fromstring(raw)
            except Exception:
                continue

            for el in root.findall(".//sm:sitemap/sm:loc", ns):
                loc = (el.text or "").strip()
                if loc and loc not in visited:
                    queue.append(loc)
            for el in root.findall(".//sm:url/sm:loc", ns):
                loc = (el.text or "").strip()
                if not self._is_shop_url(loc):
                    continue
                shop = loc if loc.endswith("/") else loc + "/"
                if shop in self._seen_shops:
                    continue
                self._seen_shops.add(shop)
                item = self._scrape_detail(shop)
                if item:
                    kept += 1
                    yield item
                    if kept % 500 == 0:
                        self.logger.info("サイトマップ経路: 採用 %d 件", kept)
        self.logger.info("サイトマップ経路 完了: 採用 %d 件", kept)

    # ---------------- 一覧 ----------------

    def _list_first_page(self, list_url: str):
        """一覧URLの1ページ目を取得（キャッシュあり）。"""
        if list_url in self._page1:
            return self._page1[list_url]
        soup = self.get_soup(list_url)
        time.sleep(self.LIST_DELAY)
        if len(self._page1) > 256:
            self._page1.clear()
        self._page1[list_url] = soup
        return soup

    def _hit_count(self, list_url: str) -> int | None:
        soup = self._list_first_page(list_url)
        return self._hit_count_from_soup(soup) if soup is not None else None

    @staticmethod
    def _hit_count_from_soup(soup) -> int | None:
        text = _WS.sub(" ", soup.get_text(" ", strip=True))
        vals = []
        for m in _HIT_LABELLED.finditer(text):
            try:
                vals.append(int(m.group(1).replace(",", "")))
            except ValueError:
                pass
        if vals:
            return max(vals)
        for m in _HIT_BARE.finditer(text):
            try:
                vals.append(int(m.group(1).replace(",", "")))
            except ValueError:
                pass
        return max(vals) if vals else None

    def _split_until_under_limit(self, list_url: str, depth: int) -> list[str]:
        """該当件数が上限を超える一覧を、ページ内リンクで再帰的に細分化する。"""
        count = self._hit_count(list_url)

        if count is not None and count <= self.SPLIT_LIMIT:
            return [list_url]
        if depth >= self.MAX_SPLIT_DEPTH:
            if count:
                self.logger.warning(
                    "細分化の上限深さに到達（%s: %d 件 > %d）。取りこぼす可能性あり",
                    list_url, count, self.SPLIT_LIMIT,
                )
            return [list_url]

        children = self._child_lists(list_url)
        if not children:
            if count:
                self.logger.warning(
                    "細分化リンクが見つからず上限超過のまま巡回（%s: %d 件）", list_url, count
                )
            return [list_url]

        self.logger.info(
            "細分化: %s (%s 件) → 子一覧 %d 本 [depth=%d]",
            list_url, f"{count:,}" if count else "不明", len(children), depth,
        )
        leaves: list[str] = []
        for child in children:
            leaves.extend(self._split_until_under_limit(child, depth + 1))
        return list(dict.fromkeys(leaves))

    def _child_lists(self, list_url: str) -> list[str]:
        """一覧ページ内から、より細かい一覧URL（ジャンル別・サブエリア別）を集める。"""
        soup = self._list_first_page(list_url)
        if soup is None:
            return []
        out: dict[str, None] = {}
        for a in soup.find_all("a", href=True):
            href = self._abs(a["href"])
            if href == list_url or _ROBOTS_DENY.search(href):
                continue
            if _LIST_GENRE.match(href) or _LIST_AREAM.match(href):
                out.setdefault(href, None)
        return list(out)

    def _iter_shop_urls(self, list_url: str) -> Iterator[str]:
        """1つの一覧URLを p=1..MAX_PAGE で巡回し、店舗URLを逐次返す。"""
        for page in range(1, self.MAX_PAGE + 1):
            if page == 1:
                soup = self._list_first_page(list_url)
            else:
                soup = self.get_soup(f"{list_url}?p={page}")
                time.sleep(self.LIST_DELAY)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s (p=%d)", list_url, page)
                return

            found = 0
            for a in soup.find_all("a", href=True):
                href = self._abs(a["href"])
                if not self._is_shop_url(href):
                    continue
                found += 1
                yield href if href.endswith("/") else href + "/"
            if found == 0:
                return

    @staticmethod
    def _abs(href: str) -> str:
        href = (href or "").strip()
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return "https://r.gnavi.co.jp" + href
        return href

    @staticmethod
    def _is_shop_url(href: str) -> bool:
        m = _SHOP_URL.match(href)
        if not m:
            return False
        shop_id = m.group(1)
        if shop_id in _NOT_SHOP:
            return False
        return bool(re.search(r"\d", shop_id))

    # ---------------- 詳細: 店舗トップの JSON-LD ----------------

    def _scrape_detail(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        ld = self._find_restaurant_jsonld(soup)
        if ld is None:
            # HTMLフォールバックはしない（/map/ の050転送番号を拾う事故を防ぐため）
            self.logger.debug("JSON-LD(Restaurant) が無いためスキップ: %s", shop_url)
            return None

        name = self._clean(str(ld.get("name") or ""))
        tel = self._clean(str(ld.get("telephone") or ""))
        if not name or not tel:
            return None

        addr = ld.get("address") or {}
        if isinstance(addr, list):
            addr = addr[0] if addr else {}
        if not isinstance(addr, dict):
            addr = {}

        locality = self._clean(str(addr.get("addressLocality") or ""))
        street = self._clean(str(addr.get("streetAddress") or ""))
        post = self._clean(str(addr.get("postalCode") or ""))
        pref = self._normalize_pref(
            str(addr.get("addressRegion") or ""), f"{locality} {street}"
        )

        if pref not in TARGET_PREFS:
            return None

        # 住所は市区町村以降（addressLocality に県が含まれる表記ゆれを吸収）
        rest = _PREF_HEAD.sub("", " ".join(p for p in (locality, street) if p)).strip()

        data = {
            Schema.URL: self._clean(str(ld.get("url") or "")) or shop_url,
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.PREF: pref,
            Schema.ADDR: rest,
        }
        zm = _ZIP.search(post)
        if zm:
            data[Schema.POST_CODE] = zm.group(1)

        cuisine = ld.get("servesCuisine")
        if isinstance(cuisine, list):
            cuisine = " / ".join(str(c) for c in cuisine if c)
        if cuisine:
            data[Schema.CAT_SITE] = self._clean(str(cuisine))
        return data

    @staticmethod
    def _find_restaurant_jsonld(soup) -> dict | None:
        """@type="Restaurant" 系の構造化データを探す（配列・@graph も走査）。"""
        def walk(node):
            if isinstance(node, dict):
                t = node.get("@type")
                types = t if isinstance(t, list) else [t]
                for x in types:
                    if not x:
                        continue
                    sx = str(x)
                    if sx.endswith("Restaurant") or sx in ("FoodEstablishment", "LocalBusiness"):
                        return node
                for key in ("@graph", "mainEntity", "itemListElement"):
                    if key in node:
                        found = walk(node[key])
                        if found:
                            return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        best = None
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text() or ""
            try:
                parsed = json.loads(raw)
            except (ValueError, json.JSONDecodeError):
                continue
            found = walk(parsed)
            if found is None:
                continue
            if found.get("telephone"):   # telephone を持つものを優先
                return found
            best = best or found
        return best

    @staticmethod
    def _normalize_pref(region: str, addr_text: str) -> str:
        """addressRegion の表記ゆれ（「東京」等）と欠損を吸収する。"""
        region = _WS.sub("", region or "")
        if region in TARGET_PREFS:
            return region
        if region:
            for suffix in ("都", "道", "府", "県"):
                if region + suffix in TARGET_PREFS:
                    return region + suffix
            m = _PREF_ANY.search(region)
            if m:
                return m.group(1)
        m = _PREF_ANY.search(addr_text or "")
        return m.group(1) if m else ""

    @staticmethod
    def _clean(text: str) -> str:
        return _WS.sub(" ", (text or "")).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GnaviScraper()
    scraper.execute("https://www.gnavi.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ぐるなび（43道府県） — 飲食店情報スクレイパー (site_id: gnavi_3)

対象サイト: https://www.gnavi.co.jp/  (一覧・店舗詳細は r.gnavi.co.jp)

取得対象:
    - ぐるなび掲載の飲食店のうち、東京都・神奈川県・埼玉県・千葉県を除く43道府県
      （1都3県は既存サイト gnavi が担当。こちらでは取得しない）
    - 取得URL / 名称 / 都道府県 / 郵便番号 / 住所 / TEL / サイト定義業種・ジャンル
      （取得日時は Pipeline が自動付与）

取得フロー（逐次ストリーミング・1並列）:
    1. 引数 url（https://www.gnavi.co.jp/）のホストから一覧ホスト r.gnavi.co.jp を導出し、
       県別一覧 /area/{slug}/rs/ を起点にする
    2. 一覧は「1ページ30件 / p=334 が上限」なので、該当件数（「が N件見つかりました」）が
       上限を超える一覧はページ内のエリア別一覧（県→areal→aream。互いに素）へ分割する
    3. 一覧を1ページ読むごとに、各店舗トップ /{shop_id}/ の JSON-LD(Restaurant) を取得して
       即 yield する。県同士は1件ずつラウンドロビンで進める（1県の一時的な遅延で全体が
       止まらないようにするため。並列処理ではない）

県ごとの保存と再開（チェックポイント）:
    - output/gnavi_3_checkpoint/{slug}.jsonl  … その県で取得済みの行（1行=1店舗）
    - output/gnavi_3_checkpoint/{slug}.state.json … 一覧URLごとの完了ページ番号と県の完了フラグ
    - 途中停止後の再実行では、保存済みの行を最初に再出力し（最終CSVは close() 時にしか
      作られないため）、完了済みの県・一覧ページは取得をスキップして続きから巡回する
    - 全県が完了したら finalize() でチェックポイントを削除する（次回は最初から取り直す）。
      CHECKPOINT_MAX_AGE_DAYS を過ぎた古いチェックポイントも破棄する

robots.txt (User-agent: *) の遵守:
    - 都道府県ディレクトリ /{pref}/ は Disallow → 使わない（/area/{slug}/rs/ を使う）
    - 検索パラメータ付き一覧（?area= ?cuisine= ?sort= など）と sc_type= 等の計測パラメータは
      Disallow → 使わない。ページ送りの ?p=N のみ使用（Disallow 対象外）
    - /api/ /rsapi/ /restpage_api/ /rest/ 等の API 系パスは使わない
    - /area/*/entertainment|karaoke|entertainmentrest/kods00118|00119|00239/ も巡回しない

注意:
    - User-Agent はブラウザを偽装しないクローラ UA（NetHarvestBot/1.0）
    - TEL は店舗トップの JSON-LD `telephone` のみ。/map/ の電話番号はネット予約用の
      050転送番号なので使わない（既存 gnavi の調査結果）
    - リクエスト間隔は REQUEST_INTERVAL（2秒）で一覧・詳細すべてに適用する。基底の
      DELAY は yield ごとの待機なので、チェックポイント再出力が遅くならないよう
      ITEM_DELAY=0 にして、待機は通信の直前で行う

実行方法:
    python scripts/sites/food/gnavi_3.py
"""

import json
import re
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, Iterator
from urllib.parse import urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# --- 対象43道府県（1都3県を除く。並びは北から） --------------------------------
TARGET_PREFS: dict[str, str] = {
    "北海道": "hokkaido", "青森県": "aomori", "岩手県": "iwate", "宮城県": "miyagi",
    "秋田県": "akita", "山形県": "yamagata", "福島県": "fukushima",
    "茨城県": "ibaraki", "栃木県": "tochigi", "群馬県": "gunma",
    "新潟県": "niigata", "富山県": "toyama", "石川県": "ishikawa", "福井県": "fukui",
    "山梨県": "yamanashi", "長野県": "nagano", "岐阜県": "gifu", "静岡県": "shizuoka",
    "愛知県": "aichi", "三重県": "mie", "滋賀県": "shiga", "京都府": "kyoto",
    "大阪府": "osaka", "兵庫県": "hyogo", "奈良県": "nara", "和歌山県": "wakayama",
    "鳥取県": "tottori", "島根県": "shimane", "岡山県": "okayama", "広島県": "hiroshima",
    "山口県": "yamaguchi", "徳島県": "tokushima", "香川県": "kagawa", "愛媛県": "ehime",
    "高知県": "kochi", "福岡県": "fukuoka", "佐賀県": "saga", "長崎県": "nagasaki",
    "熊本県": "kumamoto", "大分県": "oita", "宮崎県": "miyazaki", "鹿児島県": "kagoshima",
    "沖縄県": "okinawa",
}
# 既存サイト gnavi が担当する1都3県（こちらでは出力しない）
EXCLUDED_PREFS = {"東京都", "神奈川県", "埼玉県", "千葉県"}

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

# 店舗URLのパス: /{shop_id}/ （1セグメント・英数・数字を必ず含む）
_SHOP_PATH = re.compile(r"^/([0-9a-z]{4,14})/$")
_NOT_SHOP = {
    "area", "eki", "sitemap", "search", "rs", "special", "feature",
    "plan", "coupon", "news", "ranking", "help", "guide", "about",
}
# エリア別一覧（県ページに areal、areal ページに aream が並ぶ。互いに素）
_LIST_AREA_PATH = re.compile(r"^/area/area[a-z]\d+/rs/$")
# robots.txt で Disallow の一覧
_ROBOTS_DENY = re.compile(
    r"/area/[^/]+/(entertainment|karaoke|entertainmentrest)/kods00(118|119|239)/"
)

_HIT_SUMMARY = re.compile(r"が([\d,]{1,12})件見つかりました")
_ZIP = re.compile(r"(\d{3}-?\d{4})")
_WS = re.compile(r"\s+")


class Gnavi3Scraper(StaticCrawler):
    """ぐるなび 43道府県 飲食店情報スクレイパー"""

    # UA 偽装はしない（ブラウザを名乗らないクローラ UA）。基盤既定の Chrome/94 UA は
    # 店舗ページで read timeout（tarpit）になるが、クローラ UA なら即 200 が返る（2026-11 実測）
    USER_AGENT = "NetHarvestBot/1.0"

    DELAY = 2.0             # 通信間隔の基準（実際の待機は REQUEST_INTERVAL で通信直前に行う）
    ITEM_DELAY = 0.0        # yield ごとの待機は不要（チェックポイント再出力を速くするため）
    REQUEST_INTERVAL = 2.0  # 一覧・詳細を含む全リクエストの最小間隔（秒）
    # 店舗ページは断続的に read timeout することがある（既存 gnavi の調査結果）。
    # 長く待たずに見切って次の店舗へ進み、失敗分は県の最後に1回だけ再試行する。
    TIMEOUT = 10
    MAX_PAGE = 334          # 一覧のページング上限 (p=334 まで)
    SPLIT_LIMIT = 10_020    # 1一覧URLから辿れる上限 (334 × 30)
    MAX_SPLIT_DEPTH = 3
    LIST_RETRY = 3          # 一覧ページ取得失敗時の試行回数
    CHECKPOINT_MAX_AGE_DAYS = 30

    def _setup(self):
        """read timeout の多重リトライを抑止し、標準的な Accept 系ヘッダを付ける。"""
        super()._setup()
        retries = Retry(
            total=None, connect=2, read=0, status=3, backoff_factor=1.0,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,*/*;q=0.8"),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

    def prepare(self):
        self._page1: dict[str, object] = {}
        self._seen_shops: set[str] = set()
        self._last_request = 0.0
        self._ckpt_dir = Path(self.local_output_dir) / "gnavi_3_checkpoint"
        self._states: dict[str, dict] = {}
        self._all_done = False

    # ---------------- メイン ----------------

    def parse(self, url: str) -> Generator[dict, None, None]:
        parts = urlsplit(url)
        host = parts.netloc.split(":", 1)[0]
        host = host[4:] if host.startswith("www.") else host
        self._base = f"{parts.scheme or 'https'}://r.{host}"

        self._load_or_reset_checkpoints()

        # 1) 保存済みの行を再出力（最終CSVは close() 時にのみ生成されるため）
        replayed = 0
        for pref_jp, slug in TARGET_PREFS.items():
            for row in self._read_rows(slug):
                shop = row.get(Schema.URL, "")
                if shop in self._seen_shops:
                    continue
                self._seen_shops.add(shop)
                replayed += 1
                yield row
        if replayed:
            self.logger.info("チェックポイントから %d 件を再出力しました", replayed)

        # 2) 未完了の県をラウンドロビンで巡回（1件ずつ交互に。並列ではない）
        streams = [
            (pref_jp, slug, self._iter_pref_shops(pref_jp, slug))
            for pref_jp, slug in TARGET_PREFS.items()
            if not self._states[slug].get("done")
        ]
        failed: dict[str, list[str]] = {slug: [] for _, slug, _ in streams}
        kept = 0
        active = list(streams)
        while active:
            still = []
            for pref_jp, slug, gen in active:
                try:
                    shop_url = next(gen)
                except StopIteration:
                    continue
                still.append((pref_jp, slug, gen))
                if shop_url in self._seen_shops:
                    continue
                item, ok = self._scrape_detail(shop_url)
                if not ok:
                    failed[slug].append(shop_url)
                    continue
                self._seen_shops.add(shop_url)
                if item:
                    self._append_row(slug, item)
                    kept += 1
                    yield item
            active = still

        # 3) 取得に失敗した店舗を1回だけ再試行し、県の完了を記録する
        for pref_jp, slug, _ in streams:
            for shop_url in failed[slug]:
                if shop_url in self._seen_shops:
                    continue
                item, ok = self._scrape_detail(shop_url)
                if not ok:
                    self.logger.warning("%s: 店舗ページ取得失敗のためスキップ: %s", pref_jp, shop_url)
                    continue
                self._seen_shops.add(shop_url)
                if item:
                    self._append_row(slug, item)
                    kept += 1
                    yield item
            state = self._states[slug]
            if not state.get("list_failed"):
                state["done"] = True
                self._save_state(slug)
                self.logger.info("%s: 完了", pref_jp)
            else:
                self.logger.warning("%s: 一覧の取得失敗があるため未完了のまま（次回続きから）", pref_jp)

        self._all_done = all(s.get("done") for s in self._states.values())
        self.logger.info("新規取得 %d 件 / 再出力 %d 件 / 全県完了=%s", kept, replayed, self._all_done)

    def finalize(self):
        # 全県が完了したらチェックポイントを消し、次回の定期実行は最初から取り直す
        if getattr(self, "_all_done", False) and self._ckpt_dir.exists():
            shutil.rmtree(self._ckpt_dir, ignore_errors=True)
            self.logger.info("全県完了のためチェックポイントを削除しました")

    # ---------------- 県ごとの巡回 ----------------

    def _iter_pref_shops(self, pref_jp: str, slug: str) -> Iterator[str]:
        root = f"{self._base}/area/{slug}/rs/"
        state = self._states[slug]
        state["list_failed"] = False
        yield from self._iter_split(root, slug, depth=0)

    def _iter_split(self, list_url: str, slug: str, depth: int) -> Iterator[str]:
        state = self._states[slug]
        if state["pages"].get(list_url, 0) >= self.MAX_PAGE:
            return  # 前回までに巡回完了済みの一覧
        soup = self._list_first_page(list_url)
        if soup is None:
            state["list_failed"] = True
            return
        count = self._hit_count(soup)
        if depth == 0:
            self.logger.info("=== %s: 掲載件数 %s ===", slug, f"{count:,}" if count else "不明")
        if count is None or count <= self.SPLIT_LIMIT or depth >= self.MAX_SPLIT_DEPTH:
            yield from self._iter_shop_urls(list_url, slug)
            return
        children = self._child_lists(soup, list_url)
        if not children:
            self.logger.warning("細分化できず上限超過のまま巡回: %s (%d 件)", list_url, count)
            yield from self._iter_shop_urls(list_url, slug)
            return
        self.logger.info("細分化: %s (%s 件) → 子一覧 %d 本", list_url, f"{count:,}", len(children))
        for child in children:
            yield from self._iter_split(child, slug, depth + 1)

    def _iter_shop_urls(self, list_url: str, slug: str) -> Iterator[str]:
        """一覧を p=1..MAX_PAGE で巡回する。ページ単位で完了を記録し、再開時は続きから。"""
        state = self._states[slug]
        start = state["pages"].get(list_url, 0) + 1
        for page in range(start, self.MAX_PAGE + 1):
            if page == 1:
                soup = self._list_first_page(list_url)
            else:
                soup = self._get_list(f"{list_url}?p={page}")
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s (p=%d)", list_url, page)
                state["list_failed"] = True
                return
            shops = self._shop_urls(soup)
            if not shops:
                break
            for shop in shops:
                yield shop
            # このページの全店舗を処理し終えた（呼び出し側が次を要求した）時点で記録
            state["pages"][list_url] = page
            self._save_state(slug)
        state["pages"][list_url] = self.MAX_PAGE  # 一覧の終端まで到達
        self._save_state(slug)

    # ---------------- 通信 ----------------

    def _fetch(self, url: str):
        """全リクエストの間隔を REQUEST_INTERVAL 以上空けて get_soup する。"""
        wait = self.REQUEST_INTERVAL - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            self._last_request = time.monotonic()
        return soup

    def _get_list(self, url: str):
        for attempt in range(self.LIST_RETRY):
            soup = self._fetch(url)
            if soup is not None:
                return soup
            time.sleep(min(2 ** (attempt + 2), 30))
        return None

    def _list_first_page(self, list_url: str):
        if list_url in self._page1:
            return self._page1[list_url]
        soup = self._get_list(list_url)
        if len(self._page1) > 64:
            self._page1.clear()
        if soup is not None:
            self._page1[list_url] = soup
        return soup

    # ---------------- 一覧の解析 ----------------

    @staticmethod
    def _hit_count(soup) -> int | None:
        text = _WS.sub("", soup.get_text("", strip=True))
        m = _HIT_SUMMARY.search(text)
        return int(m.group(1).replace(",", "")) if m else None

    def _child_lists(self, soup, list_url: str) -> list[str]:
        areas: dict[str, None] = {}
        for a in soup.find_all("a", href=True):
            path = self._path(a["href"])
            if path is None or _ROBOTS_DENY.search(path):
                continue
            child = f"{self._base}{path}"
            if child != list_url and _LIST_AREA_PATH.match(path):
                areas.setdefault(child, None)
        return list(areas)

    def _shop_urls(self, soup) -> list[str]:
        shops: dict[str, None] = {}
        for a in soup.find_all("a", href=True):
            path = self._path(a["href"])
            if path is None:
                continue
            m = _SHOP_PATH.match(path)
            if not m or m.group(1) in _NOT_SHOP or not re.search(r"\d", m.group(1)):
                continue
            shops.setdefault(f"{self._base}{path}", None)
        return list(shops)

    def _path(self, href: str) -> str | None:
        """r.gnavi.co.jp 内のリンクのパスだけを返す（?sc_type= 等のクエリは落とす）。"""
        href = (href or "").strip().split("#", 1)[0].split("?", 1)[0]
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith(self._base):
            href = href[len(self._base):]
        if not href.startswith("/"):
            return None
        return href

    # ---------------- 詳細: 店舗トップの JSON-LD ----------------

    def _scrape_detail(self, shop_url: str) -> tuple[dict | None, bool]:
        """(item, 取得成功か) を返す。対象外・JSON-LD無しは (None, True)。"""
        soup = self._fetch(shop_url)
        if soup is None:
            return None, False
        ld = self._find_restaurant_jsonld(soup)
        if ld is None:
            self.logger.debug("JSON-LD(Restaurant) 無し: %s", shop_url)
            return None, True

        name = self._clean(str(ld.get("name") or ""))
        if not name:
            return None, True

        addr = ld.get("address") or {}
        if isinstance(addr, list):
            addr = addr[0] if addr else {}
        if not isinstance(addr, dict):
            addr = {}
        locality = self._clean(str(addr.get("addressLocality") or ""))
        street = self._clean(str(addr.get("streetAddress") or ""))
        post = self._clean(str(addr.get("postalCode") or ""))
        pref = self._normalize_pref(str(addr.get("addressRegion") or ""), f"{locality} {street}")

        # 1都3県（既存 gnavi の担当）と都道府県不明は出力しない
        if pref not in TARGET_PREFS:
            return None, True

        rest = _PREF_HEAD.sub("", " ".join(p for p in (locality, street) if p)).strip()
        cuisine = ld.get("servesCuisine")
        if isinstance(cuisine, list):
            cuisine = " / ".join(str(c) for c in cuisine if c)
        zm = _ZIP.search(post)

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: zm.group(1) if zm else "",
            Schema.ADDR: rest,
            Schema.TEL: self._clean(str(ld.get("telephone") or "")),
            Schema.CAT_SITE: self._clean(str(cuisine or "")),
        }, True

    @staticmethod
    def _find_restaurant_jsonld(soup) -> dict | None:
        def walk(node):
            if isinstance(node, dict):
                t = node.get("@type")
                for x in (t if isinstance(t, list) else [t]):
                    sx = str(x or "")
                    if sx.endswith("Restaurant") or sx in ("FoodEstablishment", "LocalBusiness"):
                        return node
                for key in ("@graph", "mainEntity"):
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
            try:
                parsed = json.loads(script.string or script.get_text() or "")
            except ValueError:
                continue
            found = walk(parsed)
            if found is None:
                continue
            if found.get("telephone"):
                return found
            best = best or found
        return best

    @staticmethod
    def _normalize_pref(region: str, addr_text: str) -> str:
        region = _WS.sub("", region or "")
        m = _PREF_ANY.search(region)
        if m:
            return m.group(1)
        for suffix in ("都", "道", "府", "県"):
            if region and _PREF_ANY.fullmatch(region + suffix):
                return region + suffix
        m = _PREF_ANY.search(addr_text or "")
        return m.group(1) if m else ""

    @staticmethod
    def _clean(text: str) -> str:
        return _WS.sub(" ", (text or "")).strip()

    # ---------------- チェックポイント（県ごとの保存・再開） ----------------

    def _load_or_reset_checkpoints(self):
        self._ckpt_dir.mkdir(parents=True, exist_ok=True)
        meta_path = self._ckpt_dir / "_meta.json"
        meta = {}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except ValueError:
                meta = {}
        started = meta.get("started_at")
        expired = True
        if started:
            try:
                age = datetime.now() - datetime.fromisoformat(started)
                expired = age > timedelta(days=self.CHECKPOINT_MAX_AGE_DAYS)
            except ValueError:
                pass
        if expired:
            if started:
                self.logger.info("古いチェックポイント(%s開始)を破棄して最初から取得します", started)
            shutil.rmtree(self._ckpt_dir, ignore_errors=True)
            self._ckpt_dir.mkdir(parents=True, exist_ok=True)
            meta_path.write_text(
                json.dumps({"started_at": datetime.now().isoformat(timespec="seconds")}),
                encoding="utf-8",
            )
        for slug in TARGET_PREFS.values():
            path = self._ckpt_dir / f"{slug}.state.json"
            state = {"pages": {}, "done": False}
            if path.exists():
                try:
                    state.update(json.loads(path.read_text(encoding="utf-8")))
                except ValueError:
                    self.logger.warning("状態ファイルを読めないため県を最初から: %s", path)
            self._states[slug] = state

    def _save_state(self, slug: str):
        path = self._ckpt_dir / f"{slug}.state.json"
        tmp = path.with_suffix(".tmp")
        state = {k: v for k, v in self._states[slug].items() if k != "list_failed"}
        tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    def _append_row(self, slug: str, item: dict):
        with open(self._ckpt_dir / f"{slug}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    def _read_rows(self, slug: str) -> Iterator[dict]:
        path = self._ckpt_dir / f"{slug}.jsonl"
        if not path.exists():
            return
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue  # 書き込み途中で停止した最終行など
                if isinstance(row, dict):
                    yield row


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Gnavi3Scraper()
    scraper.execute("https://www.gnavi.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

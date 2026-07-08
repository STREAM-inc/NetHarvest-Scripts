"""
一般社団法人日本自動車整備振興会連合会 (JASPA) — 地区ブロック配下の整備振興会HPを
BFS巡回して「整備工場（店舗）」情報を収集するクローラー。

取得対象:
    - 整備工場（店舗）の 名称 / 住所 / TEL / FAX
      ※ 振興会（自動車整備振興会）そのものの情報は取得対象外（除外フィルタで落とす）

取得フロー:
    1. 引数 url（例: .../association/districts/hokkaido.html）= 地区ブロックの一覧ページ。
       ここに「○○地方自動車整備振興会」ごとの 名称・住所・TEL・HP(URL) が表で並ぶ。
    2. 各振興会の HP を起点に、同一ホスト内を BFS（幅優先）で巡回する。
       「工場検索 / 会員 / shop / list」等のシグナルを持つリンクを優先し、
       深さ・ページ数に上限を設けて過負荷を防ぐ（= 同時実行/巡回量の制限）。
    3. 各ページから TEL を起点に「名称+住所+TEL」の店舗ブロックを抽出し、
       振興会自身の連絡先やナビ要素は除外して 1 件ずつ即 yield する。

同時実行/負荷の制限:
    - フレームワークのキャッシュはインスタンス属性を共有しスレッドセーフでないため、
      巡回は逐次（単一ワーカー）で行う。代わりに MAX_DEPTH / MAX_PAGES_PER_ASSOC /
      MAX_SHOPS_PER_ASSOC で 1 振興会あたりの巡回量を厳格に上限化し、
      「ワーカーが増えすぎてプログラムが落ちる」事態を防ぐ。

実行方法:
    python bin/smoke_test.py scripts/sites/auto/jaspa.py \
        "https://www.jaspa.or.jp/association/districts/hokkaido.html" --limit 3 --timeout 90
    python scripts/sites/auto/jaspa.py
    docker compose exec worker python /app/bin/run_flow.py --site-id jaspa
"""

import re
import sys
from collections import deque
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 電話/FAX 番号 (市外局番始まり)。前後の数字連結は除外。
_TEL_RE = re.compile(r"(?<![\d-])0\d{1,4}[-－]\d{1,4}[-－]\d{3,4}(?![\d-])")
# 郵便番号 (電話番号の一部を誤検出しないよう 〒 マーカー必須)
_POST_RE = re.compile(r"〒\s*(\d{3}[-－]?\d{4})")
# 住所らしさ (市区郡町村を含む)
_ADDR_HINT_RE = re.compile(r"[市区郡町村]")
# 47 都道府県
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 振興会HPへのリンク検出用: BFS で優先的にたどるシグナル
_LINK_HREF_SIGNAL = re.compile(
    r"shop|kojo|kojyo|kojou|list|member|kaiin|search|kensaku|map|seibi|koujou|factory",
    re.I,
)
_LINK_TEXT_SIGNAL = re.compile(r"工場|検索|会員|整備|店舗|事業場|マップ|一覧|指定|認証")
# BFS で除外するパス (ログイン/会員専用/バイナリ等)
_SKIP_PATH_RE = re.compile(
    r"lognin|/member/sec|/pages/|login|logout|/wp-admin|"
    r"\.(pdf|jpe?g|png|gif|svg|css|js|zip|xls|xlsx|doc|docx|mp4|ico)(\?|$)",
    re.I,
)
# 店舗名として不適格 (振興会/組合/ナビ等)
_BAD_NAME_RE = re.compile(
    r"自動車整備振興会|整備事業協同組合|振興会|協同組合|連合会|JASPA|"
    r"お問|問い?合|サイトマップ|ログイン|会員専用|ページの先頭|MENU|HOME$|"
    r"検索結果|一覧$|Copyright|All Rights"
)


class JaspaScraper(StaticCrawler):
    """JASPA 地区ブロック → 各整備振興会HP を BFS 巡回して整備工場情報を収集する。"""

    DELAY = 1.0
    # --- 1 振興会あたりの巡回量上限（過負荷/暴走防止 = 同時実行の代替制御） ---
    MAX_DEPTH = 2            # HP から最大 2 ホップ (HP → 一覧/検索 → 詳細)
    MAX_PAGES_PER_ASSOC = 40  # 1 振興会あたり取得するページ数の上限
    MAX_SHOPS_PER_ASSOC = 800  # 1 振興会あたり yield する店舗数の上限

    EXTRA_COLUMNS = ["整備振興会", "FAX"]

    # ------------------------------------------------------------------ #
    #  取得ユーティリティ (エンコーディング自動判定つき get_soup)
    # ------------------------------------------------------------------ #
    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        """URL を取得し、文字コード (utf-8 / cp932 / euc-jp) を判定して soup を返す。

        振興会HPは Shift_JIS(cp932) / EUC-JP / UTF-8 が混在するため、
        StaticCrawler 既定の apparent_encoding では機種依存文字(㈱等)が化ける。
        ここで meta/ヘッダの charset を優先し、Shift 系は cp932 に正規化する。
        """
        def _fetch() -> str:
            self.logger.info("取得中: %s", url)
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return self._decode(resp)

        try:
            html = self._fetch_html_cached(url, variant="", fetcher=_fetch)
            if html is None:
                return None
            return bs4.BeautifulSoup(html, "html.parser")
        except requests.exceptions.RequestException as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("通信エラー (スキップ): %s — %s", url, e)
                return None
            raise

    @staticmethod
    def _decode(resp: requests.Response) -> str:
        raw = resp.content
        hint = ""
        ctype = resp.headers.get("Content-Type", "").lower()
        m = re.search(r"charset=([\w\-]+)", ctype)
        if m:
            hint = m.group(1).lower()
        else:
            head = raw[:3000].lower()
            hm = re.search(rb"charset=[\"']?([\w\-]+)", head)
            if hm:
                hint = hm.group(1).decode("ascii", "ignore").lower()
        if hint in ("shift_jis", "shift-jis", "sjis", "x-sjis", "windows-31j", "cp932", "ms932"):
            enc = "cp932"
        elif hint in ("euc-jp", "eucjp", "x-euc-jp"):
            enc = "euc-jp"
        elif hint in ("utf-8", "utf8"):
            enc = "utf-8"
        else:
            ap = (resp.apparent_encoding or "utf-8").lower()
            enc = "cp932" if ap in ("shift_jis", "sjis", "windows-1252") else ap
        try:
            return raw.decode(enc, errors="replace")
        except LookupError:
            return raw.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------ #
    #  Phase A: 地区ブロックページから各整備振興会 (名称/HP/TEL) を発見
    # ------------------------------------------------------------------ #
    def _discover_associations(self, soup: bs4.BeautifulSoup, base_url: str) -> list[dict]:
        assocs: list[dict] = []
        seen_hp: set[str] = set()
        # 振興会ごとの table.b_normal を優先。無ければ全 table を対象。
        tables = soup.select("table.b_normal") or soup.find_all("table")
        for tbl in tables:
            text = tbl.get_text("\n", strip=True)
            name_m = re.search(r"([^\n]*?自動車整備振興会)", text)
            if not name_m:
                continue
            hp = None
            for a in tbl.find_all("a", href=True):
                href = a["href"].strip()
                if not href.startswith("http"):
                    continue
                host = urlparse(href).netloc
                if "jaspa.or.jp" in host or not host:
                    continue
                hp = href
                break
            if not hp or hp in seen_hp:
                continue
            seen_hp.add(hp)
            tel_m = _TEL_RE.search(text)
            assocs.append({
                "name": re.sub(r"\s+", " ", name_m.group(1)).strip(),
                "hp": hp,
                "tel": tel_m.group(0).replace("－", "-") if tel_m else "",
            })
        return assocs

    # ------------------------------------------------------------------ #
    #  Phase B: 1 振興会 HP を BFS 巡回して整備工場を抽出
    # ------------------------------------------------------------------ #
    def _crawl_association(self, assoc: dict, assoc_tels: set[str]) -> Generator[dict, None, None]:
        start = assoc["hp"]
        host = urlparse(start).netloc
        queue: deque = deque([(start, 0)])
        seen_urls: set[str] = {start}
        seen_shops: set[tuple] = set()
        pages = 0
        yielded = 0

        while queue and pages < self.MAX_PAGES_PER_ASSOC and yielded < self.MAX_SHOPS_PER_ASSOC:
            page_url, depth = queue.popleft()
            pages += 1
            soup = self.get_soup(page_url)
            if soup is None:
                continue

            for shop in self._extract_shops(soup, page_url, assoc_tels):
                key = (shop.get(Schema.NAME, ""), shop.get(Schema.TEL, ""))
                if key in seen_shops:
                    continue
                seen_shops.add(key)
                shop["整備振興会"] = assoc["name"]
                yielded += 1
                yield shop
                if yielded >= self.MAX_SHOPS_PER_ASSOC:
                    break

            if depth >= self.MAX_DEPTH:
                continue
            # 子リンクをスコア付けして優先度順にキューへ
            scored: list[tuple] = []
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue
                if _SKIP_PATH_RE.search(href):
                    continue
                full = urljoin(page_url, href).split("#")[0]
                if urlparse(full).netloc != host or full in seen_urls:
                    continue
                score = 0
                if _LINK_HREF_SIGNAL.search(urlparse(full).path):
                    score += 2
                if _LINK_TEXT_SIGNAL.search(a.get_text(" ", strip=True)):
                    score += 1
                # HP直下(depth 0)は全リンク許可、以降はシグナルのあるものだけ辿る
                if depth >= 1 and score == 0:
                    continue
                scored.append((score, full))
            scored.sort(key=lambda x: x[0], reverse=True)
            for _score, full in scored:
                if full not in seen_urls:
                    seen_urls.add(full)
                    queue.append((full, depth + 1))

    def _extract_shops(self, soup: bs4.BeautifulSoup, url: str, assoc_tels: set[str]) -> list[dict]:
        """ページ内の TEL を起点に「名称+住所+TEL」ブロックを抽出する。"""
        work = bs4.BeautifulSoup(str(soup), "html.parser")
        for tag in work(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        blocks: dict[int, bs4.Tag] = {}
        for node in work.find_all(string=_TEL_RE):
            blk = node.parent
            if blk is None:
                continue
            # TEL を含みつつ「住所らしさ」を持つ最小ブロックまで登る
            for _ in range(6):
                if blk.parent is None:
                    break
                btxt = blk.get_text(" ", strip=True)
                if _ADDR_HINT_RE.search(btxt) and len(btxt) < 500:
                    break
                blk = blk.parent
            blocks.setdefault(id(blk), blk)

        results: list[dict] = []
        for blk in blocks.values():
            item = self._parse_block(blk, url, assoc_tels)
            if item:
                results.append(item)
        return results

    def _parse_block(self, blk: bs4.Tag, url: str, assoc_tels: set[str]) -> dict | None:
        btext = blk.get_text("\n", strip=True)
        lines = [ln.strip() for ln in btext.split("\n") if ln.strip()]

        tels = [t.replace("－", "-") for t in _TEL_RE.findall(btext)]
        if not tels:
            return None
        # FAX 判定: 'FAX' ラベル直近の番号を FAX とみなす
        fax = ""
        fm = re.search(r"(?:FAX|ＦＡＸ|ﾌｧｸｽ|Fax)[^0-9]{0,4}(0\d{1,4}[-－]\d{1,4}[-－]\d{3,4})", btext)
        if fm:
            fax = fm.group(1).replace("－", "-")
        tel = next((t for t in tels if t != fax), tels[0])
        # 振興会自身の代表番号は店舗ではないので除外
        if tel in assoc_tels:
            return None

        # 名称: 見出し優先 → 強調 → 先頭の非住所/非電話行
        name = ""
        head = blk.find(["h1", "h2", "h3", "h4"])
        if head:
            name = head.get_text(" ", strip=True)
        if not name:
            strong = blk.find(["strong", "b", "a"])
            if strong:
                name = strong.get_text(" ", strip=True)
        if not name:
            for ln in lines:
                if _TEL_RE.search(ln) or "〒" in ln or _ADDR_HINT_RE.search(ln):
                    continue
                if re.fullmatch(r"[\d\s\-：:／/（）()【】]+", ln):
                    continue
                name = ln
                break
        name = re.sub(r"\s+", " ", name).strip()
        if not name or len(name) < 2 or _BAD_NAME_RE.search(name):
            return None

        # 郵便番号 / 住所
        post = ""
        pm = _POST_RE.search(btext)
        if pm:
            post = pm.group(1).replace("－", "-")
        addr = ""
        for ln in lines:
            if ln == name or _TEL_RE.search(ln):
                continue
            cleaned = _POST_RE.sub("", ln).strip("　 ")
            if _ADDR_HINT_RE.search(cleaned) and "振興会" not in cleaned and "組合" not in cleaned:
                addr = cleaned
                break
        if not addr and not post:
            # 住所も郵便番号も取れないブロックは店舗情報として弱すぎるため除外
            return None

        pref = ""
        pfm = _PREF_RE.match(addr)
        if pfm:
            pref = pfm.group(1)
            addr = addr[pfm.end():].strip()

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            "FAX": fax,
        }
        return item

    # ------------------------------------------------------------------ #
    #  エントリポイント
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("地区ブロックページを取得できませんでした: %s", url)
            return

        assocs = self._discover_associations(soup, url)
        self.logger.info("整備振興会を %d 件発見: %s", len(assocs), [a["name"] for a in assocs])
        if not assocs:
            return

        assoc_tels = {a["tel"] for a in assocs if a["tel"]}
        # 進捗表示: 件数は事前に確定できないため未設定 (None)
        self.total_items = None

        for assoc in assocs:
            self.logger.info("巡回開始: %s (%s)", assoc["name"], assoc["hp"])
            try:
                yield from self._crawl_association(assoc, assoc_tels)
            except requests.exceptions.RequestException as e:
                self.logger.warning("振興会HP巡回エラー (%s): %s", assoc["hp"], e)
                continue


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JaspaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jaspa.or.jp/association/districts/hokkaido.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

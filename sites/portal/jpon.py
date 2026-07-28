"""
住所でポン! 2012年版 — 電話帳ディレクトリ (jpon.xyz)

取得対象:
    - 2012年版電話帳 全47都道府県 × 市区町村 × 町字単位 の全レコード

取得フロー:
    1. トップ /2012/index.html から47都道府県の URL を収集
    2. 各都道府県 /2012/{pref_id}/index.html から市区町村 URL を収集
    3. 各市区町村 /2012/{pref_id}/{city_id}/index.html から町字 URL を収集
    4. 各町字 /2012/{pref_id}/{city_id}/{district_id}.html?all で全件を一括取得
       - 表示はマスク (03-3795-****) だが、a[href="/s/2012/{phone}"] に完全な電話番号
       - addressRegion / addressLocality は span の content 属性に完全値
       - 町名は h2 の階層 ("東京都 世田谷区 三宿 の電話帳") から抽出
       - span.entry = 転入 / span.exit = 転出 / 無印 = 現役

実行方法:
    python scripts/sites/portal/jpon.py
    python bin/run_flow.py --site-id jpon

規模 (実測サンプリング 300ページに基づく):
    - 町字ページ 307,463件 (47都道府県 / 1,897市区町村)
    - 1ページ平均 25.5レコード → 全国 約780万レコード
    - URLリストには 404 が約12%混在する (サイト側で消えたページ)

注意:
    - サイトは過剰アクセスのレート制限を明示している。DELAY=3.0 推奨。
      DELAY は「ページ取得の間隔」として _fetch() 内で適用する。
      レコード単位で待つと 780万 × 3秒 = 270日規模になり完走できないため、
      ITEM_DELAY = 0 で基底クラスのアイテム間ウェイトを無効化している。
    - ステルス対策として ブラウザ風 Accept-Language / Accept-Encoding 等を付与。
    - 取りこぼし対策: 各ページのサイト表示件数「全N」と抽出行数を突き合わせ、
      一致したページのみ jpon_done.txt に記録する。不一致・通信失敗は
      jpon_failed.txt に残り、同じコマンドの再実行で自動的に再試行される。
    - 全国フルは DELAY=3.0 で約10.7日。中断しても再実行で続きから再開する。
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://jpon.xyz"
TOP_URL = "https://jpon.xyz/2012/index.html"

_OUTPUT_DIR = _project_root / "output"
_URL_CHECKPOINT = _OUTPUT_DIR / "jpon_urls.json"
# 取得と件数検証まで成功したページ（再実行時はスキップする）
_DONE_CHECKPOINT = _OUTPUT_DIR / "jpon_done.txt"
# 404 で恒久的に存在しないページ（スキップするが done とは区別して記録する）
_GONE_CHECKPOINT = _OUTPUT_DIR / "jpon_404.txt"
# 一時的な失敗・件数不一致（done に入れないので次回実行で自動リトライされる）
_FAILED_CHECKPOINT = _OUTPUT_DIR / "jpon_failed.txt"
# 件数不一致の詳細（サイト申告値と抽出値の突き合わせ結果）
_MISMATCH_LOG = _OUTPUT_DIR / "jpon_mismatch.txt"

# 1町字ページあたりの平均レコード数。全国300ページの実測サンプリングに基づく概算値で、
# 進捗ログの ETA 分母にのみ使う（取得ロジックには影響しない）。
# 町字ごとのばらつきが極端(0〜1,537件)なため、都道府県単位ではこの平均から大きく
# 外れることがある。ETA はあくまで目安。
_EST_RECORDS_PER_PAGE = 72.2

# 表示文字列のスペース / タブ / 改行 / 全角スペースをまとめて正規化する
_WHITESPACE_RE = re.compile(r"\s+")

# /s/2012/{phone} 形式の href から完全な電話番号を取り出すための正規表現
_PHONE_HREF_RE = re.compile(r"/s/\d+/(\d[\d\-]+)")

# 町字ページに表示される件数サマリー「全440 ＋転入4 －転出15」の「全N」。
# 取りこぼし検知に使う（サイト申告値と実際の抽出行数を突き合わせる）。
_SITE_TOTAL_RE = re.compile(r"全([\d,]+)")


def _clean(s) -> str:
    if s is None:
        return ""
    return _WHITESPACE_RE.sub(" ", str(s).replace("　", " ")).strip()


def _load_checkpoint(path: Path) -> set[str]:
    """1行1URL のチェックポイントファイルを集合として読み込む"""
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _append_lines(path: Path, lines: list[str]) -> None:
    """チェックポイントファイルに追記する（空リストなら何もしない）"""
    if not lines:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


class JponScraper(StaticCrawler):
    """住所でポン! 2012年版 電話帳スクレイパー"""

    # サイト側で「過剰なアクセスは規制」と明示しているため保守的に設定。
    # このサイトでは DELAY = 「ページ取得の間隔」として使う（_fetch() 内で待機）。
    DELAY = 3.0

    # 1町字ページから平均25件・最大460件ほど取れるため、アイテム単位で待つと
    # 全国780万件 × 3秒 = 270日規模になり完走できない。待機はページ側に寄せる。
    ITEM_DELAY = 0.0

    # ブラウザ風のヘッダーで bot 判定を回避（ステルス）
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = ["市区町村", "町名", "エリアコード", "ステータス", "詳細URL"]

    # --pref で1都道府県だけに絞って実行する場合に設定される (None = 全国)。
    # 基底クラスが __init__ のオーバーライドを禁じているためクラス属性で持ち、
    # 実行側から scraper.pref_id = "13" のように設定する。
    pref_id: str | None = None

    def _setup(self):
        super()._setup()
        # 直近に取得したページの状態（取りこぼし判定に使う）
        self._page_error: str | None = None
        self._page_expected: int | None = None
        self._page_rows: int = 0
        # 取得成功ページのサイト申告件数の合計。最終的な CSV 行数と突き合わせる
        self.expected_total: int = 0
        # ブラウザらしい追加ヘッダーを送る（StaticCrawler ベースのステルス対策）
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Sec-Ch-Ua": '"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Referer": f"{BASE_URL}/",
        })

    # -------------------------------------------------------------------------
    # 取得（ページ間ウェイト + ステータス判別）
    # -------------------------------------------------------------------------

    def _fetch(self, url: str) -> bs4.BeautifulSoup | None:
        """ページ間ウェイトを入れて取得する。

        基底の get_soup() はウェイトを持たず、404 と一時的な通信エラーを
        どちらも None で返すため区別できない。取りこぼしを正しく扱うには
        「恒久的に無いページ(404)」と「後で再試行すべき失敗」を分ける必要が
        あるので、ここで直接セッションを叩いて status_code を保持する。

        取得できなかった理由は self._page_error に入れる ("404" / "network: ..." 等)。
        """
        self._page_error = None
        time.sleep(self.DELAY)
        try:
            response = self.session.get(url, timeout=self.TIMEOUT)
        except requests.exceptions.RequestException as e:
            self._page_error = f"network: {type(e).__name__}: {e}"
            self.error_count += 1
            self.logger.warning("通信エラー: %s — %s", url, e)
            return None

        if response.status_code == 404:
            self._page_error = "404"
            return None
        if response.status_code != 200:
            self._page_error = f"http {response.status_code}"
            self.error_count += 1
            self.logger.warning("HTTP %s: %s", response.status_code, url)
            return None

        # 文字化け対策（get_soup と同じ方針: ヘッダーの charset を優先）
        content_type = response.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            response.encoding = response.apparent_encoding
        return bs4.BeautifulSoup(response.text, "html.parser")

    @property
    def _url_checkpoint(self) -> Path:
        """URL一覧チェックポイントのパス。

        --pref 指定時は専用ファイルに分ける。全国用の jpon_urls.json に
        1県分だけを保存してしまうと、次の全国実行がその部分リストを
        「収集済み」として再利用し、残り46県を丸ごと取りこぼすため。
        """
        if self.pref_id:
            return _OUTPUT_DIR / f"jpon_urls_pref{self.pref_id}.json"
        return _URL_CHECKPOINT

    def parse(self, url: str) -> Generator[dict, None, None]:
        url_checkpoint = self._url_checkpoint

        # 1-3. URL 収集 (チェックポイントがあればスキップ)
        if url_checkpoint.exists():
            with open(url_checkpoint, encoding="utf-8") as f:
                district_urls = json.load(f)
            self.logger.info("チェックポイントから町字URL %d件を読み込みました", len(district_urls))
        else:
            pref_urls = self._collect_prefecture_urls(url)
            if self.pref_id:
                wanted = f"/2012/{self.pref_id}/index.html"
                pref_urls = [u for u in pref_urls if u.endswith(wanted)]
                if not pref_urls:
                    raise ValueError(
                        f"--pref {self.pref_id} に対応する都道府県ページが見つかりません"
                    )
                self.logger.info("都道府県を絞り込み: pref_id=%s", self.pref_id)
            self.logger.info("都道府県数: %d", len(pref_urls))

            city_urls: list[str] = []
            for pref_url in pref_urls:
                try:
                    city_urls.extend(self._collect_city_urls(pref_url))
                except Exception as e:
                    self.logger.warning("市区町村一覧取得失敗 %s: %s", pref_url, e)
                    continue
            self.logger.info("市区町村数: %d", len(city_urls))

            district_urls: list[str] = []
            for city_url in city_urls:
                try:
                    district_urls.extend(self._collect_district_urls(city_url))
                except Exception as e:
                    self.logger.warning("町字一覧取得失敗 %s: %s", city_url, e)
                    continue

            url_checkpoint.parent.mkdir(parents=True, exist_ok=True)
            with open(url_checkpoint, "w", encoding="utf-8") as f:
                json.dump(district_urls, f)
            self.logger.info("URLチェックポイント保存: %d件 → %s", len(district_urls), url_checkpoint)

        # 取得済み(= 件数検証まで通った)と 404 をスキップ対象としてロードする。
        # 一時的な失敗は done に入れていないので、ここで自動的に再試行対象に戻る。
        done_urls = _load_checkpoint(_DONE_CHECKPOINT)
        gone_urls = _load_checkpoint(_GONE_CHECKPOINT)
        skip_urls = done_urls | gone_urls
        if skip_urls:
            self.logger.info(
                "スキップ: 取得済み %d件 + 404 %d件", len(done_urls), len(gone_urls)
            )

        remaining = [u for u in district_urls if u not in skip_urls]
        # total_items はアイテム(レコード)数の分母として使われるため、
        # ページ数ではなく推定レコード数を入れて ETA を意味のある値にする。
        self.total_items = int(len(remaining) * _EST_RECORDS_PER_PAGE)
        self.logger.info(
            "町字ページ: 全 %d件 / 今回取得 %d件 (推定レコード数 約%s件)",
            len(district_urls), len(remaining), f"{self.total_items:,}",
        )

        # 4. 各町字 ?all で全件取得
        pending_done: list[str] = []
        pending_gone: list[str] = []
        pending_failed: list[str] = []
        stats = {"pages": 0, "records": 0, "gone": 0, "failed": 0, "mismatch": 0}

        for district_url in remaining:
            yielded = 0
            try:
                for item in self._scrape_district(district_url):
                    yielded += 1
                    yield item
                verdict = self._page_verdict(district_url, yielded)
            except Exception as e:
                self.logger.warning("町字ページ取得失敗 %s: %s", district_url, e)
                verdict = "failed"

            stats["pages"] += 1
            stats["records"] += yielded
            if verdict == "done":
                # サイト申告件数を積み上げる。ページ単位の照合では捕まえられない
                # 「パイプライン側での行の取りこぼし」を最後に検出するために使う。
                # (base.py はアイテム処理中の例外を CONTINUE_ON_ERROR で
                #  スキップするため、yield した行が CSV に入らないことがある)
                self.expected_total += self._page_expected or yielded
                pending_done.append(district_url)
            elif verdict == "gone":
                pending_gone.append(district_url)
                stats["gone"] += 1
            else:
                # 取得失敗・件数不一致は done に入れない → 次回実行で再試行される
                pending_failed.append(district_url)
                stats["failed"] += 1
                if verdict == "mismatch":
                    stats["mismatch"] += 1

            if len(pending_done) >= 100:
                _append_lines(_DONE_CHECKPOINT, pending_done)
                pending_done.clear()
            if len(pending_gone) >= 50:
                _append_lines(_GONE_CHECKPOINT, pending_gone)
                pending_gone.clear()
            if len(pending_failed) >= 50:
                _append_lines(_FAILED_CHECKPOINT, pending_failed)
                pending_failed.clear()

        _append_lines(_DONE_CHECKPOINT, pending_done)
        _append_lines(_GONE_CHECKPOINT, pending_gone)
        _append_lines(_FAILED_CHECKPOINT, pending_failed)

        self.logger.info(
            "今回処理: %s ページ / %s レコード（404 %s件 / 未取得 %s件 うち件数不一致 %s件）",
            f"{stats['pages']:,}", f"{stats['records']:,}",
            f"{stats['gone']:,}", f"{stats['failed']:,}", f"{stats['mismatch']:,}",
        )

        if stats["failed"]:
            # 取りこぼしが残っているのでチェックポイントは消さない。
            # 同じコマンドを再実行すれば失敗分だけを取得しにいく。
            self.logger.warning(
                "未取得 %s件 が残っています。チェックポイントは保持します。"
                "同じコマンドを再実行すると未取得分のみ再試行します（一覧: %s）",
                f"{stats['failed']:,}", _FAILED_CHECKPOINT,
            )
        elif self.pref_id:
            # 1県のみの実行。done/404 は全国実行でもそのまま有効な記録なので消さない
            # （消すと同じページを取り直すことになる）。URL一覧だけ片付ける。
            if url_checkpoint.exists():
                url_checkpoint.unlink()
            self.logger.info(
                "pref_id=%s の全ページ取得完了。done/404 の記録は全国実行に引き継ぎます",
                self.pref_id,
            )
        else:
            for p in (_URL_CHECKPOINT, _DONE_CHECKPOINT, _GONE_CHECKPOINT, _FAILED_CHECKPOINT):
                if p.exists():
                    p.unlink()
            self.logger.info("全ページ取得完了。チェックポイントをクリアしました")

    def _page_verdict(self, district_url: str, yielded: int) -> str:
        """1ページの処理結果を判定する。

        Returns:
            "done"     — サイト申告件数と抽出件数が一致（取りこぼしなし）
            "gone"     — 404。恒久的に存在しないのでスキップ対象に記録する
            "mismatch" — 件数不一致。取りこぼしなので done にせず再試行させる
            "failed"   — 通信エラー等
        """
        if self._page_error == "404":
            return "gone"
        if self._page_error is not None:
            return "failed"

        expected = self._page_expected
        found = self._page_rows

        # サマリー表示が無い/読めないページは突き合わせ不能。件数が取れていれば完了扱い。
        if expected is None:
            self.logger.debug("件数サマリーなし: %s (抽出 %d件)", district_url, found)
            return "done"

        if found != expected:
            self.logger.warning(
                "件数不一致 %s: サイト申告 全%d件 / 抽出 %d件 → 未取得として再試行対象にします",
                district_url, expected, found,
            )
            _append_lines(
                _MISMATCH_LOG, [f"{district_url}\texpected={expected}\tfound={found}"]
            )
            return "mismatch"

        # 行は全部取れているが、名前が空でパイプラインに渡らなかった行がある場合
        if yielded != found:
            self.logger.warning(
                "名前が空の行をスキップ %s: 行 %d件 中 %d件 のみ出力",
                district_url, found, yielded,
            )
        return "done"

    # -------------------------------------------------------------------------
    # 階層別ヘルパー
    # -------------------------------------------------------------------------

    def _collect_prefecture_urls(self, top_url: str) -> list[str]:
        """トップ /2012/index.html から /2012/{pref_id}/index.html の URL を収集する"""
        soup = self.get_soup(top_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"][href$="/index.html"]'):
            href = a.get("href", "")
            # /2012/{pref_id}/index.html の形（深さ 2 階層）のみ拾う
            if not re.match(r"^/2012/\d+/index\.html$", href):
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _collect_city_urls(self, pref_url: str) -> list[str]:
        """都道府県 /2012/{pref}/index.html から /2012/{pref}/{city}/index.html の URL を収集する"""
        time.sleep(self.DELAY)
        soup = self.get_soup(pref_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"][href$="/index.html"]'):
            href = a.get("href", "")
            if not re.match(r"^/2012/\d+/\d+/index\.html$", href):
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _collect_district_urls(self, city_url: str) -> list[str]:
        """市区町村 /2012/{pref}/{city}/index.html から /2012/{pref}/{city}/{district}.html を収集する。

        ページ内のリンクは ?p=1 が付いた形 (/2012/27/4/1.html?p=1) になっているため、
        正規化して ?all 付きに揃える。
        """
        time.sleep(self.DELAY)
        soup = self.get_soup(city_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"]'):
            href = a.get("href", "")
            # /2012/{pref}/{city}/{district}.html のパターン
            m = re.match(r"^/2012/\d+/\d+/\d+\.html", href)
            if not m:
                continue
            # ?p=1 等のクエリを除去して ?all を付ける
            base_path = m.group(0)
            full = urljoin(BASE_URL, base_path) + "?all"
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _scrape_district(self, district_url: str) -> Generator[dict, None, None]:
        """町字ページ ?all から全レコードを yield する。

        取りこぼし検知のため、サイトが表示している件数「全N」を self._page_expected に、
        実際に見つけた行数を self._page_rows に記録する。判定は _page_verdict() が行う。
        """
        self._page_expected = None
        self._page_rows = 0

        soup = self._fetch(district_url)
        if soup is None:
            return

        # 階層情報を抽出: h2 = "住所でポン！ 2012年版 東京都 世田谷区  三宿 の電話帳"
        h2 = soup.select_one("h2")
        district_name = ""
        if h2:
            # "三宿 の電話帳" 部分を取り出す
            h2_text = _clean(h2.get_text(" ", strip=True))
            m = re.search(r"([^ 　]+)\s*の電話帳\s*$", h2_text)
            if m:
                district_name = m.group(1)

        # URL からエリアコード (例: 27-4-1) を抽出
        area_code = ""
        m = re.search(r"/2012/(\d+)/(\d+)/(\d+)\.html", district_url)
        if m:
            area_code = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # サイト表示の件数「全440 ＋転入4 －転出15」を取りこぼし検証用に記録する
        m = _SITE_TOTAL_RE.search(soup.get_text(" ", strip=True))
        if m:
            self._page_expected = int(m.group(1).replace(",", ""))

        # 全 <tr itemtype="https://schema.org/Person"> を取得
        # itemtype は http:// 表記のページもあり得るため両方を対象にする
        rows = soup.select(
            'tr[itemtype="https://schema.org/Person"], tr[itemtype="http://schema.org/Person"]'
        )
        self._page_rows = len(rows)
        for tr in rows:
            try:
                item = self._parse_row(tr, district_url, district_name, area_code)
            except Exception as e:
                self.logger.warning("行解析失敗 %s: %s", district_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _parse_row(
        self,
        tr,
        source_url: str,
        district_name: str,
        area_code: str,
    ) -> dict | None:
        """1 レコード行 (<tr>) を辞書に変換する"""
        # 電話番号: 表示は ****でマスクされているが、a[href="/s/2012/{phone}"] に完全番号
        tel = ""
        detail_url = ""
        phone_a = tr.select_one('td.p a[href*="/s/"]')
        if phone_a:
            href = phone_a.get("href", "")
            m = _PHONE_HREF_RE.search(href)
            if m:
                tel = m.group(1)
            detail_url = urljoin(BASE_URL, href)

        # 名前
        name = ""
        name_span = tr.select_one('td.n [itemprop="name"]')
        if name_span:
            name = _clean(name_span.get_text())

        # 都道府県 (addressRegion content 属性)
        prefecture = ""
        region_el = tr.select_one('[itemprop="addressRegion"]')
        if region_el:
            prefecture = _clean(region_el.get("content") or region_el.get_text())

        # 市区町村 (addressLocality content 属性)
        city = ""
        locality_el = tr.select_one('[itemprop="addressLocality"]')
        if locality_el:
            city = _clean(locality_el.get("content") or locality_el.get_text())

        # ステータス: span.entry=転入 / span.exit=転出 / 無印=現役
        if tr.select_one("td.p span.entry"):
            status = "転入"
        elif tr.select_one("td.p span.exit"):
            status = "転出"
        else:
            status = "現役"

        # 住所: 市区町村 + 町名 (番地以下はサイト側で隠蔽されているため取得不能)
        addr = f"{city}{district_name}" if district_name else city

        return {
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.PREF: prefecture,
            Schema.ADDR: addr,
            Schema.URL: source_url,
            "市区町村": city,
            "町名": district_name,
            "エリアコード": area_code,
            "ステータス": status,
            "詳細URL": detail_url,
        }


def _recover_partial_csv(partial_path: Path) -> None:
    """中断された実行の一時CSV（ヘッダーなし）を復旧して最終CSVを生成する"""
    import csv
    from datetime import datetime
    from src.const.schema import Schema

    all_fieldnames = list(Schema.COLUMNS) + JponScraper.EXTRA_COLUMNS
    output_path = partial_path.parent / (
        f"{datetime.now().strftime('%Y%m%d')}_jpon_recovered_{partial_path.stem}.csv"
    )

    row_count = 0
    with open(partial_path, "r", encoding="utf-8", newline="") as f_in, \
         open(output_path, "w", encoding="utf-8-sig", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(all_fieldnames)
        for row in reader:
            writer.writerow(row)
            row_count += 1

    print(f"復旧完了: {output_path}")
    print(f"行数: {row_count:,}件")


if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="住所でポン! スクレイパー")
    parser.add_argument(
        "--recover",
        metavar="PARTIAL_CSV",
        help="中断された実行の一時CSVファイルを指定して最終CSVを復旧する",
    )
    parser.add_argument(
        "--pref",
        metavar="PREF_ID",
        help=(
            "1都道府県だけを取得する (例: --pref 13)。段階的な本番投入や"
            "試験実行に使う。全国は約13日かかるため、まず1県で通すことを推奨。"
            "取得済み記録は全国実行にそのまま引き継がれる。"
        ),
    )
    args = parser.parse_args()

    if args.recover:
        _recover_partial_csv(Path(args.recover))
        sys.exit(0)

    scraper = JponScraper()
    if args.pref:
        scraper.pref_id = args.pref
        scraper.site_name = f"jpon_pref{args.pref}"

    scraper.execute(TOP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count:,}")

    # 端から端までの取りこぼし検証:
    # サイトが申告した件数の合計 == CSV に入った件数 なら欠損なし。
    # ページ単位の照合だけでは、パイプライン側で行が落ちた場合を検出できない。
    expected = scraper.expected_total
    actual = scraper.item_count
    print(f"サイト申告合計: {expected:,}")
    if expected == actual:
        print("✅ 取りこぼしなし (サイト申告件数と一致)")
    else:
        print(f"⚠ 差分 {expected - actual:+,} 件 — パイプライン側で行が落ちている可能性があります")

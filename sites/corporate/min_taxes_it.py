"""
補助金データベース(min-taxes)IT系 — みんなの税金「補助金採択データベース」から IT 系補助金の交付先法人を取得

取得対象:
    - 補助金名が IT 系の交付実績 (1 交付 = 1 行。同一法人が複数行になる)
      IT導入補助金 / デジタル化・AI導入補助金 / ものづくり補助金のデジタル・DX・AI 枠 /
      補助金名に IT・デジタル・DX・AI・情報化・システム・ソフトウェア を含むもの
    - 法人のみ・全期間・全国。法人番号(13桁)が公表されていない行は除外 (REQUIRE_CO_NUM)

取得フロー:
    一覧ページ (url) と同一オリジンの公開 JSON API `/api/hojokin` を使う
    (ページ内の検索フォームが叩いているもの)。
      /api/hojokin?q={語}&target=title&pref={都道府県}&page=N
    並びは合計交付額の降順で、全国一括だと小規模企業に到達しづらいため
    47 都道府県 × 検索語で分割し、truncated=false になるまでページ送りする。
    検索語は部分一致・全角半角を区別しないため、半角/全角の両方を投げる。
    同じ交付が複数の検索語にヒットするので (法人番号, 補助金名, 交付決定日, 金額, 実施機関) で重複除外。
    API の rate_limited 応答は retry_after だけ待って再試行 (上限あり)。

実行方法:
    python scripts/sites/corporate/min_taxes_it.py
    docker compose exec worker python /app/bin/run_flow.py --site-id min_taxes_it

出典: みんなの税金 (min-taxes.com) 補助金採択データベース
    (転載・引用ポリシー: 出典明記+リンクで利用可 https://www.min-taxes.com/reprint/)
"""

import csv
import json
import logging
import queue
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.base import CrawlAbortedError
from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 中断したランの続きから取るための途中再開ファイル (手動で置く。Git 管理外の output/cache/)
_RESUME_FILE = _project_root / "output" / "cache" / "min_taxes_it_resume.json"
_BEACON_LOCK = threading.Lock()
_RATE_LOCK = threading.Lock()

_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# API の title 検索は部分一致・全角/半角を区別するため両方投げる。
# 「IT」で IT導入補助金、「AI」でデジタル化・AI導入補助金を、
# 「デジタル」「DX」「AI」でものづくり補助金のデジタル/DX/AI 枠を拾う。
# 並びは「合計額降順 → 法人番号なし行が先」なので、法人番号なしの旧 IT導入補助金
# (2017〜2019 年 Webアーカイブ復元分) が大量に先頭に来る「IT」は最後に回す。
_QUERIES = ["AI", "ＡＩ", "DX", "ＤＸ", "デジタル", "情報化", "システム", "ソフトウェア", "IT", "ＩＴ"]

# 部分一致の誤ヒット (英単語の一部など) を落とすための最終判定
_IT_TITLE = re.compile(
    r"(?<![A-Za-zＡ-Ｚａ-ｚ])(?:IT|ＩＴ|AI|ＡＩ|DX|ＤＸ)(?![A-Za-zＡ-Ｚａ-ｚ])"
    r"|デジタル|情報化|システム|ソフトウェア"
)

_PREF_RE = re.compile(r"^(" + "|".join(_PREFS) + ")")

# サイトの絞り込み「法人種別」と同じ区分を名称から判定 (先にマッチしたもの優先)
_ETYPE_RULES = [
    ("株式会社", r"株式会社|（株）|\(株\)"),
    ("有限会社", r"有限会社|（有）|\(有\)"),
    ("合同・合資・合名会社", r"合同会社|合資会社|合名会社"),
    ("NPO法人", r"特定非営利活動法人|ＮＰＯ法人|NPO法人"),
    ("医療法人", r"医療法人"),
    ("社会福祉法人", r"社会福祉法人"),
    ("学校法人・大学", r"学校法人|国立大学法人|公立大学法人|大学"),
    ("独立行政法人", r"独立行政法人|国立研究開発法人"),
    ("社団法人", r"社団法人"),
    ("財団法人", r"財団法人"),
    ("組合・金庫・商工会", r"組合|金庫|商工会|中央会|連合会"),
]
_ETYPE_RULES = [(label, re.compile(p)) for label, p in _ETYPE_RULES]


def _etype(name: str) -> str:
    for label, pat in _ETYPE_RULES:
        if pat.search(name):
            return label
    return "その他"


class MinTaxesIt(StaticCrawler):
    """補助金データベース(min-taxes)IT系 スクレイパー"""

    DELAY = 1.5
    # 基盤は既定で 1 件出力するごとに DELAY 秒待つ (ITEM_DELAY=None)。この API は 1 ページで
    # 最大 20 社を返し、ページ取得の間隔は _iter_companies が DELAY で空けているので、
    # 件ごとの待機は相手サーバーの負荷に関係なく速度だけを 40 件/分に落としていた。
    ITEM_DELAY = 0.0
    TIMEOUT = 30
    REQUIRE_CO_NUM = True  # 備考「法人番号は必須」: 法人番号の無い行は出力しない
    # rate limit を受けたら最長 MAX_RATE_WAIT 秒待って 1 回だけ解除を確かめる。
    # retry_after は ~14h を返すが、実際は数分〜数十分で解除されることがある (2026-11-24 確認)。
    # 30 分 × 60 回 = 最長 30 時間待つ。
    MAX_RATE_RETRY = 60
    MAX_RATE_WAIT = 30 * 60
    # 同時に取得する組 (県×検索語) の数。4 並列では数十リクエストで HTTP 500 → 429 になった
    # (2026-11-24 確認) ため 1 本にする。
    CONCURRENCY = 1
    # 名前解決の失敗など一時的な通信エラーの再試行回数 (待機は 60 秒 × 回数、最長 10 分)
    MAX_NET_RETRY = 10

    EXTRA_COLUMNS = ["法人種別", "補助金名", "実施機関", "交付決定日", "交付金額"]

    def _get(self, full: str):
        """GET する。名前解決の失敗など一時的な通信エラーは待って再試行し、
        それでも失敗したら打ち切る (CrawlAbortedError: 部分データを完了扱いで納品しない)。"""
        for n in range(1, self.MAX_NET_RETRY + 1):
            try:
                return self.session.get(full, timeout=self.TIMEOUT, headers={"Accept": "application/json"})
            except requests.exceptions.RequestException as e:
                if n >= self.MAX_NET_RETRY:
                    raise CrawlAbortedError(f"通信エラーが続いたため打ち切り: {full} — {e}") from e
                wait = min(60 * n, 600)
                logger.warning("通信エラー (%s) %d秒後に再試行 %d/%d — %s", full, wait, n, self.MAX_NET_RETRY, e)
                self._sleep_with_heartbeat(wait)
                if self._stopped():
                    raise CrawlAbortedError("停止要求") from e

    def _api(self, api_url: str, params: dict) -> dict:
        """API を 1 回叩く。rate_limited は retry_after 待ちで上限付きリトライ。"""
        full = f"{api_url}?{urllib.parse.urlencode(params)}"
        for attempt in range(self.MAX_RATE_RETRY):
            # 他のスレッドが rate limit を受けていたら、その解除時刻まで全スレッドで待つ
            self._wait_shared_rate_limit()
            resp = self._get(full)
            data = None
            try:
                data = resp.json()
            except ValueError:
                pass
            if isinstance(data, dict) and data.get("error") == "rate_limited":
                # 1 日上限に当たると retry_after が数万秒 (〜14h) になる。600 秒で 6 回
                # 諦めると全件取り直しになるので、上限まで待ってから続きを取る。
                if attempt + 1 >= self.MAX_RATE_RETRY:
                    break  # 最後の試行で長時間待っても再リクエストしないので待たずに諦める
                try:
                    retry_after = int(float(data.get("retry_after") or 60))
                except (TypeError, ValueError):
                    retry_after = 60
                wait = min(retry_after + 5, self.MAX_RATE_WAIT)
                logger.warning("rate_limited (%s) %d秒待機 attempt=%d", full, wait, attempt + 1)
                self._set_shared_rate_limit(wait)
                continue
            if resp.status_code == 429:
                if attempt + 1 >= self.MAX_RATE_RETRY:
                    break
                wait = min(60 * (attempt + 1), 600)
                logger.warning("HTTP 429 (%s) %d秒待機", full, wait)
                self._set_shared_rate_limit(wait)
                continue
            resp.raise_for_status()
            if not isinstance(data, dict):
                raise RuntimeError(f"JSON 以外の応答: {full}")
            return data
        raise CrawlAbortedError(f"rate limit が解除されませんでした: {full}")

    def _sleep_with_heartbeat(self, seconds: int):
        """長い待機中も 10 分ごとに進捗ビーコンとログを出す。

        自己修復基盤の停滞検知は run_progress (ビーコン) の更新時刻を見るため、
        ログだけでは 1 時間で「停滞」と判定される。待機中もビーコンを打つ。
        """
        end = time.time() + seconds
        while True:
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(left, 60))
            if self._stopped():
                return
            if int(left) // 600 == int(end - time.time()) // 600:
                continue  # ビーコンとログは 10 分ごと
            self._ping()
            if end - time.time() > 0:
                logger.info("rate_limited 待機中 残り %d 秒", int(end - time.time()))

    def _set_shared_rate_limit(self, seconds: int):
        """rate limit の解除時刻を全スレッドで共有する (遅い方に合わせる)。"""
        with _RATE_LOCK:
            self._rl_until = max(getattr(self, "_rl_until", 0.0), time.time() + seconds)

    def _wait_shared_rate_limit(self):
        with _RATE_LOCK:
            left = getattr(self, "_rl_until", 0.0) - time.time()
        if left > 0:
            self._sleep_with_heartbeat(int(left) + 1)

    def _ping(self):
        """進捗ビーコンを打つ。複数スレッドから呼ばれるのでロックで直列化する
        (ビーコン自体が間隔を間引くので、毎ページ呼んでも DB 書き込みは増えない)。"""
        services = getattr(self, "_services", None)
        if services is None:
            return
        with _BEACON_LOCK:
            services.beacon.ping()

    def _stopped(self) -> bool:
        stop = getattr(self, "_stop", None)
        return stop is not None and stop.is_set()

    def parse(self, url: str):
        # www. 付きは毎回 min-taxes.com へ 301 リダイレクトされ、1 ページに 2 リクエストかかるため
        # 最初からリダイレクト先を叩く
        api_url = urllib.parse.urljoin(url, "/api/hojokin").replace("://www.min-taxes.com", "://min-taxes.com", 1)
        self._seen = set()

        done = set()
        resume = self._load_resume()
        if resume:
            done = {tuple(k) for k in resume.get("done", [])}
            yield from self._seed_rows(resume.get("seed_csv", ""))

        combos = [(pref, q) for pref in _PREFS for q in _QUERIES if (pref, q) not in done]
        logger.info("取得対象 %d 組 (県×検索語、完了済み %d 組をスキップ) を %d 並列で取得",
                    len(combos), len(done), self.CONCURRENCY)

        # 各スレッドが 1 組 (県×検索語) をページ送りし、会社をキューへ流す。
        # 行の生成・重複除外・件数集計はこのスレッド (ジェネレータ) だけで行う。
        out = queue.Queue(maxsize=200)
        _END = object()
        self._stop = threading.Event()

        def put(x):
            # 呼び出し側が止まった (例外・途中終了) ときに put で固まらないよう、停止フラグを見る
            while not self._stop.is_set():
                try:
                    out.put(x, timeout=1)
                    return
                except queue.Full:
                    continue

        def worker(pref, q):
            try:
                for c in self._iter_companies(api_url, pref, q):
                    if self._stop.is_set():
                        return
                    put(c)
            except BaseException as e:  # 呼び出し側で再送出する
                put(e)
            finally:
                put(_END)

        pool = ThreadPoolExecutor(max_workers=self.CONCURRENCY)
        try:
            for pref, q in combos:
                pool.submit(worker, pref, q)
            remaining = len(combos)
            while remaining:
                item = out.get()
                if item is _END:
                    remaining -= 1
                    continue
                if isinstance(item, BaseException):
                    # 基盤は parse() の通常の例外を「取得済みデータで継続」として完了扱いにし、
                    # 部分データを S3 へ送ってしまう (2026-11-24 発生)。打ち切りとして送出する。
                    if isinstance(item, CrawlAbortedError):
                        raise item
                    raise CrawlAbortedError(f"取得中のエラーで打ち切り: {item}") from item
                yield from self._rows(item, url)
        finally:
            self._stop.set()
            pool.shutdown(wait=False, cancel_futures=True)

        # 完走したら再開ファイルを使用済みにし、次回の通常実行で使われないようにする
        if resume:
            used = _RESUME_FILE.with_name(_RESUME_FILE.name + ".used")
            try:
                _RESUME_FILE.replace(used)
                logger.info("途中再開ファイルを使用済みにしました: %s", used)
            except OSError as e:
                logger.warning("途中再開ファイルを使用済みにできません (次回実行前に削除が必要): %s — %s", _RESUME_FILE, e)

    def _iter_companies(self, api_url: str, pref: str, q: str):
        """1 組 (県×検索語) を終端までページ送りし、会社を順に返す。"""
        page = 1
        seen_sigs = set()
        while True:
            params = {"q": q, "target": "title", "pref": pref}
            if page > 1:
                params["page"] = page
            data = self._api(api_url, params)
            companies = data.get("companies") or []
            # API は最終ページを過ぎても同じ最終ページを truncated=true で返し続ける
            # (2026-11 確認: デジタル×東京都で page 2000 と 2361 が同一)。
            # 既に取得したページと同じ中身が返ったら終端とみなす
            # (直前ページだけでなく全履歴と比べ、交互に返る場合も止める)。
            sig = tuple((c.get("corp_number"), c.get("name")) for c in companies)
            if sig and sig in seen_sigs:
                logger.info("終端検出 (取得済みページと同一) q=%s pref=%s page=%d", q, pref, page)
                break
            seen_sigs.add(sig)
            # 法人番号のない行ばかりのページが続くと行が出ずビーコンが止まるので、ページ単位でも打つ
            self._ping()
            yield from companies
            if not data.get("truncated") or not companies:
                break
            page += 1
            if self._stopped():
                return
            time.sleep(self.DELAY)
        logger.info("完了 q=%s pref=%s (%d ページ)", q, pref, page)

    def _load_resume(self) -> dict | None:
        """途中再開ファイル (output/cache/min_taxes_it_resume.json) があれば読む。

        中断したランの続きから取るための手動運用ファイル。形式:
            {"seed_csv": "output/aborted/<中断ランの CSV>", "done": [["北海道", "AI"], ...]}
        seed_csv の行を先に出力し、done の組 (県×検索語) は取り直さない。
        引き継いだ行の取得日時は、パイプラインが今回の出力時刻で付け直す。

        ラン中にこのファイルは更新しない。再開したランがまた途中で止まったら、
        seed_csv をそのランの CSV (output/aborted/ に退避される) に差し替え、
        done に完了した組を書き足してから再実行する (ログの「完了 q=… pref=…」で分かる)。
        """
        path = _RESUME_FILE
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise RuntimeError(f"途中再開ファイルを読めません: {path} — {e}") from e
        logger.info("途中再開ファイルを使用: %s (完了済み %d 組)", path, len(data.get("done", [])))
        return data

    def _seed_rows(self, seed_csv: str):
        """中断ランの CSV の行を、重複除外キーに登録しながらそのまま出力する。"""
        if not seed_csv:
            return
        path = Path(seed_csv)
        if not path.is_absolute():
            path = _project_root / path
        if not path.exists():
            raise RuntimeError(f"途中再開の元 CSV がありません: {path}")
        n = 0
        with path.open(encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                key = (r.get(Schema.CO_NUM) or r.get(Schema.NAME, ""), r.get("補助金名", ""),
                       r.get("交付決定日", ""), r.get("交付金額", ""), r.get("実施機関", ""))
                if key in self._seen:
                    continue
                self._seen.add(key)
                n += 1
                yield {
                    Schema.NAME: r.get(Schema.NAME, ""),
                    Schema.CO_NUM: r.get(Schema.CO_NUM, ""),
                    Schema.PREF: r.get(Schema.PREF, ""),
                    Schema.ADDR: r.get(Schema.ADDR, ""),
                    Schema.URL: r.get(Schema.URL, ""),
                    **{col: r.get(col, "") for col in self.EXTRA_COLUMNS},
                }
        logger.info("途中再開: 中断ランの %d 行を引き継ぎ (%s)", n, path)

    def _rows(self, c: dict, url: str):
        name = (c.get("name") or "").strip()
        co_num = re.sub(r"\D", "", str(c.get("corp_number") or ""))
        if len(co_num) != 13:
            co_num = ""
        if self.REQUIRE_CO_NUM and not co_num:
            return
        location = (c.get("location") or "").strip()
        m = _PREF_RE.match(location)
        pref = m.group(1) if m else ""
        addr = location if location and location != pref else ""

        for r in c.get("records") or []:
            title = (r.get("title") or "").strip()
            if not title or not _IT_TITLE.search(title):
                continue
            amount = r.get("amount")
            amount_s = str(int(amount)) if amount is not None else ""
            date = (r.get("date") or "").strip()
            gov = (r.get("government") or "").strip()
            key = (co_num or name, title, date, amount_s, gov)
            if key in self._seen:
                continue
            self._seen.add(key)
            yield {
                Schema.NAME: name,
                Schema.CO_NUM: co_num,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.URL: url,
                "法人種別": _etype(name),
                "補助金名": title,
                "実施機関": gov,
                "交付決定日": date,
                "交付金額": amount_s,
            }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MinTaxesIt()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.min-taxes.com/hojokin-database/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
LINE公式アカウント (page.line.me) — STREAMX-PRIME の STX に登録された LINE URL を巡回する

一覧ページを辿る通常のサイトと違い、巡回対象は STREAMX-PRIME (STX) の LINE 列。
    1. STREAMX-PRIME の SQL エクスポート API で `li IS NOT NULL` の行を取得する
       (エンドポイントは環境変数 STREAMX_API_BASE、既定 http://stream09:8000/api/v1)
    2. LINE ID ごとに重複を除き、https://page.line.me/<ID> を取得する
    3. ページ内の __NEXT_DATA__ (JSON) からアカウント情報を取り出し、1件ごとに yield

ページの状態:
    - 公開プロフィールあり        → 営業状態「取得」で各項目を出力
    - 友だち追加ページへ転送       → 「プロフィール非公開」(アカウント名なし)
    - 404 / アカウント情報なし     → 「存在せず」
    - 個人ID (~xxxx) は公式アカウントページが無いため対象外

取得項目: アカウント名 / LINE検索ID / 友だち数 / 認証バッジ / 住所 / 郵便番号 / TEL /
          Webサイト / 営業時間 / 最寄駅 / 取得元 STX の名称・住所・TEL・法人番号
自由記述 (ステータスメッセージ・投稿) は取得しない。

負荷: 4並列・各0.5秒間隔 (約5件/秒)。22万件で約12時間。429/403 は全体を一時停止して再試行。

実行方法:
    docker compose exec worker python /app/bin/run_flow.py --site-id line_official_account
    (動作確認: LINE_OA_LIMIT=50 を付けると先頭50件だけ取得)
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.const.schema import Schema
from src.framework.base import CrawlAbortedError
from src.framework.static import StaticCrawler

_STX_API = os.environ.get("STREAMX_API_BASE", "http://stream09:8000/api/v1").rstrip("/")
_STX_QUERY = "SELECT name, pref, addr, tel, corp_no, li FROM stx WHERE li IS NOT NULL"
_WORKERS = 4
_REQ_INTERVAL = 0.5  # 各ワーカーのリクエスト間隔(秒)
_ABORT_AFTER_CONSECUTIVE = 50  # 制限応答・ページ構造不一致がこの件数連続したら中止
_EXPORT_DEADLINE_SEC = 2 * 3600  # STX エクスポート待ちの上限

_LINE_URL_RE = re.compile(r"https?://line\.me/R/ti/p/(.+)$")
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
_DAY_JP = {"MONDAY": "月", "TUESDAY": "火", "WEDNESDAY": "水", "THURSDAY": "木",
           "FRIDAY": "金", "SATURDAY": "土", "SUNDAY": "日"}
_BADGE_JP = {"certified": "認証済み", "premium": "プレミアム", "standard": "未認証"}

_EXTRA_SEARCH_ID = "LINE検索ID"
_EXTRA_FRIENDS = "LINE友だち数"
_EXTRA_BADGE = "LINEバッジ"
_EXTRA_STATION = "最寄駅"
_EXTRA_STX_NAME = "STX名称"
_EXTRA_STX_ADDR = "STX住所"
_EXTRA_STX_TEL = "STX_TEL"
_EXTRA_STX_ROWS = "STX該当件数"


class _SystemicError(RuntimeError):
    """IP ブロックやページ構造変更など、1件ではなく全体に関わる失敗。"""


def _norm_id(url: str) -> str | None:
    m = _LINE_URL_RE.match((url or "").strip())
    if not m:
        return None
    i = m.group(1).strip().replace("%40", "@")
    if i.startswith("~"):
        return None
    return i.lstrip("@").split("?")[0].split("/")[0].lower() or None


class LineOfficialAccountScraper(StaticCrawler):
    DELAY = 0.0  # 待機は並列取得側で行う
    EXTRA_COLUMNS: list[str] = [
        _EXTRA_SEARCH_ID, _EXTRA_FRIENDS, _EXTRA_BADGE, _EXTRA_STATION,
        _EXTRA_STX_NAME, _EXTRA_STX_ADDR, _EXTRA_STX_TEL, _EXTRA_STX_ROWS,
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        try:
            targets = self._load_targets()
        except CrawlAbortedError:
            raise
        except Exception as e:  # 取り込み失敗を「0件で完了」にしない
            raise CrawlAbortedError(f"STX から LINE URL を取得できませんでした: {e}") from e
        limit = int(os.environ.get("LINE_OA_LIMIT", "0") or 0)
        ids = list(targets)
        if limit:
            ids = ids[:limit]
        self.total_items = len(ids)
        self.logger.info("LINE ID %d 件を取得します (STX 行 %d)", len(ids), sum(len(v) for v in targets.values()))

        self._pause_until = 0.0
        self._lock = threading.Lock()
        systemic = 0
        ex = ThreadPoolExecutor(_WORKERS)
        try:
            for start in range(0, len(ids), 2000):
                futs = {ex.submit(self._fetch, i): i for i in ids[start:start + 2000]}
                for fu in as_completed(futs):
                    i = futs[fu]
                    try:
                        rec = fu.result()
                    except _SystemicError as e:
                        systemic += 1
                        self.logger.warning("取得失敗(%d件連続): %s (%s)", systemic, i, e)
                        if systemic >= _ABORT_AFTER_CONSECUTIVE:
                            raise CrawlAbortedError(
                                f"制限応答またはページ構造不一致が {systemic} 件連続したため中止: {e}")
                        continue
                    except Exception as e:  # 1件の失敗で全体を止めない
                        self.logger.warning("取得失敗: %s (%s)", i, e)
                        continue
                    systemic = 0
                    if rec is None:
                        continue
                    rows = targets[i]
                    rec[Schema.LINE] = rows[0].get("li", "")
                    rec[_EXTRA_STX_NAME] = rows[0].get("name", "")
                    rec[_EXTRA_STX_ADDR] = rows[0].get("addr", "")
                    rec[_EXTRA_STX_TEL] = rows[0].get("tel", "")
                    rec[Schema.CO_NUM] = rows[0].get("corp_no", "") or ""
                    rec[_EXTRA_STX_ROWS] = len(rows)
                    yield rec
        finally:
            ex.shutdown(wait=True, cancel_futures=True)

    # ------------------------------------------------------------------ STX
    def _load_targets(self) -> dict[str, list[dict]]:
        """STREAMX-PRIME から LINE 列のある STX 行を取り、LINE ID ごとにまとめる。"""
        import csv
        import io

        local_csv = os.environ.get("LINE_OA_CSV")  # 動作確認用: STX エクスポート済み CSV を直接読む
        if local_csv:
            csv_text = Path(local_csv).read_text(encoding="utf-8-sig")
            return self._group_targets(csv.DictReader(io.StringIO(csv_text)))

        r = requests.post(f"{_STX_API}/stx/export/start",
                          json={"state": {"mode": "sql", "query": _STX_QUERY, "filename": "stx_line"}}, timeout=300)
        r.raise_for_status()
        job_id = r.json()["job_id"]
        self.logger.info("STX エクスポート開始 job=%s", job_id)
        # events ストリームはサーバー側で約10分ごとに閉じるため、終了状態になるまで開き直す
        status = None
        deadline = time.time() + _EXPORT_DEADLINE_SEC
        while status not in ("done", "error") and time.time() < deadline:
            try:
                with requests.get(f"{_STX_API}/stx/export/events/{job_id}", stream=True, timeout=600) as s:
                    for line in s.iter_lines(decode_unicode=True):
                        if line and line.startswith("data:"):
                            status = json.loads(line[5:]).get("status")
                            if status in ("done", "error"):
                                break
            except requests.RequestException as e:
                self.logger.warning("STX エクスポート進捗の取得を再試行: %s", e)
                time.sleep(10)
        if status != "done":
            raise RuntimeError(f"STX エクスポートが完了しませんでした: status={status}")
        csv_text = None
        for attempt in range(5):
            try:
                d = requests.get(f"{_STX_API}/stx/export/download/{job_id}", timeout=1800)
                d.raise_for_status()
                csv_text = d.content.decode("utf-8-sig")
                break
            except requests.RequestException as e:
                self.logger.warning("STX ダウンロード再試行 %d: %s", attempt + 1, e)
                time.sleep(10)
        if csv_text is None:
            raise RuntimeError("STX エクスポートをダウンロードできませんでした")

        return self._group_targets(csv.DictReader(io.StringIO(csv_text)))

    @staticmethod
    def _group_targets(reader) -> dict[str, list[dict]]:
        # エクスポートのヘッダーは日本語ラベルで返る
        key = {"名称": "name", "都道府県": "pref", "住所": "addr", "TEL": "tel", "法人番号": "corp_no", "LINE": "li"}
        targets: dict[str, list[dict]] = {}
        for row in reader:
            r2 = {key.get(k, k): (v or "") for k, v in row.items()}
            i = _norm_id(r2.get("li", ""))
            if i:
                targets.setdefault(i, []).append(r2)
        if not targets:
            raise RuntimeError("STX から LINE URL を1件も取得できませんでした")
        return targets

    # ------------------------------------------------------------- page.line.me
    def _fetch(self, line_id: str) -> dict | None:
        url = f"https://page.line.me/{line_id}"
        last = None
        for attempt in range(6):
            while time.time() < self._pause_until:
                time.sleep(1)
            time.sleep(_REQ_INTERVAL * random.uniform(0.7, 1.3))
            try:
                resp = self.session.get(url, timeout=30, headers={"Accept-Language": "ja"})
            except requests.RequestException as e:
                last = e
                time.sleep(5 * (attempt + 1))
                continue
            if resp.status_code == 404:
                return self._status_only(url, "存在せず")
            if resp.status_code in (403, 429) or resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                if attempt >= 2:  # 3回で打ち切り、連続件数で全体の中止を判断する
                    raise _SystemicError(last)
                wait = 30 * 2 ** attempt
                with self._lock:
                    self._pause_until = max(self._pause_until, time.time() + wait)
                self.logger.warning("HTTP %s: %s — 全体を %d 秒停止", resp.status_code, line_id, wait)
                continue
            if "line.me/R/ti/p/" in resp.url:
                return self._status_only(url, "プロフィール非公開")
            m = _NEXT_DATA_RE.search(resp.text)
            if not m:
                if attempt >= 1:  # 1回だけ再取得し、それでも無ければページ構造の変更を疑う
                    raise _SystemicError("__NEXT_DATA__ なし")
                continue
            props = json.loads(m.group(1)).get("props", {}).get("pageProps", {})
            ds = props.get("initialDataString")
            acc = json.loads(ds).get("account") if ds else None
            if not acc:
                return self._status_only(url, "存在せず")
            return self._parse_account(url, acc)
        raise RuntimeError(f"再試行上限: {last}")

    @staticmethod
    def _status_only(url: str, status: str) -> dict:
        return {Schema.URL: url, Schema.STS_NM: status}

    @staticmethod
    def _parse_account(url: str, acc: dict) -> dict:
        ai = acc.get("accountInfo") or {}
        pf = acc.get("profile") or {}
        rec = {
            Schema.URL: url,
            Schema.STS_NM: "取得",
            Schema.NAME: pf.get("name", ""),
            _EXTRA_SEARCH_ID: ai.get("basicSearchId", ""),
            _EXTRA_FRIENDS: ai.get("friendCount", ""),
            _EXTRA_BADGE: _BADGE_JP.get(pf.get("badgeType"), pf.get("badgeType") or ""),
        }
        for p in acc.get("pluginList") or []:
            if p.get("type") != "basicInfo":
                continue
            c = p.get("content") or {}
            ad = c.get("address") or {}
            rec[Schema.PREF] = ad.get("provinceName") or ""
            rec[Schema.ADDR] = "".join(x or "" for x in (ad.get("cityName"), ad.get("baseAddress"), ad.get("detailAddress")))
            rec[Schema.POST_CODE] = ad.get("postalCode") or ""
            rec[Schema.TEL] = (c.get("phoneContacts") or {}).get("tel") or ""
            rec[Schema.HP] = " ".join(w.get("url", "") for w in ((c.get("websites") or {}).get("items") or []))
            wt = c.get("workingTime") or {}
            parts = []
            for w in wt.get("workingTimeDetailList") or []:
                times = ",".join(f"{t.get('startTime')}-{t.get('endTime')}" for t in w.get("salesTimeList") or [])
                parts.append(f"{_DAY_JP.get(w.get('dayOfWeek'), w.get('dayOfWeek'))}{times or '休'}")
            rec[Schema.TIME] = " ".join(parts)
            if wt.get("memo"):
                rec[Schema.HOLIDAY] = wt["memo"]
            ns = c.get("nearestStation") or ""
            rec[_EXTRA_STATION] = " ".join(map(str, ns)) if isinstance(ns, list) else str(ns)
        return rec


if __name__ == "__main__":
    LineOfficialAccountScraper().execute("https://page.line.me/")

"""
プレスリリース (valuepress / value-press.com) — 掲載企業情報スクレイパー

取得対象:
    各企業の「企業ページ」/corporation/{id} に紐づく企業プロフィール
    (企業名・住所・ホームページURL・代表者名・サイト内業種 ほか)。

取得フロー:
    1. ルート (トップページ) を取得し、サーバーレンダリングされた新着
       /pressrelease/{id} リンクから最新記事 ID を求める。
    2. その ID から降順に /pressrelease/{id} を辿り、詳細ページ下部の企業情報
       テーブルにある /corporation/{id} リンクから企業 ID を得る (新着記事から
       辿るため、最新登録企業を優先的に収集できる)。
    3. 企業 ID を重複除外しつつ、企業プロフィール API
       /api/v1/user_accounts/{id} を叩いて構造化データを取得し、即 yield する
       (Pattern B / 早期 yield)。この API は Vue 製の企業ページ
       (/corporation/{id}) が内部的に読む JSON で、住所・HP URL・代表者・業種を
       静的取得できる。認証は静的トークン "Token VP_API" のみ (CSRF 不要)。

備考:
    - 削除・非公開のプレスリリースはトップへ 302 されるため allow_redirects=False。
    - 企業 API は存在しない/退会 ID に対し 404 を返すためスキップする。

実行方法:
    python scripts/sites/corporate/value_press.py
    docker compose exec worker python /app/bin/run_flow.py --site-id value_press
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests
from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# トップページ / 詳細ページから ID を抜き出す
_PR_ID_PATTERN = re.compile(r"/pressrelease/(\d+)")
_CORP_ID_PATTERN = re.compile(r"/corporation/(\d+)")

# 企業プロフィール API のパス。企業ページ (/corporation/{id}) が内部的に読む。
_CORP_API_PATH = "api/v1/user_accounts/{id}"
# API 認証ヘッダ。フロント JS に埋め込まれた静的トークン (GET は CSRF 不要)。
_CORP_API_HEADERS = {
    "Authorization": "Token VP_API",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

# 降順スキャンの上限 (最新から遡るプレスリリース件数の安全上限)。valuepress は
# 37 万件超の記事を持ち、一覧が JS 後読みのため静的には連番 ID を新しい順に辿る
# しかない。1 回の実行で無制限に走らないよう上限を設ける。
_MAX_SCAN = 60000


def _clean(node) -> str:
    """テキストを取り出して連続空白を 1 個に畳む。"""
    if node is None:
        return ""
    text = node if isinstance(node, str) else node.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", str(text)).strip()


def _to_ymd(value: str) -> str:
    """"2021/3/26" のような日付を "2021-03-26" に正規化する。"""
    if not value:
        return ""
    m = re.match(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", value.strip())
    if not m:
        return ""
    y, mth, d = m.groups()
    return f"{y}-{int(mth):02d}-{int(d):02d}"


class ValuePressScraper(StaticCrawler):
    """プレスリリース (valuepress) 掲載企業スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["プレスリリースタイトル", "配信日", "プレスリリースURL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート (起点) として使う。詳細/API URL はここから派生させる。
        root = url.rstrip("/")

        # --- 1. トップページから最新記事 ID を求める ---
        home = self.get_soup(url)
        if home is None:
            self.logger.warning("トップページ取得失敗: %s", url)
            return
        ids = sorted(
            {int(m) for m in _PR_ID_PATTERN.findall(str(home))}, reverse=True
        )
        if not ids:
            self.logger.warning("新着プレスリリース ID を検出できませんでした")
            return
        start_id = ids[0]
        self.logger.info("最新プレスリリース ID = %s から降順に走査します", start_id)

        # --- 2. ID 降順に記事を辿り、企業 ID を求めて企業プロフィールを yield ---
        seen_corp: set[int] = set()
        floor_id = max(1, start_id - _MAX_SCAN)
        for pr_id in range(start_id, floor_id - 1, -1):
            pr_url = f"{root}/pressrelease/{pr_id}"
            try:
                corp_id, meta = self._extract_corp(pr_url)
            except Exception as e:  # 個別記事の失敗は握って継続
                self.logger.warning("記事解析失敗 %s — %s", pr_url, e)
                continue
            if corp_id is None or corp_id in seen_corp:
                continue
            seen_corp.add(corp_id)

            try:
                item = self._scrape_corporation(root, corp_id)
            except Exception as e:
                self.logger.warning("企業取得失敗 corporation/%s — %s", corp_id, e)
                continue
            if not item:
                continue

            item["プレスリリースタイトル"] = meta.get("title", "")
            item["配信日"] = meta.get("date", "")
            item["プレスリリースURL"] = pr_url
            yield item

        if floor_id > 1:
            self.logger.info(
                "スキャン上限 (%s 件) に到達したため終了しました (ID %s まで)",
                _MAX_SCAN,
                floor_id,
            )

    def _extract_corp(self, pr_url: str) -> tuple[int | None, dict]:
        """プレスリリース詳細から企業 ID と記事メタ (タイトル/配信日) を取り出す。

        削除・非公開の記事はトップ (/) へ 302 されるため、リダイレクトを追わず、
        200 かつ企業情報テーブルを持つページのみ採用する。
        """
        soup = self._get_no_redirect(pr_url)
        if soup is None:
            return None, {}

        table = soup.select_one("table.companyTbl")
        if table is None:
            # リダイレクト先や想定外レイアウト → スキップ
            return None, {}

        a = table.select_one('a[href*="/corporation/"]')
        corp_id: int | None = None
        if a and a.get("href"):
            m = _CORP_ID_PATTERN.search(a["href"])
            if m:
                corp_id = int(m.group(1))

        meta = {
            "title": _clean(soup.select_one("h1.articleHd")),
            "date": _clean(soup.select_one("#press_datetime")),
        }
        return corp_id, meta

    def _scrape_corporation(self, root: str, corp_id: int) -> dict | None:
        """企業プロフィール API から 1 社分を組み立てる。

        企業ページ /corporation/{id} が内部的に読む JSON を直接叩く。存在しない/
        退会企業は 404 のため None を返す。
        """
        api_url = urljoin(root + "/", _CORP_API_PATH.format(id=corp_id))
        data = self._get_json(api_url)
        if not data:
            return None

        name = _clean(data.get("name"))
        if not name:
            return None

        # 住所: city (都道府県) + address (市区町村以降) + address_building
        pref = _clean(data.get("city"))
        addr = _clean(data.get("address"))
        building = _clean(data.get("address_building"))
        if building:
            addr = f"{addr} {building}".strip()

        # 企業ページ (= 詳細ページ) の絶対 URL を取得元 URL とする
        detail_url = urljoin(root + "/", f"corporation/{corp_id}")

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: _clean(data.get("transcription")),
            Schema.PREF: pref,
            Schema.POST_CODE: _clean(data.get("zip_code")),
            Schema.ADDR: addr,
            Schema.REP_NM: _clean(data.get("delegate_name")),
            Schema.CAT_SITE: _clean(data.get("genre")),
            Schema.HP: _clean(data.get("url")),
            Schema.LOB: _clean(data.get("description")),
            Schema.CAP: _clean(data.get("capital")),
            Schema.OPEN_DATE: _to_ymd(_clean(data.get("foundation_date"))),
        }

        fb = _clean(data.get("facebook"))
        if fb:
            item[Schema.FB] = fb
        tw = _clean(data.get("twitter"))
        if tw:
            item[Schema.X] = tw
        return item

    def _get_no_redirect(self, url: str) -> BeautifulSoup | None:
        """リダイレクトを追わずに取得し、200 のときのみ soup を返す。"""
        try:
            resp = self.session.get(
                url, timeout=self.TIMEOUT, allow_redirects=False
            )
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            self.logger.warning("通信エラー (スキップ): %s — %s", url, e)
            return None
        if resp.status_code != 200:
            return None
        content_type = resp.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            resp.encoding = resp.apparent_encoding
        return BeautifulSoup(resp.text, "html.parser")

    def _get_json(self, url: str) -> dict | None:
        """企業プロフィール API を叩き、200/JSON のときのみ dict を返す。"""
        try:
            resp = self.session.get(
                url, timeout=self.TIMEOUT, headers=_CORP_API_HEADERS
            )
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            self.logger.warning("通信エラー (スキップ): %s — %s", url, e)
            return None
        if resp.status_code != 200:
            return None
        try:
            data = resp.json()
        except ValueError:
            return None
        return data if isinstance(data, dict) else None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ValuePressScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.value-press.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

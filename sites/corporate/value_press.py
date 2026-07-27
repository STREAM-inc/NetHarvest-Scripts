"""
プレスリリース (valuepress / value-press.com) — プレスリリース掲載企業情報スクレイパー

取得対象:
    - 各プレスリリース詳細ページ (/pressrelease/{id}) 下部の「企業情報」テーブル
      (企業名・代表者名・業種) と、記事メタ情報 (タイトル・配信日・企業ページURL)

取得フロー:
    1. ルート (トップページ) を取得し、サーバーレンダリングされた新着
       /pressrelease/{id} リンクから最新記事 ID を求める。
    2. その ID から降順に /pressrelease/{id} を辿り、詳細ページを 1 件取得する
       ごとに即 yield する (Pattern B / 早期 yield)。
    3. 削除・非公開の記事はトップへ 302 リダイレクトされるため、リダイレクトを
       追わず (allow_redirects=False) 200 かつ table.companyTbl を持つページのみ採用。

備考 (呼び出し時のコンテキスト) について:
    住所・会社URL・事業内容 は valuepress の静的 HTML には含まれない。企業プロフィール
    (/corporation/{id}) は JS (Vue) で後読みされ、記事本文中の「所在地」等は自由記述の
    プロース (掲載企業が任意に書いたもの) で構造化されていない。よって静的に安定取得できる
    構造化フィールド (企業名・代表者名・業種) のみを対象とする。

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

# 降順スキャンの上限 (最新から遡る件数の安全上限)。valuepress は 37 万件超の記事を
# 持ち、一覧が JS 後読みのため静的には連番 ID を新しい順に辿るしかない。1 回の実行で
# 無制限に走らないよう上限を設ける。上限到達時は log を残して終了する。
_MAX_SCAN = 20000


def _clean(node) -> str:
    """テキストを取り出して連続空白を 1 個に畳む。"""
    if node is None:
        return ""
    text = node if isinstance(node, str) else node.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


class ValuePressScraper(StaticCrawler):
    """プレスリリース (valuepress) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["プレスリリースタイトル", "配信日", "企業ページURL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート (起点) として使う。詳細ページ URL はここから派生させる。
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

        # --- 2. ID 降順に詳細を辿り、取得次第 yield ---
        floor_id = max(1, start_id - _MAX_SCAN)
        for pr_id in range(start_id, floor_id - 1, -1):
            detail_url = f"{root}/pressrelease/{pr_id}"
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # 個別記事の失敗は握って継続
                self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

        if floor_id > 1:
            self.logger.info(
                "スキャン上限 (%s 件) に到達したため終了しました (ID %s まで)",
                _MAX_SCAN,
                floor_id,
            )

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """プレスリリース詳細ページから 1 件分を組み立てる。

        削除・非公開の記事はトップ (/) へ 302 されるため、リダイレクトを追わず、
        200 かつ企業情報テーブルを持つページのみ採用する。
        """
        soup = self._get_no_redirect(detail_url)
        if soup is None:
            return None

        table = soup.select_one("table.companyTbl")
        if table is None:
            # リダイレクト先や想定外レイアウト → スキップ
            return None

        # 企業情報テーブル (th → td) を辞書化
        rows: dict[str, object] = {}
        for tr in table.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                rows[_clean(th)] = td

        name = _clean(rows.get("企業名"))
        if not name:
            return None

        rep = _clean(rows.get("代表者名"))
        genre = _clean(rows.get("業種"))

        # 企業ページ (/corporation/{id}) への絶対 URL
        corp_url = ""
        name_td = rows.get("企業名")
        if name_td is not None and not isinstance(name_td, str):
            a = name_td.select_one('a[href^="/corporation/"]')
            if a and a.get("href"):
                corp_url = urljoin(detail_url, a["href"])

        title_el = soup.select_one("h1.articleHd")
        date_el = soup.select_one("#press_datetime")

        return {
            Schema.NAME: name,
            Schema.REP_NM: rep,
            Schema.CAT_SITE: genre,
            Schema.URL: detail_url,
            "プレスリリースタイトル": _clean(title_el),
            "配信日": _clean(date_el),
            "企業ページURL": corp_url,
        }

    def _get_no_redirect(self, url: str) -> BeautifulSoup | None:
        """リダイレクトを追わずに取得し、200 のときのみ soup を返す。

        session.get は test_runner のソフトタイムアウトでラップされる呼び出し。
        """
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

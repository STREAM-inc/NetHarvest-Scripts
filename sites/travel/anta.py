"""
全国旅行業協会（ANTA）会員 — anta

取得対象:
    - 正会員検索 (/search/anta_meibo.cgi): 47 都道府県 + 観光庁 (登録行政庁) 別に
      検索し、各会員の 社名商号・登録番号・登録日／入会日・住所・代表者・
      TEL・FAX・営業所数・URL を取得する。想定 約 5,300 件。

取得フロー:
    1. 検索フォーム (equal3=都道府県コード) を GET し、20 件/ページの会員一覧を取得
    2. 1 会員 = 1 テーブル。ラベル(td)／値(td) を読み取り 1 件ごとに即 yield (Pattern B)
    3. 「次のページ」リンクの data= トークンを *生のまま* 辿って次ページへ

    ※ ページャの data= トークンは `%` を区切り文字に使う独自形式で、サーバは
      URL エンコードせず生バイトで照合する (`%5332` の `%53` を 'S' に復号すると
      壊れる)。requests は自動で `%53`→'S' に復号してしまうため、2 ページ目以降は
      urllib.request で生クエリのまま取得する。1 ページ目 (equal3=N のみ) は
      トークンが無いので通常の get_soup(requests) を使う。

    ※ ルート URL は sites.yml の url (https://www.anta.or.jp/search/)。
      CGI URL は同一ホスト配下の anta_meibo.cgi を urljoin で派生させる。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/anta.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id anta
"""

import html
import re
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭の 〒郵便番号 と 都道府県
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class Anta(StaticCrawler):
    """全国旅行業協会（ANTA）会員 スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "登録番号",
        "登録日／入会日",
        "FAX番号",
        "営業所数",
    ]

    # 検索フォーム equal3 の登録行政庁コード (1-47 = 都道府県, 50 = 観光庁)
    _PREF_CODES = [str(n) for n in range(1, 48)] + ["50"]

    # 1 都道府県あたりのページ巡回上限 (無限ループ保険。5,332 件/20 = 最大 267 ページ)
    _MAX_PAGES = 400

    def parse(self, url: str):
        cgi_url = urljoin(url, "anta_meibo.cgi")

        for code in self._PREF_CODES:
            # --- 1 ページ目: トークン無しなので通常の requests 経由 ---
            page_url = f"{cgi_url}?equal3={code}"
            try:
                soup = self.get_soup(page_url)
            except Exception as e:
                self.logger.warning("一覧取得失敗 equal3=%s: %s", code, e)
                continue
            if soup is None:
                continue

            # 初回に総件数 (検索結果 N 件中) を進捗表示用へ
            if not self.total_items:
                m = re.search(r"検索結果\s*([\d,]+)\s*件中", soup.get_text())
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))

            seen_tokens: set[str] = set()
            for _ in range(self._MAX_PAGES):
                for item in self._parse_companies(soup, page_url):
                    yield item

                next_href = self._find_next_href(soup)
                if not next_href:
                    break
                # 生トークンを含むクエリはそのまま (%53 を復号しない) 辿る必要がある
                next_url = urljoin(cgi_url, next_href)
                query = next_url.split("?", 1)[1] if "?" in next_url else ""
                if query in seen_tokens:  # 同一ページの繰り返しを検知して停止
                    break
                seen_tokens.add(query)
                soup = self._fetch_raw(next_url)
                if soup is None:
                    break

    # ------------------------------------------------------------------
    def _parse_companies(self, soup: bs4.BeautifulSoup, page_url: str):
        """一覧ページ soup から会員テーブルを 1 件ずつ dict にして返す。"""
        for table in soup.find_all("table"):
            if "社名商号" not in table.get_text():
                continue
            kv: dict[str, bs4.Tag] = {}
            for tr in table.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True)
                    if label:
                        kv[label] = cells[1]

            def txt(*keys: str) -> str:
                for k in keys:
                    for label, cell in kv.items():
                        if label.startswith(k):
                            return cell.get_text(" ", strip=True)
                return ""

            name = txt("社名商号")
            if not name:
                continue

            # 住所: 〒郵便番号 都道府県 + 住所 を分解
            raw_addr = txt("住所")
            post_code = pref = ""
            addr = raw_addr
            pm = _POST_PATTERN.search(addr)
            if pm:
                post_code = pm.group(1)
                addr = addr[pm.end():].strip()
            prm = _PREF_PATTERN.match(addr)
            if prm:
                pref = prm.group(1)
                addr = addr[prm.end():].strip()

            # URL (会社ホームページ): アンカー href 優先
            hp = ""
            for label, cell in kv.items():
                if label.startswith("ＵＲＬ") or label.startswith("URL"):
                    a = cell.find("a", href=True)
                    hp = a["href"].strip() if a else cell.get_text(strip=True)
                    break

            yield {
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR: addr,
                Schema.TEL: txt("ＴＥＬ", "TEL"),
                Schema.REP_NM: txt("代表者"),
                Schema.HP: hp,
                Schema.URL: page_url,
                "登録番号": txt("登録番号"),
                "登録日／入会日": txt("登録日／入会日", "登録日"),
                "FAX番号": txt("ＦＡＸ", "FAX"),
                "営業所数": txt("営業所数"),
            }

    @staticmethod
    def _find_next_href(soup: bs4.BeautifulSoup) -> str | None:
        """「次のページ」リンクの href を返す (無ければ None)。"""
        for a in soup.find_all("a", href=True):
            if "data=" in a["href"] and "次" in a.get_text():
                return html.unescape(a["href"])
        return None

    def _fetch_raw(self, url: str) -> bs4.BeautifulSoup | None:
        """生クエリ (data= トークン) を保ったまま取得する。

        requests は `%53`→'S' のように既存のパーセントエスケープを復号してしまい、
        サーバが生バイトで照合する data= トークンを壊す。urllib.request は
        クエリを改変せず送るため、ページャ専用にこちらを使う。
        """
        req = urllib.request.Request(url, headers={"User-Agent": self.USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=self.TIMEOUT) as resp:
                raw = resp.read()
        except Exception as e:
            self.logger.warning("次ページ取得失敗 %s: %s", url, e)
            return None
        return bs4.BeautifulSoup(raw, "html.parser", from_encoding="shift_jis")


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Anta()
    # 🔒 sites.yml の url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://www.anta.or.jp/search/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

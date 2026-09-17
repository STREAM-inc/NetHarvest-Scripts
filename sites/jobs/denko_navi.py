"""
電工ナビ — 電気工事士・電気主任技術者専門の求人情報サイト (www.denko-navi.com)

取得対象:
    掲載中の求人票 (全件)。求人を出している企業の会社名・本社所在地・代表者・
    資本金・従業員数・売上高・事業内容などの企業情報と、募集職種・雇用形態・
    勤務地といった構造化された求人情報を取得する。

取得フロー:
    トップページ (= sites.yml の url) にはエリア版 (関東/東海/関西/九州/北海道/
    東北/北陸/中国) へのリンクだけが並ぶ。

        トップ (/)
          └ エリア版 (/kanto など)
              └ 市区町村ページ (/kanto/DC11101 … DC + JIS 市区町村コード)
                  └ 求人詳細 (/kyujin/{求人ID})

    市区町村ページは掲載求人が存在する市区町村のみリンクされる (全 8 エリア合計で
    約 550 ページ)。ページ送りは `?page=N` だが、範囲外のページ番号を指定しても
    1 ページ目と同じ内容が 200 で返る (404 にならない) ため、
        - `.result-num .num-txt` の総件数に達した
        - そのページの求人 ID がすべて取得済み
    のいずれかで打ち切る。

    1 求人につき詳細ページを 1 回取得し、その場で即 yield する (Pattern B)。
    1 つの求人は勤務地に含まれるすべての市区町村ページに掲載されるため、
    求人 ID で全体重複排除を行う。

注意点:
    - 全文検索 /kyujin/search-result は GET/POST とも 404 を返すため使用しない。
      列挙は上記のエリア → 市区町村の階層のみで行う。
    - 電話番号はサイト上に一切掲載されていない (応募はフォーム経由) ため
      Schema.TEL は取得できない。
    - 同時接続を増やすとサイト側に接続拒否されるため、DELAY は 3.0 秒以上を保つ。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/denko_navi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id denko_navi
"""

import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Generator

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# エリア版のスラッグ (利用規約 第1条に列挙されている 8 版)
_REGION_SLUGS = (
    "hokkaido",
    "tohoku",
    "kanto",
    "hokuriku",
    "tokai",
    "kansai",
    "chugoku",
    "kyushu",
)

# 市区町村ページ (/{region}/DC{JISコード}) と求人詳細 (/kyujin/{ID})
_CITY_PATH_RE = re.compile(r"^/(?:%s)/DC\d+$" % "|".join(_REGION_SLUGS))
_JOB_PATH_RE = re.compile(r"^/kyujin/(\d+)$")

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 詳細ページ th ラベル → EXTRA_COLUMNS のカラム名
# 長文の自由記述になりやすい項目 (仕事内容・給与・応募条件・福利厚生・選考方法 等) は
# 著作権リスクのため取得しない。
_EXTRA_LABELS = {
    "募集職種": "募集職種",
    "雇用形態": "雇用形態",
    "勤務地": "勤務地",
    "年間休日": "年間休日",
    "残業": "残業",
    "受動喫煙防止措置事項": "受動喫煙防止措置事項",
    "更新日": "更新日",
    "事業所": "事業所",
    "主要取引先": "主要取引先",
}

# 本社所在地の先頭に付くことがある郵便番号 (「〒060-0042」「060-0042」)
_POST_CODE_RE = re.compile(r"^[〒]?\s*(\d{3}[-‐－]?\d{4})\s*")

# 1市区町村あたりのページ送り上限 (無限ループ防止の保険)
_MAX_PAGES_PER_CITY = 60


def _clean(text: str) -> str:
    """改行・連続空白を 1 スペースに正規化する。"""
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


def _to_iso_date(text: str) -> str:
    """「2018年10月」「2018年10月5日」→「2018-10-01」「2018-10-05」に正規化する。"""
    if not text:
        return ""
    m = re.search(r"(\d{4})\s*[年/\-\.]\s*(\d{1,2})(?:\s*[月/\-\.]\s*(\d{1,2}))?", text)
    if not m:
        return ""
    year, month, day = m.group(1), int(m.group(2)), int(m.group(3) or 1)
    return f"{year}-{month:02d}-{day:02d}"


class DenkoNaviScraper(StaticCrawler):
    """電工ナビ スクレイパー (静的)"""

    # Imperva Incapsula が前段にいるため、全 HTTP リクエストの前に DELAY を挟む。
    # (基盤の item 間ウェイトは _fetch() の待機と二重になるので 0 にする)
    DELAY = 3.0
    ITEM_DELAY = 0.0
    EXTRA_COLUMNS = ["求人ID", "求人特徴"] + list(_EXTRA_LABELS.values())

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_jobs: set[str] = set()

        for region_url in self._region_urls(url):
            for city_url in self._city_urls(region_url):
                yield from self._crawl_city(city_url, seen_jobs)

    def _fetch(self, url: str) -> bs4.BeautifulSoup | None:
        """負荷軽減のため必ず DELAY 秒待ってから取得する。"""
        time.sleep(self.DELAY)
        return self.get_soup(url)

    # ------------------------------------------------------------------
    # 列挙
    # ------------------------------------------------------------------
    def _region_urls(self, url: str) -> list[str]:
        """トップページからエリア版 (関東/東海/…) の URL を取得する。"""
        soup = self._fetch(url)
        if soup is None:
            return []

        urls: list[str] = []
        for a in soup.select("a[href]"):
            absolute = urllib.parse.urljoin(url, a["href"])
            path = urllib.parse.urlparse(absolute).path.rstrip("/")
            if path.lstrip("/") in _REGION_SLUGS and absolute not in urls:
                urls.append(absolute)

        if not urls:
            # トップページの構成変更に備えたフォールバック
            urls = [urllib.parse.urljoin(url, slug) for slug in _REGION_SLUGS]
        return urls

    def _city_urls(self, region_url: str) -> list[str]:
        """エリア版ページから市区町村ページ (/{region}/DC{JISコード}) を取得する。"""
        soup = self._fetch(region_url)
        if soup is None:
            return []

        urls: list[str] = []
        for a in soup.select("a[href]"):
            absolute = urllib.parse.urljoin(region_url, a["href"])
            path = urllib.parse.urlparse(absolute).path
            if _CITY_PATH_RE.match(path) and absolute not in urls:
                urls.append(absolute)
        return urls

    def _crawl_city(self, city_url: str, seen_jobs: set[str]) -> Generator[dict, None, None]:
        """市区町村ページをページ送りしながら、求人詳細を 1 件ずつ yield する。"""
        city_seen: set[str] = set()
        total = None

        for page in range(1, _MAX_PAGES_PER_CITY + 1):
            page_url = city_url if page == 1 else f"{city_url}?page={page}"
            soup = self._fetch(page_url)
            if soup is None:
                return

            if total is None:
                total = self._result_total(soup)

            page_ids = self._job_ids(soup)
            if not page_ids:
                return
            # 範囲外のページは 1 ページ目と同じ内容が返るため、新規 ID が
            # 1 件も無ければ最終ページとみなす
            new_ids = [job_id for job_id in page_ids if job_id not in city_seen]
            if not new_ids:
                return
            city_seen.update(new_ids)

            for job_id in new_ids:
                if job_id in seen_jobs:
                    continue
                seen_jobs.add(job_id)
                detail_url = urllib.parse.urljoin(city_url, f"/kyujin/{job_id}")
                item = self._parse_detail(detail_url, job_id)
                if item:
                    yield item

            if total is not None and len(city_seen) >= total:
                return

    @staticmethod
    def _result_total(soup: bs4.BeautifulSoup) -> int | None:
        """検索結果の総件数 (`検索結果:<span class="num-txt">9</span>件`) を返す。"""
        num = soup.select_one(".result-num .num-txt")
        if num is None:
            return None
        digits = re.sub(r"[^\d]", "", num.get_text())
        return int(digits) if digits else None

    @staticmethod
    def _job_ids(soup: bs4.BeautifulSoup) -> list[str]:
        """一覧ページ内の求人 ID を出現順に (重複を除いて) 返す。"""
        ids: list[str] = []
        for a in soup.select(".mod-jobResultBox-wrap a[href]"):
            m = _JOB_PATH_RE.match(urllib.parse.urlparse(a["href"]).path)
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
        return ids

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, job_id: str) -> dict | None:
        soup = self._fetch(detail_url)
        if soup is None:
            return None

        # 「募集要項」と「企業情報」の 2 テーブル。重複するラベルは会社名のみで
        # 内容も同一のため、まとめて 1 つの辞書に展開する。
        fields: dict[str, str] = {}
        for table in soup.select("table.mod-table1"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                value = _clean(td.get_text(" ", strip=True))
                if label and value and label not in fields:
                    fields[label] = value

        name = fields.get("会社名", "")
        if not name:
            return None

        # 本社所在地が無い求人は勤務地を住所として採用する
        address = fields.get("本社所在地") or fields.get("勤務地", "")
        address = re.sub(r"\s*[<＜]アクセス[>＞].*$", "", address).strip()
        post_code = ""
        post_match = _POST_CODE_RE.match(address)
        if post_match:
            post_code = post_match.group(1).replace("‐", "-").replace("－", "-")
            if "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"
            address = address[post_match.end():].strip()
        pref_match = _PREF_PATTERN.search(address)

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref_match.group(1) if pref_match else "",
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.REP_NM: fields.get("代表者", ""),
            Schema.EMP_NUM: fields.get("従業員数", ""),
            Schema.LOB: fields.get("事業内容", ""),
            Schema.CAP: fields.get("資本金", ""),
            Schema.SALES: fields.get("売上高", ""),
            Schema.HP: self._homepage(fields.get("ホームページアドレス", "")),
            Schema.OPEN_DATE: _to_iso_date(fields.get("設立", "")),
            "求人ID": job_id,
            "求人特徴": self._feature_tags(soup),
        }
        for label, column in _EXTRA_LABELS.items():
            value = fields.get(label, "")
            if column == "更新日":
                value = _to_iso_date(value)
            item[column] = value
        return item

    @staticmethod
    def _homepage(value: str) -> str:
        """ホームページ欄から URL 部分だけを取り出す。"""
        m = re.search(r"https?://\S+", value)
        return m.group(0) if m else value

    @staticmethod
    def _feature_tags(soup: bs4.BeautifulSoup) -> str:
        """求人特徴アイコン (経験者優遇・完全週休2日制 等) を「/」区切りで返す。"""
        tags: list[str] = []
        for span in soup.select(".mod-iconSearchKey .icon"):
            text = _clean(span.get_text())
            if text and text not in tags:
                tags.append(text)
        return " / ".join(tags)


if __name__ == "__main__":
    scraper = DenkoNaviScraper()
    scraper.execute("https://www.denko-navi.com/")

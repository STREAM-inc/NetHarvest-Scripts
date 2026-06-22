"""
Workin.jp 岡山県 — 求人情報サイト Workin.jp の岡山県エリア求人スクレイパー

取得対象:
    - 岡山県の求人掲載企業・事業所情報 (募集企業 / 勤務先事業所 / 求人条件)

取得フロー:
    一覧ページ (/okayama/search?srchBtn=1) から各求人の詳細ページ URL を収集し、
    詳細ページ内の __NEXT_DATA__ (Next.js のサーバーサイド埋め込み JSON) を
    パースして構造化フィールドを取得する。detail を 1 件取得するごとに即 yield する
    (Pattern B / 早期 yield)。ページネーションは url から派生した &page=N を辿り、
    新規の求人 ID が得られなくなった時点で終了する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/workin.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id workin
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページ URL: /<pref>/jobs/<offer_id>/<index>
_JOB_HREF_RE = re.compile(r"/jobs/(\d+)/(\d+)")
# 勤務地文字列の先頭にある郵便番号
_POSTCODE_RE = re.compile(r"(\d{3}-?\d{4})")
_MAX_PAGES = 50  # 安全弁 (岡山県は実質 1 ページだが将来のエリア拡張に備える)


def _clean(value) -> str:
    """HTML 片を含む文字列を整形する (<br>/&nbsp; を空白化し、連続空白を畳む)。"""
    if value is None:
        return ""
    s = str(value)
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = s.replace("&nbsp;", " ").replace("　", " ").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _with_page(url: str, page: int) -> str:
    """url にページ番号クエリを付与する (引数 url を唯一のルートとして派生させる)。"""
    if page <= 1:
        return url
    parts = urlsplit(url)
    query = [(k, v) for (k, v) in parse_qsl(parts.query) if k != "page"]
    query.append(("page", str(page)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class WorkinScraper(StaticCrawler):
    """Workin.jp 岡山県 求人企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "雇用形態",
        "職種名",
        "勤務先事業所",
        "給与",
        "勤務時間",
        "アクセス",
        "市区町村",
        "募集企業所在地",
        "募集企業郵便番号",
        "売上高",
        "特徴タグ",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        while page <= _MAX_PAGES:
            list_url = _with_page(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 詳細ページ URL を重複なく収集
            detail_urls: list[str] = []
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                m = _JOB_HREF_RE.search(href)
                if not m:
                    continue
                key = f"{m.group(1)}/{m.group(2)}"
                if key in seen:
                    continue
                seen.add(key)
                detail_urls.append(urljoin(url, href))

            if page == 1:
                self.total_items = len(detail_urls)

            if not detail_urls:
                break  # 新規求人が無くなったら終了 (= 最終ページ)

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別求人のエラーはスキップして継続
                    self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item  # 1 件取得ごとに即 yield

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        nd = soup.find("script", id="__NEXT_DATA__")
        if nd is None or not nd.string:
            return None
        try:
            offer = json.loads(nd.string)["props"]["pageProps"]["initialProps"]["offer"]
        except (KeyError, ValueError, TypeError):
            return None

        # 募集企業 (顧客企業) を名称の主体とし、無ければ勤務先事業所名で代替
        name = _clean(offer.get("kokyaku_name")) or _clean(offer.get("yago_name"))
        if not name:
            return None

        # 会社概要 (label: value)
        company = {c.get("label"): c.get("value") for c in (offer.get("company_info") or [])}

        # 勤務地: 先頭の郵便番号と「受動喫煙対策」以降を分離
        kinmuchi_raw = _clean(offer.get("kinmuchi"))
        kinmuchi = re.split(r"受動喫煙対策", kinmuchi_raw)[0].strip()
        post_code = ""
        m = _POSTCODE_RE.match(kinmuchi)
        if m:
            post_code = m.group(1)
            kinmuchi = kinmuchi[m.end():].strip()

        # 職種カテゴリ (サイト定義業種)
        shokushu = offer.get("shokushu") or {}
        cat_parts = [shokushu.get("shokushu_gr_name"), shokushu.get("shokushu_name")]
        cat_site = "／".join(p for p in cat_parts if p)

        # 市区町村
        areas = offer.get("areas") or []
        shiku = _clean(areas[0].get("shiku_name")) if areas else ""

        # 給与 (求人説明の pay 項目)
        pay = ""
        for d in (offer.get("offerDescriptionItems") or []):
            if d.get("type") == "pay":
                pay = _clean(d.get("content"))
                break

        # 特徴タグ
        tags = "／".join(_clean(t.get("text")) for t in (offer.get("tags") or []) if t.get("text"))

        item = {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.PREF: _clean(offer.get("prefectureName")),
            Schema.POST_CODE: post_code,
            Schema.ADDR: kinmuchi,
            Schema.TEL: _clean(offer.get("daihyo_tel")) or _clean(offer.get("obo_tel")),
            Schema.REP_NM: _clean(company.get("代表者")),
            Schema.EMP_NUM: _clean(company.get("従業員数")),
            Schema.LOB: _clean(company.get("事業内容")) or _clean(offer.get("naiyo")),
            Schema.OPEN_DATE: _clean(company.get("創業・設立")),
            Schema.HP: _clean(offer.get("url")),
            Schema.CAT_SITE: cat_site,
            "雇用形態": _clean(offer.get("occupationCategory")),
            "職種名": _clean(offer.get("offerTitle")),
            "勤務先事業所": _clean(offer.get("yago_name")),
            "給与": pay,
            "勤務時間": _clean(offer.get("kinmu")),
            "アクセス": _clean(offer.get("tsukin")),
            "市区町村": shiku,
            "募集企業所在地": _clean(offer.get("addr_name3")),
            "募集企業郵便番号": _clean(offer.get("yubin_no")),
            "売上高": _clean(company.get("売上高")),
            "特徴タグ": tags,
        }
        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = WorkinScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://workin.jp/okayama/search?srchBtn=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
ミツケル (mycareer-jp.com) — 外国人材向け求人サイト「ミツケルにほんのしごと」のクローラー

取得対象:
    - /area/FW (外国人材向け求人) の一覧に掲載された求人 (推定 約 477 件 / 16 ページ)
    - 各求人の詳細ページ (/kyujin/{id}) から会社名・勤務地・募集職種などを取得

取得フロー:
    1. 一覧ページ (?page=N) をページネーションで巡回し /kyujin/{id} へのリンクを収集
    2. クランプ (末尾以降は最終ページを返す) を検知するため、新規 ID が無くなったら停止
    3. 各詳細ページの募集要項テーブル (table.mod-table1) と breadcrumb をパース
    4. 詳細を 1 件取得するごとに即 yield (Pattern B / 早期 yield)

備考対応:
    - 引数 url (= sites.yml の url = https://mycareer-jp.com/area/FW) を唯一のルートとして使用。
      area フィルタ (FW) は url に内包され、ページ送りは url からの派生 (?page=N) で維持される。
    - サイト共通の相談用電話番号 (075-342-5075) は全求人で同一のため TEL には採用しない。
    - 仕事内容/給与備考/応募資格/待遇/休日休暇/勤務時間 等の長文プロースは
      著作権リスク回避のため取得対象から除外。

実行方法:
    python scripts/sites/jobs/mycareer_jp.py
    docker compose exec worker python /app/bin/run_flow.py --site-id mycareer_jp
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 勤務地などから都道府県を抽出するためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class MycareerJpScraper(StaticCrawler):
    """ミツケル (mycareer-jp.com) スクレイパー"""

    DELAY = 1.5
    MAX_PAGES = 100  # クランプ無限ループ防止のセーフティ

    EXTRA_COLUMNS = [
        "求人ID",
        "お仕事No.",
        "募集職種",
        "雇用形態",
        "給与",
        "応募方法",
        "面接地",
        "職種カテゴリ",
        "特徴・条件",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        for page in range(1, self.MAX_PAGES + 1):
            list_url = self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            hrefs = [
                a.get("href")
                for a in soup.select(".mod-jobResultBox h2 a[href]")
                if a.get("href") and re.search(r"/kyujin/\d+", a.get("href"))
            ]
            # クランプ検知用に新規 ID を抽出
            new_hrefs = []
            for href in hrefs:
                m = re.search(r"/kyujin/(\d+)", href)
                if not m:
                    continue
                jid = m.group(1)
                if jid in seen:
                    continue
                seen.add(jid)
                new_hrefs.append((jid, urljoin(url, href)))

            if not new_hrefs:
                # 末尾以降は最終ページがクランプ返却されるため、新規が無ければ終了
                self.logger.info("新規求人なし。ページ %d で巡回終了 (累計 %d 件)", page, len(seen))
                break

            self.total_items = len(seen)

            for jid, detail_url in new_hrefs:
                try:
                    item = self._scrape_detail(jid, detail_url)
                    if item and item.get(Schema.NAME):
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                    continue

    def _page_url(self, url: str, page: int) -> str:
        """引数 url を起点にページ送り URL を派生させる。"""
        if page <= 1:
            return url
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}page={page}"

    def _scrape_detail(self, jid: str, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 募集要項テーブル (th -> td)
        fields: dict[str, str] = {}
        table = soup.select_one("table.mod-table1")
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    fields[_clean(th.get_text())] = _clean(td.get_text(" "))

        # breadcrumb: ['ホーム', 都道府県, ...エリア..., 職種カテゴリ]
        crumbs = [
            _clean(a.get_text())
            for a in soup.select(".breadcrumb a")
            if _clean(a.get_text()) and _clean(a.get_text()) != "ホーム"
        ]
        pref = crumbs[0] if crumbs else ""
        category = crumbs[-1] if len(crumbs) > 1 else ""

        work_place = fields.get("勤務地", "")
        # 都道府県は breadcrumb 優先、無ければ勤務地から抽出
        if not _PREF_PATTERN.fullmatch(pref):
            m = _PREF_PATTERN.search(work_place)
            if m:
                pref = m.group(1)

        # 特徴・条件 (メリットアイコン) を構造化短ラベルとして収集
        icons = [
            _clean(s.get_text())
            for s in soup.select(".mod-iconSearchKey .icon, .icon-merit")
        ]
        # 重複除去 (順序維持)
        seen_icon: set[str] = set()
        merits = []
        for ic in icons:
            if ic and ic not in seen_icon:
                seen_icon.add(ic)
                merits.append(ic)

        return {
            Schema.NAME: fields.get("受付担当者", ""),
            Schema.URL: url,
            Schema.PREF: pref,
            Schema.ADDR: work_place,
            "求人ID": jid,
            "お仕事No.": fields.get("お仕事No.", ""),
            "募集職種": fields.get("募集職種", ""),
            "雇用形態": fields.get("雇用形態", ""),
            "給与": fields.get("給与", ""),
            "応募方法": fields.get("応募方法", ""),
            "面接地": fields.get("面接地", ""),
            "職種カテゴリ": category,
            "特徴・条件": " / ".join(merits),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MycareerJpScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://mycareer-jp.com/area/FW")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

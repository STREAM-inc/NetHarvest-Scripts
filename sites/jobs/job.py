"""
メリット（Webメリット｜鳥取・島根のおしごとサーバー）— 求人企業情報スクレイパー

取得対象:
    - job.merit-inc.net の求人検索結果に掲載された各求人の企業情報
      (企業名・勤務地・電話番号・HP・勤務時間・休日・職種ジャンル 等)

取得フロー:
    一覧（検索結果, CakePHP 形式ページネーション /search/page:N）→ 各求人の詳細ページ。
    一覧ボックスから企業名・キーワード(エリア/雇用形態/職種)・更新日・詳細URLを取得し、
    詳細ページ1件を取得するたびに即 yield する (Pattern B / 早期 yield)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id job
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページのみで意味を持つラベル → 抽出対象 (自由記述プロースの本文系は意図的に除外)
_DETAIL_LABEL_MAP = {
    "勤務地": Schema.ADDR,
    "時間": Schema.TIME,
    "休日": Schema.HOLIDAY,
}

# 電話番号らしき文字列 (ハイフン区切りの数字)
_TEL_PATTERN = re.compile(r"0\d{1,4}[-－]\d{1,4}[-－]\d{3,4}")

# 都道府県抽出
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|(?:大阪|京都)府|"
    r"(?:神奈川|和歌山|鹿児島|"
    r"青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|新潟|富山|石川|福井|"
    r"山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|鳥取|島根|岡山|広島|山口|徳島|"
    r"香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|沖縄)県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class MeritJobScraper(StaticCrawler):
    """メリット（Webメリット）求人企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "雇用形態", "最終更新日", "求人ID"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)とする。ページURLはここから派生させる。
        page = 1
        while True:
            page_url = self._page_url(url, page)
            soup = self.get_soup(page_url)
            if soup is None:
                break
            boxes = soup.select(".search--listBox")
            if not boxes:
                break

            for box in boxes:
                try:
                    item = self._parse_list_box(box, url)
                    if item is None:
                        continue
                    detail = self._scrape_detail(item[Schema.URL])
                    if detail:
                        item.update({k: v for k, v in detail.items() if v})
                    if item.get(Schema.NAME):
                        yield item  # 詳細1件取得ごとに即 yield
                except Exception as e:  # 個別アイテムのエラーは握りつぶして継続
                    self.logger.warning("アイテム解析でエラー (スキップ): %s", e)
                    continue

            page += 1

    @staticmethod
    def _page_url(root_url: str, page: int) -> str:
        """ルートURLから CakePHP 形式のページURLを生成する。page=1 はルートそのまま。"""
        sp = urlsplit(root_url)
        if page <= 1:
            # fragment (#pageTitleH1) は除去して取得する
            return urlunsplit((sp.scheme, sp.netloc, sp.path, sp.query, ""))
        path_n = f"{sp.path.rstrip('/')}/page:{page}"
        return urlunsplit((sp.scheme, sp.netloc, path_n, sp.query, ""))

    def _parse_list_box(self, box, root_url: str) -> dict | None:
        """一覧ボックスから企業名・キーワード・更新日・詳細URLを取得。"""
        a = box.select_one("h2.search--listBox__shopName a[href]")
        if not a:
            return None
        href = a.get("href", "")
        detail_url = urljoin(root_url, href)
        m = re.search(r"/view/(\d+)", href)
        job_id = m.group(1) if m else ""

        # キーワード span: [0]=エリア, [1]=雇用形態, [2]=職種ジャンル
        kws = [_clean(s.get_text()) for s in box.select("span.search-itemKeyword--default")]

        update = ""
        ud = box.select_one(".updateDate")
        if ud:
            um = re.search(r"(\d{4}/\d{1,2}/\d{1,2})", ud.get_text())
            update = um.group(1) if um else ""

        return {
            Schema.NAME: _clean(a.get_text()),
            Schema.URL: detail_url,
            Schema.CAT_SITE: kws[2] if len(kws) > 2 else "",
            "エリア": kws[0] if len(kws) > 0 else "",
            "雇用形態": kws[1] if len(kws) > 1 else "",
            "最終更新日": update,
            "求人ID": job_id,
        }

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから 勤務地/時間/休日 と 会社情報(電話・HP) を抽出。"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {}

        for tr in soup.select(".search-sgDate__table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())

            field = _DETAIL_LABEL_MAP.get(label)
            if field is not None:
                val = _clean(td.get_text(" "))
                if field == Schema.ADDR:
                    val = val.replace("地図を表示する", "").strip()
                if val and not data.get(field):
                    data[field] = val
                continue

            # 会社情報: 各 div に 企業名 / 住所 / 電話 / 業種(URL含む) が並ぶ
            if label == "会社情報":
                divs = [_clean(d.get_text(" ")) for d in td.find_all("div")]
                for d in divs:
                    if not data.get(Schema.TEL):
                        tm = _TEL_PATTERN.search(d)
                        if tm:
                            data[Schema.TEL] = tm.group(0).replace("－", "-")
                # HP は会社情報セル内のリンクから取得
                hp = td.find("a", href=re.compile(r"^https?://"))
                if not hp:
                    um = re.search(r"https?://[^\s　]+", td.get_text(" "))
                    if um:
                        data[Schema.HP] = um.group(0)
                else:
                    data[Schema.HP] = hp["href"]

        # 都道府県を住所から分離
        addr = data.get(Schema.ADDR)
        if addr:
            pm = _PREF_PATTERN.match(addr)
            if pm:
                data[Schema.PREF] = pm.group(1)

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MeritJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://job.merit-inc.net/job_offering/job_offerings/search?area_id_1=&area_id_2=&area_id_3=&area_id_4=&employment_id=&job_id=&time_id=&tag_id=&x=163&y=50#pageTitleH1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
paiza転職 (paiza) — ITエンジニア向け求人情報サイト スクレイパー

取得対象:
    - 求人一覧 (/career/job_offers) に埋め込まれた JSON から採用企業を列挙
    - 各採用企業の企業詳細 (/recruiters/{id}?job_offer_id={oid}) から企業情報を補完
    - 採用企業単位で 1 行を出力

取得フロー:
    1. ルート URL に ?page=N を付けて一覧ページを取得 (20社/ページ)
    2. <script id="js-react-data-pc-career-job-offers"> の JSON から
       採用企業 (mainJobOffer.recruiter) を取得
    3. 採用企業ごとに企業詳細ページを 1 回だけ取得し、企業情報 (dl) をパース
    4. 企業情報を 1 行にまとめて即 yield (Pattern B)

備考:
    - 事業概況 / 役員略歴 / 拠点・関連会社 などの段落形式の自由記述 (プロース) は
      著作権リスク回避の観点から取得対象から除外している。
    - フィルター指示は無いため全採用企業を取得する。
    - 企業詳細ページは job_offer_id に依存せず企業ごとに同一内容のため、企業単位で 1 回だけ取得する。

実行方法:
    python scripts/sites/jobs/paiza.py
    docker compose exec worker python /app/bin/run_flow.py --site-id paiza
"""

import json
import math
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 一覧ページに埋め込まれた JSON データを持つ <script> の id
_DATA_SCRIPT_ID = "js-react-data-pc-career-job-offers"

# 1 ページあたりの採用企業数 (総ページ数の算出に使用)
_RECRUITERS_PER_PAGE = 20

# 全都道府県 (本社所在地から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 代表者氏名の役職プレフィックス判定 ("代表取締役　山田 太郎" → 役職 + 氏名 に分割)
_POSITION_KEYWORDS = ("代表取締役", "取締役", "代表", "社長", "会長", "CEO", "COO", "CTO", "理事", "所長", "院長")

# 企業詳細 dl のラベル → EXTRA カラム名 (構造化された短い値のみ。自由記述プロースは含めない)
_COMPANY_EXTRA_LABELS = {
    "株式公開": "株式公開",
    "外部資金/調達額": "外部資金_調達額",
    "主要株主": "主要株主",
    "主要取引先": "主要取引先",
    "事業構成比": "事業構成比",
    "売上高": "売上高",
    "営業利益": "営業利益",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class PaizaScraper(StaticCrawler):
    """paiza転職 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "株式公開",
        "外部資金_調達額",
        "主要株主",
        "主要取引先",
        "事業構成比",
        "売上高",
        "営業利益",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        max_page = None
        while True:
            list_url = f"{url}?page={page}"
            data = self._fetch_list_data(list_url)
            if not data:
                break

            search_result = data.get("searchResult", {})
            groups = search_result.get("jobOffersByRecruiters", [])
            if not groups:
                break

            # 初回ページで総件数・総ページ数を確定して進捗表示を有効化
            if max_page is None:
                total_recruiters = search_result.get("totalJobOffersByRecruitersCount") or 0
                self.total_items = total_recruiters
                max_page = math.ceil(total_recruiters / _RECRUITERS_PER_PAGE) if total_recruiters else None
                self.logger.info(
                    "paiza 採用企業: %d 社 (推定 %s ページ)",
                    total_recruiters, max_page,
                )

            for group in groups:
                main = group.get("mainJobOffer") or {}
                recruiter = main.get("recruiter") or {}
                recruiter_id = recruiter.get("id")
                if recruiter_id is None:
                    continue

                # 企業詳細は job_offer_id に依存せず企業ごとに同一内容のため 1 回だけ取得
                company = self._scrape_company(url, recruiter_id, main.get("id"))

                # 採用企業単位で 1 行を出力
                item = self._build_item(url, recruiter, company)
                if item:
                    yield item

            if max_page is not None and page >= max_page:
                break
            page += 1

    def _fetch_list_data(self, list_url: str) -> dict | None:
        """一覧ページを取得し、埋め込み JSON を辞書で返す。"""
        soup = self.get_soup(list_url)
        if soup is None:
            return None
        script = soup.find("script", id=_DATA_SCRIPT_ID)
        if script is None or not script.string:
            self.logger.warning("一覧 JSON が見つかりません: %s", list_url)
            return None
        try:
            return json.loads(script.string)
        except (ValueError, TypeError) as e:  # noqa: BLE001
            self.logger.error("一覧 JSON のパースに失敗: %s — %s", list_url, e)
            return None

    def _scrape_company(self, url: str, recruiter_id, offer_id) -> dict:
        """企業詳細ページから企業情報を取得し、辞書で返す。失敗時は空辞書。"""
        path = f"/recruiters/{recruiter_id}"
        if offer_id is not None:
            path += f"?job_offer_id={offer_id}"
        detail_url = urljoin(url, path)
        soup = self.get_soup(detail_url)
        company: dict = {"_detail_url": detail_url}
        if soup is None:
            return company

        dl = soup.select_one("dl.m-definitions-responsive")
        if dl is None:
            return company

        labels = dl.find_all("dt")
        values = dl.find_all("dd")
        info: dict[str, str] = {}
        for dt, dd in zip(labels, values):
            label = _clean(dt.get_text())
            value = _clean(dd.get_text(" "))
            if label:
                info[label] = value

        # Schema へマッピングする企業情報
        addr = info.get("本社所在地", "")
        if addr:
            m = _PREF_PATTERN.match(addr)
            if m:
                company[Schema.PREF] = m.group(1)
                company[Schema.ADDR] = addr[m.end():].strip()
            else:
                company[Schema.ADDR] = addr

        rep = info.get("代表者氏名", "")
        if rep:
            pos, name = self._split_rep(rep)
            if pos:
                company[Schema.POS_NM] = pos
            company[Schema.REP_NM] = name

        if info.get("従業員数"):
            company[Schema.EMP_NUM] = info["従業員数"]
        if info.get("資本金"):
            company[Schema.CAP] = info["資本金"]
        if info.get("事業内容"):
            company[Schema.LOB] = info["事業内容"]
        if info.get("設立年月"):
            company[Schema.OPEN_DATE] = info["設立年月"]

        # 構造化された短い企業情報のみ EXTRA に格納 (自由記述プロースは除外)
        for label, col in _COMPANY_EXTRA_LABELS.items():
            if info.get(label):
                company[col] = info[label]

        return company

    @staticmethod
    def _split_rep(rep: str) -> tuple[str, str]:
        """"代表取締役　山田 太郎" を (役職, 氏名) に分割する。役職が無ければ ("", 全体)。"""
        parts = rep.split(maxsplit=1)
        if len(parts) == 2 and any(kw in parts[0] for kw in _POSITION_KEYWORDS):
            return parts[0], parts[1].strip()
        return "", rep

    def _build_item(self, url: str, recruiter: dict, company: dict) -> dict | None:
        name = _clean(recruiter.get("name"))
        if not name:
            return None

        # 採用企業の取得 URL (企業詳細ページ)
        item_url = company.get("_detail_url") or urljoin(url, f"/recruiters/{recruiter.get('id')}")

        item: dict = {
            Schema.URL: item_url,
            Schema.NAME: name,
        }

        # 企業情報をマージ (_detail_url 等の内部キーは除外)
        for k, v in company.items():
            if not str(k).startswith("_"):
                item[k] = v

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = PaizaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://paiza.jp/career/job_offers")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

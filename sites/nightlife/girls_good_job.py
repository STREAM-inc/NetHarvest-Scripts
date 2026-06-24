"""
ガールズグッジョブ — すすきのガールズバー求人サイト クローラー

取得対象:
    - 掲載されているガールズバー各店舗の求人/店舗情報
    - 店名 / 都道府県 / 住所 / TEL / 事業内容 / 営業(勤務)時間 / 休日 /
      会社名 / 時給 / 制服 / 接客スタイル / 路面店 / アクセス / 勤務地 /
      勤務日 / 定休日 / 求人タグ

取得フロー (Pattern B: 一覧→詳細 即 yield):
    1. ルート url から一覧ページ ({url}girlsbar_kyujin/) を取得
    2. 一覧の article.index_block 内 a.more_btn から各店舗の詳細 URL を収集
    3. 各詳細ページ (/girlsbar_kyujin/{slug}/) を 1 件取得するごとに即 yield
       (全件収集してから一括 yield はしない)

    ※ ページネーションは存在せず、全店舗が一覧 1 ページに掲載される。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/girls_good_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id girls_good_job
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭の都道府県を切り出す (このサイトは札幌＝北海道のみだが汎用化)
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 一覧/詳細 URL のパス
_LIST_PATH = "girlsbar_kyujin/"
# 店名タイトルの末尾サフィックス
_TITLE_SUFFIX = "の求人情報"


class GirlsGoodJobScraper(StaticCrawler):
    """ガールズグッジョブ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "会社名",
        "時給",
        "制服",
        "接客スタイル",
        "路面店",
        "アクセス",
        "勤務地",
        "勤務日",
        "休日",
        "求人タグ",
    ]

    def parse(self, url: str):
        # ルート url から一覧ページ URL を派生させる (url を唯一の起点とする)
        list_url = urljoin(url, _LIST_PATH)
        soup = self.get_soup(list_url)
        if not soup:
            self.logger.warning("一覧ページを取得できませんでした: %s", list_url)
            return

        # 各店舗の詳細リンク (a.more_btn) を収集
        detail_urls = []
        seen = set()
        for a in soup.select("article.index_block a.more_btn[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            detail_url = urljoin(list_url, href)
            if detail_url not in seen:
                seen.add(detail_url)
                detail_urls.append(detail_url)

        # フォールバック: more_btn が見つからない場合は詳細パスのリンクを総当たり
        if not detail_urls:
            for a in soup.select(f'a[href*="/{_LIST_PATH}"]'):
                href = urljoin(list_url, a.get("href", "").strip())
                m = re.search(r"/girlsbar_kyujin/([a-z0-9_-]+)/?$", href)
                if m and href not in seen:
                    seen.add(href)
                    detail_urls.append(href)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception:  # noqa: BLE001
                self.logger.exception("詳細ページの解析に失敗: %s", detail_url)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            self.logger.warning("詳細ページを取得できませんでした: %s", url)
            return None

        # 店名: h1 "○○の求人情報" からサフィックスを除去
        name = ""
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(strip=True)
            if name.endswith(_TITLE_SUFFIX):
                name = name[: -len(_TITLE_SUFFIX)].strip()

        # 求人サマリ + 勤務情報 (company_info を除く info_list) をラベル→値で集約
        summary = {}
        for dl in soup.select("dl.info_list:not(.company_info)"):
            summary.update(self._dl_pairs(dl))

        # 運営会社情報 (会社名 / 住所 / TEL / 事業内容)
        company = {}
        comp_dl = soup.select_one("dl.company_info")
        if comp_dl:
            company = self._dl_pairs(comp_dl)

        # 求人タグ (チェック済みのもの)
        tags = [t.get_text(" ", strip=True) for t in soup.select(".tag_list .job_tag.-checked")]
        tags = [re.sub(r"\s+", "", t) for t in tags if t]

        # 住所から都道府県を分離
        addr_raw = company.get("住所", "")
        pref, addr = "", addr_raw
        m = _PREF_RE.search(addr_raw)
        if m:
            pref = m.group(1)
            addr = addr_raw[m.end():].strip()
        elif "札幌" in addr_raw:
            # 都道府県表記が省略された札幌の住所は北海道で補完
            pref = "北海道"

        return {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: company.get("TEL", ""),
            Schema.LOB: company.get("事業内容", ""),
            Schema.TIME: summary.get("勤務時間", ""),
            Schema.HOLIDAY: summary.get("定休日", ""),
            Schema.CAT_SITE: "ガールズバー",
            # EXTRA_COLUMNS
            "会社名": company.get("会社名", ""),
            "時給": summary.get("時給", ""),
            "制服": summary.get("制服", ""),
            "接客スタイル": summary.get("接客スタイル", ""),
            "路面店": summary.get("路面店", ""),
            "アクセス": summary.get("アクセス", ""),
            "勤務地": summary.get("勤務地", ""),
            "勤務日": summary.get("勤務日", ""),
            "休日": summary.get("休日", ""),
            "求人タグ": "/".join(tags),
        }

    @staticmethod
    def _dl_pairs(dl) -> dict:
        """dl 内の dt→dd ペアを辞書化 (同一ラベルは後勝ち)"""
        pairs = {}
        dts = dl.find_all("dt", recursive=False) or dl.find_all("dt")
        dds = dl.find_all("dd", recursive=False) or dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(" ", strip=True)
            val = dd.get_text(" ", strip=True)
            if key:
                pairs[key] = val
        return pairs


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GirlsGoodJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://girls-good-job.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
HOTWORKS（山口）ホットワークス — 山口県のナイトワーク求人情報スクレイパー

取得対象:
    - 山口県 (/yamaguchi/) に掲載中のナイトワーク求人 (フロアレディ / コンパニオン等)
    - 店舗名 / 都道府県 / 住所 / TEL / 営業時間 / 店休日 / 業種(サイト定義) / 取得URL
    - サイト固有(EXTRA): 職種 / 給与 / 勤務地 / 勤務時間 / 休日休暇 / 応募資格 / 待遇 /
      アクセス / 座席数

取得フロー:
    1. 引数 url (= /yamaguchi/) を起点に、求人一覧 (url + "job/") を ?p=N でページ送りし、
       求人詳細への相対リンク (job/{id}) を収集する (トップページ url 上の求人カードも併合)。
    2. 各求人詳細ページ (job/{id}) の 3 つの <dl>
       (募集情報 / 応募アピール / 店舗情報) からラベル→値を辞書化し、Schema / EXTRA へ展開。
       応募アピール本文・お客様へのメッセージ等の自由記述プロースは著作権配慮のため取得しない。
    3. 詳細 1 件を取得するごとに即 yield する (Pattern B / 早期 yield)。

備考のフィルタ方針:
    呼び出し時の備考は「募集情報」全般 (求人の掲載内容) を尊重する指示であり、地域や期間の
    絞り込み条件は含まれない。対象地域は引数 url (/yamaguchi/) 自体で確定するため、
    parse() 内での追加フィルタは実装しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/hotworks.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id hotworks
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 求人詳細への相対リンク (例: job/1051)
_JOB_HREF_RE = re.compile(r"(?:^|/)job/(\d+)$")
# 固定電話・携帯番号 (最初の 1 本を代表 TEL に採用)
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")

# 詳細ページの <dl> ラベル → EXTRA カラム名
# (Schema に該当しない、構造化された短い求人・店舗情報のみ。自由記述プロースは含めない)
_EXTRA_LABELS = {
    "職種": "職種",
    "給与": "給与",
    "勤務地": "勤務地",
    "勤務時間": "勤務時間",
    "休日休暇": "休日休暇",
    "応募資格": "応募資格",
    "待遇": "待遇",
    "アクセス": "アクセス",
    "座席数": "座席数",
}


class HotworksScraper(StaticCrawler):
    """HOTWORKS（山口）ホットワークス スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = list(_EXTRA_LABELS.values())

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        job_urls = self._collect_job_urls(url)
        self.total_items = len(job_urls)
        self.logger.info("対象求人URL: %d件", len(job_urls))

        for job_url in job_urls:
            try:
                record = self._scrape_detail(job_url)
            except Exception as e:  # 個別求人の失敗は握りつぶして続行
                self.logger.warning("詳細取得失敗: %s (%s)", job_url, e)
                continue
            if record:
                self.logger.info(
                    "取得: %s (%s)",
                    record.get(Schema.NAME) or "?",
                    record.get(Schema.ADDR) or "",
                )
                yield record

    # ------------------------------------------------------------------ #
    # 求人詳細URLの収集 (トップページ + job/ 一覧のページ送り)
    # ------------------------------------------------------------------ #

    def _collect_job_urls(self, root_url: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _absorb(soup: BeautifulSoup) -> int:
            added = 0
            for a in soup.find_all("a", href=True):
                m = _JOB_HREF_RE.search(a["href"])
                if not m:
                    continue
                detail_url = urljoin(root_url, f"job/{m.group(1)}")
                if detail_url not in seen:
                    seen.add(detail_url)
                    ordered.append(detail_url)
                    added += 1
            return added

        # 1) トップページ (= 引数 url) 上の求人カード
        top = self.get_soup(root_url)
        if top is not None:
            _absorb(top)

        # 2) 求人一覧 job/ を ?p=N でページ送り (新規リンクが無くなるまで)
        list_url = urljoin(root_url, "job/")
        page = 1
        while True:
            soup = self.get_soup(f"{list_url}?p={page}")
            if soup is None:
                break
            added = _absorb(soup)
            if added == 0:
                break
            page += 1

        return ordered

    # ------------------------------------------------------------------ #
    # 求人詳細ページの解析
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        labels = self._collect_dl_labels(soup)
        name = labels.get("店名", "")
        if not name:
            return None

        item: dict = {
            Schema.URL: url,
            Schema.NAME: name,
        }

        # 業種 (スナック / ラウンジ 等) → サイト定義業種
        if labels.get("業種"):
            item[Schema.CAT_SITE] = labels["業種"]

        # 住所: 所在地 (店舗情報) を採用。都道府県は勤務地 (山口県…) から抽出しフォールバックに所在地
        addr = labels.get("所在地", "")
        pref = self._extract_pref(labels.get("勤務地", "")) or self._extract_pref(addr)
        if pref:
            item[Schema.PREF] = pref
        if addr:
            item[Schema.ADDR] = addr

        # TEL: 「083-… 090-…（携帯）」等から先頭 1 本を代表番号として採用 (正規化は Pipeline)
        tel = self._first_tel(labels.get("電話番号", ""))
        if tel:
            item[Schema.TEL] = tel

        # 営業時間 / 店休日 (店舗情報)
        if labels.get("営業時間"):
            item[Schema.TIME] = labels["営業時間"]
        if labels.get("店休日"):
            item[Schema.HOLIDAY] = labels["店休日"]

        # EXTRA (構造化された求人・店舗情報)
        for label, col in _EXTRA_LABELS.items():
            if labels.get(label):
                item[col] = labels[label]

        return item

    # ------------------------------------------------------------------ #
    # ヘルパー
    # ------------------------------------------------------------------ #

    @staticmethod
    def _collect_dl_labels(soup: BeautifulSoup) -> dict:
        """ページ内の全 <dl> の dt→dd ペアを 1 つの辞書にマージして返す。"""
        labels: dict = {}
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for i, dt in enumerate(dts):
                key = dt.get_text(" ", strip=True)
                if not key or i >= len(dds):
                    continue
                val = dds[i].get_text(" ", strip=True)
                if key not in labels or (val and not labels[key]):
                    labels[key] = val
        return labels

    @staticmethod
    def _extract_pref(text: str) -> str:
        if not text:
            return ""
        m = _PREF_PATTERN.search(text)
        return m.group(1) if m else ""

    @staticmethod
    def _first_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(0) if m else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HotworksScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.hotworks.jp/yamaguchi/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
エール株式会社 (HERP careers) — 求人一覧スクレイパー

取得対象:
    - エール株式会社 の全求人 (HERP careers 採用ページ /v1/yell)
    - 各求人詳細の「応募概要」(給与 / 勤務地 / 雇用形態 / 勤務体系 / 試用期間 / 福利厚生)
    - 各求人詳細の「企業情報」(企業名 / 設立年月 / 本社所在地 / 資本金 / 従業員数)

取得フロー (一覧 → 詳細, Pattern B):
    1. 一覧ページ (引数 url) を GET し、求人カードのリンク
       (a.requisition-list-card__header-anchor) を収集する。
       ※ HERP の採用ページはサーバサイドレンダリングされており全求人が 1 ページに出る
         (ページネーション無し)。
    2. 各詳細ページを 1 件取得するごとに即 yield する。

備考対応:
    - 応募概要・企業情報の各フィールドは、呼び出し時の備考で明示的に取得指示があるため、
      自由記述を含むものも含めて EXTRA / Schema へ取り込む。
    - 求人案件本文 (仕事概要 / 必須スキル / 歓迎スキル / 求める人物像 / 募集キャッチコピー) は
      備考で指示が無い長文プロースのため、著作権リスク回避として取得しない。
    - 備考にフィルタ条件 (エリア・期間等) の指定は無いため parse() にフィルタは実装しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/https_herp_careers_v1_yell.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id https_herp_careers_v1_yell
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

# 都道府県プレフィックス (住所先頭から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class HerpCareersV1YellScraper(StaticCrawler):
    """エール株式会社 (HERP careers) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種名",      # 求人タイトル (短いラベル)
        "給与",        # 応募概要
        "勤務地",      # 応募概要
        "雇用形態",    # 応募概要
        "勤務体系",    # 応募概要
        "試用期間",    # 応募概要
        "福利厚生",    # 応募概要
    ]

    def parse(self, url: str):
        # 引数 url を唯一のルート (起点) とする
        soup = self.get_soup(url)
        if soup is None:
            return

        anchors = soup.select("a.requisition-list-card__header-anchor[href]")
        # 念のためフォールバック (構造変化時)
        if not anchors:
            anchors = [
                a for a in soup.select("a[href]")
                if re.search(r"/v1/[^/]+/[\w-]+$", a.get("href", ""))
            ]

        # 重複除去しつつ詳細 URL を url から派生
        detail_urls = []
        seen = set()
        for a in anchors:
            href = a.get("href", "").strip()
            if not href:
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                detail_urls.append(full)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # 個別エラーはログして継続
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 求人タイトル・企業名
        job_title = self._text(soup.select_one(".requisition-header__name"))
        company = self._text(soup.select_one(".requisition-header__company"))

        # kv-table (応募概要 + 企業情報) を th ラベルでまとめて辞書化
        kv = {}
        for tr in soup.select(".kv-table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text("\n", strip=True)
            if label and label not in kv:
                kv[label] = value

        # 企業名は kv-table 優先、無ければヘッダ
        name = kv.get("企業名") or company

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.CAP: kv.get("資本金", ""),
            Schema.EMP_NUM: kv.get("従業員数", ""),
            Schema.OPEN_DATE: self._normalize_date(kv.get("設立年月", "")),
            "職種名": job_title,
            "給与": kv.get("給与", ""),
            "勤務地": kv.get("勤務地", ""),
            "雇用形態": kv.get("雇用形態", ""),
            "勤務体系": kv.get("勤務体系", ""),
            "試用期間": kv.get("試用期間", ""),
            "福利厚生": kv.get("福利厚生", ""),
        }

        # 本社所在地 → 都道府県 + 住所
        hq = kv.get("本社所在地", "")
        if hq:
            m = _PREF_PATTERN.match(hq)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = hq[m.end():].strip()
            else:
                item[Schema.ADDR] = hq
        else:
            item[Schema.ADDR] = ""

        return item

    @staticmethod
    def _text(el) -> str:
        return el.get_text(" ", strip=True) if el else ""

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """'2013年6月4日' -> '2013-06-04', '2013年6月' -> '2013-06'。失敗時は原文。"""
        if not raw:
            return ""
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", raw)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = re.search(r"(\d{4})年(\d{1,2})月", raw)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}"
        m = re.search(r"(\d{4})年", raw)
        if m:
            return m.group(1)
        return raw


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HerpCareersV1YellScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://herp.careers/v1/yell")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

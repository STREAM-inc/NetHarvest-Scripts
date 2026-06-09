"""
ミニモ (minimo) — 美容サロンスタッフのプロフィール情報スクレイパー

取得対象:
    - 全6ジャンル (ヘア / ネイル / マツエク / エステ・リラク / 眉毛 / その他美容) の
      掲載スタッフのプロフィール情報

取得フロー:
    一覧ページ (/list/{genre}/0/c0/0?p=N) を全ジャンル・全ページ巡回して
    スタッフ詳細ページ (/r/{id}) のURLを収集し、詳細を1件ずつ取得して即 yield する。
    (Pattern B: 途中で中断しても無駄な通信が起きない)

備考対応:
    - エリア / 営業時間 / 支払方法 / クレカ / 名称 を取得 (備考要望)
    - 電話番号: /r/{id}/tel が JS モーダル描画のため静的HTMLに存在せず取得不可
    - 「施術について」は長文の自由記述プロースのため著作権リスクで除外

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/minimodel.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id minimodel
"""

import re
import sys
from pathlib import Path
from typing import Generator

# scripts/sites/beauty/minimodel.py → プロジェクトルートは .parent x4
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class MinimodelScraper(StaticCrawler):
    """ミニモ (minimo) スタッフプロフィールスクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "エリア",
        "クレジットカード",
        "席数",
        "駐車場",
        "その他",
    ]

    BASE = "https://minimodel.jp"

    # ジャンルID → ジャンル名 (一覧巡回・ログ用)
    GENRES = {
        1: "ヘア",
        2: "ネイル",
        3: "マツエク",
        4: "エステ・リラク",
        5: "眉毛",
        6: "その他美容",
    }

    # スタッフ詳細URL: /r/{id} (末尾の /menu, /tel 等は除く)
    _RID_RE = re.compile(r"^/r/([A-Za-z0-9]+)(?:[/?#]|$)")
    # title: "{スタッフ名}のプロフィール・予約({サロン名}) - ミニモ"
    _TITLE_RE = re.compile(r"^(.+?)のプロフィール・予約[（(](.+?)[)）]")
    # meta description: "「{スタッフ}({職種})」のプロフィール情報。{住所}周辺にある「{サロン}」で…"
    _ROLE_RE = re.compile(r"[（(]([^（）()]+)[)）]」のプロフィール情報")
    _ADDR_RE = re.compile(r"のプロフィール情報。(.+?)周辺にある")

    # 営業時間の曜日 → Schema 定数
    _DOW_MAP = {
        "月": Schema.TIME_MON,
        "火": Schema.TIME_TUE,
        "水": Schema.TIME_WED,
        "木": Schema.TIME_THU,
        "金": Schema.TIME_FRI,
        "土": Schema.TIME_SAT,
        "日": Schema.TIME_SUN,
    }

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全ジャンル・全ページを巡回し、スタッフ詳細を1件ずつ取得して yield する。"""
        seen: set[str] = set()

        for genre_id, genre_name in self.GENRES.items():
            page = 1
            while True:
                list_url = f"{self.BASE}/list/{genre_id}/0/c0/0?p={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    # 通信エラー (404含む) → このジャンルの末尾と判断
                    break

                page_ids = self._extract_staff_ids(soup)
                new_ids = [i for i in page_ids if i not in seen]
                if not page_ids:
                    # スタッフが1件も無い → 末尾ページ
                    break

                self.logger.info(
                    "[%s] p%d: %d件 (新規 %d件)",
                    genre_name, page, len(page_ids), len(new_ids),
                )

                for sid in new_ids:
                    seen.add(sid)
                    detail_url = f"{self.BASE}/r/{sid}"
                    try:
                        item = self._scrape_detail(detail_url)
                        if item:
                            yield item
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                        continue

                page += 1

    def _extract_staff_ids(self, soup) -> list[str]:
        """一覧ページから重複を除いたスタッフID (/r/{id}) を出現順で返す。"""
        ids: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href^="/r/"]'):
            m = self._RID_RE.match(a.get("href", ""))
            if m:
                sid = m.group(1)
                if sid not in seen:
                    seen.add(sid)
                    ids.append(sid)
        return ids

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # --- 名称 (スタッフ名) / サロン名 (施設名) : title から ---
        title = soup.title.get_text(strip=True) if soup.title else ""
        m = self._TITLE_RE.match(title)
        if m:
            data[Schema.NAME] = m.group(1).strip()
            data[Schema.FAC_NAME] = m.group(2).strip()
        else:
            # フォールバック: H1 ("{サロン} ( {kana} ) {スタッフ}" 等)
            h1 = soup.find("h1")
            if h1:
                data[Schema.NAME] = h1.get_text(" ", strip=True)

        # --- 職種 (CAT_SITE) / 住所 (ADDR) : meta description から ---
        md = soup.find("meta", attrs={"name": "description"})
        desc = md.get("content", "") if md else ""
        mr = self._ROLE_RE.search(desc)
        if mr:
            data[Schema.CAT_SITE] = mr.group(1).strip()
        ma = self._ADDR_RE.search(desc)
        if ma:
            # 例: "渋谷区神宮前・表参道駅" → 駅情報を除いた住所部分を採用
            addr = ma.group(1).split("・")[0].strip()
            data[Schema.ADDR] = addr

        # --- dl (dt/dd) の構造化フィールドをラベルで判定して取得 ---
        labels = self._collect_dl(soup)

        if "エリア" in labels:
            data["エリア"] = labels["エリア"]
        if "席数" in labels:
            data["席数"] = labels["席数"]
        if "駐車場" in labels:
            data["駐車場"] = labels["駐車場"]
        if "その他" in labels:
            data["その他"] = labels["その他"]

        # --- 営業時間 (全文 + 曜日別) ---
        hours = labels.get("営業時間", "")
        if hours:
            data[Schema.TIME] = hours
            self._parse_hours(hours, data)

        # --- お支払い (支払方法 + クレジットカードブランド) ---
        pay = labels.get("お支払い", "")
        if pay:
            data[Schema.PAYMENTS] = pay
            mc = re.search(r"クレジットカード\s*(.+?)(?:\s*その他決済|$)", pay)
            if mc:
                data["クレジットカード"] = mc.group(1).strip()

        # 名称が取れなければ無効レコードとして破棄
        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _collect_dl(soup) -> dict:
        """全 dl の dt → dd を辞書化する。dd は改行を保持して取得する。"""
        labels: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                key = dt.get_text(strip=True)
                val = dd.get_text("\n", strip=True)
                if key and key not in labels:
                    labels[key] = val
        return labels

    def _parse_hours(self, hours: str, data: dict) -> None:
        """"月 10:00-22:00" 形式の各行を曜日別カラムに展開する。"""
        for line in hours.splitlines():
            line = line.strip()
            if not line:
                continue
            dow = line[0]
            col = self._DOW_MAP.get(dow)
            if col:
                rest = line[1:].strip()
                if rest:
                    data[col] = rest


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MinimodelScraper()
    scraper.execute("https://minimodel.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

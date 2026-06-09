"""
ダンサガ (dancesagasu.net) — ダンススクール・ダンス教室検索ポータル スクレイパー

取得対象:
    - 全国のダンススクール / ダンス教室の基本情報
      (名称・ジャンル・所在地・TEL・HP・曜日別レッスン時間)

取得フロー:
    一覧 (search.php?prefcode={1..47}&page={N})  ← 都道府県別・1ページ10件
      └─ 詳細 (school.php?school_num={N})         ← th/td テーブルを解析

    一覧→詳細 (Pattern B): 詳細を1件取得するごとに即 yield する。
    途中で中断しても無駄な通信が起きないようにするため。

    ※ ルート URL は parse() の引数 url のみを起点とする (SSOT = sites.yml の url)。
      一覧・詳細 URL はすべて urljoin(url, ...) で派生させる。

注意 — TEL の取得率が低い理由:
    セレクタ/正規表現のバグではなく、出典データ自体の欠落が主因。多くの店舗で電話番号が
    「未登録」と表記されており、詳細テーブルにも tel: リンクにもページ内のどこにも実番号が
    存在しない (掲載が無いだけ)。掲載がある店舗も JDAC 系列など共通の IP 電話 (050-…) を
    共有しているケースが多い。詳細は _scrape_detail() の TEL 抽出部コメント参照。

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/dancesagasu.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id dancesagasu
"""

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

# 都道府県 (JIS コード順 1..47)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

_DAY_SCHEMA = {
    "月": Schema.TIME_MON,
    "火": Schema.TIME_TUE,
    "水": Schema.TIME_WED,
    "木": Schema.TIME_THU,
    "金": Schema.TIME_FRI,
    "土": Schema.TIME_SAT,
    "日": Schema.TIME_SUN,
}

_PREF_CODES = range(1, 48)  # 1..47
_MAX_PAGES = 300  # 暴走防止の安全上限 (通常はクランプ検出で停止)


def _clean(s) -> str:
    """全角空白・nbsp・BOM・連続空白を除去して整形する。"""
    if s is None:
        return ""
    s = str(s).replace("　", " ").replace("\xa0", " ").replace("﻿", "")
    return re.sub(r"\s+", " ", s).strip()


class DancesagasuScraper(StaticCrawler):
    """ダンサガ (dancesagasu.net) ダンススクール検索ポータル スクレイパー"""

    DELAY = 1.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url を唯一のルートとして一覧/詳細 URL を派生させる
        search_url = urljoin(url, "search.php")
        seen: set[str] = set()  # 全体での school_num 重複除去

        for prefcode in _PREF_CODES:
            prev_ids: list[str] = []
            for page in range(1, _MAX_PAGES + 1):
                list_url = f"{search_url}?prefcode={prefcode}&page={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                ids = self._extract_school_ids(soup)
                # 0 件 → この都道府県は終了
                if not ids:
                    break
                # 末尾を超えると直前ページを繰り返す (クランプ) → 終了
                if ids == prev_ids:
                    break
                prev_ids = ids

                new_ids = [i for i in ids if i not in seen]
                if not new_ids:
                    # 既出のみ (重複ページ) → この都道府県は終了
                    break

                for school_num in new_ids:
                    seen.add(school_num)
                    detail_url = urljoin(url, f"school.php?school_num={school_num}")
                    try:
                        item = self._scrape_detail(detail_url)
                    except Exception as e:  # 個別アイテムのエラーは握りつぶして継続
                        self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                        continue
                    if item and item.get(Schema.NAME):
                        yield item

    @staticmethod
    def _extract_school_ids(soup) -> list[str]:
        """一覧ページから school_num を出現順 (重複除去) で取得する。"""
        ids: list[str] = []
        for a in soup.select("article.search_list_items a[href*='school.php']"):
            m = re.search(r"school_num=(\d+)", a.get("href", ""))
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
        return ids

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # --- TEL (p.schoolDt_header_txt_tel.tel) ---
        # 取得率が低いのは「セレクタ/正規表現のバグ」ではなく "出典データの欠落" が主因。
        #   ・電話番号は p.schoolDt_header_txt_tel.tel に平文で入る (このセレクタで正しく拾える)。
        #   ・ただしサイト側で多くの店舗が「未登録」表記になっており、詳細テーブルにも tel: リンクにも
        #     実番号が一切存在しない (= ページ内のどこからも取得不能。そもそも掲載が無いだけ)。
        #   ・掲載がある店舗も JDAC 系列など共通の IP 電話 (050-…) を共有しているケースが多い。
        # → 「未登録」プレースホルダは数字を含まず自然に除外されるが、明示的にスキップして意図を明確化。
        #    実番号があるときのみ TEL を設定する。
        tel_el = soup.select_one("p.schoolDt_header_txt_tel, p.tel")
        if tel_el:
            tel_txt = _clean(tel_el.get_text())
            if "未登録" not in tel_txt:
                m = re.search(r"0\d[\d\-]{7,}", tel_txt)
                if m:
                    item[Schema.TEL] = m.group(0)

        # --- 詳細テーブル (th/td) の走査 ---
        labels: dict[str, object] = {}
        for tr in soup.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            if key and key not in labels:
                labels[key] = td

        # 名称
        if "名称" in labels:
            item[Schema.NAME] = _clean(labels["名称"].get_text())

        # ジャンル → サイト定義ジャンル
        if "ジャンル" in labels:
            item[Schema.CAT_SITE] = _clean(labels["ジャンル"].get_text())

        # 所在地 → 郵便番号 / 都道府県 / 住所
        if "所在地" in labels:
            addr_raw = _clean(labels["所在地"].get_text())
            mp = _POST_PATTERN.search(addr_raw)
            if mp:
                item[Schema.POST_CODE] = mp.group(1)
                addr_raw = _clean(addr_raw[mp.end():])
            mpref = _PREF_PATTERN.match(addr_raw)
            if mpref:
                item[Schema.PREF] = mpref.group(1)
                item[Schema.ADDR] = _clean(addr_raw[mpref.end():])
            else:
                item[Schema.ADDR] = addr_raw

        # ホームページ → HP (出現率低: 無い場合は空)
        if "ホームページ" in labels:
            a = labels["ホームページ"].find("a", href=True)
            item[Schema.HP] = a["href"].strip() if a else _clean(labels["ホームページ"].get_text())

        # 曜日別レッスン時間 (月..日)
        for day, schema_col in _DAY_SCHEMA.items():
            if day in labels:
                val = _clean(labels[day].get_text())
                if val:
                    item[schema_col] = val

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DancesagasuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://dancesagasu.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

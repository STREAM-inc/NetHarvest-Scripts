"""
キャバクラ求人 ホッケ — キャバクラ・ガールズバー求人情報

取得対象:
    - 店舗名 / 都道府県 / 住所 / TEL / 業種 / 営業時間 / 定休日 / HP
    - 求人番号 / エリア / 時給 / 勤務時間 / 卓数 / 在籍人数 / 衣装 / VIPルーム
    - このお店について: 料金システム / 年齢層 / 客層 / NG / 罰金
    - お店から: 求めるタイプ / SNS
    - キャバクラ求人 ホッケから: 職場の雰囲気 / 担当者
    - 出勤について: 出勤体制
    - お給料について: 体験入店 / 本入店時給 / お給料システム / 各種歩合 / 給与日 / 日払い / 控除 / 保証期間
    - 待遇について: 送迎 / ヘアメイク / レンタル衣装・靴・鞄 / フェイクカクテル / カラオケ / 終電上がり
    - 採用に関して: 面接可能時間帯 / 応募者年齢層 / 資格 / 身分証明書

取得フロー:
    1. {url}store/ → {url}store/page/{N}/ をページ送り (10件/ページ, 最大22ページ)
    2. 各 li.store--box から求人番号・店舗名・エリア・職種・時給・TEL・詳細URL を取得
    3. 詳細ページ div.box-recruit の全セクション (このお店について / お店から /
       待遇について / 採用に関して / お給料について / 出勤について 等) の
       全ラベル→値を抽出し、対応カラムへ展開
    4. 1件取得するごとに即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/night_works.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id night_works
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 住所に都道府県が明記されない政令指定都市 → 都道府県マッピング
_CITY_PREF_MAP = {
    "札幌市": "北海道",
    "函館市": "北海道",
    "旭川市": "北海道",
    "仙台市": "宮城県",
    "さいたま市": "埼玉県",
    "千葉市": "千葉県",
    "横浜市": "神奈川県",
    "川崎市": "神奈川県",
    "相模原市": "神奈川県",
    "新潟市": "新潟県",
    "静岡市": "静岡県",
    "浜松市": "静岡県",
    "名古屋市": "愛知県",
    "京都市": "京都府",
    "大阪市": "大阪府",
    "堺市": "大阪府",
    "神戸市": "兵庫県",
    "岡山市": "岡山県",
    "広島市": "広島県",
    "北九州市": "福岡県",
    "福岡市": "福岡県",
    "熊本市": "熊本県",
}


def _extract_pref(addr: str) -> str:
    m = _PREF_PATTERN.match(addr)
    if m:
        return m.group(1)
    for city, pref in _CITY_PREF_MAP.items():
        if addr.startswith(city):
            return pref
    return ""


class NightWorksScraper(StaticCrawler):
    """キャバクラ求人 ホッケ スクレイパー"""

    DELAY = 1.5

    # 一覧ページ由来のカラム (parse 内で個別にセット)
    _LIST_COLUMNS = ("求人番号", "エリア", "時給", "勤務時間")

    EXTRA_COLUMNS = [
        # 一覧ページ由来
        "求人番号", "エリア", "時給", "勤務時間",
        # このお店について
        "卓数", "在籍人数", "衣装", "VIPルーム",
        "料金システム", "年齢層", "客層", "NG", "罰金",
        # お店から
        "求めるタイプ", "SNS",
        # キャバクラ求人 ホッケから
        "職場の雰囲気", "担当者",
        # 出勤について (勤務時間は一覧ページ由来カラムへ)
        "出勤体制",
        # お給料について
        "体験入店", "本入店時給", "お給料システム", "各種歩合",
        "給与日", "日払い", "控除", "保証期間",
        # 待遇について
        "送迎", "ヘアメイク", "レンタル衣装・靴・鞄", "フェイクカクテル",
        "カラオケ", "終電上がり",
        # 採用に関して
        "面接可能時間帯", "応募者年齢層", "資格", "身分証明書",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        store_url = url.rstrip("/") + "/store/"

        first_soup = self.get_soup(store_url)
        if first_soup is None:
            return

        # ページ数取得
        total_pages = 1
        pager = first_soup.select_one(".wp-pagenavi")
        if pager:
            page_nums = [
                int(a.get("href", "").rstrip("/").split("/")[-1])
                for a in pager.select('a[href*="/page/"]')
                if "/page/" in a.get("href", "")
            ]
            if page_nums:
                total_pages = max(page_nums)

        self.total_items = 10 * total_pages  # 推定件数

        page = 1
        while page <= total_pages:
            if page == 1:
                soup = first_soup
            else:
                soup = self.get_soup(f"{store_url}page/{page}/")
                if soup is None:
                    break

            items = soup.select("li.store--box")
            if not items:
                break

            for item in items:
                try:
                    link = item.select_one("a.link-detail")
                    if not link:
                        continue
                    detail_url = link.get("href", "")

                    # 一覧ページからの取得
                    num_el = item.select_one("div.number")
                    job_number = (
                        num_el.get_text(strip=True).replace("求人番号：", "")
                        if num_el
                        else ""
                    )

                    name_el = item.select_one("h2.name")
                    name = name_el.get_text(strip=True) if name_el else ""

                    area = cat_site = hourly = work_hours = ""
                    for info_li in item.select("ul.info li"):
                        label_div = info_li.select_one("[data-select='none']")
                        value_divs = info_li.select("div")
                        label_text = label_div.get_text(strip=True) if label_div else ""
                        value_text = value_divs[-1].get_text(strip=True) if value_divs else ""
                        if label_text == "エリア":
                            area = value_text
                        elif label_text == "職種":
                            cat_site = value_text
                        elif label_text == "時給":
                            hourly = value_text
                        elif label_text == "勤務時間":
                            work_hours = value_text

                    tel_a = item.select_one("a[href*='tel:']")
                    tel = tel_a.get("href", "").replace("tel:", "") if tel_a else ""

                    detail = self._scrape_detail(detail_url)

                    row = {
                        Schema.NAME: name,
                        Schema.URL: detail_url,
                        Schema.TEL: tel,
                        Schema.PREF: detail.get("pref", ""),
                        Schema.ADDR: detail.get("addr", ""),
                        Schema.TIME: detail.get("営業時間", ""),
                        Schema.HOLIDAY: detail.get("定休日", ""),
                        Schema.CAT_SITE: cat_site,
                        Schema.HP: detail.get("hp", ""),
                        "求人番号": job_number,
                        "エリア": area,
                        "時給": hourly,
                        "勤務時間": work_hours,
                    }
                    # 詳細ページ由来の追加カラムはラベル名をそのままキーに展開
                    for col in self.EXTRA_COLUMNS:
                        if col in self._LIST_COLUMNS:
                            continue
                        row[col] = detail.get(col, "")

                    yield row
                except Exception as e:
                    self.logger.warning(f"Item error on page {page}: {e}")
                    continue

            page += 1

    def _scrape_detail(self, url: str) -> dict:
        """詳細ページの全 box-recruit セクションを横断し、ラベル→値を抽出する。

        返り値の dict は各行ラベル (例: 送迎 / 面接可能時間帯 / 体験入店 ...) を
        そのままキーに持つ。加えて住所処理由来の "addr" / "pref"、HP 由来の "hp"
        を持つ。
        """
        soup = self.get_soup(url)
        if soup is None:
            return {}

        result: dict = {}

        for box in soup.select("div.box-recruit"):
            for li in box.select("ul.recruit > li.flex"):
                span = li.select_one("div > span")
                label_text = span.get_text(strip=True) if span else ""
                if not label_text:
                    continue

                # HP は表示テキストではなく a[href] から取得 (インスタ埋め込みURLは除外)
                if label_text == "HP":
                    a = li.select_one("a[href]")
                    if a:
                        href = a.get("href", "")
                        if "instagram.com/p/" not in href:
                            result["hp"] = href
                    continue

                value_div = li.select("div > div")
                value_text = value_div[-1].get_text(strip=True) if value_div else ""

                if label_text == "住所":
                    addr_clean = value_text.split("アクセス")[0].strip()
                    result["addr"] = addr_clean
                    result["pref"] = _extract_pref(addr_clean)

                # ラベル名をそのままキーに保持 (待遇・採用・給料等の全カラム対応)
                result[label_text] = value_text

        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NightWorksScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://night-works.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

# scripts/sites/jobs/guppy.py
"""
GUPPY (グッピー, www.guppy.jp) — 全職種・事業所単位 求人スクレイパー

取得対象:
    - 57 職種コードすべてを巡回
    - 同一事業所 (勤務先名 + 正規化住所が一致) は 1 レコードに集約
    - 推定対象: 約 15〜25 万事業所

取得カラム:
    Schema (11): URL(=初出求人URL), NAME, PREF, ADDR, HP, CAT_SITE,
                 TIME, LOB, REP_NM, EMP_NUM, OPEN_DATE
    EXTRA  (3): 最寄駅, アクセス, 初出時の職種コード

取得フロー:
    for code in CATEGORY_CODES:
        for page in 2..N:                    # page=1 はランディング
            /{code}?page={page} を GET
            div.box.box-jobitem から求人 URL 収集
            各求人 URL → dl.l-def 解析
              → (勤務先名, 正規化住所) が初出なら yield

実行方法:
    python scripts/sites/jobs/guppy.py
    python bin/run_flow.py --site-id guppy
"""

import re
import sys
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_BASE_URL = "https://www.guppy.jp"

# サイト「職種を選択」セクションから抽出した 57 職種コード
CATEGORY_CODES = [
    # 歯科
    "dds", "dh", "dt", "da",
    # 医科・薬剤
    "ic", "md", "apo", "ns", "pns", "phn", "mw",
    # 検査・技術
    "rt", "mt", "me",
    # リハビリ
    "st", "ort", "pt", "ot",
    # 心理
    "cp", "cpp",
    # 管理・補助
    "him", "ra", "na", "pa", "po",
    # 栄養・調理
    "nrd", "nu", "ck", "ks",
    # 福祉・事務
    "msw", "wc", "mc",
    # 介護
    "hh", "ccw", "csw", "psw", "cm", "fd", "swo", "spm", "fss", "ca",
    "cgw", "sw", "ls",
    # 保育
    "kt", "cw", "acw",
    # その他
    "cc", "jdr", "acu", "mas", "bwt", "th",
    # 治験・販売
    "cra", "crc", "otc",
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_ADDRESS_NOISE_PATTERN = re.compile(r"Googleマップで表示|Googleマップで見る")


class GuppyScraper(StaticCrawler):
    """GUPPY 全職種・事業所単位 スクレイパー"""

    DELAY = 0.7
    EXTRA_COLUMNS = [
        "最寄駅",
        "アクセス",
        "初出時の職種コード",
    ]

    def parse(self, url: str):
        """
        57 職種 × 全ページを巡回し、(勤務先名, 住所) をキーに事業所単位で yield する。
        引数 `url` は参照のみ（起点 URL のログ用途）。
        """
        seen_keys: set[tuple[str, str]] = set()
        total_visited = 0
        total_yielded = 0

        for code in CATEGORY_CODES:
            detail_urls = self._collect_detail_urls(code)
            self.logger.info(
                "[%s] 求人 URL 収集完了: %d 件", code, len(detail_urls)
            )
            for d_url in detail_urls:
                total_visited += 1
                try:
                    item = self._scrape_facility(d_url, code)
                    if item is None:
                        continue
                    key = self._facility_key(item)
                    if key is None or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    yield item
                    total_yielded += 1
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", d_url, e)
                    continue
            self.logger.info(
                "[%s] 処理完了。訪問 %d / 累計事業所 %d",
                code, total_visited, total_yielded,
            )

        self.logger.info(
            "全体完了: 求人 %d 件アクセス → 事業所 %d 件",
            total_visited, total_yielded,
        )

    # ------------------------------------------------------------------
    # 求人URL収集
    # ------------------------------------------------------------------

    def _collect_detail_urls(self, code: str) -> list[str]:
        """1 職種について ?page=2 から順に全ページを巡回し、求人 URL を重複排除で返す"""
        seen: set[str] = set()
        urls: list[str] = []
        # ?page=1 はランディング（カード 0 件）なので page=2 から開始
        page = 2
        while True:
            list_url = f"{_BASE_URL}/{code}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            cards = soup.select("div.box.box-jobitem")
            if not cards:
                break

            new_on_page = 0
            for card in cards:
                a = card.select_one(f'a.box-jobitem-block[href^="/{code}/"]')
                if a is None:
                    a = card.select_one('a[href^="/"]')
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith("/"):
                    href = _BASE_URL + href
                href = re.sub(r"\?.*$", "", href)
                if not re.match(rf"^{_BASE_URL}/{code}/\d+$", href):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                urls.append(href)
                new_on_page += 1

            self.logger.info(
                "[%s] page=%d: %d cards, %d new (累計 %d)",
                code, page, len(cards), new_on_page, len(urls),
            )

            next_link = soup.select_one(f'a[href*="page={page + 1}"]')
            if not next_link:
                break
            page += 1

        return urls

    # ------------------------------------------------------------------
    # 詳細ページ → 事業所単位レコード構築
    # ------------------------------------------------------------------

    def _scrape_facility(self, detail_url: str, code: str) -> dict | None:
        """求人詳細ページから事業所単位のレコード dict を返す。NAME 不在なら None。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {Schema.URL: detail_url, "初出時の職種コード": code}

        # dl.l-def は想定 4 つ: 募集要項 / 勤務先情報 / 法人情報 / 応募方法
        dls = soup.select("dl.l-def")
        dl_dicts = [self._dl_to_dict(dl) for dl in dls]
        place: dict = dl_dicts[1] if len(dl_dicts) >= 2 else {}
        corp: dict = dl_dicts[2] if len(dl_dicts) >= 3 else {}

        name = place.get("勤務先名", "")
        if name:
            name = re.sub(r"\s*スピード返信.*$", "", name).strip()
            item[Schema.NAME] = name

        address_raw = place.get("住所", "")
        if address_raw:
            cleaned = _ADDRESS_NOISE_PATTERN.sub("", address_raw)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            m = _PREF_PATTERN.match(cleaned)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = cleaned[m.end():].strip()
            else:
                item[Schema.ADDR] = cleaned

        hp = place.get("ホームページ", "")
        if hp:
            m = re.search(r"https?://\S+", hp)
            item[Schema.HP] = m.group(0) if m else hp.strip()

        if place.get("業種"):
            item[Schema.CAT_SITE] = place["業種"]
        if place.get("診療時間"):
            item[Schema.TIME] = place["診療時間"]
        if place.get("最寄駅"):
            item["最寄駅"] = place["最寄駅"]
        if place.get("アクセス"):
            item["アクセス"] = place["アクセス"]

        if corp.get("事業内容"):
            item[Schema.LOB] = corp["事業内容"]
        if corp.get("代表者"):
            item[Schema.REP_NM] = corp["代表者"]
        if corp.get("従業員"):
            item[Schema.EMP_NUM] = corp["従業員"]
        if corp.get("設立"):
            item[Schema.OPEN_DATE] = corp["設立"]

        if Schema.NAME not in item:
            return None
        return item

    @staticmethod
    def _facility_key(item: dict) -> tuple[str, str] | None:
        """事業所識別キー: (勤務先名, 住所) を空白除去で正規化したタプル"""
        name = item.get(Schema.NAME, "")
        addr = item.get(Schema.ADDR, "")
        if not name:
            return None
        norm_name = re.sub(r"\s+", "", name)
        norm_addr = re.sub(r"\s+", "", addr)
        return (norm_name, norm_addr)

    @staticmethod
    def _dl_to_dict(dl) -> dict[str, str]:
        """dl の dt/dd ペアを辞書化（空白は単一スペース正規化、先勝ち）"""
        result: dict[str, str] = {}
        dts = dl.find_all("dt", recursive=True)
        dds = dl.find_all("dd", recursive=True)
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            if not key:
                continue
            value = dd.get_text(" ", strip=True)
            value = re.sub(r"\s+", " ", value).strip()
            if key not in result and value:
                result[key] = value
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GuppyScraper()
    scraper.execute("https://www.guppy.jp/")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

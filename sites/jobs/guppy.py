# scripts/sites/jobs/guppy.py
"""
GUPPY (グッピー, www.guppy.jp) — 医療・介護・福祉 求人スクレイパー（全職種）

取得対象:
    - 45 職種コードすべて (acu/apo/ns/pt/ot/hh/ccw/cm/dds/dh/…) を巡回
    - 各詳細ページから 27 カラム
      * Schema: URL, NAME, PREF, ADDR, HP, LOB, REP_NM, EMP_NUM,
               OPEN_DATE, CAT_SITE, TIME
      * EXTRA : 職種コード, 募集職種, 雇用形態, 給与, 給与補足, 諸手当,
               仕事内容, 応募資格, 勤務時間・休憩, 休日休暇, 年間休日,
               福利厚生, 社会保険, 最寄駅, アクセス, 選考プロセス

取得フロー:
    for code in CATEGORY_CODES:
        for page in 2..N:                    # page=1 はランディング
            /{code}?page={page} を GET
            div.box.box-jobitem から詳細 URL 収集
            詳細 URL → 4 つの dl.l-def を解析 → yield

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

# sitemap.xml から列挙した 45 職種コード
CATEGORY_CODES = [
    "acu", "acw", "apo", "bwt", "cc", "ccw", "cgw", "ck", "cm", "cp",
    "cpp", "csw", "cw", "da", "dds", "dh", "dt", "fss", "hh", "ic",
    "jdr", "kt", "ls", "mas", "mc", "md", "me", "mt", "na", "nrd",
    "ns", "nu", "ort", "ot", "pa", "pns", "psw", "pt", "ra", "rt",
    "spm", "st", "sw", "swo", "th",
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 住所末尾に混入する UI ラベル
_ADDRESS_NOISE_PATTERN = re.compile(r"Googleマップで表示|Googleマップで見る")


class GuppyScraper(StaticCrawler):
    """GUPPY 全職種 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種コード",
        "募集職種",
        "雇用形態",
        "給与",
        "給与補足",
        "諸手当",
        "仕事内容",
        "応募資格",
        "勤務時間・休憩",
        "休日休暇",
        "年間休日",
        "福利厚生",
        "社会保険",
        "最寄駅",
        "アクセス",
        "選考プロセス",
    ]

    def parse(self, url: str):
        """
        45 職種コード × 全ページを巡回し、詳細ページを 1 件ずつ yield する。
        引数 `url` は参照のみ（起点 URL のログ用途）。
        """
        total_collected = 0
        for code in CATEGORY_CODES:
            detail_urls = self._collect_detail_urls(code)
            self.logger.info("[%s] 詳細 URL 収集完了: %d 件", code, len(detail_urls))
            for d_url in detail_urls:
                try:
                    item = self._scrape_detail(d_url, code)
                    if item:
                        yield item
                        total_collected += 1
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", d_url, e)
                    continue
            self.logger.info("[%s] 処理完了。累計 yield: %d", code, total_collected)

    def _collect_detail_urls(self, code: str) -> list[str]:
        """1 職種について ?page=2 から順に全ページを巡回し、詳細 URL を重複排除で返す"""
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
                # クエリを除去
                href = re.sub(r"\?.*$", "", href)
                # 職種コード配下の数値 ID のみ許可
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

            # 次ページリンクがなければ終了
            next_link = soup.select_one(f'a[href*="page={page + 1}"]')
            if not next_link:
                break
            page += 1

        return urls

    def _scrape_detail(self, detail_url: str, code: str) -> dict | None:
        """詳細ページを解析して 1 レコード分の dict を返す"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {Schema.URL: detail_url, "職種コード": code}

        # dl.l-def は想定 4 つ: 募集要項 / 勤務先情報 / 法人情報 / 応募方法
        dls = soup.select("dl.l-def")
        dl_dicts = [self._dl_to_dict(dl) for dl in dls]

        # インデックス別に結合（同名キーは後勝ちを避けるためリストでまとめる）
        youkou: dict = dl_dicts[0] if len(dl_dicts) >= 1 else {}
        place: dict = dl_dicts[1] if len(dl_dicts) >= 2 else {}
        corp: dict = dl_dicts[2] if len(dl_dicts) >= 3 else {}
        apply_info: dict = dl_dicts[3] if len(dl_dicts) >= 4 else {}

        # --- Schema マッピング ---
        name = place.get("勤務先名", "")
        if name:
            # 「施設名 スピード返信 この勤務先は平均...」の末尾ラベルを除去
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

        if corp.get("事業内容"):
            item[Schema.LOB] = corp["事業内容"]
        if corp.get("代表者"):
            item[Schema.REP_NM] = corp["代表者"]
        if corp.get("従業員"):
            item[Schema.EMP_NUM] = corp["従業員"]
        if corp.get("設立"):
            item[Schema.OPEN_DATE] = corp["設立"]

        # --- EXTRA_COLUMNS ---
        extra_mapping = {
            "募集職種": "募集職種",
            "雇用形態": "雇用形態",
            "給与": "給与",
            "給与補足": "給与補足",
            "諸手当の内訳": "諸手当",
            "仕事内容": "仕事内容",
            "応募資格": "応募資格",
            "勤務時間・休憩": "勤務時間・休憩",
            "休日休暇": "休日休暇",
            "年間休日": "年間休日",
            "福利厚生": "福利厚生",
            "社会保険": "社会保険",
        }
        for src, dst in extra_mapping.items():
            v = youkou.get(src)
            if v:
                item[dst] = v

        if place.get("最寄駅"):
            item["最寄駅"] = place["最寄駅"]
        if place.get("アクセス"):
            item["アクセス"] = place["アクセス"]

        if apply_info.get("選考プロセス"):
            item["選考プロセス"] = apply_info["選考プロセス"]

        if Schema.NAME not in item:
            return None
        return item

    @staticmethod
    def _dl_to_dict(dl) -> dict[str, str]:
        """dl の dt/dd ペアを辞書化（空白は単一スペース正規化）"""
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

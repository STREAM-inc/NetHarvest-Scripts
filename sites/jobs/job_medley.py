# scripts/sites/jobs/job_medley.py
"""
ジョブメドレー (job-medley.com) — あん摩マッサージ指圧師 求人スクレイパー

取得対象:
    - 全国 47 都道府県の /mas/pref{N}/ 一覧 (約 7,570 件)
    - 各求人詳細ページから 24 カラム
      * Schema: URL, NAME, PREF, ADDR, LOB, CAT_SITE, TIME, HOLIDAY, OPEN_DATE
      * EXTRA : 求人タイトル, 雇用形態, 募集職種, 給与, 給与の備考, 待遇,
               教育体制・研修, 勤務時間, 休日, 長期休暇・特別休暇,
               応募要件, 歓迎要件, 選考プロセス, 最寄り駅, 事業所スタッフ構成

取得フロー:
    都道府県 (pref1..pref47) をループ
      → /mas/prefN/?page=M を最終ページまで取得
      → .c-job-offer-card から詳細 URL を重複除外しつつ収集
    各詳細ページ (/mas/{job_id}/) を取得
      → 事業所情報セクション + 募集内容セクション + h1 を解析
      → Schema / EXTRA にマッピング

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job_medley.py

    # Prefect Flow 経由（全件）
    python bin/run_flow.py --site-id job_medley
"""

import re
import sys
from pathlib import Path

# scripts/sites/jobs/job_medley.py から見て 4 階層上が src/ を持つプロジェクトルート
root_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_BASE_URL = "https://job-medley.com"
_PREF_IDS = list(range(1, 48))  # pref1..pref47

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class JobMedleyScraper(StaticCrawler):
    """ジョブメドレー あん摩マッサージ指圧師 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "募集職種",
        "給与",
        "給与の備考",
        "待遇",
        "教育体制・研修",
        "勤務時間",
        "休日",
        "長期休暇・特別休暇",
        "応募要件",
        "歓迎要件",
        "選考プロセス",
        "最寄り駅",
        "事業所スタッフ構成",
    ]

    def parse(self, url: str):
        """
        47 都道府県の一覧を巡回して詳細 URL を収集し、各詳細を解析して yield する。
        引数 `url` (sites.yml の起点 URL) は参照のみで使用しない。
        """
        detail_urls = self._collect_all_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページ URL 収集完了: %d 件", self.total_items)

        for d_url in detail_urls:
            try:
                item = self._scrape_detail(d_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", d_url, e)
                continue

    def _collect_all_detail_urls(self) -> list[str]:
        """全都道府県 × 全ページから詳細 URL を重複除外しつつ収集する"""
        seen: set[str] = set()
        urls: list[str] = []

        for pref_id in _PREF_IDS:
            pref_base = f"{_BASE_URL}/mas/pref{pref_id}/"
            page = 1
            while True:
                list_url = f"{pref_base}?page={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                cards = soup.select(".c-job-offer-card")
                if not cards:
                    break

                new_on_page = 0
                for card in cards:
                    a = card.select_one("h3 a")
                    if not a:
                        continue
                    href = a.get("href", "")
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = _BASE_URL + href
                    # /mas/XXXX/?ref_page=... → /mas/XXXX/
                    href = re.sub(r"\?.*$", "", href)
                    if href in seen:
                        continue
                    seen.add(href)
                    urls.append(href)
                    new_on_page += 1

                self.logger.info(
                    "pref%d page=%d: %d cards, %d new (累計 %d)",
                    pref_id, page, len(cards), new_on_page, len(urls),
                )

                # 「次へ」(page+1) 相当のリンクがなければ最終ページ
                next_link = soup.select_one(f'a[href*="page={page + 1}"]')
                if not next_link:
                    break
                page += 1

        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページを解析して 1 レコード分の dict を返す"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {Schema.URL: detail_url}

        # h1 → 求人タイトル, 雇用形態
        h1 = soup.select_one("h1")
        if h1:
            title = h1.get_text(strip=True)
            item["求人タイトル"] = title
            m = re.search(r"（([^（）]+)）$", title)
            if m:
                item["雇用形態"] = m.group(1)

        # 事業所情報 セクション
        facility_section = self._find_section(soup, "事業所情報")
        if facility_section:
            fac_name = facility_section.select_one('a[href*="/facility/"]')
            if fac_name:
                item[Schema.NAME] = fac_name.get_text(strip=True)

            # アクセス: 住所 (<p> 1つめ), 最寄り駅 (<p> 2つめ)
            access_h3 = self._find_h3(facility_section, "アクセス")
            if access_h3:
                access_body = access_h3.find_next_sibling()
                if access_body:
                    ps = access_body.find_all("p")
                    address_text = ps[0].get_text(" ", strip=True) if len(ps) >= 1 else ""
                    station_text = ps[1].get_text("\n", strip=True) if len(ps) >= 2 else ""
                    if address_text:
                        m = _PREF_PATTERN.match(address_text)
                        if m:
                            item[Schema.PREF] = m.group(1)
                            item[Schema.ADDR] = address_text[m.end():].strip()
                        else:
                            item[Schema.ADDR] = address_text
                    if station_text:
                        item["最寄り駅"] = station_text

            fac_fields = self._extract_h3_pairs(
                facility_section, skip={"アクセス", "募集職種", "法人・施設名"}
            )
            if "施設・サービス形態" in fac_fields:
                item[Schema.CAT_SITE] = fac_fields["施設・サービス形態"]
            if "設立年月日" in fac_fields:
                item[Schema.OPEN_DATE] = fac_fields["設立年月日"]
            if "営業時間" in fac_fields:
                item[Schema.TIME] = fac_fields["営業時間"]
            if "休業日" in fac_fields:
                item[Schema.HOLIDAY] = fac_fields["休業日"]
            if "スタッフ構成" in fac_fields:
                item["事業所スタッフ構成"] = fac_fields["スタッフ構成"]

        # 募集内容 セクション
        content_section = self._find_section(soup, "募集内容")
        if content_section:
            content_fields = self._extract_h3_pairs(content_section)
            mapping: dict[str, object] = {
                "募集職種": "募集職種",
                "仕事内容": Schema.LOB,
                "給与": "給与",
                "給与の備考": "給与の備考",
                "待遇": "待遇",
                "教育体制・研修": "教育体制・研修",
                "勤務時間": "勤務時間",
                "休日": "休日",
                "長期休暇・特別休暇": "長期休暇・特別休暇",
                "応募要件": "応募要件",
                "歓迎要件": "歓迎要件",
                "選考プロセス": "選考プロセス",
            }
            for label, key in mapping.items():
                v = content_fields.get(label)
                if v:
                    item[key] = v

        if Schema.NAME not in item:
            return None
        return item

    @staticmethod
    def _find_section(soup, heading: str):
        """<h2>{heading}</h2> を持つ親要素 (= セクション全体) を返す"""
        for h2 in soup.select("h2"):
            if h2.get_text(strip=True) == heading:
                return h2.parent
        return None

    @staticmethod
    def _find_h3(section, heading: str):
        for h3 in section.select("h3"):
            if h3.get_text(strip=True) == heading:
                return h3
        return None

    @staticmethod
    def _extract_h3_pairs(section, skip: set[str] | None = None) -> dict[str, str]:
        """セクション配下の h3 → 次の兄弟要素のテキスト、を dict に詰める"""
        skip = skip or set()
        result: dict[str, str] = {}
        for h3 in section.select("h3"):
            label = h3.get_text(strip=True)
            if label in skip or label in result:
                continue
            sib = h3.find_next_sibling()
            if sib is None:
                continue
            value = sib.get_text("\n", strip=True)
            if value:
                result[label] = value
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JobMedleyScraper()
    scraper.execute("https://job-medley.com/mas/")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

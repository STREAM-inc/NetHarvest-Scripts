# scripts/sites/jobs/job_medley.py
"""
ジョブメドレー (job-medley.com) — 事業所単位 全職種 求人スクレイパー

取得対象:
    - 全国 47 都道府県 × 58 職種コード の一覧から事業所URLを集約
    - 同一事業所が複数職種を募集している場合は 1 レコードに集約
    - 推定対象: 約 15〜20 万事業所

取得カラム:
    Schema (8): URL(=facility URL), NAME, PREF, ADDR, CAT_SITE, TIME, HOLIDAY, OPEN_DATE
    EXTRA (5): 募集職種, 最寄り駅, 事業所スタッフ構成, 診療科目・サービス形態, 初出時の職種コード

取得フロー:
    1. 58 職種 × 47 都道府県 の一覧をページネーションで全巡回し求人URL集合を作成
    2. 各求人詳細にアクセスし、事業所URL (/facility/{id}/) を抽出
    3. 事業所URLが初出のときのみ、その求人詳細ページの「事業所情報」セクションから
       事業所単位のレコードを構築して yield

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

# ジョブメドレーの全職種コード（58 種）— トップページの「職種を選択」から抽出
_JOB_CODES = [
    # 医科 (16)
    "dr", "apo", "ans", "mn", "phn", "na", "rt", "mt", "ce",
    "nrd", "cp", "csw", "otc", "mc", "crc", "pc",
    # 歯科 (4)
    "dds", "dh", "dt", "da",
    # 介護 (14)
    "hh", "la", "cm", "mg", "km", "ls", "fss", "nm", "dcm",
    "apl", "ck", "ctd", "cc", "dm",
    # 保育 (4)
    "cw", "kt", "acw", "asc",
    # リハビリ／代替医療 (8)
    "pt", "st", "ot", "ort", "jdr", "mas", "acu", "bwt",
    # ヘルスケア／美容 (7)
    "hs", "bar", "nt", "et", "est", "ba", "ins",
    # 共通職 (5)
    "sr", "ow", "clr", "drv", "etc",
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class JobMedleyScraper(StaticCrawler):
    """ジョブメドレー 全職種 事業所単位 スクレイパー"""

    DELAY = 0.7
    EXTRA_COLUMNS = [
        "募集職種",
        "最寄り駅",
        "事業所スタッフ構成",
        "診療科目・サービス形態",
        "初出時の職種コード",
    ]

    def parse(self, url: str):
        """
        58 職種 × 47 都道府県の一覧を全巡回 → 求人URL収集 → 詳細から事業所URL抽出 →
        事業所単位で重複排除しレコード化する。引数 `url` は参照のみで使用しない。
        """
        detail_urls = self._collect_all_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("求人 URL 収集完了: %d 件", self.total_items)

        seen_facilities: set[str] = set()
        for d_url, job_code in detail_urls:
            try:
                soup = self.get_soup(d_url)
                if soup is None:
                    continue

                facility_url = self._extract_facility_url(soup)
                if not facility_url or facility_url in seen_facilities:
                    continue
                seen_facilities.add(facility_url)

                item = self._build_facility_record(soup, facility_url, job_code)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("求人詳細取得失敗: %s (%s)", d_url, e)
                continue

        self.logger.info(
            "事業所収集完了: 求人 %d 件 → 事業所 %d 件",
            len(detail_urls), len(seen_facilities),
        )

    # ------------------------------------------------------------------
    # 求人URL収集 (58 職種 × 47 都道府県 × 全ページ)
    # ------------------------------------------------------------------

    def _collect_all_detail_urls(self) -> list[tuple[str, str]]:
        """全職種・全都道府県・全ページから求人URLを (url, job_code) のタプルで返す"""
        seen: set[str] = set()
        urls: list[tuple[str, str]] = []

        for job_code in _JOB_CODES:
            job_total_before = len(urls)
            for pref_id in _PREF_IDS:
                pref_base = f"{_BASE_URL}/{job_code}/pref{pref_id}/"
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
                        href = re.sub(r"[?#].*$", "", href)
                        if href in seen:
                            continue
                        seen.add(href)
                        urls.append((href, job_code))
                        new_on_page += 1

                    self.logger.info(
                        "%s pref%d page=%d: %d cards, %d new (累計 %d)",
                        job_code, pref_id, page, len(cards), new_on_page, len(urls),
                    )

                    next_link = soup.select_one(f'a[href*="page={page + 1}"]')
                    if not next_link:
                        break
                    page += 1

            self.logger.info(
                "職種 %s 収集完了: %d 件 (累計 %d)",
                job_code, len(urls) - job_total_before, len(urls),
            )

        return urls

    # ------------------------------------------------------------------
    # 詳細ページ → 事業所URL抽出 / 事業所レコード構築
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_facility_url(soup) -> str | None:
        """求人詳細ページから事業所URL (/facility/{id}/) を抽出して正規化する"""
        a = soup.select_one('a[href*="/facility/"]')
        if not a:
            return None
        href = a.get("href", "") or ""
        if not href:
            return None
        if href.startswith("/"):
            href = _BASE_URL + href
        href = re.sub(r"[?#].*$", "", href)
        if not href.endswith("/"):
            href += "/"
        return href

    def _build_facility_record(
        self, soup, facility_url: str, job_code: str
    ) -> dict | None:
        """求人詳細ページの「事業所情報」セクションから事業所単位のレコードを構築する"""
        item: dict = {Schema.URL: facility_url}

        facility_section = self._find_section(soup, "事業所情報")
        if facility_section is None:
            return None

        fac_name_el = facility_section.select_one('a[href*="/facility/"]')
        if fac_name_el:
            item[Schema.NAME] = fac_name_el.get_text(strip=True)

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
            facility_section, skip={"アクセス", "法人・施設名"}
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
        if "募集職種" in fac_fields:
            item["募集職種"] = fac_fields["募集職種"]

        # 募集内容 → 診療科目・サービス形態（医科系で出現する追加ラベル）
        content_section = self._find_section(soup, "募集内容")
        if content_section:
            content_fields = self._extract_h3_pairs(content_section)
            if "診療科目・サービス形態" in content_fields:
                item["診療科目・サービス形態"] = content_fields["診療科目・サービス形態"]

        item["初出時の職種コード"] = job_code

        if Schema.NAME not in item:
            return None
        return item

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------

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
    scraper.execute("https://job-medley.com/")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

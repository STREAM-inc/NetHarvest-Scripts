"""
Baseconnect (Musubu) — 上場企業の基本情報クローラー

取得対象:
    - 上場11市場 (東証プライム/スタンダード/グロース, TOKYO PRO Market,
      名証メイン/ネクスト/プレミア, 福岡証券取引所, Q-Board,
      札幌証券取引所, アンビシャス) に属する全企業 (約5,000〜6,000社)

    未上場企業は数百万件あり全件クロール不可のため対象外とする。
    必要なら `LISTING_MARKETS` に "未上場" を追加して再実行する。

取得フロー:
    1. 各上場市場の一覧ページを `?page=N` でページネーション
    2. 各ページから UUID (detail URL) を抽出
    3. 詳細ページを取得し、dl/dt/dd + aria-label セクションから全フィールドを抽出

注意:
    - 従業員数・電話番号・売上高などの多くは未ログイン状態では "情報あり" の
      プレースホルダとなる。これは欠損ではなくサイト仕様。
    - robots.txt は CloudFront 403 のため取得不可だが、/users, /*/new, /*/edit 等は
      そもそも本スクレイパーのスコープ外。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/baseconnect.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id baseconnect
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://baseconnect.in"
ITEMS_PER_PAGE = 50
MAX_PAGES_PER_MARKET = 500  # 安全上限

LISTING_MARKETS = [
    "東証プライム",
    "東証スタンダード",
    "東証グロース",
    "TOKYO PRO Market",
    "名証メイン",
    "名証ネクスト",
    "名証プレミア",
    "福岡証券取引所",
    "Q-Board",
    "札幌証券取引所",
    "アンビシャス",
]

_UUID_RE = re.compile(
    r'href="(/companies/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"'
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_CITY_PATTERN = re.compile(r"([^\d\s]+?[市区町村])")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _join(items: list, sep: str = " / ") -> str:
    seen: set[str] = set()
    out: list[str] = []
    for i in items:
        t = _clean(i)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return sep.join(out)


class BaseconnectScraper(StaticCrawler):
    """Baseconnect (baseconnect.in) 上場企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "証券番号", "最終更新日", "上場市場", "上場日", "決算月",
        "事業所数", "工場数", "新卒採用人数", "売上高増加率", "従業員増加率",
        "代表者年齢", "代表者誕生日", "事業内容キーワード", "特徴",
        "市区町村", "Ullet URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_detail_urls: set[str] = set()
        all_detail_urls: list[str] = []

        # --- Phase 1: 全市場の一覧をクロールして UUID を収集 ---
        for market in LISTING_MARKETS:
            self.logger.info("市場一覧取得中: %s", market)
            try:
                market_urls = list(self._collect_detail_urls(market, seen_detail_urls))
                all_detail_urls.extend(market_urls)
                self.logger.info(
                    "  -> %s から %d 件 (累計 %d 件)",
                    market, len(market_urls), len(all_detail_urls),
                )
            except Exception as e:
                self.logger.warning("市場 %s の一覧取得失敗: %s", market, e)
                continue

        self.total_items = len(all_detail_urls)
        self.logger.info("収集済み全詳細URL: %d 件", self.total_items)

        # --- Phase 2: 各詳細ページをスクレイピング ---
        for detail_url in all_detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細取得失敗 (スキップ): %s — %s", detail_url, e
                )
                continue
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 一覧ページから詳細 URL を収集
    # ------------------------------------------------------------------

    def _collect_detail_urls(
        self, market: str, seen: set[str]
    ) -> Generator[str, None, None]:
        encoded_market = quote(market, safe="")
        base_market_url = f"{BASE_URL}/companies/listing_market/{encoded_market}"

        for page in range(1, MAX_PAGES_PER_MARKET + 1):
            list_url = base_market_url if page == 1 else f"{base_market_url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            html_text = str(soup)
            links = list(dict.fromkeys(_UUID_RE.findall(html_text)))
            new_count = 0
            for link in links:
                full = urljoin(BASE_URL, link)
                if full in seen:
                    continue
                seen.add(full)
                new_count += 1
                yield full

            if new_count == 0:
                break

            # 次ページ存在確認
            next_link = soup.find(
                "a",
                href=lambda h: bool(h)
                and f"/companies/listing_market/{encoded_market}?page={page + 1}" in h,
            )
            if not next_link:
                break

            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 詳細ページスクレイピング
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.TEL: "",
            Schema.HP: "",
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
        }

        # --- 社名 (h1) ---
        h1 = soup.find("h1")
        h1_text = _clean(h1.get_text()) if h1 else ""
        # "社名（都道府県○○市 / 市場名）" の形式
        m = re.match(r"^(.+?)（(.+?)）$", h1_text)
        if m:
            data[Schema.NAME] = m.group(1).strip()
        else:
            data[Schema.NAME] = h1_text

        # --- 事業内容 (h1 直下の h2) ---
        lob = ""
        if h1:
            h2 = h1.find_next("h2")
            if h2:
                lob_txt = _clean(h2.get_text())
                # "基本情報" 等の見出しは除外
                if lob_txt and len(lob_txt) > 10 and "基本情報" not in lob_txt:
                    lob = lob_txt
        data[Schema.LOB] = lob

        # --- 法人番号・証券番号・最終更新日 ---
        corp_section = soup.find(
            "section", attrs={"aria-label": lambda v: v and "法人番号" in v}
        )
        co_num = ""
        sec_num = ""
        updated = ""
        if corp_section:
            text = corp_section.get_text(" ", strip=True)
            m_corp = re.search(r"法人番号\s*(\d{13})", text)
            if m_corp:
                co_num = m_corp.group(1)
            m_sec = re.search(r"証券番号\s*(\d{4,5})", text)
            if m_sec:
                sec_num = m_sec.group(1)
            m_upd = re.search(r"最終更新日\s*(\d{4}年\d{1,2}月\d{1,2}日)", text)
            if m_upd:
                updated = m_upd.group(1)
        data[Schema.CO_NUM] = co_num
        data["証券番号"] = sec_num
        data["最終更新日"] = updated

        # --- 業界一覧 (CAT_SITE) ---
        industry_section = soup.find(
            "section", attrs={"aria-label": "業界一覧"}
        )
        industries: list[str] = []
        if industry_section:
            for a in industry_section.find_all(["a", "span"]):
                t = _clean(a.get_text())
                if t and "業界" in t:
                    industries.append(t)
        data[Schema.CAT_SITE] = _join(industries)

        # --- dl/dt/dd 形式のフィールドを全て辞書化 ---
        dl_fields: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = _clean(dt.get_text())
            for br in dd.find_all("br"):
                br.replace_with("\n")
            val = _clean(dd.get_text(" "))
            if key:
                dl_fields[key] = val

        data[Schema.OPEN_DATE] = dl_fields.get("設立年月", "")
        data[Schema.CAP] = dl_fields.get("資本金", "")
        data[Schema.SALES] = dl_fields.get("売上高", "")
        data[Schema.EMP_NUM] = dl_fields.get("従業員数", "")
        data[Schema.REP_NM] = dl_fields.get("名前", "")
        data["上場市場"] = dl_fields.get("上場市場", "")
        data["上場日"] = dl_fields.get("上場日", "")
        data["決算月"] = dl_fields.get("決算月", "")
        data["事業所数"] = dl_fields.get("事業所数", "")
        data["工場数"] = dl_fields.get("工場数", "")
        data["新卒採用人数"] = dl_fields.get("新卒採用人数", "")
        # "売上高増加率" または "売上高増加率 （2023年度）" 等の揺れに対応
        data["売上高増加率"] = next(
            (v for k, v in dl_fields.items() if k.startswith("売上高増加率")), ""
        )
        data["従業員増加率"] = dl_fields.get("従業員増加率", "")
        data["代表者年齢"] = dl_fields.get("年齢", "")
        data["代表者誕生日"] = dl_fields.get("誕生日", "")

        # --- 住所 (郵便番号 + 都道府県 + 住所に分解) ---
        addr_raw = dl_fields.get("住所（登記住所）", "") or dl_fields.get(
            "住所", ""
        )
        post_code = ""
        pref = ""
        city = ""
        addr_rest = ""
        if addr_raw:
            m_post = _POST_CODE_RE.search(addr_raw)
            if m_post:
                raw_post = m_post.group(1)
                if "-" not in raw_post:
                    raw_post = f"{raw_post[:3]}-{raw_post[3:]}"
                post_code = raw_post
                addr_raw = _POST_CODE_RE.sub("", addr_raw).strip()
            m_pref = _PREF_PATTERN.match(addr_raw)
            if m_pref:
                pref = m_pref.group(1)
                addr_rest = addr_raw[m_pref.end():].strip()
                m_city = _CITY_PATTERN.match(addr_rest)
                if m_city:
                    city = m_city.group(1)
            else:
                addr_rest = addr_raw
        data[Schema.POST_CODE] = post_code
        data[Schema.PREF] = pref
        data[Schema.ADDR] = addr_rest
        data["市区町村"] = city

        # --- 事業内容キーワード / 特徴 ---
        keywords: list[str] = []
        features: list[str] = []
        for h3 in soup.find_all("h3"):
            h3_text = _clean(h3.get_text())
            if h3_text == "事業内容キーワード":
                parent = h3.parent
                if parent:
                    for tag in parent.find_all(["span", "a"]):
                        t = _clean(tag.get_text())
                        if t and t != h3_text and len(t) < 40:
                            keywords.append(t)
            elif h3_text == "特徴":
                parent = h3.parent
                if parent:
                    for tag in parent.find_all(["span", "a"]):
                        t = _clean(tag.get_text())
                        if t and t != h3_text and len(t) < 40:
                            features.append(t)
        data["事業内容キーワード"] = _join(keywords[:30])
        data["特徴"] = _join(features[:10])

        # --- Ullet URL ---
        ullet_section = soup.find(
            "section", attrs={"aria-label": lambda v: v and "Ullet" in v}
        )
        ullet_url = ""
        if ullet_section:
            a = ullet_section.find("a", href=re.compile(r"ullet\.com"))
            if a:
                ullet_url = a.get("href", "")
        data["Ullet URL"] = ullet_url

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaseconnectScraper()
    scraper.execute(f"{BASE_URL}/companies/listing_market")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

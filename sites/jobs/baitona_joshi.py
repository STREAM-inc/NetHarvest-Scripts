"""
バイトな女子 — 全国求人企業情報スクレイパー（baitona-joshi.jp）

取得対象:
    一覧ページの shop カード経由で詳細ページに遷移し、以下を取得
        - 企業情報セクション (.detail-secondary__company)
            社名 / 代表者 / 所在地 / 事業内容 / 企業URL
        - 勤務地情報セクション (.workLocationInformation)
            ジャンル / 勤務地 / アクセス / URL
        - dataLayer.push 情報
            shop_id / 地方 / 都道府県 / 市区町村 / 最寄駅 / 大業種 / 中業種

取得フロー:
    47都道府県ループ
    → 一覧 /{pref_slug}/ → /{pref_slug}/page{N}/ をページ末尾まで巡回
    → 各 .f-shopList .shop の詳細リンク (/shop/{id}/) へ遷移
    → 詳細ページで企業情報・勤務地情報を取得
    → shop_id で重複排除

実行方法:
    python scripts/sites/jobs/baitona_joshi.py
    python bin/run_flow.py --site-id baitona_joshi
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

BASE_URL = "https://baitona-joshi.jp"

PREFECTURES: list[tuple[str, str]] = [
    ("hokkaido", "北海道"), ("aomori", "青森県"), ("iwate", "岩手県"),
    ("miyagi", "宮城県"), ("akita", "秋田県"), ("yamagata", "山形県"),
    ("fukushima", "福島県"), ("ibaraki", "茨城県"), ("tochigi", "栃木県"),
    ("gunma", "群馬県"), ("saitama", "埼玉県"), ("chiba", "千葉県"),
    ("tokyo", "東京都"), ("kanagawa", "神奈川県"), ("niigata", "新潟県"),
    ("toyama", "富山県"), ("ishikawa", "石川県"), ("fukui", "福井県"),
    ("yamanashi", "山梨県"), ("nagano", "長野県"), ("gifu", "岐阜県"),
    ("shizuoka", "静岡県"), ("aichi", "愛知県"), ("mie", "三重県"),
    ("shiga", "滋賀県"), ("kyoto", "京都府"), ("osaka", "大阪府"),
    ("hyogo", "兵庫県"), ("nara", "奈良県"), ("wakayama", "和歌山県"),
    ("tottori", "鳥取県"), ("shimane", "島根県"), ("okayama", "岡山県"),
    ("hiroshima", "広島県"), ("yamaguchi", "山口県"), ("tokushima", "徳島県"),
    ("kagawa", "香川県"), ("ehime", "愛媛県"), ("kochi", "高知県"),
    ("fukuoka", "福岡県"), ("saga", "佐賀県"), ("nagasaki", "長崎県"),
    ("kumamoto", "熊本県"), ("oita", "大分県"), ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"), ("okinawa", "沖縄県"),
]

_SHOP_ID_RE = re.compile(r"/shop/(\d+)/?$")
_DATALAYER_RE = re.compile(r"'(shop_[a-z0-9_]+)'\s*:\s*'([^']*)'")
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _multiline(text) -> str:
    if text is None:
        return ""
    s = re.sub(r"[ \t]+", " ", str(text))
    s = re.sub(r"\n\s*", "\n", s).strip()
    return s


class BaitonaJoshiScraper(StaticCrawler):
    """バイトな女子 求人企業情報スクレイパー（全国47都道府県巡回）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "shop_id",
        "地方",
        "市区町村",
        "最寄駅",
        "ジャンル",
        "アクセス",
        "求人キャッチコピー",
        "募集職種",
        "雇用形態",
        "給与",
        "勤務時間",
        "休日",
        "募集要項",
        "求人特徴タグ",
        "企業紹介",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        for pref_slug, pref_ja in PREFECTURES:
            list_url = f"{BASE_URL}/{pref_slug}/"
            self.logger.info("都道府県: %s (%s)", pref_ja, list_url)
            yield from self._scrape_pref(pref_slug, pref_ja, seen_ids)

    def _scrape_pref(
        self, pref_slug: str, pref_ja: str, seen: set
    ) -> Generator[dict, None, None]:
        page_no = 1
        while True:
            list_url = (
                f"{BASE_URL}/{pref_slug}/"
                if page_no == 1
                else f"{BASE_URL}/{pref_slug}/page{page_no}/"
            )
            soup = self.get_soup(list_url)
            if soup is None:
                break

            shops = soup.select(".f-shopList .shop")
            if not shops:
                break

            new_in_page = 0
            for shop in shops:
                a = shop.select_one(".shop__name[href]") or shop.select_one(
                    "a[href*='/shop/']"
                )
                if not a:
                    continue
                href = a.get("href", "").strip()
                detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                detail_url = detail_url.split("?")[0].split("#")[0]
                m = _SHOP_ID_RE.search(detail_url)
                if not m:
                    continue
                shop_id = m.group(1)
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                new_in_page += 1

                # 一覧側で取れる情報をまず確保
                sub_el = shop.select_one(".shop__subtitle__text") or shop.select_one(
                    ".shop__subtitle"
                )
                list_subtitle = _clean(sub_el.get_text()) if sub_el else ""

                try:
                    item = self._scrape_detail(detail_url, shop_id, pref_ja)
                except Exception as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item is None:
                    continue
                if list_subtitle and not item.get("求人キャッチコピー"):
                    item["求人キャッチコピー"] = list_subtitle
                if item.get(Schema.NAME):
                    yield item

            self.logger.info(
                "%s page%d: %d件 (新規 %d件)",
                pref_ja,
                page_no,
                len(shops),
                new_in_page,
            )

            # 次ページ判定: pager に page{N+1} のリンクがあるか
            next_url = f"/{pref_slug}/page{page_no + 1}/"
            has_next = bool(soup.select_one(f'a[href*="{next_url}"]'))
            if not has_next:
                break
            page_no += 1

    def _scrape_detail(
        self, url: str, shop_id: str, pref_ja: str
    ) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.PREF: pref_ja,
            "shop_id": shop_id,
        }

        # ── dataLayer 抽出 (shop_prefecture, shop_city, shop_area, shop_biz, shop_biz2 など)
        dl_blob = ""
        for s in soup.find_all("script"):
            text = s.string or s.get_text() or ""
            if "shop_id" in text and "dataLayer" in text:
                dl_blob = text
                break
        if dl_blob:
            kv = dict(_DATALAYER_RE.findall(dl_blob))
            if kv.get("shop_prefecture"):
                data[Schema.PREF] = kv["shop_prefecture"]
            if kv.get("shop_region"):
                data["地方"] = kv["shop_region"]
            if kv.get("shop_city"):
                data["市区町村"] = kv["shop_city"]
            if kv.get("shop_area"):
                data["最寄駅"] = kv["shop_area"]
            if kv.get("shop_biz"):
                data[Schema.CAT_LV1] = kv["shop_biz"]
            if kv.get("shop_biz2"):
                data[Schema.CAT_LV2] = kv["shop_biz2"]

        # ── 企業情報 セクション
        company = soup.select_one(".detail-secondary__company")
        if company:
            for li in company.select(".company-item"):
                title_el = li.select_one(".company-title")
                value_el = li.select_one(".company-text") or li.select_one(
                    ".company-link"
                )
                if not title_el:
                    continue
                label = title_el.get_text(strip=True)
                value = _multiline(value_el.get_text("\n")) if value_el else ""
                href = value_el.get("href") if value_el and value_el.name == "a" else ""

                if label == "社名":
                    if value:
                        data[Schema.NAME] = value
                elif label == "代表者":
                    data[Schema.REP_NM] = value
                elif label == "所在地":
                    addr = value
                    pm = _POST_CODE_RE.search(addr)
                    if pm:
                        data[Schema.POST_CODE] = pm.group(1)
                        addr = _POST_CODE_RE.sub("", addr).strip()
                    data[Schema.ADDR] = addr
                elif label == "事業内容":
                    data[Schema.LOB] = value
                elif label == "企業URL":
                    data[Schema.HP] = href or value

        # ── 勤務地情報 セクション
        wloc = soup.select_one(".workLocationInformation")
        if wloc:
            genre_parts: list[str] = []
            for li in wloc.select(".workLocationInformation__item"):
                title_el = li.select_one(".detail-secondary__title")
                if not title_el:
                    continue
                label = title_el.get_text(strip=True)

                if label == "ジャンル":
                    for el in li.select("a, span"):
                        t = _clean(el.get_text())
                        if t and t not in genre_parts:
                            genre_parts.append(t)
                elif label == "アクセス":
                    txt = li.select_one(".recruit-shopText")
                    if txt:
                        data["アクセス"] = _multiline(txt.get_text("\n"))
                elif label == "勤務地":
                    txt = li.select_one(".recruit-shopText")
                    if txt and not data.get(Schema.ADDR):
                        wt = _multiline(txt.get_text("\n"))
                        # 〒xxx-xxxx 単独や都道府県名のみのことが多いが、住所が無いときに補完
                        pm = _POST_CODE_RE.search(wt)
                        if pm:
                            data[Schema.POST_CODE] = data.get(
                                Schema.POST_CODE
                            ) or pm.group(1)
                        wt = _POST_CODE_RE.sub("", wt).strip()
                        if wt and wt != data.get(Schema.PREF):
                            data[Schema.ADDR] = wt
                elif label == "URL":
                    a = li.select_one("a.recruit-shopLink, a.shop-url, a[href]")
                    if a and not data.get(Schema.HP):
                        data[Schema.HP] = a.get("href", "").strip()
            if genre_parts:
                data["ジャンル"] = " / ".join(genre_parts)

        # ── 求人キャッチコピー (詳細ページ側にも有れば優先)
        sub = soup.select_one(".detail-secondary__subtitle")
        if sub:
            data["求人キャッチコピー"] = _clean(sub.get_text())

        # ── 募集職種 / 給与 / 雇用形態 (.shop__info テーブルがあれば)
        info = soup.select_one("table.shop__info")
        if info:
            tds = info.select("tbody tr td")
            if len(tds) >= 1:
                data["募集職種"] = _clean(tds[0].get_text(" "))
            if len(tds) >= 2:
                salary_cell = tds[1]
                emp_icons = [
                    _clean(e.get_text())
                    for e in salary_cell.select(".is-employmentIcon")
                ]
                if emp_icons:
                    data["雇用形態"] = " / ".join(dict.fromkeys(emp_icons))
                data["給与"] = _clean(salary_cell.get_text(" "))[:300]

        # ── jobCard__detail__item: 勤務時間 / 休日 / 募集要項
        for it in soup.select(".jobCard__detail__item"):
            title_el = it.select_one(".jobCard__detail__title") or it.select_one(
                "h3, h4, dt"
            )
            text_el = it.select_one(".jobCard__detail__text") or it.select_one("p")
            if not title_el or not text_el:
                continue
            label = title_el.get_text(strip=True)
            text = _multiline(text_el.get_text("\n"))
            if not text:
                continue
            if label == "勤務時間" and not data.get("勤務時間"):
                data["勤務時間"] = text[:500]
            elif label == "休日" and not data.get("休日"):
                data["休日"] = text[:500]
            elif label == "募集要項" and not data.get("募集要項"):
                data["募集要項"] = text[:500]

        # ── 求人特徴タグ (jobMerits)
        merit_tags: list[str] = []
        for sec in soup.select(".jobMerits__section"):
            for el in sec.select(".jobMerits__tag, li, [class*='item']"):
                t = _clean(el.get_text())
                if t and len(t) < 30 and t not in merit_tags:
                    merit_tags.append(t)
        if merit_tags:
            data["求人特徴タグ"] = " / ".join(merit_tags[:60])

        # ── 企業紹介 (detail-secondary__point)
        intro = soup.select_one("#introduce_text") or soup.select_one(
            ".detail-secondary__point"
        )
        if intro:
            data["企業紹介"] = _multiline(intro.get_text("\n"))[:1000]

        # NAME が取れない場合は h1 からフォールバック
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                name = _clean(h1.get_text())
                # "{社名}の求人情報" → 社名のみ
                name = re.sub(r"の求人情報$", "", name)
                if name:
                    data[Schema.NAME] = name

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitonaJoshiScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

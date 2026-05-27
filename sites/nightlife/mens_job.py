"""
MENSJOB (mens-job.jp) — メンズナイトワーク求人サイト掲載店舗スクレイパー

取得対象:
    - キャバクラ / ラウンジ / コンカフェ 等ナイトワーク系 掲載全店舗

取得フロー:
    1. shoplist-sitemap.xml から全店舗URL を収集 (約142件)
    2. 各詳細ページから h1・th/td テーブル・article CSS クラスで店舗情報を抽出

実行方法:
    python scripts/sites/nightlife/mens_job.py
    python bin/run_flow.py --site-id mens_job
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://mens-job.jp"
SHOPLIST_SITEMAP = f"{BASE_URL}/shoplist-sitemap.xml"

_SHOP_URL_RE = re.compile(r"https://mens-job\.jp/shoplist/[^/]+/$")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# industry-* CSS クラス → 日本語業種名
_INDUSTRY_MAP: dict[str, str] = {
    "kyabakura":         "キャバクラ",
    "snack":             "スナック",
    "girlsbar":          "ガールズバー",
    "lounge":            "ラウンジ",
    "membership_lounge": "会員制ラウンジ",
    "concafe":           "コンカフェ",
    "club":              "クラブ",
    "nightclub":         "ナイトクラブ",
    "cabaret":           "キャバレー",
    "hostclub":          "ホストクラブ",
    "maidcafe":          "メイドカフェ",
    "bar":               "バー",
    "darts_bar":         "ダーツバー",
}


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


def _multiline(text: str) -> str:
    if not text:
        return ""
    lines = [_clean(ln) for ln in text.replace("\r", "\n").split("\n")]
    result: list[str] = []
    prev_empty = False
    for ln in lines:
        if ln == "":
            if not prev_empty and result:
                result.append("")
            prev_empty = True
        else:
            result.append(ln)
            prev_empty = False
    return "\n".join(result).strip()


def _get_th_td(soup, label: str) -> str:
    """th テキストに label を含む行の td テキストを返す。"""
    for th in soup.find_all("th"):
        if label in _clean(th.get_text()):
            td = th.find_next("td")
            if td:
                return _multiline(td.get_text("\n", strip=False))
    return ""


def _extract_tel(raw: str) -> str:
    """TEL フィールドから電話番号部分のみ抽出する。
    例: '090-1234-5678(24時間受付！)' → '090-1234-5678'
    """
    if not raw:
        return ""
    m = re.search(r"[\d０-９]{2,4}[-－ー]?[\d０-９]{2,4}[-－ー]?[\d０-９]{3,4}", raw)
    return m.group() if m else raw


class MensJobScraper(StaticCrawler):
    """MENSJOB (mens-job.jp) 掲載店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "最寄り駅",
        "職種給与",
        "応募資格",
        "待遇",
        "応募方法",
        "キャッチコピー",
    ]

    def parse(self, url: str):
        resp = self.session.get(SHOPLIST_SITEMAP, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        shop_urls = [
            el.text.strip()
            for el in root.iter()
            if el.tag.endswith("loc") and el.text and _SHOP_URL_RE.match(el.text.strip())
        ]
        self.total_items = len(shop_urls)
        self.logger.info("shoplist-sitemap から %d 件収集", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception:
                self.logger.exception("詳細取得失敗: %s", shop_url)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 店舗名・読み仮名: <h1><span id="entry_shop_name">名前</span><span>カナ</span></h1>
        name, kana = "", ""
        h1 = soup.select_one("h1")
        if h1:
            spans = h1.find_all("span", recursive=False)
            name_span = h1.select_one("#entry_shop_name") or (spans[0] if spans else None)
            if name_span:
                name = _clean(name_span.get_text())
            if len(spans) >= 2:
                kana = _clean(spans[1].get_text())
            if not name:
                name = _clean(h1.get_text(" ", strip=True))

        if not name:
            return None

        # 業種・エリア: article/body の CSS クラスから取得
        cat_site, area = "", ""
        target = soup.find("article") or soup.find("body")
        if target:
            class_str = " ".join(target.get("class", []))
            slugs = re.findall(r"industry-(\S+)", class_str)
            if slugs:
                cat_site = _INDUSTRY_MAP.get(slugs[0], slugs[0].replace("-", ""))
            area_slugs = re.findall(r"area-(\S+)", class_str)
            area = ", ".join(area_slugs)

        # キャッチコピー
        desc_el = soup.select_one("p.shop_description")
        catch = _clean(desc_el.get_text()) if desc_el else ""

        # 住所 → 都道府県・住所を分離
        addr_raw = _clean(_get_th_td(soup, "住所"))
        pref, addr = "", addr_raw
        m = _PREF_PATTERN.match(addr_raw)
        if m:
            pref = m.group(1)
            addr = addr_raw[m.end():].strip()

        # 公式HP: "URL" を含む th 行の td > a[href]
        hp = ""
        for th in soup.find_all("th"):
            th_text = _clean(th.get_text())
            if "URL" in th_text or "HP" in th_text or "ホームページ" in th_text:
                td = th.find_next("td")
                if td:
                    a = td.find("a", href=True)
                    if a:
                        href = a["href"].strip()
                        if href.startswith("http") and "mens-job.jp" not in href:
                            hp = href
                    if not hp:
                        txt = _clean(td.get_text())
                        if txt.startswith("http") and "mens-job.jp" not in txt:
                            hp = txt
                break

        return {
            Schema.URL:       url,
            Schema.NAME:      name,
            Schema.NAME_KANA: kana,
            Schema.PREF:      pref,
            Schema.ADDR:      addr,
            Schema.TEL:       _extract_tel(_get_th_td(soup, "TEL")),
            Schema.CAT_SITE:  cat_site,
            Schema.TIME:      _get_th_td(soup, "勤務時間"),
            Schema.HOLIDAY:   _clean(_get_th_td(soup, "休日")),
            Schema.HP:        hp,
            "エリア":          area,
            "最寄り駅":        _clean(_get_th_td(soup, "最寄")),
            "職種給与":        _get_th_td(soup, "職種"),
            "応募資格":        _get_th_td(soup, "資格"),
            "待遇":            _get_th_td(soup, "待遇"),
            "応募方法":        _get_th_td(soup, "応募方法"),
            "キャッチコピー":  catch,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MensJobScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

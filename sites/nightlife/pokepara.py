"""
ポケパラ (www.pokepara.jp) — 全国のキャバクラ/クラブ/ラウンジ等 掲載店舗スクレイパー

取得対象:
    - sitemap_shop.ashx に列挙された全国・全エリアの掲載店舗 (約 8,700 店)
    - 店舗情報ページ (pokepara.jp): 名称 / 都道府県 / 住所 / TEL / ジャンル / HP /
      定休日 / 営業時間 / エリア / LINE
    - 求人情報ページ (pokepara-tainew.jp ※店舗ページ内のリンクから派生して取得):
      給与 / 体入時給 / 入店時給 / 職種 / 衣装 / 勤務日 / アクセス / 資格 / メール

取得フロー:
    1. ルート URL から sitemap_shop.ashx を導出し、全店舗 URL を列挙 (全エリア網羅)
    2. 各店舗ページ (pokepara.jp) を取得し、店舗情報カラムを抽出
    3. 店舗ページ内の「女性求人情報ページ」リンク (pokepara-tainew.jp) を取得し、
       求人カラムを補完 (Pattern B: 1 店舗ごとに即 yield)

注:
    - 給与 / 勤務日 / 資格 は自由記述を含むが、依頼 (備考) で明示的に列挙された
      カラムのため取得対象に含める。
    - 求人ページが存在しない (掲載のみ・求人なし) 店舗は求人カラムを空文字で出力する。

実行方法:
    python scripts/sites/nightlife/pokepara.py
    python bin/run_flow.py --site-id pokepara
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


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
_TAINYU_TIME = re.compile(r"体入時給[^\d]{0,8}([\d,]+\s*円(?:[～~]?(?:以上)?)?)")
_NYUTEN_TIME = re.compile(r"入店時給[^\d]{0,8}([\d,]+\s*円(?:[～~]?(?:以上)?)?)")
_LINE_HREF = re.compile(r"line\.me|lin\.ee", re.I)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _clean(addr)
    if not addr:
        return "", ""
    m = _PREF_PATTERN.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return "", addr


class PokeparaScraper(StaticCrawler):
    """ポケパラ (www.pokepara.jp) 掲載店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "給与",
        "体入時給",
        "入店時給",
        "職種",
        "衣装",
        "勤務日",
        "アクセス",
        "資格",
        "メール",
    ]

    # ------------------------------------------------------------------
    # 一覧 (sitemap) → 詳細
    # ------------------------------------------------------------------
    def parse(self, url: str):
        # ルート URL から店舗 sitemap を導出 (別ルートはハードコードしない)
        sitemap_url = urljoin(url, "/sitemap_shop.ashx")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            self.total_items = 0
            return

        shop_urls = [
            _clean(loc.get_text())
            for loc in soup.find_all("loc")
            if "shop" in loc.get_text()
        ]
        self.total_items = len(shop_urls)

        for shop_url in shop_urls:
            try:
                item = self._scrape_shop(shop_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("店舗解析エラー (スキップ): %s — %s", shop_url, e)
                continue
            if item:
                yield item

    # ------------------------------------------------------------------
    # 店舗情報ページ (pokepara.jp)
    # ------------------------------------------------------------------
    def _scrape_shop(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        # --- 名称 / ジャンル / エリア (meta keywords が最も構造的) ---
        name = ""
        h1 = soup.select_one("h1")
        if h1:
            name = _clean(h1.get_text(" ", strip=True).split(" - ")[0])

        genre = ""
        area = ""
        kw_el = soup.select_one("meta[name=keywords]")
        if kw_el and kw_el.get("content"):
            parts = [p.strip() for p in kw_el["content"].split(",") if p.strip()]
            # 形式: 名称, カナ, エリア, ジャンル, ポケパラ
            if len(parts) >= 4:
                genre = parts[-2]
                area = parts[-3]
            if not name and parts:
                name = parts[0]

        # --- 店舗情報 (#shop_detaile: table 版/div 版の両レイアウトに対応) ---
        info = self._extract_shop_info(soup.select_one("#shop_detaile"))

        addr_raw = info.get("住所", "")
        pref, addr_rest = _split_pref(addr_raw.replace("\n", " "))
        address = addr_raw if not pref else f"{pref}{addr_rest}"

        tel = ""
        if info.get("電話番号"):
            tel = re.sub(r"[（(].*$", "", _clean(info["電話番号"].split("\n")[0])).strip()

        item = {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: genre,
            Schema.HP: info.get("HP", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.LINE: info.get("LINE", ""),
            "エリア": area,
            "給与": "",
            "体入時給": "",
            "入店時給": "",
            "職種": "",
            "衣装": "",
            "勤務日": "",
            "アクセス": "",
            "資格": "",
            "メール": "",
        }

        # --- 求人情報ページ (pokepara-tainew.jp) を店舗ページのリンクから派生取得 ---
        recruit_a = soup.select_one('a[href*="pokepara-tainew.jp"]')
        if recruit_a and recruit_a.get("href"):
            try:
                self._fill_recruit(_clean(recruit_a["href"]), item)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("求人ページ解析エラー: %s — %s", recruit_a.get("href"), e)

        return item

    @staticmethod
    def _extract_shop_info(container) -> dict:
        """#shop_detaile から店舗情報を抽出する (table 版/div 版 両対応)。

        いずれのレイアウトもラベルは `.de_title` で示され、値はその直後の兄弟要素
        (table 版は次 tr、div 版は h3/p 等) に並ぶ。ラベル文字列はレイアウト間で
        揺れる (電話番号/TEL, Webサイト/ウェブ 等) ため正規化して返す。
        """
        out = {"住所": "", "電話番号": "", "営業時間": "", "HP": "", "定休日": "", "LINE": ""}
        if container is None:
            return out

        for marker in container.select(".de_title"):
            img = marker.find("img")
            label = _clean(marker.get_text()) or (_clean(img.get("alt")) if img else "")
            if not label:
                continue

            texts: list[str] = []
            hrefs: list[str] = []
            for sib in marker.find_next_siblings():
                cls = sib.get("class") or []
                if "de_title" in cls:
                    break
                if "more" in cls:  # 「地図で確認する」等のリンクブロックは除外
                    continue
                for more in sib.select("div.more, .more"):
                    more.decompose()
                hrefs.extend(a.get("href", "") for a in sib.select("a[href]"))
                txt = _clean(sib.get_text("\n"))
                if txt:
                    texts.append(txt)
            value = "\n".join(texts).strip()

            # 外部リンク (LINE/HP) はラベルに依らず href から検出する
            for href in hrefs:
                if href and _LINE_HREF.search(href):
                    out["LINE"] = href

            if label in ("住所",):
                out["住所"] = value
            elif label in ("電話番号", "TEL", "Tel"):
                out["電話番号"] = value
            elif label in ("営業時間",):
                out["営業時間"] = value
            elif label in ("定休日",):
                out["定休日"] = value
            elif label in ("Webサイト", "ウェブ", "ホームページ", "公式サイト", "WEB", "HP"):
                for href in hrefs:
                    if href.startswith("http") and "pokepara" not in href and not _LINE_HREF.search(href):
                        out["HP"] = href
                        break
        return out

    # ------------------------------------------------------------------
    # 求人情報ページ (pokepara-tainew.jp)
    # ------------------------------------------------------------------
    def _fill_recruit(self, recruit_url: str, item: dict) -> None:
        soup = self.get_soup(recruit_url)
        if soup is None:
            return

        # 募集要項 (table.basicinfos: td[0]=ラベル, td[1]=値)
        basic: dict = {}
        bt = soup.select_one("table.basicinfos")
        if bt:
            for tr in bt.select("tr"):
                tds = tr.select("td")
                if len(tds) >= 2:
                    basic[_clean(tds[0].get_text())] = _clean(tds[1].get_text(" "))

        pay = basic.get("給与", "")
        item["給与"] = pay
        if pay:
            m = _TAINYU_TIME.search(pay)
            if m:
                item["体入時給"] = _clean(m.group(1))
            m = _NYUTEN_TIME.search(pay)
            if m:
                item["入店時給"] = _clean(m.group(1))
        item["職種"] = basic.get("職種", "")
        item["勤務日"] = basic.get("勤務日", "")
        item["資格"] = basic.get("資格", "")

        # アクセス: 勤務地の MAP 以降に記載される最寄り駅・道順
        workplace = basic.get("勤務地", "")
        if workplace:
            m = re.search(r"(?:→?MAP)\s*(.+)$", workplace)
            if m:
                item["アクセス"] = _clean(m.group(1))

        # 衣装: 待遇タグから「衣装」を含む語句を抽出
        for tr in soup.select("table.info_01 tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td and "待遇" in th.get_text():
                tags = re.split(r"[ 　]+", _clean(td.get_text(" ")))
                costume = [t for t in tags if "衣装" in t]
                if costume:
                    item["衣装"] = " ".join(dict.fromkeys(costume))
                break

        # メール / LINE (求人ページ側を優先補完)
        mail = soup.select_one('a[href^="mailto:"]')
        if mail:
            item["メール"] = _clean(mail["href"].replace("mailto:", ""))
        if not item.get(Schema.LINE):
            line_a = soup.find("a", href=_LINE_HREF)
            if line_a and line_a.get("href"):
                item[Schema.LINE] = line_a["href"]


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PokeparaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.pokepara.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

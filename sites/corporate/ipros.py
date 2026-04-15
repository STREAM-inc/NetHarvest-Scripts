"""
IPROS ものづくり — 製造業向け企業情報スクレイパー

取得対象:
    - mono.ipros.com に登録されている全企業（約47,000社）
    - 業種(business_class)別に一覧をループし、各企業の詳細ページから情報を取得する

取得フロー:
    業種一覧 → 業種ごとに /search/company/?business_class=N&p=K を全ページ巡回
    → 各企業の詳細ページ /company/detail/{id}/ を取得 → CSV出力

実行方法:
    python scripts/sites/corporate/ipros.py
    python bin/run_flow.py --site-id ipros
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs, unquote

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://mono.ipros.com"

# 業種コード一覧 (トップページ「業種から企業を探す」より)
BUSINESS_CLASSES: list[tuple[int, str]] = [
    (33, "製造・加工受託"), (54, "その他"), (12, "産業用機械"), (15, "機械要素・部品"),
    (34, "その他製造"), (24, "IT・情報通信"), (35, "商社・卸売り"), (17, "産業用電気機器"),
    (31, "建材・資材・什器"), (25, "ソフトウェア"), (18, "電子部品・半導体"), (10, "樹脂・プラスチック"),
    (49, "サービス業"), (23, "試験・分析・測定"), (11, "鉄/非鉄金属"), (27, "環境"),
    (4, "化学"), (20, "自動車・輸送機器"), (32, "印刷業"), (40, "情報通信業"),
    (16, "民生用電気機器"), (28, "エネルギー"), (6, "ゴム製品"), (14, "食品機械"),
    (19, "光学機器"), (13, "ロボット"), (2, "繊維"), (3, "紙・パルプ"),
    (38, "電気・ガス・水道業"), (30, "医薬品・バイオ"), (39, "倉庫・運輸関連業"), (7, "ガラス・土石製品"),
    (1, "飲食料品"), (26, "CAD／CAM"), (36, "小売"), (45, "教育・研究機関"),
    (29, "医療機器"), (9, "セラミックス"), (8, "木材"), (37, "運輸業"),
    (44, "医療・福祉"), (5, "石油・石炭製品"), (22, "造船・重機"), (21, "航空・宇宙"),
    (52, "水産・農林業"), (47, "公益・特殊・独立行政法人"), (200026, "設備"),
    (50, "自営業"), (200012, "研究・開発用機器・装置"), (46, "官公庁"),
    (200027, "素材・材料"), (53, "鉱業"), (43, "金融・証券・保険業"),
    (200004, "化粧品"), (51, "個人"), (42, "飲食店・宿泊業"),
    (48, "警察・消防・自衛隊"), (200009, "試薬・薬品原料"),
    (200011, "実験器具・消耗品"), (200019, "受託研究"),
]

POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
TEL_RE = re.compile(r"TEL[：:]\s*([\d\-－()（）]+)")
LATLNG_RE = re.compile(r"query=([\d.\-]+)[%,\s]+([\d.\-]+)")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class IprosScraper(StaticCrawler):
    """IPROS ものづくり 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["キャッチコピー", "主要取引先", "事業拠点", "緯度", "経度", "ロゴURL", "最終更新日", "企業ID"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for code, name in BUSINESS_CLASSES:
            self.logger.info("業種 [%s] %s 開始", code, name)
            page = 1
            while True:
                list_url = f"{BASE_URL}/search/company/?business_class={code}&p={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                items = soup.select("section.search-result-company-item")
                if not items:
                    break

                if page == 1 and self.total_items == 0:
                    # 初回ページで概数を出すための簡易設定 (進捗表示用)
                    total_el = soup.select_one(".pagination-control__display-label-total")
                    if total_el:
                        m = re.search(r"([\d,]+)", total_el.get_text())
                        if m:
                            self.logger.info("  業種総件数: %s 件", m.group(1))

                for item in items:
                    a = item.select_one("a[href*='/company/detail/']")
                    if not a:
                        continue
                    detail_path = a.get("href", "").rstrip("/")
                    company_id = detail_path.rsplit("/", 1)[-1]
                    if company_id in seen:
                        continue
                    seen.add(company_id)

                    detail_url = urljoin(BASE_URL, detail_path + "/")
                    try:
                        data = self._scrape_detail(detail_url, company_id, name, item)
                        if data:
                            yield data
                    except Exception as e:
                        self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                        continue

                # 次ページ判定
                next_link = soup.find("a", string=re.compile(r"次へ"))
                if not next_link or not next_link.get("href"):
                    break
                page += 1

    def _scrape_detail(self, url: str, company_id: str, biz_class: str, list_item) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.CAT_SITE: biz_class,
            "企業ID": company_id,
        }

        # 一覧アイテムから取得できる情報
        name_link = list_item.select_one(".search-result-company-item__name-link")
        if name_link:
            data[Schema.NAME] = _clean(name_link.get_text())
        catch = list_item.select_one(".search-result-company-item__description")
        if catch:
            data["キャッチコピー"] = _clean(catch.get_text())
        logo_img = list_item.select_one(".search-result-company-item__image img")
        if logo_img:
            data["ロゴURL"] = logo_img.get("src", "")

        # 詳細ページのテーブル
        for tr in soup.select("table.company-detail__table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            val = _clean(td.get_text(" "))
            if key == "企業名" and not data.get(Schema.NAME):
                data[Schema.NAME] = val
            elif key == "従業員数":
                data[Schema.EMP_NUM] = val
            elif key == "連絡先":
                # 〒xxx-xxxx 住所 TEL：xxx
                raw = td.get_text("\n")
                m = POST_RE.search(raw)
                if m:
                    data[Schema.POST_CODE] = m.group(1)
                # 住所抽出 (郵便番号と TEL/地図で見る を除いた行)
                addr_text = ""
                for line in raw.splitlines():
                    line = line.strip()
                    if not line or line.startswith("〒") or line.startswith("TEL"):
                        continue
                    addr_text = line.replace("地図で見る", "").strip()
                    if addr_text:
                        break
                if addr_text:
                    pm = PREF_RE.match(addr_text)
                    if pm:
                        data[Schema.PREF] = pm.group(1)
                        data[Schema.ADDR] = addr_text[pm.end():].strip()
                    else:
                        data[Schema.ADDR] = addr_text
                tm = TEL_RE.search(raw)
                if tm:
                    data[Schema.TEL] = tm.group(1).strip()
            elif key == "主要取引先":
                data["主要取引先"] = val
            elif key == "事業拠点":
                data["事業拠点"] = val
            elif key == "業種":
                # 詳細ページの業種を優先
                data[Schema.CAT_SITE] = val

        # 設立, 資本金 等
        for item_el in soup.select(".company-summary__description-item"):
            label = item_el.select_one(".company-summary__description-label")
            if not label:
                continue
            label_text = _clean(label.get_text())
            value_text = _clean(item_el.get_text(" ").replace(label_text, "", 1))
            if label_text == "設立":
                data[Schema.OPEN_DATE] = value_text
            elif label_text == "資本金":
                data[Schema.CAP] = value_text

        # HP URL (externalLink リダイレクトを解決)
        hp_a = soup.select_one("a[href*='externalLink'][href*='url=']")
        if hp_a:
            qs = parse_qs(urlparse(hp_a["href"]).query)
            if qs.get("url"):
                data[Schema.HP] = unquote(qs["url"][0])

        # 緯度経度
        map_a = soup.select_one("a[href*='google.com/maps']")
        if map_a:
            mm = LATLNG_RE.search(map_a["href"])
            if mm:
                data["緯度"] = mm.group(1)
                data["経度"] = mm.group(2)

        # 最終更新日
        upd = soup.select_one(".company-summary__description-update")
        if upd:
            data["最終更新日"] = _clean(upd.get_text()).replace("最終更新日：", "")

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = IprosScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

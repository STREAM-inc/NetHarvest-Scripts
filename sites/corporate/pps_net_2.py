"""
新電力ネット 小売電気事業者一覧 (pps-net.org/ppscompany_reg/retail)

取得対象:
    - 国内の小売電気事業者 約1,024社 (20件/ページ × 52ページ)
    - 一覧: 名称 / 電力販売量(千kWh) / 発電最大出力(kW) / 発電実績(千kWh) /
            CO2排出量(t-CO2/kWh) / 詳細ページURL
    - 詳細 (/ppscompany/{id}): 供給地域・対象需要(低圧/高圧/特別高圧)・
            料金プラン(家庭向けプラン有無/プラン名)・本社住所・TEL・HP・
            資本金・設立・従業員数 (ラベル駆動。後者3項目は現状サイト側に掲載なし)

取得フロー:
    1. 一覧ページ (/ppscompany_reg/retail, 2ページ目以降は /page/{n}) の
       table.scroll-tbl から 20 社ずつ行を読む
    2. 各行の名称リンク (/ppscompany/{id}) を辿り詳細ページを取得
    3. 一覧の指標と詳細の企業情報を 1 レコードに統合し、その場で即 yield (Pattern B)
    4. 一覧が取得できない / 行が無いページに達したら終了 (53ページ目は404)

備考の指示:
    - 電力販売量が空欄の事業者も除外せずそのまま保持する
    - 既存の pps_net サイト定義 (/agency 代理店一覧) とは別物

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/pps_net_2.py

    # Prefect Flow 経由 (全件)
    python bin/run_flow.py --site-id pps_net_2
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)
_PREF_PATTERN = re.compile("(" + "|".join(_PREFECTURES) + ")")

# 詳細ページ URL (/ppscompany/16811)
_DETAIL_RE = re.compile(r"/ppscompany/(\d+)")
# ページ送りリンク (/ppscompany_reg/retail/page/52)
_PAGE_RE = re.compile(r"/page/(\d+)")
# 販売量セル "3721211千kWh" / "13091.43835千kWh"
_KWH_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")
# 設立「2015年4月1日」→ YYYY-MM-DD 変換用
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# 詳細ページ「電力会社・団体情報」テーブルのラベル → 出力先
_INFO_LABELS = {
    "住所": "address",
    "担当部署": "担当部署",
    "電話番号": "tel",
    "供給地域（予定含む）": "供給地域",
    "ホームページ": "hp",
    "メールお問い合わせ": "email",
    "料金プランページ": "料金プランページ",
    "調整後排出係数": "調整後排出係数",
    "CO2フリープランの有無": "CO2フリープランの有無",
    "JEPX会員": "JEPX会員",
    # 掲載されていれば拾う (2026-09 時点では未掲載)
    "資本金": "capital",
    "設立": "founded",
    "設立年月日": "founded",
    "従業員数": "employees",
}

# 販売量内訳テーブルの行ラベル → 出力カラム
_DEMAND_ROWS = {
    "特別高圧": "販売量_特別高圧(千kWh)",
    "高圧": "販売量_高圧(千kWh)",
    "低圧（電灯）": "販売量_低圧電灯(千kWh)",
    "低圧（電力）": "販売量_低圧電力(千kWh)",
    "最終保障供給": "販売量_最終保障供給(千kWh)",
    "離島供給": "販売量_離島供給(千kWh)",
    "合計": "販売量_合計(千kWh)",
}

# 料金プランの見出し接頭辞 → (有無カラム, プラン名カラム or None)
_PLAN_SECTIONS = [
    ("家庭・個人用", "家庭向けプラン有無", "家庭向け料金プラン名"),
    ("低圧法人", "低圧法人向けプラン有無", None),
    ("卒FIT", "卒FIT買取プラン有無", None),
]


def _clean(text: str) -> str:
    """全角スペース・連続空白を整理した文字列を返す。"""
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


class PpsNetRetailScraper(StaticCrawler):
    """新電力ネット 小売電気事業者一覧スクレイパー"""

    DELAY = 1.0
    # 一覧 52 ページ + 想定外の増ページに備えた安全マージン
    MAX_PAGES = 80

    EXTRA_COLUMNS = [
        "company_id",
        "詳細ページURL",
        "電力販売量(千kWh)",
        "発電最大出力(kW)",
        "発電実績(千kWh)",
        "CO2排出量(t-CO2/kWh)",
        "供給地域",
        "供給エリア",
        "対象需要",
        "販売量_対象月",
        "販売量_特別高圧(千kWh)",
        "販売量_高圧(千kWh)",
        "販売量_低圧電灯(千kWh)",
        "販売量_低圧電力(千kWh)",
        "販売量_最終保障供給(千kWh)",
        "販売量_離島供給(千kWh)",
        "販売量_合計(千kWh)",
        "家庭向けプラン有無",
        "家庭向け料金プラン名",
        "低圧法人向けプラン有無",
        "卒FIT買取プラン有無",
        "料金プランページ",
        "調整後排出係数",
        "CO2フリープランの有無",
        "JEPX会員",
        "担当部署",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        seen: set[str] = set()
        last_page: int | None = None

        for page in range(1, self.MAX_PAGES + 1):
            list_url = base if page == 1 else f"{base}/page/{page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.info("一覧ページを取得できなかったため終了: %s", list_url)
                break

            if page == 1:
                last_page = self._detect_last_page(soup)
                if last_page:
                    self.total_items = last_page * 20
                    logger.info("一覧: 全 %s ページ", last_page)

            rows = self._list_rows(soup)
            if not rows:
                logger.info("行が無いため終了: %s", list_url)
                break
            logger.info("ページ %s: %s 件", page, len(rows))

            for row in rows:
                item = self._parse_list_row(row, list_url)
                if not item:
                    continue
                if item["company_id"] in seen:
                    continue
                seen.add(item["company_id"])

                detail_url = item["詳細ページURL"]
                detail_soup = self.get_soup(detail_url)
                if detail_soup is not None:
                    item.update(self._parse_detail(detail_soup))
                else:
                    logger.warning("詳細ページを取得できません: %s", detail_url)
                # 電力販売量が空欄の事業者も除外せず yield する (備考の指示)
                yield item

            if last_page and page >= last_page:
                break

    # ------------------------------------------------------------------ 一覧
    def _detect_last_page(self, soup: bs4.BeautifulSoup) -> int | None:
        """wp-pagenavi のリンクから最終ページ番号を求める。"""
        pages = [
            int(m.group(1))
            for a in soup.select(".wp-pagenavi a[href]")
            if (m := _PAGE_RE.search(a["href"]))
        ]
        return max(pages) if pages else None

    def _list_rows(self, soup: bs4.BeautifulSoup) -> list[bs4.Tag]:
        """一覧テーブルのデータ行 (ヘッダー除く) を返す。"""
        table = soup.select_one("table.scroll-tbl")
        if table is None:
            return []
        rows = []
        for tr in table.select("tr"):
            if tr.find("th") and not tr.find("td"):
                continue  # ヘッダー行
            if tr.select_one("a[href*='/ppscompany/']"):
                rows.append(tr)
        return rows

    def _parse_list_row(self, row: bs4.Tag, list_url: str) -> dict | None:
        link = row.select_one("a[href*='/ppscompany/']")
        if link is None:
            return None
        detail_url = urljoin(list_url, link["href"])
        m = _DETAIL_RE.search(detail_url)
        if not m:
            return None

        cells = [_clean(c.get_text(" ", strip=True)) for c in row.find_all(["th", "td"])]
        name = _clean(link.get_text(" ", strip=True)) or (cells[0] if cells else "")
        values = cells[1:5] + [""] * 4  # 列欠けに備えて埋める

        item = {key: "" for key in Schema.COLUMNS}
        item.update({key: "" for key in self.EXTRA_COLUMNS})
        item.update({
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.CAT_SITE: "小売電気事業者",
            "company_id": m.group(1),
            "詳細ページURL": detail_url,
            "電力販売量(千kWh)": values[0],
            "発電最大出力(kW)": values[1],
            "発電実績(千kWh)": values[2],
            "CO2排出量(t-CO2/kWh)": values[3],
        })
        return item

    # ------------------------------------------------------------------ 詳細
    def _parse_detail(self, soup: bs4.BeautifulSoup) -> dict:
        data: dict[str, str] = {}
        info = self._parse_info_table(soup)

        address = info.get("address", "")
        pref = ""
        if address:
            if pm := _PREF_PATTERN.search(address):
                pref = pm.group(1)
                address = address[pm.end():].strip()
        data[Schema.PREF] = pref
        data[Schema.ADDR] = address
        data[Schema.TEL] = info.get("tel", "")
        data[Schema.HP] = info.get("hp", "")
        data[Schema.WEBSITE] = info.get("hp", "")
        data[Schema.EMAIL] = info.get("email", "")
        data[Schema.CAP] = info.get("capital", "")
        data[Schema.EMP_NUM] = info.get("employees", "")
        data[Schema.OPEN_DATE] = self._normalize_date(info.get("founded", ""))

        for key in ("供給地域", "料金プランページ", "調整後排出係数",
                    "CO2フリープランの有無", "JEPX会員", "担当部署"):
            data[key] = info.get(key, "")
        data["供給エリア"] = info.get("供給エリア", "")

        data.update(self._parse_demand(soup))
        data.update(self._parse_plans(soup))
        return data

    def _parse_info_table(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        """「電力会社・団体情報」テーブルをラベル駆動で読む。"""
        heading = next(
            (h for h in soup.select("h3") if "団体情報" in h.get_text()), None
        )
        if heading is None:
            return {}
        table = heading.find_next("table")
        if table is None:
            return {}

        info: dict[str, str] = {}
        for tr in table.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if th is None or td is None:
                continue
            label = _clean(th.get_text(" ", strip=True)).split("※")[0].strip()
            key = _INFO_LABELS.get(label)
            if key is None:
                continue
            value = _clean(td.get_text(" ", strip=True))
            if key == "供給地域":
                # 「関東、中部、近畿 ⇒エリア別電力会社一覧（…）」の案内部分を落とす
                value = value.split("⇒")[0].strip()
                areas = [
                    _clean(a.get_text())
                    for a in td.select("a[href*='/ppscompany/']")
                    if _clean(a.get_text())
                ]
                info["供給エリア"] = "|".join(dict.fromkeys(areas))
            elif key in ("hp", "料金プランページ"):
                href = td.find("a", href=True)
                value = _clean(href["href"]) if href and href["href"] else value
            elif key == "email":
                href = td.find("a", href=True)
                if href and href["href"].startswith("mailto:"):
                    value = href["href"][len("mailto:"):]
            elif key == "調整後排出係数" and not re.search(r"\d", value):
                value = ""  # 未掲載時は単位だけが残るため空に揃える
            if value in ("－", "-", "—"):
                value = ""
            info[key] = value
        return info

    def _parse_demand(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        """販売量内訳テーブルから月次実績と対象需要 (低圧/高圧/特別高圧) を得る。"""
        data: dict[str, str] = {}
        heading = next(
            (h for h in soup.select("h3") if _clean(h.get_text()) == "電力の販売量"), None
        )
        if heading is None:
            return data

        table = None
        for sib in heading.find_next_siblings():
            if sib.name in ("h2", "h3"):
                break
            found = sib if sib.name == "table" else sib.find("table") if hasattr(sib, "find") else None
            if found is not None and found.find("th"):
                header = [_clean(c.get_text()) for c in found.select("tr")[0].find_all(["th", "td"])]
                if any("前月比" in h for h in header):
                    table = found
                    data["販売量_対象月"] = header[1] if len(header) > 1 else ""
                    break
        if table is None:
            return data

        served: list[str] = []
        for tr in table.select("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = _clean(cells[0].get_text())
            column = _DEMAND_ROWS.get(label)
            if column is None:
                continue
            raw = _clean(cells[1].get_text())
            m = _KWH_RE.search(raw)
            value = m.group(0).replace(",", "") if m else ""
            data[column] = value
            try:
                positive = float(value) > 0
            except ValueError:
                positive = False
            if positive and label in ("特別高圧", "高圧", "低圧（電灯）", "低圧（電力）"):
                category = "低圧" if label.startswith("低圧") else label
                if category not in served:
                    served.append(category)

        order = ["特別高圧", "高圧", "低圧"]
        data["対象需要"] = "/".join(c for c in order if c in served)
        return data

    def _parse_plans(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        """料金プランセクションごとに有無と (家庭向けのみ) プラン名を取る。"""
        data: dict[str, str] = {}
        for prefix, presence_col, name_col in _PLAN_SECTIONS:
            heading = next(
                (h for h in soup.select("h3") if _clean(h.get_text()).startswith(prefix)),
                None,
            )
            if heading is None:
                data[presence_col] = ""
                continue

            names: list[str] = []
            for sib in heading.find_next_siblings():
                if sib.name in ("h2", "h3"):
                    break
                table = sib if sib.name == "table" else sib.find("table") if hasattr(sib, "find") else None
                if table is None:
                    continue
                for tr in table.select("tr"):
                    cells = tr.find_all(["th", "td"])
                    if not cells or cells[0].name == "th":
                        continue
                    plan = _clean(cells[0].get_text(" ", strip=True))
                    if plan and plan != "プラン名":
                        names.append(plan)
                break

            data[presence_col] = "有" if names else "無"
            if name_col:
                data[name_col] = "|".join(dict.fromkeys(names))
        return data

    @staticmethod
    def _normalize_date(value: str) -> str:
        """「2015年4月1日」→「2015-04-01」。変換できなければ原文のまま返す。"""
        if not value:
            return ""
        m = _DATE_RE.search(value)
        if not m:
            return value
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PpsNetRetailScraper()
    scraper.execute("https://pps-net.org/ppscompany_reg/retail")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

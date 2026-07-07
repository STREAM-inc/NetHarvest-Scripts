"""
ブンナビ (文化放送就職ナビ 2028) — 掲載企業の会社概要データ取得

取得対象:
    - 企業検索結果 (sh_result.php) に掲載される全企業の会社概要
    - 企業名 / 業種 / 本社所在地 / 代表者 / 電話番号 / 従業員数 / 資本金 /
      設立年月日 / 平均年収・平均年齢 / 上場情報 等 (構造化フィールドのみ)

取得フロー:
    1. 検索結果一覧 (sh_result.php?page=N) を 1 ページ (40 社) ずつ巡回
    2. 各ボックスの比較用チェックボックス値から企業コード ccd2 を取得
    3. 会社データページ (cn_data.php?ccd2=NN) を 1 社ずつ取得して即 yield
    ※ 一覧のリンク先が cn_recruit.php の企業も cn_data.php?ccd2= で会社概要を取得できる

備考 (著作権配慮):
    - 「東洋経済・DATA特色」「特色」等の自由記述プロースは取得しない
    - 会社四季報の詳細財務指標 (当期利益/ROE 等) はライセンスデータのため取得しない
    - 会社概要として一般的な構造化フィールド (住所/TEL/代表者/資本金 等) のみ取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/bunnabi_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bunnabi_2
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"(\d{3}-\d{4})")


class Bunnabi2Crawler(StaticCrawler):
    """ブンナビ2028 (文化放送就職ナビ) スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "業種",            # 一覧のカテゴリ表記 (例: 自動車・輸送機器)
        "英文社名",
        "従業員数(連結)",
        "平均年収",
        "平均年齢",
        "上場年月日",
        "上場市場名",
        "証券コード",
        "事業構成・セグメント",
    ]

    def parse(self, url: str):
        page = 1
        first = True
        while True:
            sep = "&" if "?" in url else "?"
            list_url = f"{url}{sep}page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            boxes = soup.select("div.rn_searchBox")
            if not boxes:
                break

            if first:
                # 進捗表示用の概算件数 (最終ページ番号 × 1 ページあたり件数)
                last_page = max(
                    [int(a.get_text(strip=True)) for a in soup.select("a")
                     if a.get_text(strip=True).isdigit()] or [1]
                )
                self.total_items = last_page * len(boxes)
                first = False

            for box in boxes:
                try:
                    cb = box.select_one('input[name="compare"]')
                    ccd2 = cb.get("value") if cb else None
                    if not ccd2:
                        continue

                    name_el = box.select_one("span.name")
                    cat_el = box.select_one("span.category")
                    list_name = name_el.get_text(strip=True) if name_el else ""
                    category = cat_el.get_text(strip=True).strip("［］[]") if cat_el else ""

                    detail_url = urljoin(url, f"cn_data.php?ccd2={ccd2}")
                    item = self._scrape_detail(detail_url, list_name, category)
                    if item:
                        yield item
                except Exception as e:  # noqa: BLE001
                    logger.warning("企業ボックス処理エラー (スキップ): %s", e)
                    continue

            page += 1

    def _scrape_detail(self, url: str, list_name: str, category: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 企業名 (詳細ページ優先、無ければ一覧の名称)
        h2 = soup.select_one("h2.rn_companyName")
        name = h2.get_text(strip=True) if h2 else list_name

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.CAT_SITE: category,
            "業種": category,
        }

        kv = self._company_data_kv(soup)

        # 本社所在地 → 郵便番号 / 都道府県 / 住所
        addr_raw = kv.get("本社所在地", "")
        if addr_raw:
            m = _POST_PATTERN.search(addr_raw)
            if m:
                item[Schema.POST_CODE] = m.group(1)
                addr_raw = addr_raw[m.end():]
            addr = addr_raw.replace("〒", "").replace("　", " ").strip()
            pm = _PREF_PATTERN.search(addr)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr[pm.start():].strip()
            else:
                item[Schema.ADDR] = addr

        # 代表者 → 役職 + 代表者名 (例: "代表執行役　三部　敏宏")
        rep_raw = kv.get("代表者", "")
        if rep_raw:
            parts = re.split(r"[\s　]+", rep_raw.strip(), maxsplit=1)
            if len(parts) == 2:
                item[Schema.POS_NM] = parts[0]
                item[Schema.REP_NM] = parts[1]
            else:
                item[Schema.REP_NM] = rep_raw.strip()

        # TEL
        tel = kv.get("本社電話番号", "")
        if tel and tel != "-":
            item[Schema.TEL] = tel

        # 従業員数 (単独) → EMP_NUM、(連結) → EXTRA
        emp = kv.get("従業員数(単独)", "")
        if emp:
            item[Schema.EMP_NUM] = re.sub(r"（.*?）", "", emp).strip()
        item["従業員数(連結)"] = re.sub(r"（.*?）", "", kv.get("従業員数(連結)", "")).strip()

        # 設立年月日
        est = kv.get("設立年月日", "")
        if est and est != "-":
            item[Schema.OPEN_DATE] = est

        # 資本金 (財務データ内、単独/連結いずれか非"-"の値)
        item[Schema.CAP] = self._capital(soup)

        # その他構造化フィールド (EXTRA)
        item["英文社名"] = kv.get("英文社名", "")
        item["平均年収"] = self._compact(kv.get("平均年収", "").split("業種平均")[0])
        item["平均年齢"] = self._compact(kv.get("平均年齢", ""))
        listed = kv.get("上場年月日", "")
        item["上場年月日"] = "" if listed.strip() == "-" else listed
        item["上場市場名"] = kv.get("上場市場名", "")
        item["証券コード"] = kv.get("証券コード", "")
        item["事業構成・セグメント"] = self._compact(kv.get("事業構成・セグメント", ""))

        return item

    @staticmethod
    def _compact(text: str) -> str:
        """数値と単位の間の空白を詰めて 1 行に整形する。"""
        return re.sub(r"\s+", " ", text).replace(" 万", "万").replace(" 歳", "歳").strip()

    def _company_data_kv(self, soup) -> dict:
        """「■会社データ」見出し直下の dl (dt/dd) を辞書化する。"""
        kv: dict = {}
        heading = soup.find(
            "h4", string=lambda s: bool(s) and "会社データ" in s
        )
        block = heading.find_next("div", class_="rn_companyInfo02") if heading else None
        if block is None:
            return kv
        for dl in block.find_all("dl", recursive=False):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            # dt 内のヘルプ「？」リンクを除去してラベルを取得
            for a in dt.find_all("a"):
                a.extract()
            label = re.sub(r"[\s　？]+", "", dt.get_text())
            value = dd.get_text(" ", strip=True)
            kv[label] = value
        return kv

    def _capital(self, soup) -> str:
        """財務データ内の資本金 (非"-") を返す。連結を優先。"""
        found = ""
        for dl in soup.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            if re.sub(r"[\s　？]+", "", dt.get_text()) == "資本金":
                v = dd.get_text(" ", strip=True)
                if v and v != "-":
                    found = v  # 後方 (連結) で上書き
        return found


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Bunnabi2Crawler()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://bunnabi.jp/2028/sh_result.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

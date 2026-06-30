"""
ブンナビ2027（文化放送就職ナビ）— 掲載企業の財務・会社 DATA

取得対象:
    - sh_result.php の全件検索結果(約8,556社 / 40件×214ページ)に掲載される企業
    - 各社の財務-DATA ページ (cn_data.php?ccd2=N) から会社概要・財務指標を取得

取得フロー:
    一覧 (sh_result.php?page=N, div.rn_company) で企業の財務-DATAリンク(ccd2)を取得し、
    1社ずつ詳細 (cn_data.php?ccd2=N) を開いて会社概要 + 単独/連結の最新期財務指標を抽出。
    1社取得するごとに即 yield する (Pattern B / 早期 yield)。

エンコーディング:
    Shift_JIS(CP932)。Content-Type に charset が無く、StaticCrawler.get_soup() が
    apparent_encoding(chardet=SHIFT_JIS と判定)で正しくデコードするためオーバーライド不要。

著作権配慮:
    「東洋経済・DATA特色」等の自由記述プロースは取得しない(構造化された数値・コード・
    ラベルのみを対象とする)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/bunnabi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bunnabi
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

_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _norm_label(text: str) -> str:
    """dt ラベルを正規化する(全空白除去 + ？/? 除去)。"""
    return re.sub(r"\s+", "", (text or "")).replace("？", "").replace("?", "")


def _clean_value(text: str) -> str:
    """dd 値を正規化する(連続空白を1つに、'-' は空文字に)。"""
    v = re.sub(r"\s+", "", text or "").strip()
    return "" if v in ("-", "－", "") else v


class Bunnabi(StaticCrawler):
    """ブンナビ2027（文化放送就職ナビ）スクレイパー"""

    DELAY = 1.5
    ITEMS_PER_PAGE = 40

    # サイト固有カラム(構造化された数値・コード・ラベルのみ。自由記述プロースは含めない)
    EXTRA_COLUMNS = [
        "英文社名",
        "従業員数_連結",
        "平均年齢",
        "平均年収",
        "上場年月日",
        "上場廃止年月日",
        "上場市場名",
        "証券コード",
        "単元株数",
        # 財務データ[単独] 最新期
        "決算年月_単独",
        "売上高_単独",
        "営業利益_単独",
        "経常利益_単独",
        "当期利益_単独",
        "一株当たり当期利益_単独",
        "発行済み株式数_単独",
        "総資産_単独",
        "自己資本_単独",
        "有利子負債_単独",
        "繰越損益_単独",
        "自己資本比率_単独",
        # 財務データ[連結] 最新期
        "決算年月_連結",
        "売上高_連結",
        "営業利益_連結",
        "経常利益_連結",
        "当期利益_連結",
        "一株当たり当期利益_連結",
        "総資産_連結",
        "自己資本_連結",
        "利益剰余金_連結",
        "含み損益_連結",
        "有利子負債_連結",
        "自己資本比率_連結",
        "ROA_連結",
        "ROE_連結",
        "総資産経常利益率_連結",
    ]

    # ─────────────────────────────── 一覧 ───────────────────────────────

    def parse(self, url: str):
        """url(sh_result.php)を唯一のルートとして全ページを巡回し、1社ずつ yield する。"""
        page = 1
        last_page = None

        while True:
            list_url = f"{url}?page={page}"
            list_soup = self.get_soup(list_url)
            if list_soup is None:
                break

            companies = list_soup.select("div.rn_company")
            if not companies:
                break

            if last_page is None:
                last_page = self._detect_last_page(list_soup)
                if last_page:
                    self.total_items = last_page * self.ITEMS_PER_PAGE

            for comp in companies:
                a = comp.select_one("a[href*='cn_data.php']")
                if not a:
                    continue
                detail_url = urljoin(url, a.get("href", ""))

                name_el = comp.select_one("span.name")
                cat_el = comp.select_one("span.category")
                base = {
                    Schema.NAME: name_el.get_text(strip=True) if name_el else "",
                    Schema.CAT_SITE: (
                        cat_el.get_text(strip=True).strip("［］[]") if cat_el else ""
                    ),
                }

                try:
                    item = self._scrape_detail(detail_url, base)
                except Exception as exc:  # noqa: BLE001 — 1社の失敗で全体を止めない
                    logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, exc)
                    continue
                if item:
                    yield item

            if last_page and page >= last_page:
                break
            page += 1

    @staticmethod
    def _detect_last_page(soup) -> int | None:
        """ページャ(javascript:page_change('N'))から最終ページ番号を求める。"""
        nums = []
        for a in soup.select("a[href*='page_change']"):
            m = re.search(r"page_change\(\s*'?(\d+)'?\s*\)", a.get("href", ""))
            if m:
                nums.append(int(m.group(1)))
        return max(nums) if nums else None

    # ─────────────────────────────── 詳細 ───────────────────────────────

    def _scrape_detail(self, detail_url: str, base: dict) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = dict(base)
        item[Schema.URL] = detail_url

        h2 = soup.find("h2")
        if h2 and h2.get_text(strip=True):
            item[Schema.NAME] = h2.get_text(strip=True)

        # --- 会社概要(columnType でない dl の dt/dd マップ) ---
        prof: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            if "columnType" in dl.get("class", []):
                continue
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not (dt and dd):
                continue
            key = _norm_label(dt.get_text(" ", strip=True))
            if key and key not in prof:
                prof[key] = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()

        # 住所 → 郵便番号 / 都道府県 / 住所
        addr_raw = prof.get("本社所在地", "")
        if addr_raw:
            pm = _POST_RE.search(addr_raw)
            if pm:
                item[Schema.POST_CODE] = pm.group(1)
                addr_raw = addr_raw[pm.end():]
            addr_raw = addr_raw.replace("　", " ").strip()
            prm = _PREF_RE.search(addr_raw)
            if prm:
                item[Schema.PREF] = prm.group(1)
            item[Schema.ADDR] = addr_raw

        item[Schema.TEL] = _clean_value(prof.get("本社電話番号", ""))
        item[Schema.REP_NM] = prof.get("代表者", "")
        item[Schema.OPEN_DATE] = prof.get("設立年月日", "")
        item[Schema.LOB] = _clean_value(prof.get("事業構成・セグメント", ""))

        # 従業員数(単独): 先頭の数値部分のみ
        emp_solo = prof.get("従業員数(単独)", "")
        em = re.search(r"[\d,]+", emp_solo)
        item[Schema.EMP_NUM] = em.group(0).replace(",", "") if em else ""

        item["英文社名"] = prof.get("英文社名", "")
        item["従業員数_連結"] = prof.get("従業員数(連結)", "")
        item["平均年齢"] = prof.get("平均年齢", "")
        item["平均年収"] = prof.get("平均年収", "")
        item["上場年月日"] = prof.get("上場年月日", "")
        item["上場廃止年月日"] = _clean_value(prof.get("上場廃止年月日", ""))
        item["上場市場名"] = _clean_value(prof.get("上場市場名", ""))
        item["証券コード"] = _clean_value(prof.get("証券コード", ""))
        item["単元株数"] = _clean_value(prof.get("単元株数", ""))

        # --- 財務データ(単独/連結)の最新期 ---
        solo, cons = self._parse_financials(soup)

        item["決算年月_単独"] = solo.get("決算年月", "")
        item["売上高_単独"] = solo.get("売上高", "")
        item["営業利益_単独"] = solo.get("営業利益", "")
        item["経常利益_単独"] = solo.get("経常利益", "")
        item["当期利益_単独"] = solo.get("当期利益", "")
        item["一株当たり当期利益_単独"] = solo.get("一株当たり当期利益", "")
        item["発行済み株式数_単独"] = solo.get("発行済み株式数", "")
        item["総資産_単独"] = solo.get("総資産", "")
        item["自己資本_単独"] = solo.get("自己資本", "")
        item["有利子負債_単独"] = solo.get("有利子負債", "")
        item["繰越損益_単独"] = solo.get("繰越損益", "")
        item["自己資本比率_単独"] = solo.get("自己資本比率", "")

        item["決算年月_連結"] = cons.get("決算年月", "")
        item["売上高_連結"] = cons.get("売上高", "")
        item["営業利益_連結"] = cons.get("営業利益", "")
        item["経常利益_連結"] = cons.get("経常利益", "")
        item["当期利益_連結"] = cons.get("当期利益", "")
        item["一株当たり当期利益_連結"] = cons.get("一株当たり当期利益", "")
        item["総資産_連結"] = cons.get("総資産", "")
        item["自己資本_連結"] = cons.get("自己資本", "")
        item["利益剰余金_連結"] = cons.get("利益剰余金", "")
        item["含み損益_連結"] = cons.get("含み損益", "")
        item["有利子負債_連結"] = cons.get("有利子負債", "")
        item["自己資本比率_連結"] = cons.get("自己資本比率", "")
        item["ROA_連結"] = cons.get("ROA", "")
        item["ROE_連結"] = cons.get("ROE", "")
        item["総資産経常利益率_連結"] = cons.get("総資産経常利益率", "")

        # 資本金(Schema): 連結 → 単独 の順で最新値を採用
        item[Schema.CAP] = cons.get("資本金", "") or solo.get("資本金", "")

        return item

    def _parse_financials(self, soup) -> tuple[dict, dict]:
        """単独・連結それぞれの「最新期」財務指標を {ラベル: 値} で返す。"""
        groups: list[dict] = []
        current: dict | None = None

        # columnType テーブル(決算年月 行で単独/連結が区切られる)
        for dl in soup.select("dl.columnType"):
            dt = dl.find("dt")
            label = _norm_label(dt.get_text(" ", strip=True)) if dt else ""
            dds = dl.find_all("dd")
            latest = _clean_value(dds[-1].get_text(" ", strip=True)) if dds else ""
            if label == "決算年月":
                current = {"決算年月": dds[-1].get_text(strip=True) if dds else ""}
                groups.append(current)
            elif current is not None and label:
                current[label] = latest

        # 売上高/営業利益/経常利益(見出し直後の div が最新期の値)
        headline: list[tuple[str, str]] = []
        for p in soup.select("p.rn_title"):
            t = _norm_label(p.get_text(" ", strip=True))
            if t in ("売上高", "営業利益", "経常利益"):
                sib = p.find_next_sibling()
                val = _clean_value(sib.get_text(" ", strip=True)) if sib else ""
                headline.append((t, val))

        solo = groups[0] if len(groups) >= 1 else {}
        cons = groups[1] if len(groups) >= 2 else {}

        # 見出し財務: 前半3つ=単独, 後半3つ=連結
        for idx, (lbl, val) in enumerate(headline):
            target = solo if idx < 3 else cons
            target.setdefault(lbl, val)

        return solo, cons


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Bunnabi()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://bunnabi.jp/2027/sh_result.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

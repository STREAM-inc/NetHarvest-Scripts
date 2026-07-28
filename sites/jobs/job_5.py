"""
マイナビ2028 (job.mynavi.jp) — 新卒2028向け掲載企業の会社概要データ取得

取得対象:
    - サイトマップに掲載される全企業 (約10,202社) の会社概要
    - 会社名 / 業種 / 都道府県 / 郵便番号 / 住所 / TEL / 代表者名 / 役職 /
      資本金 / 従業員数 / 設立(創立)年月 / 掲載URL (構造化フィールドのみ)

取得フロー:
    1. sitemap_2028_corp.xml から企業ページ (.../search/corpN/is.html) を列挙
    2. 各企業の会社概要ページ (.../search/corpN/outline.html) を 1 社ずつ取得して即 yield
       ※ outline.html に業種・所在地・TEL・代表者・資本金・従業員数・設立 が
         table.dataTable / .category として揃っているため 1 ページ取得で完結する

robots.txt 遵守:
    - User-agent:* に Crawl-delay: 120 の指定があるため DELAY=120 とする
      (フレームワークは各 yield の後に DELAY 秒スリープするため、企業間の
       リクエスト間隔は 120 秒以上になる)
    - User-agent:* の Disallow は「?付きURL」「/conts/kigyo/*」等で、
      本クローラが辿る /28/pc/search/corpN/ 配下は許可対象

備考 (著作権配慮):
    - 「事業内容」「沿革」「研修制度」等の自由記述プロースは取得しない
    - 会社概要として一般的な構造化フィールド (住所/TEL/代表者/資本金/業種 等) のみ取得
    - 企業HP (外部リンク) はマイナビ上に露出しないため空欄

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id job_5
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

# 企業ページ列挙用サイトマップ (url のドメインから派生させる)
_SITEMAP_PATH = "/sitemap_2028_corp.xml"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"(\d{3}-\d{4})")


class Mynavi2028Crawler(StaticCrawler):
    """マイナビ2028 (job.mynavi.jp) 会社概要スクレイパー"""

    # robots.txt User-agent:* の Crawl-delay: 120 を厳守する
    DELAY = 120.0

    EXTRA_COLUMNS = []  # 取得項目はすべて Schema でカバーできる

    def parse(self, url: str):
        # サイトマップ URL は引数 url のドメインから派生させる (SSOT = sites.yml の url)
        sitemap_url = urljoin(url, _SITEMAP_PATH)
        soup = self.get_soup(sitemap_url)
        if soup is None:
            logger.warning("サイトマップを取得できませんでした: %s", sitemap_url)
            return

        # <loc> に列挙された企業ページ (.../corpN/is.html) を収集
        corp_urls = [
            loc.get_text(strip=True)
            for loc in soup.find_all("loc")
            if "/is.html" in loc.get_text()
        ]
        self.total_items = len(corp_urls)
        logger.info("企業ページ %d 件を列挙しました", len(corp_urls))

        for corp_url in corp_urls:
            try:
                # 会社概要ページ (outline.html) に構造化情報が揃っている
                outline_url = corp_url.replace("/is.html", "/outline.html")
                item = self._scrape_outline(outline_url, corp_url)
                if item:
                    yield item
            except Exception as e:  # noqa: BLE001
                logger.warning("企業処理エラー (スキップ): %s — %s", corp_url, e)
                continue

    def _scrape_outline(self, outline_url: str, corp_url: str) -> dict | None:
        soup = self.get_soup(outline_url)
        if soup is None:
            return None

        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        item = {
            Schema.URL: corp_url,        # 掲載URL (自ページ = 企業トップ is.html)
            Schema.NAME: name,
            Schema.HP: "",               # マイナビ上に外部HPは露出しないため空欄
        }

        # 業種 (.category "業種 生命保険" 等 → 先頭ラベルを除去)
        cat = soup.select_one(".category")
        if cat:
            gyoshu = re.sub(r"^\s*業種\s*", "", cat.get_text(" ", strip=True)).strip()
            if gyoshu:
                item[Schema.CAT_SITE] = gyoshu

        # table.dataTable の th/td を辞書化 (本社* を優先)
        kv = self._data_table_kv(soup)

        # 郵便番号 (本社郵便番号)
        post = self._pick(kv, ["本社郵便番号", "郵便番号"])
        if post:
            m = _POST_PATTERN.search(post)
            if m:
                item[Schema.POST_CODE] = m.group(1)

        # 所在地 → 都道府県 / 住所
        addr_raw = self._pick(kv, ["本社所在地", "所在地"])
        if addr_raw:
            addr = addr_raw.replace("〒", "").replace("　", " ").strip()
            pm = _PREF_PATTERN.search(addr)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr[pm.start():].strip()
            else:
                item[Schema.ADDR] = addr

        # TEL (本社電話番号) — "0565-28-2121（大代表）" 等の注記を除去
        tel = self._pick(kv, ["本社電話番号", "電話番号"])
        if tel:
            tel = re.sub(r"[（(].*?[）)]", "", tel).strip()
            if tel and tel != "-":
                item[Schema.TEL] = tel

        # 設立 / 創立
        est = self._pick(kv, ["設立年月日", "設立年月", "設立", "創立"])
        if est and est != "-":
            item[Schema.OPEN_DATE] = est

        # 資本金
        cap = self._pick(kv, ["資本金"])
        if cap and cap != "-":
            item[Schema.CAP] = cap

        # 従業員数 / 従業員
        emp = self._pick(kv, ["従業員数", "従業員"])
        if emp and emp != "-":
            item[Schema.EMP_NUM] = emp

        # 代表者 → 役職 + 代表者名
        rep_raw = self._pick(kv, ["代表者", "代表取締役", "代表者名"])
        if rep_raw:
            pos, rep = self._split_rep(rep_raw)
            if rep:
                item[Schema.REP_NM] = rep
            if pos:
                item[Schema.POS_NM] = pos

        return item

    @staticmethod
    def _pick(kv: dict, labels: list[str]) -> str:
        """候補ラベルのうち最初に一致した値を返す (完全一致 → 部分一致の順)。"""
        for lbl in labels:
            if lbl in kv:
                return kv[lbl]
        for lbl in labels:
            for k, v in kv.items():
                if lbl in k:
                    return v
        return ""

    def _data_table_kv(self, soup) -> dict:
        """table.dataTable 内の th/td ペアを辞書化する。先に出た値を優先。"""
        kv: dict = {}
        for table in soup.select("table.dataTable"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = re.sub(r"[\s　]+", "", th.get_text())
                if label and label not in kv:
                    kv[label] = td.get_text(" ", strip=True)
        return kv

    @staticmethod
    def _split_rep(rep_raw: str) -> tuple[str, str]:
        """代表者文字列を (役職, 氏名) に分割する。

        例: "執行役員社長 兼 CEO 森田 隆之" → ("執行役員社長 兼 CEO", "森田 隆之")
        氏名は末尾の 1〜2 トークンと推定する。
        """
        parts = [p for p in re.split(r"[\s　]+", rep_raw.strip()) if p]
        if not parts:
            return "", ""
        if len(parts) == 1:
            return "", parts[0]
        # 末尾2トークンがともに短い漢字なら「姓 名」とみなす
        if len(parts) >= 3 and len(parts[-1]) <= 4 and len(parts[-2]) <= 4:
            return " ".join(parts[:-2]), parts[-2] + " " + parts[-1]
        return " ".join(parts[:-1]), parts[-1]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Mynavi2028Crawler()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://job.mynavi.jp/28/pc/toppage/displayTopPage/index")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

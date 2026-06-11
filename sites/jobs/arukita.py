"""
アルキタ (arukita.com) — 求人企業情報スクレイパー

取得対象:
    北海道の求人媒体「アルキタ」の全求人詳細から、企業名・電話番号・住所・
    会社データ(設立/従業員数/事業内容/HP)・職種・仕事内容・給与・福利厚生を収集する。

取得フロー:
    sitemap_job.xml に列挙された全求人詳細URL (/job_detail/{id}/) を起点に、
    各詳細ページを1件ずつ取得して即 yield する (一覧→詳細 Pattern B)。
    電話番号は「応募」欄の連絡先 (Tel:...) から、会社名・設立・従業員数・HP は
    「会社データ」欄から抽出する (備考の指示に準拠)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/arukita.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id arukita
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

# 求人詳細URL (…/job_detail/{id}/) を sitemap から抽出するためのパターン
_DETAIL_RE = re.compile(r"/job_detail/[0-9]+/?$")

# 連絡先欄の電話番号 (例: "Tel:011-581-3004＜担当 佐藤＞")。
# 全角→半角の正規化は Pipeline が行うため、ここでは数字/ハイフンを素のまま拾う。
_TEL_RE = re.compile(r"(?:Tel|TEL|ＴＥＬ|電話)[：:\s]*([0-9０-９\-－ー‐]{6,})")

# 会社データ欄の各ラベル
_EST_RE = re.compile(r"\[設立\]\s*(.+)")
_EMP_RE = re.compile(r"\[従業員数\]\s*(.+)")

# 都道府県抽出 (アルキタは北海道専門媒体のため、住所は札幌市等の市区から始まる
# ことが多い。明示的な都道府県表記があればそれを優先する)
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|"
    r"富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|"
    r"島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class ArukitaScraper(StaticCrawler):
    """アルキタ 求人企業情報スクレイパー (arukita.com)

    sitemap_job.xml の全求人詳細URLを巡回し、各詳細ページから企業情報を抽出する。
    """

    DELAY = 1.5
    # 会社データ由来の事業内容(LOB)・従業員数・設立日・HP は Schema へマッピング。
    # 以下は Schema に該当しないサイト固有の構造化フィールド。
    EXTRA_COLUMNS = ["職種", "仕事内容", "給与", "福利厚生"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとし、同一ホストの sitemap_job.xml を派生させる。
        sitemap_url = urljoin(url, "/sitemap_job.xml")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            self.logger.error("サイトマップ取得失敗: %s", sitemap_url)
            return

        detail_urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = _clean(loc.get_text())
            if _DETAIL_RE.search(href) and href not in seen:
                seen.add(href)
                detail_urls.append(href)

        self.total_items = len(detail_urls)
        self.logger.info("求人詳細URL: %d件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("詳細取得エラー (スキップ): %s — %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url, Schema.PREF: "北海道"}

        # 企業名 (勤務先名 + 法人名)
        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = _clean(h1.get_text(" "))

        # 職種 (h2: jdetLabelName_text が職種名)
        h2 = soup.select_one("h2.jdetLabelName, h2.jdetLabel_name")
        if h2:
            title_el = h2.select_one(".jdetLabelName_text")
            data["職種"] = _clean(title_el.get_text() if title_el else h2.get_text(" "))

        # 連絡先 (応募欄): TEL と住所
        for dl in soup.find_all("dl"):
            dt = dl.find("dt")
            if not (dt and "連絡先" in dt.get_text()):
                continue
            dd = dl.find("dd")
            if not dd:
                break
            txt = dd.get_text("\n", strip=True)
            m = _TEL_RE.search(txt)
            if m:
                data[Schema.TEL] = m.group(1).strip()
            # TEL/担当を含まない行を住所とみなす (最後の非TEL行)
            addr_lines = [
                ln.strip()
                for ln in txt.split("\n")
                if ln.strip() and not _TEL_RE.search(ln)
            ]
            if addr_lines:
                addr = _clean(addr_lines[-1])
                data[Schema.ADDR] = addr
                pm = _PREF_RE.match(addr)
                if pm:
                    data[Schema.PREF] = pm.group(1)
            break

        # 会社データ欄 (事業内容/設立/従業員数/HP)
        for unit in soup.select("div.jdetAbout_unit"):
            h4 = unit.find("h4")
            if not (h4 and "会社データ" in h4.get_text()):
                continue
            p = unit.find("p", class_="jdetAbout_text")
            if not p:
                break
            lines = [ln.strip() for ln in p.get_text("\n").split("\n") if ln.strip()]
            for idx, ln in enumerate(lines):
                est = _EST_RE.match(ln)
                emp = _EMP_RE.match(ln)
                if est:
                    data[Schema.OPEN_DATE] = est.group(1).strip()
                elif emp:
                    data[Schema.EMP_NUM] = emp.group(1).strip()
                elif idx == 0 and not ln.startswith("["):
                    # 先頭行は事業内容 (例: "食肉卸業") — 短い構造化ラベル
                    data[Schema.LOB] = ln
            a = p.find("a", href=True)
            if a:
                data[Schema.HP] = a["href"].strip()
            break

        # 募集要項 (「仕事内容」「給与」「福利厚生」項目)
        for dl in soup.select("dl.jdetRecruitment_item"):
            dt = dl.find("dt")
            if not dt:
                continue
            label = dt.get_text(strip=True)
            if label not in ("仕事内容", "給与", "福利厚生"):
                continue
            dd = dl.find("dd")
            if dd:
                data[label] = _clean(dd.get_text(" "))

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ArukitaScraper()
    # 🔒 sites.yml に登録する url と完全一致させる (SSOT = sites.yml)。
    scraper.execute("https://www.arukita.com/job_list")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

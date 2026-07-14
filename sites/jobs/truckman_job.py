"""
トラックマンJOB — トラック・ドライバー求人ポータル (truckman-job.com)

取得対象:
    - 全国求人検索結果に掲載された各求人の企業情報・勤務地・電話番号など

取得フロー:
    一覧ページ (?page=N) を巡回して求人詳細 (/kyujin/{id}) へのリンクを収集し、
    各詳細ページを 1 件ずつ取得して即 yield する (Pattern B)。
    電話番号は詳細ページ内の「電話応募」モーダル (.tel-Modal) に静的に埋め込まれている。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/truckman_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id truckman_job
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _first_line(td) -> str:
    """td の最初のテキスト行のみを返す (◆や※で始まる補足プロースを除外する)。"""
    if td is None:
        return ""
    for chunk in td.stripped_strings:
        t = _clean(chunk)
        if t:
            return t
    return ""


class TruckmanJobScraper(StaticCrawler):
    """トラックマンJOB スクレイパー (truckman-job.com)"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "お仕事No",
        "勤務先名",
        "職種",
        "雇用形態",
        "給与",
        "最寄り駅",
        "加入保険の種類",
        "就労期間・契約期間",
        "更新日",
    ]

    # 詳細テーブル (募集要項) のラベル → EXTRA カラム名
    _EXTRA_LABEL_MAP = {
        "お仕事No.": "お仕事No",
        "職種": "職種",
        "雇用形態": "雇用形態",
        "給与": "給与",
        "最寄り駅/交通": "最寄り駅",
        "加入保険の種類": "加入保険の種類",
        "就労期間/契約期間": "就労期間・契約期間",
    }

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        seen: set[str] = set()
        while True:
            page_url = f"{url}?page={page}"
            soup = self.get_soup(page_url)

            detail_paths = []
            for a in soup.select(".mod-jobResultBox a[href*='/kyujin/']"):
                href = a.get("href")
                if not href:
                    continue
                path = urlparse(href).path
                if re.search(r"/kyujin/\d+", path) and path not in seen:
                    seen.add(path)
                    detail_paths.append(href)

            if not detail_paths:
                break

            for href in detail_paths:
                detail_url = urljoin(url, href)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, exc)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            page += 1

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)

        # 募集要項テーブル (th/td) をラベル辞書化
        rows: dict[str, "object"] = {}
        for tr in soup.select("table.mod-table1 tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                rows[_clean(th.get_text())] = td

        item = {Schema.URL: url}

        # 名称 = 掲載企業名 (企業情報テーブル)。無ければ勤務先名で補完
        company = _clean(rows["掲載企業名"].get_text()) if "掲載企業名" in rows else ""
        workplace = _clean(rows["勤務先名"].get_text()) if "勤務先名" in rows else ""
        item[Schema.NAME] = company or workplace

        # 勤務地 → 都道府県 + 住所
        work_area = _clean(rows["勤務地・就業場所"].get_text()) if "勤務地・就業場所" in rows else ""
        detail_addr = _first_line(rows.get("勤務地・就業場所(市区町村以下)"))
        m = _PREF_PATTERN.match(work_area)
        if m:
            item[Schema.PREF] = m.group(1)
            rest = work_area[m.end():].strip()
            item[Schema.ADDR] = _clean(f"{rest}{detail_addr}")
        else:
            item[Schema.PREF] = ""
            item[Schema.ADDR] = _clean(f"{work_area}{detail_addr}")

        # 電話番号 = 電話応募モーダル (.tel-Modal) の tel: リンク
        tel_a = soup.select_one(".tel-Modal a[href^='tel:']")
        if tel_a:
            item[Schema.TEL] = tel_a.get("href", "").replace("tel:", "").strip()
        else:
            item[Schema.TEL] = ""

        # EXTRA (構造化された短いラベルのみ。自由記述プロースは除外)
        item["勤務先名"] = workplace
        for label, col in self._EXTRA_LABEL_MAP.items():
            item[col] = _clean(rows[label].get_text()) if label in rows else ""

        # 更新日 (日付部分のみ)
        if "更新日" in rows:
            upd = _clean(rows["更新日"].get_text())
            dm = re.search(r"\d{4}/\d{1,2}/\d{1,2}", upd)
            item["更新日"] = dm.group(0) if dm else upd
        else:
            item["更新日"] = ""

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TruckmanJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://truckman-job.com/zenkoku/search-result")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
東大阪市技術交流プラザ — モノづくり企業ディレクトリ スクレイパー

取得対象:
    - 東大阪市技術交流プラザに掲載されている全企業の基本情報
      (企業名/所在地/TEL/FAX/代表者/担当窓口/主要業務/従業員数/資本金/創業・創立年月/HP)

取得フロー:
    一覧 → 詳細 (Pattern B)
    1. 検索結果 /search/results を currentPage で全ページ巡回し、
       各企業の詳細ページ URL (/search/company/{id}) を収集
    2. 詳細ページを 1 件取得するごとに即 yield
       (途中で中断しても無駄な通信が発生しない)

ページネーション:
    GET /search/results?search=&query=&currentPage=N&displayCount=20
    (Laravel 製。GET パラメータでページ送り可能。CSRF トークン不要)
    総 1233 件 / 20 件表示 → 約 62 ページ

取得しないフィールド (除外):
    - アピールポイント (技術力 / 事業実績): 企業が記述した長文の自由記述プロース。
      著作権リスクのため除外。
    - 製品 / 保有設備 / 取扱素材テーブル: 企業ごとに列構成・行数が異なる可変構造の
      技術情報テーブルで、固定カラムにマッピングできないため除外。

実行方法:
    # ローカルテスト
    python scripts/sites/government/techplaza.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id techplaza
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

BASE = "https://www.techplaza.city.higashiosaka.osaka.jp"
RESULTS_URL = BASE + "/search/results"
DISPLAY_COUNT = 20  # サイト既定値。read timeout 回避のため小さい値で複数回ページング

# 都道府県の先頭一致抽出
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 末尾の (カナ) を分離する
_KANA_PATTERN = re.compile(r"[（(]([^（）()]+)[）)]\s*$")


def _split_kana(text: str) -> tuple[str, str]:
    """末尾の「(かな)」を本体とカナに分離する。"""
    text = (text or "").strip()
    m = _KANA_PATTERN.search(text)
    if m:
        return text[: m.start()].strip(), m.group(1).strip()
    return text, ""


class TechplazaScraper(StaticCrawler):
    """東大阪市技術交流プラザ 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX", "担当者窓口部署", "担当者名", "創業年月"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        # 総件数を初回ページから取得して進捗表示を有効化
        first = self.get_soup(
            f"{RESULTS_URL}?search=&query=&currentPage=1&displayCount={DISPLAY_COUNT}"
        )
        if first is not None:
            m = re.search(r"([\d,]+)\s*件中", first.get_text(" ", strip=True))
            if m:
                self.total_items = int(m.group(1).replace(",", ""))

        page = 1
        while True:
            page_url = (
                f"{RESULTS_URL}?search=&query=&currentPage={page}"
                f"&displayCount={DISPLAY_COUNT}"
            )
            soup = first if (page == 1 and first is not None) else self.get_soup(page_url)
            first = None
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", page_url)
                break

            detail_urls = []
            for a in soup.select('a[href*="/search/company/"]'):
                href = urljoin(BASE, a["href"])
                cid_match = re.search(r"/search/company/(\d+)", href)
                if not cid_match:
                    continue
                cid = cid_match.group(1)
                if cid in seen:
                    continue
                seen.add(cid)
                detail_urls.append(href.split("?")[0])

            if not detail_urls:
                # 企業リンクが無くなったら終端
                break

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページ解析失敗 (スキップ): %s — %s", detail_url, e)
                    continue

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 「企業名」行を含むテーブルが安定した企業基本情報テーブル。
        # 先行する製品/設備/素材テーブルは可変構造のため対象外。
        info: dict[str, str] = {}
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            labels = [
                r.find(["th", "td"]).get_text(strip=True)
                for r in rows
                if r.find(["th", "td"])
            ]
            if "企業名" not in labels:
                continue
            for tr in rows:
                cells = tr.find_all(["th", "td"])
                if len(cells) >= 2:
                    key = cells[0].get_text(" ", strip=True)
                    val = cells[1].get_text(" ", strip=True)
                    info[key] = val
            break

        if not info.get("企業名"):
            self.logger.warning("企業名が取得できませんでした: %s", url)
            return None

        name, name_kana = _split_kana(info.get("企業名", ""))
        rep_name, rep_kana = _split_kana(info.get("代表者名", ""))
        contact_name, _ = _split_kana(info.get("担当者名", ""))

        addr = info.get("所在地", "")
        pref = ""
        m = _PREF_PATTERN.match(addr)
        if m:
            pref = m.group(1)
        elif addr.startswith("東大阪市"):
            # 所在地に都道府県が無い掲載が多い。市名から大阪府を補完。
            pref = "大阪府"

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: info.get("電話番号（問合先）", ""),
            Schema.REP_NM: rep_name,
            Schema.LOB: info.get("主要業務", ""),
            Schema.EMP_NUM: info.get("従業員数", ""),
            Schema.CAP: info.get("資本金", ""),
            Schema.OPEN_DATE: info.get("創立年月", ""),
            Schema.HP: info.get("ホームページアドレス", ""),
            # EXTRA_COLUMNS
            "FAX": info.get("ファックス番号（問合先）", ""),
            "担当者窓口部署": info.get("担当者窓口部署", ""),
            "担当者名": contact_name,
            "創業年月": info.get("創業年月", ""),
        }
        # 代表者名のカナは Schema に該当列が無いため NAME_KANA とは別に保持しない
        # (REP_NM 本体のみ採用)。rep_kana は破棄。
        _ = rep_kana
        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TechplazaScraper()
    scraper.execute(BASE + "/search/results")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

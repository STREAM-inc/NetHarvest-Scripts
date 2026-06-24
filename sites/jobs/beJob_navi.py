"""
ビージョブナビ — 岩手・青森・秋田の求人情報スクレイパー (bejob-navi.jp)

取得対象:
    - 求人詳細ページの採用情報テーブル (#detail1)
    - 求人詳細ページの会社概要テーブル (2つ目の table.detail)

取得フロー:
    /job_search.php?page=N を page=1 から最終ページまで巡回
    → 各 .list_s_test から詳細URLを抽出
    → 詳細ページから採用情報・会社概要を取得

実行方法:
    python scripts/sites/jobs/bejob_navi.py
    python bin/run_flow.py --site-id bejob_navi
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

BASE_URL = "https://www.bejob-navi.jp"
LIST_URL = f"{BASE_URL}/job_search.php"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_RE = re.compile(r"〒?\s*(\d{3}[-\-ー]?\d{4})")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class BejobNaviScraper(StaticCrawler):
    """ビージョブナビ 求人情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "採用主",
        "注目ポイント",
        "仕事内容",
        "応募資格",
        "給与詳細",
        "勤務時間",
        "休日・休暇",
        "待遇",
        "勤務地",
        "応募方法",
        "応募先",
        "応募先住所",
        "応募先電話番号",
        "採用担当者",
        "詳細情報",
        "備考",
    ]

    # #detail1 の <th> ラベル → EXTRA_COLUMNS のカラム名
    _DETAIL1_LABEL_MAP = {
        "注目ポイント": "注目ポイント",
        "仕事内容": "仕事内容",
        "応募資格": "応募資格",
        "給与詳細": "給与詳細",
        "勤務時間": "勤務時間",
        "休日・休暇": "休日・休暇",
        "待遇": "待遇",
        "勤務地": "勤務地",
        "応募方法": "応募方法",
        "応募先": "応募先",
        "応募先住所": "応募先住所",
        "応募先電話番号": "応募先電話番号",
        "採用担当者": "採用担当者",
        "詳細情報": "詳細情報",
        "備考": "備考",
    }

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        while True:
            list_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            items = soup.select(".list_s_test")
            if not items:
                self.logger.info("page=%d: アイテム無し。終了します。", page)
                break

            if page == 1:
                # 総件数表示: 「検索結果 174 件（1-20件表示）」
                text = soup.get_text(" ", strip=True)
                m = re.search(r"検索結果\s*([\d,]+)\s*件", text)
                if m:
                    total = int(m.group(1).replace(",", ""))
                    self.total_items = total
                    self.logger.info("総件数: %d 件", total)

            detail_urls: list[str] = []
            for item in items:
                a = item.select_one(".list_title a[href]") or item.select_one(
                    "a[href*='s05_info']"
                )
                if not a:
                    continue
                href = a.get("href", "").strip()
                detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                detail_urls.append(detail_url)

            for detail_url in detail_urls:
                try:
                    rec = self._scrape_detail(detail_url)
                    if rec:
                        yield rec
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue

            # 次ページのリンク有無で終了判定
            has_next = bool(
                soup.select_one(f'a[href*="page={page + 1}"]')
            )
            if not has_next:
                self.logger.info("page=%d が最終ページです。", page)
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 求人タイトル
        title_el = soup.select_one(".list_title h3")
        if title_el:
            data["求人タイトル"] = _clean(title_el.get_text())

        # 雇用形態 / 採用主
        employ = soup.select_one(".employ_type")
        if employ:
            span = employ.select_one("span")
            if span:
                data["雇用形態"] = _clean(span.get_text())
            p = employ.select_one("p")
            if p:
                data["採用主"] = _clean(p.get_text())

        # #detail1: 採用情報
        detail1 = soup.select_one("#detail1")
        if detail1:
            for tr in detail1.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                value = _clean(td.get_text(" "))

                if label == "職種":
                    data[Schema.CAT_SITE] = value
                elif label == "雇用形態" and not data.get("雇用形態"):
                    data["雇用形態"] = value
                elif label in self._DETAIL1_LABEL_MAP:
                    data[self._DETAIL1_LABEL_MAP[label]] = value

        # 会社概要テーブル (2つ目の table.detail)
        detail_tables = soup.select("table.detail")
        company_table = None
        for t in detail_tables:
            if t.get("id") == "detail1":
                continue
            # 「会社名」行を含むテーブルを採用
            ths = [_clean(th.get_text()) for th in t.select("th")]
            if "会社名" in ths or "所在地" in ths:
                company_table = t
                break

        if company_table:
            for tr in company_table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                if label == "会社名":
                    data[Schema.NAME] = _clean(td.get_text())
                elif label == "所在地":
                    addr_raw = _clean(td.get_text(" "))
                    self._set_address(data, addr_raw)
                elif label == "業務内容":
                    data[Schema.LOB] = _clean(td.get_text(" "))
                elif label == "電話番号":
                    data[Schema.TEL] = _clean(td.get_text())
                elif label == "ホームページ":
                    a = td.select_one("a[href]")
                    if a:
                        data[Schema.HP] = a["href"].strip()
                    else:
                        data[Schema.HP] = _clean(td.get_text())

        # 会社名フォールバック: 採用主 → タイトル末尾の「／○○」
        if not data.get(Schema.NAME):
            if data.get("採用主"):
                data[Schema.NAME] = data["採用主"]
            elif data.get("求人タイトル"):
                m = re.search(r"／(.+?)$", data["求人タイトル"])
                if m:
                    data[Schema.NAME] = _clean(m.group(1))

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _set_address(data: dict, addr_raw: str) -> None:
        """〒xxx-xxxx 岩手県...  を分解して Schema.POST_CODE/PREF/ADDR にセット。"""
        text = addr_raw
        m = _POST_RE.search(text)
        if m:
            data[Schema.POST_CODE] = m.group(1).replace("ー", "-")
            text = _POST_RE.sub("", text).strip()

        m = _PREF_RE.match(text)
        if m:
            data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = text[m.end():].strip()
        else:
            # 都道府県が明記されていない (市名のみ等): そのまま ADDR へ
            data[Schema.ADDR] = text


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BejobNaviScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

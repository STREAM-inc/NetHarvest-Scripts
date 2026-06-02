# 神奈川夜遊びNight用 https://www.kanagawa-yoasobi.com/
"""
神奈川夜遊びNight (kanagawa-yoasobi.com) — ナイトビジネス店舗 + 求人情報スクレイパー

取得対象:
    - 店舗情報: 名称 / 都道府県 / 住所 / 郵便番号 / TEL / 営業時間 / 定休日 / 公式HP / サイト定義業種(ジャンル)
    - サイト内タグ: エリアタグ (例: 川崎／堀之内／南町)
    - 求人情報 (備考の指示): 職種 / 給与 / 勤務時間 / 応募資格 / 連絡先 / 待遇
    - その他構造化情報: 席数 / ブログURL / クーポン名 / クーポン料金

    ※ 長文の自由記述 (店舗キャッチ・お店から・仕事内容・料金詳細・クーポン詳細) は
       著作権リスク回避のため取得しない。

取得フロー:
    1. 一覧ページ (data.php?c=search&page=N) を 1 ページ目から巡回 (1ページ10件, 全6ページ ≈ 60件)
    2. 各行の店舗名と詳細リンク (data.php?c=info&item=ID) を取得
    3. 詳細ページの th/td テーブル群 + .cate (エリア/ジャンル) から全フィールドを抽出し即 yield
       (1件取得ごとに yield する Pattern B — 途中中断しても無駄な通信が起きない)

実行方法:
    python scripts/sites/nightlife/night.py            # 全件
    python scripts/sites/nightlife/night.py --sample   # 先頭ページのみ
    python bin/run_flow.py --site-id night
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"\d{3}-?\d{4}")
_NAME_KANA_RE = re.compile(r"^(.+?)[(（]([^)）]+)[)）]\s*$")
_ITEM_ID_RE = re.compile(r"item=([^&]+)")


class KanagawaYoasobiNightScraper(StaticCrawler):
    """神奈川夜遊びNight スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = [
        "エリアタグ",
        "席数",
        "ブログ",
        "クーポン",
        "クーポン料金",
        "職種",
        "給与",
        "勤務時間",
        "応募資格",
        "連絡先",
        "待遇",
    ]

    BASE_URL = "https://www.kanagawa-yoasobi.com/"
    LIST_PATH = "data.php?c=search&page={page}"
    MAX_PAGE_GUARD = 50  # 無限ループ防止

    def parse(self, url: str) -> Generator[dict, None, None]:
        sample = getattr(self, "_sample_mode", False)
        self.total_items = self._estimate_total()

        seen: set[str] = set()
        saved = 0
        failed = 0
        page = 1

        while page <= self.MAX_PAGE_GUARD:
            list_url = urljoin(self.BASE_URL, self.LIST_PATH.format(page=page))
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            rows = self._extract_list_rows(soup)
            if not rows:
                self.logger.info("一覧終端に到達: page=%d (件数0)", page)
                break

            self.logger.info("一覧ページ取得: page=%d 件数=%d", page, len(rows))

            for item_id, name in rows:
                if item_id in seen:
                    continue
                seen.add(item_id)
                detail_url = urljoin(self.BASE_URL, f"data.php?c=info&item={item_id}")
                try:
                    record = self._scrape_detail(detail_url, name)
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                    continue
                if not record:
                    failed += 1
                    self.logger.warning("詳細取得スキップ(必須欠落): %s", detail_url)
                    continue
                saved += 1
                self.logger.info(
                    "詳細取得OK: %d件目 店舗=%s", saved, record.get(Schema.NAME) or detail_url
                )
                yield record

            if sample:
                self.logger.info("サンプルモード: 先頭ページのみで終了")
                break
            page += 1

        self.logger.info("取得完了: 取得%d件 失敗/スキップ%d件", saved, failed)

    # ------------------------------------------------------------------ list

    def _estimate_total(self) -> int:
        """page_navi の最大ページ番号 × 10 で総件数を概算 (進捗表示用)。"""
        first_url = urljoin(self.BASE_URL, self.LIST_PATH.format(page=1))
        soup = self.get_soup(first_url)
        if soup is None:
            return 0
        per_page = len(self._extract_list_rows(soup))
        max_page = 1
        for a in soup.select(".page_navi a"):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        return per_page * max_page

    def _extract_list_rows(self, soup: BeautifulSoup) -> list[tuple[str, str]]:
        """一覧テーブルから (item_id, 店舗名) のリストを返す。"""
        rows: list[tuple[str, str]] = []
        seen: set[str] = set()
        for title in soup.select(".list table td.title"):
            a = title.select_one("a[href*='c=info']")
            if not a:
                continue
            m = _ITEM_ID_RE.search(a.get("href", ""))
            if not m:
                continue
            item_id = m.group(1)
            if item_id in seen:
                continue
            seen.add(item_id)
            rows.append((item_id, self._clean(a.get_text(strip=True))))
        return rows

    # ---------------------------------------------------------------- detail

    def _scrape_detail(self, url: str, list_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        labels = self._extract_table_labels(soup)
        area_tag, genre = self._extract_cate(soup)

        name = list_name or self._extract_name(soup)
        if not name:
            self.logger.warning("店舗名が空です: %s", url)
            return None

        name, kana = self._split_name_kana(name)
        pref, addr = self._split_pref(labels.get("住所", ""))

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr or labels.get("住所", ""),
            Schema.POST_CODE: self._extract_postcode(labels.get("郵便番号", "")),
            Schema.TEL: labels.get("TEL", ""),
            Schema.TIME: labels.get("営業時間", ""),
            Schema.HOLIDAY: labels.get("定休日", ""),
            Schema.HP: self._extract_url(soup, "公式HP"),
            Schema.CAT_SITE: genre,
            # --- EXTRA ---
            "エリアタグ": area_tag,
            "席数": labels.get("席", ""),
            "ブログ": self._extract_url(soup, "Blog"),
            "クーポン": labels.get("クーポン", ""),
            "クーポン料金": labels.get("クーポン料金", ""),
            "職種": labels.get("職種", ""),
            "給与": labels.get("給与", ""),
            "勤務時間": labels.get("勤務時間", ""),
            "応募資格": labels.get("応募資格", ""),
            "連絡先": labels.get("連絡先", ""),
            "待遇": labels.get("待遇", ""),
        }
        return record

    def _extract_table_labels(self, soup: BeautifulSoup) -> dict[str, str]:
        """店舗情報テーブル + .detail (求人/料金) テーブル群の th/td を辞書化。"""
        data: dict[str, str] = {}
        for table in soup.find_all("table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or td is None:
                    continue
                key = self._clean(th.get_text(" ", strip=True))
                if not key or key in data:
                    continue
                data[key] = self._clean(td.get_text(" ", strip=True))
        return data

    def _extract_cate(self, soup: BeautifulSoup) -> tuple[str, str]:
        """.cate から (エリアタグ, ジャンル) を返す。"""
        cate = soup.select_one(".cate")
        if not cate:
            return "", ""
        spans = [self._clean(s.get_text(strip=True)) for s in cate.find_all("span")]
        spans = [s for s in spans if s]
        area = spans[0] if len(spans) >= 1 else ""
        genre = spans[1] if len(spans) >= 2 else ""
        return area, genre

    def _extract_url(self, soup: BeautifulSoup, label: str) -> str:
        """指定ラベル行の td から http(s) アンカー URL を取得。"""
        for table in soup.find_all("table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or td is None:
                    continue
                if self._clean(th.get_text(strip=True)) != label:
                    continue
                for a in td.find_all("a", href=True):
                    href = a["href"].strip()
                    if href.startswith("http"):
                        return href
                text = self._clean(td.get_text(strip=True))
                return text if text.startswith("http") else ""
        return ""

    def _extract_name(self, soup: BeautifulSoup) -> str:
        h2 = soup.find("h2")
        if not h2:
            return ""
        text = self._clean(h2.get_text(strip=True))
        # "店舗名｜キャッチコピー" 形式 → キャッチは取得しない
        return text.split("｜")[0].strip()

    # ----------------------------------------------------------------- utils

    def _split_name_kana(self, raw: str) -> tuple[str, str]:
        text = self._clean(raw)
        if not text:
            return "", ""
        m = _NAME_KANA_RE.match(text)
        if m:
            return self._clean(m.group(1)), self._clean(m.group(2))
        return text, ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        address = self._clean(address)
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _extract_postcode(self, raw: str) -> str:
        m = _POST_RE.search(self._clean(raw))
        return m.group(0) if m else ""

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KanagawaYoasobiNightScraper()
    scraper._sample_mode = "--sample" in sys.argv
    scraper.execute("https://www.kanagawa-yoasobi.com/data.php?c=search&page=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
キャバサイト (cabasite.com) — 静岡・沼津エリアのキャバクラ求人スクレイパー【全カラム版】

cabasite_2 は既存 cabasite と同じ求人一覧 (search.php?type=job) を起点にしつつ、
詳細ページに存在する取得可能フィールドを「全部取る」方針で網羅する版。
cabasite との差分は EXTRA_COLUMNS に 体験時給 / 応募資格 / 勤務時間 / 指名料 を追加した点。

取得対象:
    - 店名 / カナ / 都道府県 / 住所 / TEL / 営業時間 / 定休日 / HP / 支払い方法(カード)
    - ジャンル(CAT_SITE) / エリア / 交通
    - 求人系: 給与 / 体験時給 / 応募資格 / 勤務時間 / 待遇
    - 料金システム: 時間制料金 / 飲み放題 / 延長 / お1人様の場合 /
      本指名料 / 場内指名料 / 指名料 / 同伴料 / キャストドリンク / TAX・サービス料

取得フロー:
    1. 求人一覧 search.php?run=true&type=job をページ送り (?q=0&page=N, page は 0 始まり)
    2. 一覧の各 a.disp_bl から 店名/カナ/エリア+ジャンル と詳細 URL を取得
    3. 詳細ページ info.php?type=job&id=C... の dl / table.info-table から残りの項目を取得
    4. 1 件取得するごとに即 yield (Pattern B / 早期 yield)

ページネーション:
    - 1 ページ 20 件、page=0 始まり (≈160 件)
    - 範囲外ページは固定フォールバック 1 件を返すため、
      「新規 ID が増えない」または「非満杯ページ かつ 新規 1 件以下」で停止する

実行方法:
    python scripts/sites/nightlife/cabasite_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id cabasite_2
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
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_JOB_ID_RE = re.compile(r"info\.php\?type=job&id=(C\d+)", re.IGNORECASE)

# 一覧の検索パス (parse() に渡される url を起点に urljoin で組み立てる)
_LIST_PATH = "search.php?run=true&type=job"
_PAGE_SIZE = 20


class Cabasite2Scraper(StaticCrawler):
    """キャバサイト (cabasite.com) 求人スクレイパー【全カラム版】"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア", "交通",
        # 求人系
        "給与", "体験時給", "応募資格", "勤務時間", "待遇",
        # 料金システム (詳細ページの dl / info-table から label→value で取得)
        "時間制料金", "飲み放題", "延長", "お1人様の場合",
        "本指名料", "場内指名料", "指名料", "同伴料", "キャストドリンク", "TAX・サービス料",
    ]

    # EXTRA_COLUMNS のうち、詳細ページの table.info-table / dl から
    # label→value で拾うラベル群 (_norm_label 正規化キーで照合)。
    # 表形式・定義リスト形式が混在しうるため両方から拾う。
    _DETAIL_LABELS = [
        "給与", "体験時給", "応募資格", "勤務時間", "待遇",
        "時間制料金", "飲み放題", "延長", "お1人様の場合",
        "本指名料", "場内指名料", "指名料", "同伴料", "キャストドリンク", "TAX・サービス料",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 進捗表示用の概算 (page=0 始まり × 20 件)
        self.total_items = 160

        list_base = urljoin(url, _LIST_PATH)
        seen_ids: set[str] = set()
        page = 0
        saved = 0

        while True:
            page_url = f"{list_base}&q=0&page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            listings = self._extract_listings(soup, url)
            page_ids = [lst["id"] for lst in listings]
            new_listings = [lst for lst in listings if lst["id"] not in seen_ids]

            # 全件既出 → 末尾到達
            if not new_listings:
                self.logger.info("page=%d: 新規なし。巡回終了。", page)
                break

            # フォールバックページ検出: 非満杯 かつ 新規 1 件以下 は範囲外とみなして停止
            if len(page_ids) < _PAGE_SIZE and len(new_listings) <= 1:
                self.logger.info(
                    "page=%d: 非満杯(%d件)・新規%d件 → 範囲外フォールバックとみなし終了。",
                    page, len(page_ids), len(new_listings),
                )
                break

            for lst in new_listings:
                seen_ids.add(lst["id"])
                try:
                    record = self._scrape_detail(lst)
                except Exception as e:  # 個別エラーは握りつぶして継続
                    self.logger.warning("詳細取得失敗: id=%s url=%s (%s)", lst["id"], lst["url"], e)
                    continue
                if record:
                    saved += 1
                    self.logger.info(
                        "取得OK: %d件目 page=%d 店舗=%s",
                        saved, page, record.get(Schema.NAME) or lst["url"],
                    )
                    yield record

            # 非満杯の通常ページ (実在する部分ページ) は yield 後に終了
            if len(page_ids) < _PAGE_SIZE:
                self.logger.info("page=%d: 非満杯(%d件)。巡回終了。", page, len(page_ids))
                break

            page += 1

        self.logger.info("巡回完了: 取得 %d 件", saved)

    def _extract_listings(self, soup: BeautifulSoup, root_url: str) -> list[dict]:
        """一覧ページの各求人ブロックから 店名/カナ/エリア+ジャンル/詳細URL を取得。

        各ブロック内の詳細ページへの a タグ href を抽出し、urljoin で
        引数由来の root_url (= 正規ルート URL) と安全に結合して絶対パス化する。
        詳細リンク (a タグ) 自体が取れないブロックは警告して continue でスキップ。
        """
        listings: list[dict] = []
        for a in soup.select("a.disp_bl"):
            href = (a.get("href") or "").strip()
            if not href:
                self.logger.warning("詳細リンク(href)を取得できないブロックをスキップ")
                continue

            m = _JOB_ID_RE.search(href)
            if not m:
                # type=job 以外 (blog 等) のリンクは対象外
                continue

            # 相対パスでも urljoin で root_url と安全に結合し絶対 URL 化
            detail_url = urljoin(root_url, href)

            name = self._text(a.select_one(".shop_name"))
            kana = self._text(a.select_one(".sub_shop_name"))
            area_job = self._text(a.select_one(".area_job"))
            area, genre = self._split_area_genre(area_job)
            listings.append(
                {
                    "id": m.group(1),
                    "url": detail_url,
                    "name": name,
                    "kana": kana,
                    "area": area,
                    "genre": genre,
                }
            )
        return listings

    def _scrape_detail(self, listing: dict) -> dict | None:
        soup = self.get_soup(listing["url"])
        if soup is None:
            return None

        dl = self._extract_dl(soup)
        tbl = self._extract_tables(soup)

        name = listing["name"] or self._name_from_title(soup)
        if not name:
            self.logger.warning("店名が空: %s", listing["url"])
            return None

        address = dl.get("住所", "")
        pref, addr_body = self._split_pref(address)
        tel = self._extract_tel(dl.get("TEL", ""))

        record = {
            Schema.URL: listing["url"],
            Schema.NAME: name,
            Schema.NAME_KANA: listing["kana"],
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: dl.get("営業時間", ""),
            Schema.HOLIDAY: dl.get("定休日", ""),
            Schema.HP: self._extract_hp(soup, dl),
            Schema.PAYMENTS: tbl.get("ご利用可能カード", ""),
            Schema.CAT_SITE: listing["genre"],
            "エリア": listing["area"],
            "交通": dl.get("交通", ""),
        }

        # 求人系・料金システム系を info-table 優先・dl フォールバックで補完。
        for label in self._DETAIL_LABELS:
            record[label] = tbl.get(label) or dl.get(label, "")

        return record

    def _extract_dl(self, soup: BeautifulSoup) -> dict[str, str]:
        """ページ内の全 dl の dt→dd を辞書化 (住所/交通/TEL/営業時間/定休日/URL 等)。"""
        data: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = self._norm_label(dt.get_text(strip=True))
                if label and label not in data:
                    data[label] = self._text(dd)
        return data

    def _extract_tables(self, soup: BeautifulSoup) -> dict[str, str]:
        """info-table 系テーブルの th→td を辞書化 (給与/待遇/ご利用可能カード/料金 等)。"""
        data: dict[str, str] = {}
        for table in soup.select("table.info-table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not (th and td):
                    continue
                label = self._norm_label(th.get_text(strip=True))
                if label and label not in data:
                    data[label] = self._text(td)
        return data

    def _extract_hp(self, soup: BeautifulSoup, dl: dict[str, str]) -> str:
        """dl の URL 値、無ければ dd 内の a[href] からホームページ URL を拾う。"""
        hp = dl.get("URL") or dl.get("URL（スマホ）") or ""
        if hp.startswith("http"):
            return hp
        for d in soup.find_all("dl"):
            dts = d.find_all("dt")
            dds = d.find_all("dd")
            for dt, dd in zip(dts, dds):
                if "URL" in dt.get_text(strip=True):
                    a = dd.find("a", href=True)
                    if a:
                        return a["href"].strip()
        return hp

    def _extract_tel(self, raw: str) -> str:
        m = _TEL_PATTERN.search(raw or "")
        return m.group(0) if m else self._clean(raw)

    def _name_from_title(self, soup: BeautifulSoup) -> str:
        """フォールバック: <title> 中央セグメント (…のキャバクラ｜店名｜静岡…) から店名を取得。"""
        title = soup.find("title")
        if not title:
            return ""
        parts = [p.strip() for p in re.split(r"[｜|]", title.get_text(strip=True)) if p.strip()]
        return parts[1] if len(parts) >= 3 else ""

    def _split_area_genre(self, area_job: str) -> tuple[str, str]:
        """`熱海 キャバクラ` → ("熱海", "キャバクラ")。区切りが無ければ area を空に。"""
        text = self._clean(area_job)
        if not text:
            return "", ""
        tokens = text.split(" ", 1)
        if len(tokens) == 2:
            return tokens[0].strip(), tokens[1].strip()
        return "", text

    def _split_pref(self, address: str) -> tuple[str, str]:
        address = self._clean(address)
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _norm_label(self, text: str) -> str:
        return re.sub(r"\s+", "", (text or "").replace("　", "").replace("\xa0", ""))

    def _text(self, node) -> str:
        if node is None:
            return ""
        return self._clean(node.get_text(" ", strip=True))

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

    scraper = Cabasite2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えると挙動がズレる。
    scraper.execute("https://cabasite.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

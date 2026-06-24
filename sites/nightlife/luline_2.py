"""
luline 夜のお店選びドットコム（全国版） (luline.jp/top) — ナイトワーク店舗スクレイパー

既存の東北版 (nightlife/luline.py, url=/tohoku/shop_list/search/) と同一サイトの
全国版。sites.yml の url は全国トップ (https://luline.jp/top/)。全国の店舗一覧は
/shop_list/search/ にあり (?page=N で全40ページ・約1,200件)、url の origin から派生して辿る。

取得対象:
    - 店舗名 / 名称_カナ / 都道府県 / 住所 / TEL / 営業時間 / 定休日 / HP / サイト定義業種
    - エリア / 最寄り駅 / 平均年齢 / メールアドレス
    - 各種料金 (指名・場内指名・同伴・サービス料・延長・延長方法・目安予算)
    - 求人情報 (table.arbeitMiddle): 職種 / 勤務時間 (区分ごとに分割)

著作権リスク回避のため、以下の自由記述プロースは取得しない:
    - 店舗紹介 (a.shopIntroduce の長文)
    - 求人の「資格」「待遇」(箇条書きのプロモーション文)

勤務時間のカラム分割:
    求人の「時間」は曜日ごと (平日 / 土日) や役割ごと
    (【ホールスタッフ】17:00～翌2:30 / [ア] / [社]) に分けて書かれることがあるため、
    区分ごとに別カラム (勤務時間区分N / 勤務時間N) へ分割して取得する。

取得フロー (Pattern B: 一覧→詳細, 詳細1件ごとに即 yield):
    1. 一覧ページ (origin/shop_list/search/?page=N) から店舗ブロック (.listShopWrap) を収集
       - 一覧でしか取れない 名称_カナ / エリア / 平均年齢 をここで確保
       - luline.jp 内の詳細リンク (/shop/view/N/) のみ採用 (tainew 等の外部リンクは除外)
    2. 各詳細ページ (div.accessInner table / table.systemMiddle / table.arbeitMiddle) から
       住所/TEL/MAIL/定休日/最寄り駅/料金/求人情報 等を取得
    3. 詳細を1件取得するごとに即 yield (途中中断でも無駄な通信をしない)

実行方法:
    python scripts/sites/nightlife/luline_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id luline_2
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 全国の店舗一覧パス。引数 url (= sites.yml の url, トップページ) の origin に対して
# urljoin して使う。別ドメイン/別 origin はハードコードしない (SSOT = sites.yml の url)。
_LIST_PATH = "/shop_list/search/"

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
_MAP_LINK_RE = re.compile(r"\s*Mapを開く\s*$")

# 勤務時間の解析用
#   時刻レンジ (例: "17:00～翌2:30" / "20:00～LAST" / "19:00～LAST（…）")
_TIME_RANGE_RE = re.compile(
    r"\d{1,2}[:：]\d{2}\s*[～〜~－—\-]\s*(?:翌\s*)?(?:\d{1,2}[:：]\d{2}|LAST|ラスト)"
)
#   役割/区分ラベル (例: "【ホールスタッフ】" / "[ア]" / "[社]")
_BRACKET_RE = re.compile(r"[【\[]\s*([^】\]]+?)\s*[】\]]")
#   曜日ベースの区分 (例: "平日" / "土日" / "月～金")
_DAY_PREFIX_RE = re.compile(
    r"^[\s　・]*("
    r"(?:平日|週末|土日|祝日?|"
    r"[月火水木金土日](?:[・,、~～－-][月火水木金土日])*)"
    r"[^0-9【\[]*?"
    r")(?=\d{1,2}[:：]\d{2})"
)
#   分割して取得する勤務時間スロット数 (区分ごと/曜日ごとの内訳を別カラム化)
_WORK_HOUR_SLOTS = 4

# 詳細ページの主要情報テーブル (div.accessInner table) のラベル → EXTRA カラム
_EXTRA_AREA = "エリア"
_EXTRA_STATION = "最寄り駅"
_EXTRA_AVG_AGE = "平均年齢"
# メールアドレスは Schema.EMAIL に既定義のためそちらへマッピングする
# 料金テーブル (table.systemMiddle) のラベル → EXTRA カラム
_EXTRA_FEE_NOMINATE = "指名料金"
_EXTRA_FEE_INHOUSE = "場内指名料金"
_EXTRA_FEE_ACCOMPANY = "同伴料金"
_EXTRA_FEE_SERVICE = "サービス料"
_EXTRA_FEE_EXTEND = "延長料金"
_EXTRA_FEE_EXTEND_WAY = "延長方法"
_EXTRA_FEE_BUDGET = "目安予算"
# 求人テーブル (table.arbeitMiddle) のラベル → EXTRA カラム (短い構造化値のみ)
_EXTRA_JOB = "職種"
# 勤務時間スロット (区分ラベル + 時刻レンジ) を固定カラムとして宣言。
# EXTRA_COLUMNS は実行前に確定している必要があるため、店舗ごとに動的にカラムを
# 増やせない。区分ごと/曜日ごとの内訳を _WORK_HOUR_SLOTS 個まで別カラムへ展開する。
_EXTRA_WORK_LABELS = [f"勤務時間区分{i}" for i in range(1, _WORK_HOUR_SLOTS + 1)]
_EXTRA_WORK_TIMES = [f"勤務時間{i}" for i in range(1, _WORK_HOUR_SLOTS + 1)]
# 区分1, 時刻1, 区分2, 時刻2, ... の順でペアにして並べる
_EXTRA_WORK_COLUMNS = [
    col for pair in zip(_EXTRA_WORK_LABELS, _EXTRA_WORK_TIMES) for col in pair
]


class Luline2Scraper(StaticCrawler):
    """luline 夜のお店選びドットコム（全国版） スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = [
        _EXTRA_AREA,
        _EXTRA_STATION,
        _EXTRA_AVG_AGE,
        _EXTRA_FEE_NOMINATE,
        _EXTRA_FEE_INHOUSE,
        _EXTRA_FEE_ACCOMPANY,
        _EXTRA_FEE_SERVICE,
        _EXTRA_FEE_EXTEND,
        _EXTRA_FEE_EXTEND_WAY,
        _EXTRA_FEE_BUDGET,
        _EXTRA_JOB,
        *_EXTRA_WORK_COLUMNS,
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url (トップページ) の origin から全国一覧の起点を派生させる。
        list_root = urljoin(url, _LIST_PATH)

        seen: set[str] = set()
        saved = 0
        index = 0
        page = 1
        while True:
            page_url = list_root if page == 1 else f"{list_root}?page={page}"
            list_soup = self.get_soup(page_url)
            if list_soup is None:
                break
            blocks = list_soup.select(".listShopWrap")
            if not blocks:
                break

            for block in blocks:
                listing = self._parse_listing_block(url, block)
                if not listing:
                    continue
                detail_url = listing["detail_url"]
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                index += 1

                try:
                    soup = self.get_soup(detail_url)
                    if soup is None:
                        self.logger.warning("詳細取得失敗(None): %s", detail_url)
                        continue
                    record = self._parse_detail(detail_url, soup, listing)
                except Exception as e:  # 個別アイテムの失敗は握りつぶして継続
                    self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                    continue

                if not record:
                    continue

                saved += 1
                self.logger.info(
                    "取得OK: %d 店舗=%s",
                    index,
                    record.get(Schema.NAME) or detail_url,
                )
                yield record  # ★ 詳細1件ごとに即 yield

            page += 1

        self.logger.info("取得完了: 候補%d件 取得%d件", index, saved)

    def _parse_listing_block(self, root_url: str, block) -> dict | None:
        h2 = block.select_one("h2.shopName")
        if not h2:
            return None
        name_anchor = h2.select_one("a:not(.small)")
        href = name_anchor.get("href") if name_anchor else None
        if not href:
            return None
        detail_url = urljoin(root_url, href)

        # luline.jp 内の詳細ページのみ対象 (tainew 等の外部リンクは除外)
        parsed = urlparse(detail_url)
        if parsed.netloc != urlparse(root_url).netloc or "/shop/view/" not in parsed.path:
            return None

        name = self._clean(name_anchor.get_text(strip=True))
        kana_anchor = h2.select_one("a.small")
        kana = ""
        if kana_anchor:
            kana = self._clean(kana_anchor.get_text(strip=True)).strip("()（）")

        shop_data = block.select_one("p.shopData")
        area = ""
        cat_site = ""
        if shop_data:
            type_a = shop_data.select_one("a.gtmShopListType")
            area_a = shop_data.select_one("a.gtmShopListArea")
            cat_site = self._clean(type_a.get_text(strip=True)) if type_a else ""
            area = self._clean(area_a.get_text(strip=True)) if area_a else ""

        info = self._table_to_dict(block.select_one(".shopInfoTbl table"))

        return {
            "detail_url": detail_url,
            "name": name,
            "kana": kana,
            "area": area,
            "cat_site": cat_site,
            "avg_age": info.get("平均年齢", ""),
            "time": info.get("営業時間", ""),
        }

    def _parse_detail(self, detail_url: str, soup: BeautifulSoup, listing: dict) -> dict | None:
        info = self._table_to_dict(soup.select_one("div.accessInner table"))

        name = listing.get("name", "")
        h1 = soup.select_one("h1")
        if not name and h1:
            name = self._clean(h1.get_text(strip=True))
        if not name:
            self.logger.warning("店舗名が空: %s", detail_url)
            return None

        address_raw = info.get("住所", "")
        address = _MAP_LINK_RE.sub("", address_raw).strip()
        pref, addr_body = self._split_pref(address)

        tel = self._extract_tel(info.get("TEL", ""), soup)
        hp = self._extract_hp(soup)
        fees = self._table_to_dict(soup.select_one("table.systemMiddle"))
        # 求人テーブルは <br> 区切りで複数行になるため改行を保持して取得する
        recruit = self._recruit_to_dict(soup.select_one("table.arbeitMiddle"))

        record = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: listing.get("kana", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: info.get("営業時間", "") or listing.get("time", ""),
            # 定休日: 詳細テーブル優先、無ければ求人の「休日」を採用
            Schema.HOLIDAY: info.get("定休日", "") or self._join_lines(recruit.get("休日", "")),
            Schema.HP: hp,
            Schema.CAT_SITE: (
                info.get("業種", "")
                or self._clean(recruit.get("業種", ""))
                or listing.get("cat_site", "")
            ),
            _EXTRA_AREA: info.get("エリア", "") or listing.get("area", ""),
            _EXTRA_STATION: info.get("最寄り駅", ""),
            _EXTRA_AVG_AGE: listing.get("avg_age", ""),
            Schema.EMAIL: info.get("MAIL", ""),
            _EXTRA_FEE_NOMINATE: fees.get("指名料金", ""),
            _EXTRA_FEE_INHOUSE: fees.get("場内指名料金", ""),
            _EXTRA_FEE_ACCOMPANY: fees.get("同伴料金", ""),
            _EXTRA_FEE_SERVICE: fees.get("サービス料", ""),
            _EXTRA_FEE_EXTEND: fees.get("延長料金", ""),
            _EXTRA_FEE_EXTEND_WAY: fees.get("延長方法", ""),
            _EXTRA_FEE_BUDGET: fees.get("目安予算", ""),
            _EXTRA_JOB: self._clean(recruit.get("職種", "")),
        }

        # 勤務時間: 区分(役割/曜日)ごとに分割して固定スロットカラムへ展開
        segments = self._parse_work_hours(recruit.get("時間", ""))
        for i in range(_WORK_HOUR_SLOTS):
            label, time_range = segments[i] if i < len(segments) else ("", "")
            record[_EXTRA_WORK_LABELS[i]] = label
            record[_EXTRA_WORK_TIMES[i]] = time_range

        return record

    def _table_to_dict(self, table) -> dict[str, str]:
        data: dict[str, str] = {}
        if table is None:
            return data
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = self._clean(th.get_text(strip=True))
            if not key or key in data:
                continue
            data[key] = self._clean(td.get_text(" ", strip=True))
        return data

    def _recruit_to_dict(self, table) -> dict[str, str]:
        """求人テーブルを {ラベル: 値} に変換。<br> は改行として保持する。

        「時間」の曜日/役割分割や「休日」の整形を後段で扱えるよう、
        _table_to_dict と違い改行を潰さずに保持する。
        """
        data: dict[str, str] = {}
        if table is None:
            return data
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = self._clean(th.get_text(strip=True))
            if not key or key in data:
                continue
            data[key] = td.get_text("\n", strip=True)
        return data

    def _join_lines(self, raw: str | None) -> str:
        """改行区切りの値を ' / ' でつないだ 1 行に整形する。"""
        if not raw:
            return ""
        parts = [self._clean(p) for p in re.split(r"[\r\n]+", raw)]
        return " / ".join(p for p in parts if p)

    def _parse_work_hours(self, raw: str | None) -> list[tuple[str, str]]:
        """求人「時間」を (区分ラベル, 時刻レンジ) のリストへ分解する。

        - 役割/区分が 【…】 / […] で示される場合はその中身をラベルにする
          (例: 【ホールスタッフ】17:00～翌2:30 → ("ホールスタッフ", "17:00～翌2:30"))
        - 曜日で分かれる場合は曜日表記をラベルにする (例: 平日 / 土日)
        - ラベルが無い単一時刻はラベル空で 1 件返す
        - 「週1日～OK」等の時刻を含まない説明行は無視する
        """
        if not raw:
            return []
        segments: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for line in re.split(r"[\r\n]+", raw):
            line = self._clean(line)
            if not line:
                continue
            for label, time_range in self._segments_from_line(line):
                key = (label, time_range)
                if key in seen:
                    continue
                seen.add(key)
                segments.append(key)
        return segments

    def _segments_from_line(self, line: str) -> list[tuple[str, str]]:
        """1 行から (区分ラベル, 時刻レンジ) を抽出する。

        同一行に複数の区分が並ぶ場合 (例: [ア]19:00～LAST [社]18:00～翌2:00) は、
        各時刻レンジの直前にあるラベルをそのレンジの区分として割り当てる。
        """
        # ラベル候補を出現位置つきで収集 (時刻そのものを囲む 【19:00～LAST】 は除外)
        labels: list[tuple[int, str]] = []
        for m in _BRACKET_RE.finditer(line):
            content = m.group(1).strip()
            if content and not _TIME_RANGE_RE.search(content):
                labels.append((m.start(), content))
        day_m = _DAY_PREFIX_RE.match(line)
        if day_m:
            labels.append((0, day_m.group(1).strip(" 　・,、")))
        labels.sort(key=lambda x: x[0])

        segments: list[tuple[str, str]] = []
        for tm in _TIME_RANGE_RE.finditer(line):
            pos = tm.start()
            label = ""
            for lpos, ltext in labels:
                if lpos <= pos:
                    label = ltext  # 直前のラベルを採用
                else:
                    break
            segments.append((label, self._clean(tm.group(0))))
        return segments

    def _extract_tel(self, table_value: str, soup: BeautifulSoup) -> str:
        match = _TEL_PATTERN.search(table_value or "")
        if match:
            return match.group(0)
        anchor = soup.find("a", href=re.compile(r"^tel:"))
        if anchor:
            match = _TEL_PATTERN.search(anchor.get("href", ""))
            if match:
                return match.group(0)
        return ""

    def _extract_hp(self, soup: BeautifulSoup) -> str:
        for tr in soup.select("div.accessInner table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td and self._clean(th.get_text(strip=True)) == "URL":
                anchor = td.find("a")
                if anchor and anchor.get("href"):
                    return anchor.get("href").strip()
                return self._clean(td.get_text(strip=True))
        return ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        match = _PREF_PATTERN.match(address)
        if not match:
            return "", address
        return match.group(1), address[match.end():].strip()

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

    scraper = Luline2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://luline.jp/top/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

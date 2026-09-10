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

LIST_URL = "https://haisha-yoyaku.jp/bun2sdental/list/"

# 都道府県ページは /list/category/catg/{JISコード}/ (1=北海道 … 47=沖縄県)。
# 東京都(13)を先頭に置き、少件数でも動作確認できるようにする。
PREF_NAMES = {
    1: "北海道", 2: "青森県", 3: "岩手県", 4: "宮城県", 5: "秋田県",
    6: "山形県", 7: "福島県", 8: "茨城県", 9: "栃木県", 10: "群馬県",
    11: "埼玉県", 12: "千葉県", 13: "東京都", 14: "神奈川県", 15: "新潟県",
    16: "富山県", 17: "石川県", 18: "福井県", 19: "山梨県", 20: "長野県",
    21: "岐阜県", 22: "静岡県", 23: "愛知県", 24: "三重県", 25: "滋賀県",
    26: "京都府", 27: "大阪府", 28: "兵庫県", 29: "奈良県", 30: "和歌山県",
    31: "鳥取県", 32: "島根県", 33: "岡山県", 34: "広島県", 35: "山口県",
    36: "徳島県", 37: "香川県", 38: "愛媛県", 39: "高知県", 40: "福岡県",
    41: "佐賀県", 42: "長崎県", 43: "熊本県", 44: "大分県", 45: "宮崎県",
    46: "鹿児島県", 47: "沖縄県",
}
PREF_CODES = [13] + [c for c in sorted(PREF_NAMES) if c != 13]

# 詳細URL: /bun2sdental/detail/index/id/{ID}/ (IDは英数字混在あり)
# 末尾に /tab/N/ が付く同一医院のリンクは除外する
_DETAIL_RE = re.compile(r"/bun2sdental/detail/index/id/([0-9A-Za-z]+)/?$")
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|"
    r"愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")

MAX_PAGES_PER_PREF = 1000


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\xa0", " ")).strip()


class EparkDentalScraper(StaticCrawler):
    """EPARK歯科 医院情報スクレイパー（haisha-yoyaku.jp）

    全国47都道府県の一覧をページネーションで辿り、医院詳細ページから
    名称・都道府県・住所・TEL・診療項目・アクセス・営業時間・定休日を取得する。
    """

    # Chrome / Firefox の UA は 403 で弾かれる。Safari の UA のみ 200 が返る。
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    )
    DELAY = 0.5
    ITEM_DELAY = 0  # 待機はページ取得側 (DELAY) に寄せる
    EXTRA_COLUMNS = ["診療項目", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for code in PREF_CODES:
            pref_url = urljoin(url, f"category/catg/{code}/")
            self.logger.info("都道府県 %s (catg/%d) の一覧を巡回します", PREF_NAMES[code], code)
            for detail_url in self._iter_detail_urls(pref_url):
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

    def _iter_detail_urls(self, pref_url: str) -> Generator[str, None, None]:
        """都道府県一覧を rel=next で辿りながら詳細URLを都度返す。

        範囲外の ?page=N は最終ページが返るため、ページ番号の生成ではなく
        rel="next" の有無で終端を判定する。
        """
        current = pref_url
        visited: set[str] = set()
        for _ in range(MAX_PAGES_PER_PREF):
            if not current or current in visited:
                return
            visited.add(current)

            soup = self.get_soup(current)
            if soup is None:
                return

            for a in soup.select("a[href]"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                full = urljoin(current, href).split("?")[0].split("#")[0]
                if _DETAIL_RE.search(full):
                    yield full

            next_el = soup.select_one("link[rel='next'], a[rel='next']")
            next_href = next_el.get("href") if next_el else None
            current = urljoin(current, next_href) if next_href else None

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        table = soup.select_one("table.table_clinic-base")

        # --- 名称 (医院名 行の p.main。カナは span.main_kana) ---
        name_p = table.select_one("p.main") if table else None
        if name_p:
            kana = name_p.select_one("span.main_kana")
            if kana:
                data[Schema.NAME_KANA] = _clean(kana.get_text())
                kana.extract()
            data[Schema.NAME] = _clean(name_p.get_text())
        else:
            h1 = soup.select_one("h1")
            data[Schema.NAME] = _clean(h1.get_text()) if h1 else ""

        if not data.get(Schema.NAME):
            return None

        if table:
            for tr in table.find_all("tr", recursive=True):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text(" "))
                if key == "住所":
                    self._set_address(data, td)
                elif key == "アクセス":
                    data["アクセス"] = _clean(td.get_text(" "))
                elif key == "診療項目":
                    subjects = [_clean(sp.get_text()) for sp in td.select("span.content")]
                    data["診療項目"] = "|".join(s for s in subjects if s)
                elif key == "予約":
                    tel = self._pick_tel(td)
                    if tel:
                        data[Schema.TEL] = tel
                elif key == "公式HP":
                    data[Schema.HP] = _clean(td.get_text(" "))
                elif key == "診療受付・休診日":
                    closed = td.select_one("li.closed_day_icon")
                    if closed:
                        data[Schema.HOLIDAY] = _clean(closed.get_text())
                    data[Schema.TIME] = self._parse_hours(td.select_one("table"))

        if not data.get(Schema.TEL):
            tel = self._pick_tel(soup)
            if tel:
                data[Schema.TEL] = tel
        if not data.get(Schema.TIME):
            data[Schema.TIME] = self._parse_hours(soup.select_one("table.treatment_reception_table"))

        return data

    @staticmethod
    def _set_address(data: dict, td) -> None:
        """住所セル (先頭 <p> が住所。地図リンクは除外) から住所と都道府県を取る。"""
        p = td.find("p")
        addr = _clean(p.get_text(" ")) if p else _clean(td.get_text(" "))
        addr = addr.replace("大きな地図で見る", "").strip()
        if not addr:
            return
        data[Schema.ADDR] = addr
        m = _PREF_RE.match(addr)
        if m:
            data[Schema.PREF] = m.group(1)

    @staticmethod
    def _pick_tel(scope) -> str:
        """電話番号を取得する。医院直通番号 (span.reserve_number) を優先する。"""
        for sel in ("span.reserve_number", "a.tel_reserve_btn", "a[href^='tel:']"):
            for el in scope.select(sel):
                m = _TEL_RE.search(_clean(el.get_text(" ")))
                if m:
                    return m.group(0)
        return ""

    @staticmethod
    def _parse_hours(table) -> str:
        """診療受付時間テーブルを「月: 09:30～13:00,14:30～20:00」形式に整形する。"""
        if table is None:
            return ""
        rows = table.find_all("tr")
        if not rows:
            return ""

        header = rows[0]
        days = [_clean(td.get_text()) for td in header.find_all("td")]
        if not days:
            return ""

        schedule: dict[str, list[str]] = {d: [] for d in days}
        for tr in rows[1:]:
            th = tr.find("th")
            if not th:
                continue
            span = _clean(th.get_text())
            if not re.search(r"\d{1,2}:\d{2}", span):
                continue
            for day, td in zip(days, tr.find_all("td")):
                if "●" in _clean(td.get_text()):
                    schedule[day].append(span)

        parts = [f"{d}: {','.join(v)}" for d, v in schedule.items() if v]
        return " / ".join(parts)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    EparkDentalScraper().execute("https://haisha-yoyaku.jp/bun2sdental/list/")

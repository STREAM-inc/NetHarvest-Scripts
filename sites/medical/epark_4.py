"""
対象サイト: https://fdoc.jp/clinic/list/index/rgid/446/
EPARKクリニック・病院 (fdoc.jp) 医院情報スクレイパー
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

# 都道府県一覧は /clinic/list/index/rgid/{JISコード}/ (13=東京都 など)。
# 備考指定の 13 都府県のみ巡回する。
TARGET_PREFS = {
    13: "東京都", 14: "神奈川県", 11: "埼玉県", 12: "千葉県", 23: "愛知県",
    27: "大阪府", 28: "兵庫県", 26: "京都府", 29: "奈良県", 33: "岡山県",
    34: "広島県", 40: "福岡県", 41: "佐賀県",
}
MAX_PAGES_PER_LIST = 2000

_DETAIL_RE = re.compile(r"/clinic/detail/index/id/(\d+)/?$")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
# サイト上の「歯科系」診療科目
_DENTAL_SUBJECTS = {"歯科", "小児歯科", "矯正歯科", "歯科口腔外科"}
# 予約ボタンの外部リンクのうち、予約システムのものは HP として扱わない
_BOOKING_HOSTS = ("doctorqube.com", "digikar", "fdoc.jp", "epark.jp", "haisha-yoyaku.jp",
                  "google.com", "airrsv", "ssc", "reserve", "yoyaku", "jiyuu")


def _clean(s) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


class Epark4Scraper(StaticCrawler):
    """EPARKクリニック・病院 スクレイパー

    起点 url の一覧 → 備考指定 13 都府県 (rgid=JISコード) の一覧を rel=next で辿り、
    医院詳細から名称/住所/TEL/HP/診療科目/診療区分/掲載プラン/特集ページ有無を取得する。
    """

    # Chrome / Firefox の UA は 403(「ご利用のWEBブラウザでは表示できません」)。Safari UA は 200。
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    )
    DELAY = 1.0
    ITEM_DELAY = 0  # 待機はページ取得側 (DELAY) に寄せる
    EXTRA_COLUMNS = [
        "診療区分",
        "掲載プラン",
        "契約タイプ",
        "診療内容",
        "特集ページ有無",
        "特集ページ診療内容",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        list_urls = [url] + [urljoin(url, f"/clinic/list/index/rgid/{code}/") for code in TARGET_PREFS]
        for list_url in list_urls:
            self.logger.info("一覧を巡回します: %s", list_url)
            for detail_url, contract in self._iter_list(list_url):
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url, contract)
                if item:
                    yield item

    def _iter_list(self, list_url: str) -> Generator[tuple[str, str], None, None]:
        """一覧を rel=next で辿り (詳細URL, 契約タイプ) を都度返す。"""
        current = list_url
        visited: set[str] = set()
        for _ in range(MAX_PAGES_PER_LIST):
            if not current or current in visited:
                return
            visited.add(current)
            soup = self.get_soup(current)
            if soup is None:
                return
            for li in soup.select("li.list-SearchList_Item[data-catalogid]"):
                a = li.select_one("a[href*='/clinic/detail/index/id/']")
                if not a:
                    continue
                full = urljoin(current, a["href"]).split("?")[0].split("#")[0]
                if _DETAIL_RE.search(full):
                    yield full, _clean(li.get("data-contract"))
            next_el = soup.select_one("link[rel='next'], a[rel='next']")
            next_href = next_el.get("href") if next_el else None
            current = urljoin(current, next_href) if next_href else None

    def _scrape_detail(self, url: str, contract: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        rows: dict[str, object] = {}
        for th in soup.select("th"):
            label = _clean(th.get_text())
            td = th.find_next_sibling("td")
            if label and td is not None and label not in rows:
                rows[label] = td

        name_td = rows.get("医院名")
        name = _clean(name_td.get_text(" ")) if name_td else ""
        if not name:
            h = soup.select_one("h1")
            name = _clean(h.get_text(" ")) if h else ""
        if not name:
            return None

        addr = ""
        if rows.get("住所") is not None:
            addr = _clean(rows["住所"].get_text(" "))
            addr = re.sub(r"\s*地図$", "", addr)
        m = _PREF_RE.match(addr)
        pref = m.group(1) if m else ""

        tel = ""
        if rows.get("電話番号") is not None:
            mt = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", rows["電話番号"].get_text(" "))
            tel = mt.group(0) if mt else ""

        subjects = []
        if rows.get("診療科目") is not None:
            subjects = _clean(rows["診療科目"].get_text(" ")).split(" ")
            subjects = [s for s in subjects if s]
        dental = [s for s in subjects if s in _DENTAL_SUBJECTS]
        if dental and len(dental) == len(subjects):
            kubun = "歯科"
        elif dental:
            kubun = "医科・歯科"
        elif subjects:
            kubun = "医科"
        else:
            kubun = ""

        naiyo = _clean(rows["診療内容"].get_text(" ")) if rows.get("診療内容") is not None else ""

        html = str(soup)
        is_rich_dental = bool(re.search(r"haisha-yoyaku\.jp/images/material/rich2", html))
        if kubun in ("歯科", "医科・歯科"):
            # 歯科は EPARK歯科 由来の掲載。リッチプラン医院のみ rich2* 画像を持つ
            plan = "自費リッチプラン" if is_rich_dental else "通常プラン"
        else:
            plan = "リッチ掲載" if soup.select_one("[class*='rich_']") else "通常掲載"

        features = self._features(soup)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: self._hp(soup),
            Schema.CAT_SITE: "/".join(subjects),
            Schema.URL: url,
            "診療区分": kubun,
            "掲載プラン": plan,
            "契約タイプ": contract,
            "診療内容": naiyo,
            "特集ページ有無": "有" if features else "無",
            "特集ページ診療内容": "/".join(features),
        }

    @staticmethod
    def _features(soup) -> list[str]:
        """医院詳細内で「特集」と付くリンク (例: インプラント特集) の診療内容名を返す。"""
        out: list[str] = []
        for a in soup.select("a[href]"):
            if a.find_parent(["header", "footer", "nav"]):
                continue
            text = _clean(a.get_text(" "))
            if "特集" not in text or len(text) > 40:
                continue
            nm = _clean(re.sub(r"特集.*$", "", text))
            if nm and nm not in out:
                out.append(nm)
        return out

    @staticmethod
    def _hp(soup) -> str:
        for a in soup.select("a[href^='http']"):
            label = _clean(a.get_text(" "))
            if any(k in label for k in ("ホームページ", "公式サイト", "公式HP", "医院HP", "病院HP")):
                return a["href"]
        for a in soup.select(".sw-BtnWrap a[href^='http']"):
            host = urlparse(a["href"]).netloc
            if host and not any(b in host for b in _BOOKING_HOSTS):
                return a["href"]
        return ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    Epark4Scraper().execute("https://fdoc.jp/clinic/list/index/rgid/446/")

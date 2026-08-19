"""
リラクジョブ — 全国のメンズエステ求人ポータル (menesth-job.jp)

取得対象:
    掲載されているメンズエステ店舗 (店舗単位で1レコード)。
    店名・電話番号・住所・業種・募集職種・ホームページURL を取得する。

取得フロー:
    トップページ (引数 url) の都道府県リンク (/{サイト独自番号}/) を抽出
      → 各都道府県一覧のページ送り (/{番号}/page2/, page3/ ...)
      → 各求人詳細ページ (/{番号}/shop/{ID}/)
    詳細を1件取得するごとに即 yield する (Pattern B)。

    URL の番号は JIS 都道府県コードではなくサイト独自採番のため、
    都道府県名は URL からではなくページ内のリンク表記から判定する
    (例: /8/ = 東京都, /13/ = 茨城県)。掲載があるのは 46 都道府県
    (鳥取県は該当ページなし)。

    レコード粒度は店舗単位。同一店舗が複数の募集職種を持つ場合は
    「募集職種」を ";" 区切りで1カラムに集約し、店舗としては1行にまとめる。
    同一店舗が複数の都道府県/エリア一覧に現れても店舗IDで重複排除する。

    掲載日・更新日、代表者名・担当者名・メールアドレスは
    後続工程で補完するため取得しない。
    「このお店の特徴」「POINT」等の長文の自由記述は
    著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/menesth_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id menesth_job
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県名 (URL 番号は独自採番のため、リンク表記からこのパターンで判定する)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 都道府県一覧ページのパス (例: /8/) — /8/area28/ 等の絞り込みページは除外する
_PREF_PATH_RE = re.compile(r"^/(\d+)/$")
# 店舗詳細ページのパス (例: /8/shop/17746/) — /oubo/ /kuchikomi/ は除外する
_SHOP_PATH_RE = re.compile(r"^/(\d+)/shop/(\d+)/$")
_TEL_RE = re.compile(r"0\d{1,4}[-(]?\d{1,4}[-)]?\d{3,4}")
_NUM_RE = re.compile(r"\d[\d,]*")
# 募集職種の区切り (例: "セラピスト、エステティシャン")
_JOB_SPLIT_RE = re.compile(r"[、,，/／・]")

# ページ送りの暴走防止 (1都道府県あたりの上限。東京都でも 40 ページ程度)
_MAX_PAGES_PER_PREF = 200

# 詳細ページ「お店情報」の見出し → EXTRA_COLUMNS 名
# (長文の自由記述「アクセス」の補足文などは取得しない)
_SHOPINFO_EXTRA = {
    "エリア": "エリア",
    "最寄り駅": "最寄り駅",
}


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


class MenesthJobCrawler(StaticCrawler):
    """リラクジョブ (メンズエステ求人) スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "募集職種",
        "エリア",
        "最寄り駅",
        "求人HP",
        "受付時間",
    ]

    def parse(self, url: str):
        root = url if url.endswith("/") else url + "/"

        top = self.get_soup(root)
        if top is None:
            self.logger.warning("トップページを取得できませんでした: %s", root)
            return

        pref_pages = self._collect_pref_pages(root, top)
        self.logger.info("都道府県一覧: %d件", len(pref_pages))

        seen_shops: set[str] = set()

        for pref_name, list_url in pref_pages:
            page_url = list_url
            visited: set[str] = set()

            for _ in range(_MAX_PAGES_PER_PREF):
                if not page_url or page_url in visited:
                    break
                visited.add(page_url)

                soup = self.get_soup(page_url)
                if soup is None:
                    break

                shop_urls = self._extract_shop_urls(page_url, soup)
                if not shop_urls:
                    break

                next_url = self._next_page_url(page_url, soup)

                for shop_url in shop_urls:
                    shop_id = _SHOP_PATH_RE.match(
                        urllib.parse.urlparse(shop_url).path
                    ).group(2)
                    if shop_id in seen_shops:
                        continue
                    seen_shops.add(shop_id)

                    item = self._parse_detail(shop_url, pref_name)
                    if item:
                        yield item

                page_url = next_url

    # ===============================================
    # 一覧側
    # ===============================================

    def _collect_pref_pages(self, root: str, soup) -> list[tuple[str, str]]:
        """トップページから (都道府県名, 一覧URL) を抽出する。

        URL 番号は独自採番なので、リンクの表記文字列から都道府県名を判定する。
        """
        found: dict[str, str] = {}  # 番号 → 都道府県名
        for a in soup.select("a[href]"):
            abs_url = urllib.parse.urljoin(root, a["href"].strip())
            path_match = _PREF_PATH_RE.match(urllib.parse.urlparse(abs_url).path)
            if not path_match:
                continue
            name_match = _PREF_PATTERN.match(_clean(a.get_text(" ", strip=True)))
            if not name_match:
                # 「東京」「大阪」等の略称リンクは正式名称のリンクで拾う
                continue
            found.setdefault(path_match.group(1), name_match.group(1))

        return [
            (found[num], urllib.parse.urljoin(root, f"{num}/"))
            for num in sorted(found, key=int)
        ]

    def _next_page_url(self, page_url: str, soup) -> str:
        """ページャの「次へ」(rel="next") から次ページ URL を返す。

        存在しないページ番号 (例: /1/page99/) は 404 になるため、
        ページ番号を決め打ちで進めずページャのリンクを辿る。
        """
        a = soup.select_one('ul.pager-list li.pager-item.next a[rel="next"]')
        if not a or not a.get("href"):
            return ""
        return urllib.parse.urljoin(page_url, a["href"].strip())

    def _extract_shop_urls(self, page_url: str, soup) -> list[str]:
        """一覧ページから店舗詳細ページの URL を重複なしで抽出する。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("section.f-shopDetail h2.shop-name a[href]"):
            abs_url = urllib.parse.urljoin(page_url, a["href"].strip())
            abs_url = abs_url.split("#")[0].split("?")[0]
            if not _SHOP_PATH_RE.match(urllib.parse.urlparse(abs_url).path):
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)
            urls.append(abs_url)
        return urls

    # ===============================================
    # 詳細側
    # ===============================================

    def _parse_detail(self, shop_url: str, pref_name: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        info = self._parse_shop_info(soup)
        name = info.get("店名", "")
        if not name:
            # フォールバック: 詳細上部の店舗名
            el = soup.select_one("div.shop-wrap p.shop-name")
            name = _clean(el.get_text(" ", strip=True)) if el else ""
        if not name:
            self.logger.warning("店名が取得できませんでした: %s", shop_url)
            return None

        address = info.get("住所", "")
        job_titles, region = self._parse_job_posting(soup)

        item = {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: region or self._pref_from_address(address) or pref_name,
            Schema.ADDR: address,
            Schema.TEL: self._parse_tel(soup),
            Schema.CAT_SITE: info.get("業種", ""),
            Schema.EMP_NUM: self._parse_enrolled(soup),
            Schema.LINE: self._parse_line_id(soup),
            Schema.HP: info.get("_hp", ""),
            "募集職種": job_titles,
            "求人HP": info.get("_recruit_hp", ""),
            "受付時間": self._parse_reception_time(soup),
        }
        for label, column in _SHOPINFO_EXTRA.items():
            item[column] = info.get(label, "")
        return item

    def _parse_shop_info(self, soup) -> dict:
        """「お店情報」の dt/dd を辞書化する。

        ホームページ欄は「お店のHP」(店舗公式) と「お店の求人HP」を
        別カラムに振り分ける (_hp / _recruit_hp)。
        """
        result: dict[str, str] = {}
        dl = soup.select_one("section.f-shopInfo dl.shopInfo-dl")
        if dl is None:
            return result

        label = ""
        for child in dl.find_all(["dt", "dd"], recursive=False):
            if child.name == "dt":
                label = _clean(child.get_text(" ", strip=True))
                continue
            if not label:
                continue
            if label == "ホームページ":
                for li in child.select("li.link-item"):
                    a = li.select_one("a[href]")
                    if not a:
                        continue
                    href = a["href"].strip()
                    key = "_recruit_hp" if "is-recruit" in (li.get("class") or []) else "_hp"
                    result.setdefault(key, href)
            else:
                result.setdefault(label, _clean(child.get_text(" ", strip=True)))
        return result

    def _parse_job_posting(self, soup) -> tuple[str, str]:
        """JSON-LD の JobPosting から (募集職種, 都道府県) を取得する。

        複数の JobPosting / 複数職種は ";" 区切りで1カラムに集約する。
        """
        titles: list[str] = []
        region = ""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                self.logger.debug("JSON-LD を解析できませんでした")
                continue
            for entry in data if isinstance(data, list) else [data]:
                if not isinstance(entry, dict) or entry.get("@type") != "JobPosting":
                    continue
                for part in _JOB_SPLIT_RE.split(_clean(entry.get("title") or "")):
                    part = part.strip()
                    if part and part not in titles:
                        titles.append(part)
                if not region:
                    address = (entry.get("jobLocation") or {}).get("address") or {}
                    region = _clean(address.get("addressRegion") or "")
        return ";".join(titles), region

    def _parse_tel(self, soup) -> str:
        """電話番号 (070/080/090 等のモバイル番号中心) をそのまま取得する。"""
        for el in soup.select("dl.recruit-info dd.number.showed, dd.number.showed"):
            match = _TEL_RE.search(_clean(el.get_text(" ", strip=True)))
            if match:
                return match.group(0)
        for a in soup.select('a[href^="tel:"]'):
            match = _TEL_RE.search(a["href"].replace("tel:", ""))
            if match:
                return match.group(0)
        return ""

    def _parse_line_id(self, soup) -> str:
        el = soup.select_one("#shop-line-id")
        return _clean(el.get_text(" ", strip=True)) if el else ""

    def _parse_enrolled(self, soup) -> str:
        """在籍セラピスト数 (例: 「66人」→ 66)。"""
        el = soup.select_one("dl.enrolled-dl dd.txt")
        if not el:
            return ""
        match = _NUM_RE.search(_clean(el.get_text(" ", strip=True)))
        return match.group(0).replace(",", "") if match else ""

    def _parse_reception_time(self, soup) -> str:
        el = soup.select_one("p.reception_start_time")
        return _clean(el.get_text(" ", strip=True)) if el else ""

    @staticmethod
    def _pref_from_address(address: str) -> str:
        match = _PREF_PATTERN.match(address or "")
        return match.group(1) if match else ""


if __name__ == "__main__":
    scraper = MenesthJobCrawler()
    scraper.site_name = "リラクジョブ"
    scraper.site_id = "menesth_job"
    scraper.execute("https://menesth-job.jp/")

"""
男ワーク — 風俗・ナイトワーク系の男性向け高収入求人ポータル (dan-work.com)

取得対象:
    全国9地域版に掲載されている求人 (店舗 = 1レコード, 約1,100件)。
    店名・カナ・住所・都道府県・電話番号・メール・LINE・公式HP・業種・
    雇用形態・募集職種・待遇タグなどを取得する。

取得フロー:
    トップページ (引数 url) の地域版リンク (/kantou/ など9地域)
      → 各地域の勤務地検索ページ (/{地域}/src.php?mode=area) の
        大エリア (label.bigname > a) を抽出 = その地域の全求人を網羅する単位
      → 大エリア一覧 (/{地域}/area/?area={slug}) を
        div#pager li.next のリンクで 30件ずつページ送り
      → 各求人詳細 (/{地域}/data/index.php?id={ID})
    詳細を1件取得するごとに即 yield する (Pattern B)。

    求人ID (?id=) は地域版ごとの採番で、地域をまたぐと別の店舗を指す
    (例: /kansai/...?id=446 と /kantou/...?id=446 は別店舗)。
    そのため重複排除は詳細ページの絶対URLをキーに行う。

    HTML には <base href="https://www.dan-work.com/{地域}/"> が置かれており、
    一覧内の "./data/index.php?id=..." は一覧ページURLではなく base を
    基準に解決する必要がある (誤ると /{地域}/area/data/... となり404)。

アクセス条件:
    ブラウザ系 User-Agent は全パス 403 Forbidden (Apache 側で拒否) となる。
    Googlebot の User-Agent のみ 200 が返るため USER_AGENT を上書きしている。
    robots.txt は /district/scout/ のみ Disallow で、本クローラーの巡回範囲は許可対象。
    利用規約ページは存在せず、スクレイピングを明示的に禁止する記載も見当たらない
    (フッターの著作権表示のみ)。そのため長文の自由記述
    (給与・資格・仕事内容・就業時間・担当より・先輩より・キャッチ・備考) は取得しない。

    ページの文字コードは Shift_JIS (Content-Type ヘッダーに charset 指定あり)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/dan_work_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id dan_work_2
"""

import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 地域版のパス (トップページから抽出できなかった場合のフォールバック)
_REGION_SLUGS = [
    "hokkaido", "touhoku", "hokuriku", "kantou", "toukai",
    "kansai", "chuugoku", "shikoku", "kyuusyuu",
]
# トップページの地域版リンク (例: /kantou/)
_REGION_PATH_RE = re.compile(r"^/([a-z]+)/$")
# 求人詳細ページのパス (例: /kantou/data/index.php?id=3686)
_DETAIL_PATH_RE = re.compile(r"^/([a-z]+)/data/index\.php$")

_PREF_NAMES = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_PREFIX_RE = re.compile(rf"^({_PREF_NAMES})")

# サイト独自のエリア表記 → 都道府県。
# 住所に都道府県が含まれない求人 (例: "北九州市小倉北区") のために
# パンくずのエリア名から都道府県を補完する。
_AREA_PREF = {
    "札幌": "北海道", "すすきの": "北海道", "札幌その他": "北海道",
    "北海道全域": "北海道", "北海道その他": "北海道",
    "東京": "東京都", "23区西部": "東京都", "23区東部": "東京都", "多摩": "東京都",
    "神奈川": "神奈川県", "横浜": "神奈川県", "川崎": "神奈川県", "厚木": "神奈川県",
    "埼玉": "埼玉県", "大宮": "埼玉県", "西川口": "埼玉県",
    "茨城": "茨城県", "土浦": "茨城県", "水戸": "茨城県",
    "千葉": "千葉県",
    "大阪": "大阪府", "大阪その他": "大阪府", "大阪キタ": "大阪府",
    "大阪ミナミ": "大阪府", "大阪市内": "大阪府",
    "京都": "京都府", "兵庫･姫路": "兵庫県", "福原･神戸･尼崎": "兵庫県",
    "滋賀･雄琴": "滋賀県", "奈良": "奈良県", "和歌山": "和歌山県",
    "岐阜": "岐阜県", "静岡": "静岡県", "三重": "三重県",
    "福岡": "福岡県", "福岡市": "福岡県", "北九州市": "福岡県",
    "久留米市": "福岡県", "福岡その他": "福岡県",
    "松江市": "島根県", "岡山市": "岡山県", "岡山その他": "岡山県",
    "下関市": "山口県", "山口市": "山口県",
    "広島市･中区": "広島県", "広島その他": "広島県",
}

# 詳細ページの <dt> ラベル → EXTRA_COLUMNS 名。
# 給与・資格・仕事内容・担当より・先輩より・備考は長文の自由記述のため取得しない。
_EXTRA_LABELS = {
    "勤務地": "勤務地",
    "職種": "募集職種",
    "雇用形態": "雇用形態",
    "アクセス": "アクセス",
    "平均年齢": "平均年齢",
    "平均年収": "平均年収",
}

# ページ送りの暴走防止 (1大エリアあたりの上限。東京の294件でも10ページ)
_MAX_PAGES_PER_AREA = 60


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


def _li_texts(dd) -> list[str]:
    """dd 直下の <li> のテキストを列挙する (<li> が無ければ dd 全体を1件として返す)。"""
    items = [_clean(li.get_text(" ")) for li in dd.select("li")]
    items = [t for t in items if t]
    if items:
        return items
    text = _clean(dd.get_text(" "))
    return [text] if text else []


class DanWork2Scraper(StaticCrawler):
    """男ワーク (dan-work.com) の求人情報スクレイパー。"""

    # ブラウザ系 UA は全パス 403。Googlebot の UA のみ 200 が返る。
    USER_AGENT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    DELAY = 0.5
    TIMEOUT = 30
    EXTRA_COLUMNS = [
        "地域版",
        "勤務地",
        "募集職種",
        "雇用形態",
        "待遇",
        "アクセス",
        "平均年齢",
        "平均年収",
        "グループ店",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_detail: set[str] = set()

        for region_slug, region_name in self._collect_regions(url):
            for area_url in self._collect_area_urls(url, region_slug):
                for detail_url in self._iter_detail_urls(area_url):
                    if detail_url in seen_detail:
                        continue
                    seen_detail.add(detail_url)
                    item = self._scrape_detail(detail_url, region_name)
                    if item:
                        yield item

    # ------------------------------------------------------------------ 列挙
    def _collect_regions(self, root_url: str) -> list[tuple[str, str]]:
        """トップページから地域版 (スラッグ, 日本語名) を抽出する。"""
        soup = self.get_soup(root_url)
        regions: list[tuple[str, str]] = []
        seen: set[str] = set()
        if soup is not None:
            for a in soup.select("a[href]"):
                path = urllib.parse.urlparse(urllib.parse.urljoin(root_url, a["href"])).path
                m = _REGION_PATH_RE.match(path)
                if not m or m.group(1) not in _REGION_SLUGS or m.group(1) in seen:
                    continue
                seen.add(m.group(1))
                regions.append((m.group(1), _clean(a.get_text())))
        for slug in _REGION_SLUGS:
            if slug not in seen:
                regions.append((slug, ""))
        self.logger.info("地域版: %d 件", len(regions))
        return regions

    def _collect_area_urls(self, root_url: str, region_slug: str) -> list[str]:
        """勤務地検索ページから大エリア一覧のURLを抽出する。

        大エリア (label.bigname) は中/小エリアを内包するため、
        大エリアだけを巡回すればその地域版の全求人を網羅できる。
        """
        src_url = urllib.parse.urljoin(root_url, f"{region_slug}/src.php?mode=area")
        soup = self.get_soup(src_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("label.bigname a[href]"):
            full = urllib.parse.urljoin(src_url, a["href"])
            if full not in seen:
                seen.add(full)
                urls.append(full)
        self.logger.info("%s: 大エリア %d 件", region_slug, len(urls))
        return urls

    def _iter_detail_urls(self, area_url: str) -> Generator[str, None, None]:
        """大エリア一覧をページ送りしながら求人詳細URLを列挙する。"""
        current = area_url
        for _ in range(_MAX_PAGES_PER_AREA):
            soup = self.get_soup(current)
            if soup is None:
                return

            # 一覧内の相対リンクは <base href> を基準に解決する必要がある
            base_tag = soup.select_one("base[href]")
            base_url = urllib.parse.urljoin(current, base_tag["href"]) if base_tag else current

            for a in soup.select("article.shop a[href]"):
                full = urllib.parse.urljoin(base_url, a["href"])
                parsed = urllib.parse.urlparse(full)
                if _DETAIL_PATH_RE.match(parsed.path) and "id=" in parsed.query:
                    yield full

            next_a = soup.select_one("div#pager li.next a[href]")
            if not next_a:
                return
            next_url = urllib.parse.urljoin(current, next_a["href"])
            if next_url == current:
                return
            current = next_url

    # ------------------------------------------------------------------ 詳細
    def _scrape_detail(self, url: str, region_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        main = soup.select_one("div#container main") or soup
        crumbs = [_clean(sp.get_text()) for sp in main.select("nav span[itemprop=name]")]

        data: dict[str, str] = {Schema.URL: url}
        if region_name:
            data["地域版"] = region_name

        # --- 店名 (p.shopname が無い求人があるためパンくず末尾でフォールバック) ---
        name_tag = main.select_one("p.shopname")
        name = _clean(name_tag.get_text()) if name_tag else ""
        if not name and crumbs:
            name = crumbs[-1]
        if not name:
            return None
        data[Schema.NAME] = name

        # --- 読み仮名 (説明文の "店名（カナ）" から) ---
        ct = main.select_one("p.ct")
        if ct:
            m = re.search(r"（([ァ-ヶーｦ-ﾟ・･\s]+)）", _clean(ct.get_text()))
            if m:
                data[Schema.NAME_KANA] = _clean(m.group(1))

        # --- 各 <dl> をラベル別に取り出す ---
        for dl in main.select("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = _clean(dt.get_text())

            if label in _EXTRA_LABELS:
                # 職種は "1 .店長･幹部候補" のような通し番号付きで出る
                values = [re.sub(r"^\d+\s*\.\s*", "", v) for v in _li_texts(dd)]
                data.setdefault(_EXTRA_LABELS[label], ";".join(values))
            elif label == "業種":
                data.setdefault(Schema.CAT_SITE, ";".join(_li_texts(dd)))
            elif label == "待遇":
                # dd 直下には自由記述のPRテキストも混在するため <ul><li> のタグのみ採用
                tags = [_clean(li.get_text(" ")) for li in dd.select("ul li")]
                data.setdefault("待遇", ";".join(t for t in tags if t))
            elif label == "在籍人数":
                data.setdefault(Schema.EMP_NUM, _clean(dd.get_text(" ")))
            elif label == "定休日":
                data.setdefault(Schema.HOLIDAY, _clean(dd.get_text(" ")))
            elif label == "住所":
                addr = _clean(dd.get_text(" ")).replace("地図", "").strip()
                m = _PREF_PREFIX_RE.match(addr)
                if m:
                    data.setdefault(Schema.PREF, m.group(1))
                    addr = addr[m.end():].strip()
                data.setdefault(Schema.ADDR, addr)
            elif label == "公式HP":
                links = [a["href"].strip() for a in dd.select("a[href]") if a.get("href")]
                if links:
                    data.setdefault(Schema.HP, links[0])
            elif label == "グループ店":
                shops = [_clean(a.get_text()) for a in dd.select("a")]
                data.setdefault("グループ店", ";".join(s for s in shops if s))
            elif label == "電話":
                tels = []
                for li in dd.select("li"):
                    text = _clean(li.get_text(" "))
                    text = re.sub(r"【.*?】", "", text).strip()
                    if text:
                        tels.append(text)
                if tels:
                    data.setdefault(Schema.TEL, tels[0])
            elif label == "メール":
                mails = [_clean(a.get_text()) for a in dd.select("ul.mail a")]
                mails = [m for m in mails if "@" in m]
                if mails:
                    data.setdefault(Schema.EMAIL, mails[0])
            elif label == "SNS":
                acc = dd.select_one("li.lineacount")
                if acc:
                    m = re.search(r"ID：\s*(\S+)", _clean(acc.get_text(" ")))
                    if m:
                        data.setdefault(Schema.LINE, m.group(1))

        # --- 都道府県 (住所に含まれない場合はパンくずのエリア名から補完) ---
        if not data.get(Schema.PREF):
            for crumb in crumbs:
                if _PREF_PREFIX_RE.fullmatch(crumb):
                    data[Schema.PREF] = crumb
                    break
                if crumb in _AREA_PREF:
                    data[Schema.PREF] = _AREA_PREF[crumb]
                    break

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = DanWork2Scraper()
    scraper.execute("https://www.dan-work.com")

"""
ELECAREER（エレキャリア） — 電気・設備業界に特化した専門転職サイト (elecareer.com)

取得対象:
    - 全国47都道府県に掲載されている求人票（約9,200件）と、その求人を掲載して
      いる法人の企業情報（法人名・住所・資本金・設立日・従業員数・法人URL・
      事業内容）。

取得フロー:
    トップページ (引数 url) の都道府県リンク (/jobs/{slug}) を起点に、各県の
    一覧ページを ul.page_nav の「次へ」リンクで辿る (10件/ページ)。
    一覧の div.jobbox から詳細ページ (/jobs/{id}) の URL を取り出し、
    詳細を1件取得するごとに即 yield する (Pattern B)。
    同一求人が複数県の一覧に出ることがあるため求人IDで重複除去する。

    名称 (Schema.NAME) は法人情報の「法人名」を採用し、法人情報が無い求人では
    求人情報の「勤務先」(支店名を含む) をフォールバックとして使う。
    住所 (Schema.ADDR) も法人情報の住所を優先し、無い場合は勤務地を使う。

    長文の自由記述 (仕事内容・給与詳細・休日詳細・福利厚生・求める人材・
    応募条件・従業員の声・備考・キャッチコピー) は著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/elecareer.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id elecareer
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

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_JOB_ID_RE = re.compile(r"/jobs/(\d+)")
_PREF_SLUG_RE = re.compile(r"^/jobs/([a-z]+)$")
_DATE_RE = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
_FEATURE_RE = re.compile(r"特徴\s*\n(.+)")
_TIME_RANGE_RE = re.compile(r"\d{1,2}\s*[:：]\s*\d{2}\s*[~～〜\-ー]\s*\d{1,2}\s*[:：]\s*\d{2}")
_MAX_TIME_LINE_LEN = 60  # これを超える行は説明文とみなして取得しない
_SEP_PUNCT_RE = re.compile(r"\s*/\s*([、,・])\s*/\s*")

_ITEMS_PER_PAGE = 10
_MAX_PAGES_PER_PREF = 500  # 無限ループ防止のハードリミット

# トップページから都道府県リンクを取得できなかった場合のフォールバック
_FALLBACK_PREF_SLUGS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "yamanashi", "nagano", "toyama", "ishikawa", "fukui", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kouchi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 都道府県リンクではない /jobs/ 直下のパス
_NON_PREF_SLUGS = {"search", "history", "favorites", "recents"}

# 求人情報 dl の見出し → EXTRA_COLUMNS 名 (短い構造化された値のみ)
_JOB_EXTRA_LABELS = {
    "勤務先": "勤務先",
    "雇用形態": "雇用形態",
    "活かせる資格": "活かせる資格",
    "勤務地": "勤務地",
    "最寄駅": "最寄駅",
    "残業時間": "残業時間",
    "年間休日数": "年間休日数",
    "車通勤": "車通勤",
    "受動喫煙防止措置": "受動喫煙防止措置",
    "交通費": "交通費",
    "転勤": "転勤",
}


def _clean(text: str) -> str:
    """空白・改行・NBSP を1スペースに正規化する。"""
    return re.sub(r"[\s　\xa0]+", " ", text or "").strip()


def _clean_cell(text: str) -> str:
    """dd の値を正規化する。区切り記号だけの要素 (「、」等) は前後の / を畳む。"""
    return _SEP_PUNCT_RE.sub(r"\1", _clean(text))


def _work_hours(text: str) -> str:
    """勤務時間 dd から時刻表記を含む行のみを抜き出す (長文の説明は取得しない)。"""
    lines = [_clean(line) for line in re.split(r"\s*/\s*", text or "")]
    return " / ".join(
        line
        for line in lines
        if _TIME_RANGE_RE.search(line) and len(line) <= _MAX_TIME_LINE_LEN
    )



def _label(text: str) -> str:
    """dt の見出し文字列を比較用に正規化する (末尾の NBSP 等を除去)。"""
    return re.sub(r"[\s　\xa0]+", "", text or "")


def _to_iso_date(text: str) -> str:
    """「2015年1月15日」を「2015-01-15」に変換する。変換できなければ空文字。"""
    m = _DATE_RE.search(text or "")
    if not m:
        return ""
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


class ElecareerCrawler(StaticCrawler):
    """ELECAREER（エレキャリア） スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人ID",
        "勤務先",
        "雇用形態",
        "月給",
        "年収",
        "活かせる資格",
        "勤務地",
        "最寄駅",
        "残業時間",
        "年間休日数",
        "車通勤",
        "受動喫煙防止措置",
        "交通費",
        "転勤",
        "特徴",
        "掲載日",
    ]

    def parse(self, url: str):
        seen_ids: set[str] = set()
        estimated_total = 0

        for slug in self._pref_slugs(url):
            page_url = urllib.parse.urljoin(url, f"jobs/{slug}")
            page_no = 1

            while page_url and page_no <= _MAX_PAGES_PER_PREF:
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                if page_no == 1:
                    estimated_total += self._estimate_pref_items(soup)
                    self.total_items = estimated_total

                boxes = soup.select("main div.jobbox")
                if not boxes:
                    break

                for box in boxes:
                    link = box.select_one('a[href*="/jobs/"]')
                    if not link:
                        continue
                    m = _JOB_ID_RE.search(link.get("href", ""))
                    if not m:
                        continue
                    job_id = m.group(1)
                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    detail_url = urllib.parse.urljoin(page_url, link["href"])
                    try:
                        item = self._scrape_detail(detail_url, job_id)
                    except Exception as e:
                        self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                        continue
                    if item:
                        yield item

                next_link = soup.select_one("ul.page_nav li.next a[href]")
                page_url = (
                    urllib.parse.urljoin(page_url, next_link["href"]) if next_link else None
                )
                page_no += 1

    # ------------------------------------------------------------------ 一覧

    def _pref_slugs(self, url: str) -> list[str]:
        """トップページから都道府県別一覧 (/jobs/{slug}) のスラッグを取得する。"""
        soup = self.get_soup(url)
        slugs: list[str] = []
        if soup is not None:
            for a in soup.select('a[href^="/jobs/"]'):
                m = _PREF_SLUG_RE.match(a.get("href", "").strip())
                if not m:
                    continue
                slug = m.group(1)
                if slug in _NON_PREF_SLUGS or slug in slugs:
                    continue
                slugs.append(slug)
        if not slugs:
            self.logger.warning("トップページから都道府県リンクを取得できませんでした。固定リストを使用します。")
            return list(_FALLBACK_PREF_SLUGS)
        return slugs

    def _estimate_pref_items(self, soup) -> int:
        """ページャの「Nページ」表記から都道府県あたりの件数を概算する。"""
        pager = soup.select_one("ul.page_nav")
        pages = 1
        if pager:
            m = re.search(r"(\d+)\s*ページ", pager.get_text(" ", strip=True))
            if m:
                pages = int(m.group(1))
        return pages * _ITEMS_PER_PAGE

    # ------------------------------------------------------------------ 詳細

    def _scrape_detail(self, detail_url: str, job_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {key: "" for key in self.EXTRA_COLUMNS}
        item[Schema.URL] = detail_url
        item["求人ID"] = job_id

        job_pairs, corp_pairs = self._split_tables(soup)

        # --- 求人情報 ---
        for label, column in _JOB_EXTRA_LABELS.items():
            if label in job_pairs:
                item[column] = job_pairs[label]

        item[Schema.CAT_SITE] = job_pairs.get("職種", "")
        item[Schema.TIME] = _work_hours(job_pairs.get("勤務時間", ""))

        salary = job_pairs.get("給与", "")
        for line in re.split(r"\s*/\s*", salary):
            if line.startswith("月給"):
                item["月給"] = line.split("：", 1)[-1].strip()
            elif line.startswith("年収"):
                item["年収"] = line.split("：", 1)[-1].strip()

        # --- 法人情報 ---
        item[Schema.NAME] = corp_pairs.get("法人名", "") or job_pairs.get("勤務先", "")
        item[Schema.EMP_NUM] = corp_pairs.get("従業員数", "") or job_pairs.get("従業員数", "")
        item[Schema.CAP] = corp_pairs.get("資本金", "")
        item[Schema.OPEN_DATE] = _to_iso_date(corp_pairs.get("設立日", ""))
        item[Schema.HP] = corp_pairs.get("法人URL", "")
        item[Schema.LOB] = corp_pairs.get("事業内容", "")

        address = corp_pairs.get("住所", "") or job_pairs.get("勤務地", "")
        post_code, addr = self._split_address(address)
        item[Schema.POST_CODE] = post_code
        item[Schema.ADDR] = addr
        m = _PREF_PATTERN.search(addr)
        item[Schema.PREF] = m.group(1) if m else ""

        # --- JSON-LD (特徴・掲載日) ---
        ld = self._json_ld(soup)
        if ld:
            item["掲載日"] = str(ld.get("datePosted", "") or "")
            m = _FEATURE_RE.search(str(ld.get("description", "") or ""))
            if m:
                item["特徴"] = _clean(m.group(1))
            if not item[Schema.PREF]:
                region = (ld.get("jobLocation") or {}).get("address", {}).get("addressRegion")
                item[Schema.PREF] = str(region or "")

        if not item.get(Schema.NAME):
            return None
        return item

    def _split_tables(self, soup) -> tuple[dict, dict]:
        """dl.jobtable を「求人情報」「法人情報」に振り分けて dt→dd の辞書にする。"""
        job_pairs: dict[str, str] = {}
        corp_pairs: dict[str, str] = {}

        for dl in soup.select("dl.jobtable"):
            pairs: dict[str, str] = {}
            for dt in dl.select("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                key = _label(dt.get_text())
                if not key or key in pairs:
                    continue
                pairs[key] = _clean_cell(dd.get_text(" / ", strip=True))
            if not pairs:
                continue
            # 「法人名」を持つ dl が法人情報、それ以外は求人情報
            if "法人名" in pairs:
                corp_pairs.update(pairs)
            else:
                job_pairs.update(pairs)

        return job_pairs, corp_pairs

    def _split_address(self, address: str) -> tuple[str, str]:
        """「〒815-0031 / 福岡県…」から郵便番号と住所を切り出す。"""
        text = _clean(address).replace(" / ", " ")
        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            code = m.group(1)
            post_code = code if "-" in code else f"{code[:3]}-{code[3:]}"
            text = text[: m.start()] + text[m.end():]
        return post_code, _clean(text)

    def _json_ld(self, soup) -> dict | None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "JobPosting"), None)
            if isinstance(data, dict):
                return data
        return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ElecareerCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://elecareer.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

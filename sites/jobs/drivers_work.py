"""
ドライバーズワーク — タクシー・ハイヤー専門の求人情報サイト (drivers-work.com)

取得対象:
    全国のタクシー / ハイヤー・役員運転手 / その他ドライバー求人 (約1,123件)。
    同一企業が複数拠点・複数職種で求人を出すため集約せず **1求人 = 1行** で保持する。

取得フロー:
    職種タクソノミ一覧 (/search/jobtype/{taxi,hire,other}/ + /page/N/, 20件/ページ)
    の `div.result-box[data-id]` から求人ID を取得し、詳細ページ
    (/job/{求人ID}/) を訪問して 1件取得するごとに即 yield する (Pattern B)。
    3系統の和集合で全件を網羅し、求人ID で重複排除する。

ページ構造 (詳細ページは2レイアウト):
    1) 通常掲載        : table.company-table.company (会社名/本社所在地/設立/資本金/
                         代表者名/従業員数/車両台数) + dl.m-offeringl_items (dt>h3 ラベル)
    2) ハローワーク提供: table.company-table (会社名/所在地/設立/資本金/代表者名/
                         従業員数) + dl (ラベルは「勤務地住所」「業務内容」等)
    いずれも table.m-info__inner に 営業所名 / 職種 / 勤務地 / 給与 のサマリを持つ。

電話番号について:
    求人ページに掲載されている 0120-951-263 はサイト運営元 (株式会社ミライユ) の
    共通問い合わせ窓口であり企業の直通番号ではない。誤取得を避けるため
    TEL / 電話番号 は一切取得しない。

サイトに存在しないため取得しないカラム:
    事業内容 (Schema.LOB) / 募集人数 / 郵便番号 — 全求人ページで項目自体が存在しない。
    「企業紹介」「スタッフの声」「採用担当者の声」「車両設備」は長文の自由記述のため
    著作権リスクを考慮して取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/drivers_work.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id drivers_work
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

_SITE_NAME = "ドライバーズワーク"

# 巡回する職種タクソノミ (jt_taxi-sitemap.xml 由来)。値は職種が取得できない
# ハローワーク提供求人向けのフォールバック表記。
_JOB_TYPES = {
    "taxi": "タクシードライバー",
    "hire": "ハイヤー・役員運転手",
    "other": "その他",
}

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 「代表取締役社長 加藤 寛治」→ 役職 / 氏名 に分割する
_POSITION_PATTERN = re.compile(
    r"^(代表取締役社長執行役員|代表取締役会長兼社長|代表取締役社長|代表取締役会長|"
    r"代表取締役専務|代表取締役副社長|代表取締役CEO|代表取締役|取締役社長|取締役会長|"
    r"執行役員社長|代表執行役社長|代表社員|代表者|理事長|会長|社長|代表)\s*"
)

# 元号 → 元年に対応する西暦
_ERA_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}
_ERA_PATTERN = re.compile(r"^(明治|大正|昭和|平成|令和)\s*(\d+|元)年")
_DATE_PATTERN = re.compile(r"(\d{4})\s*年\s*(?:(\d{1,2})\s*月\s*(?:(\d{1,2})\s*日)?)?")
_PAGE_PATTERN = re.compile(r"/page/(\d+)/")
_POSTED_PATTERN = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

# 詳細ページ dl の見出し → EXTRA カラム名
# (備考で明示的に取得指示のあった項目のみ。「企業紹介」「スタッフの声」
#  「採用担当者の声」「選考プロセス」等の自由記述は著作権リスクのため除外)
_DL_EXTRA = {
    "雇用形態": "雇用形態",
    "給与": "給与",
    "勤務時間": "勤務時間",
    "待遇・福利厚生": "福利厚生",
    "応募条件": "応募条件",
    "受動喫煙対策": "受動喫煙対策",
}
# 勤務地を表す dl 見出し (通常掲載 / ハローワーク提供でラベルが異なる)
_DL_WORKPLACE = ("勤務地", "勤務地住所")
_DL_HOLIDAY = "休日・休暇"
_DL_JOBTYPE = "職種"


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"[ \t　]+", " ", (text or "").replace("\xa0", " ")).strip()


def _multiline(node) -> str:
    """<br> 区切りの複数行テキストを改行なしの1行に畳んで返す。"""
    if node is None:
        return ""
    lines = [_clean(l) for l in node.get_text("\n").split("\n")]
    return " ".join(l for l in lines if l)


def _lines(node) -> list[str]:
    """<br> 区切りのテキストを空行を除いた行リストで返す。"""
    if node is None:
        return []
    return [l for l in (_clean(l) for l in node.get_text("\n").split("\n")) if l]


def _split_pref(addr: str) -> tuple[str, str]:
    """住所文字列を (都道府県, 市区町村以降) に分割する。"""
    addr = _clean(addr)
    m = _PREF_PATTERN.match(addr)
    if not m:
        return "", addr
    return m.group(1), addr[m.end():].strip()


def _normalize_founded(text: str) -> str:
    """設立表記を可能な範囲で YYYY-MM-DD / YYYY-MM / YYYY に正規化する。

    「大正9年3月」のような元号表記も西暦に換算する。年が読み取れない場合は
    原文をそのまま返す (情報を落とさないため)。
    """
    text = _clean(text)
    if not text or text == "-":
        return ""

    work = text
    era = _ERA_PATTERN.match(work)
    if era:
        num = 1 if era.group(2) == "元" else int(era.group(2))
        work = f"{_ERA_BASE[era.group(1)] + num}年" + work[era.end():]

    m = _DATE_PATTERN.search(work)
    if not m:
        return text
    year, month, day = m.group(1), m.group(2), m.group(3)
    if month and day:
        return f"{year}-{int(month):02d}-{int(day):02d}"
    if month:
        return f"{year}-{int(month):02d}"
    return year


class DriversWorkCrawler(StaticCrawler):
    """ドライバーズワーク スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "営業所名",
        "勤務地住所",
        "勤務地へのアクセス",
        "雇用形態",
        "給与",
        "勤務時間",
        "福利厚生",
        "応募条件",
        "車両台数",
        "受動喫煙対策",
        "掲載日",
        "掲載サイト名",
    ]

    def parse(self, url: str):
        seen: set[str] = set()

        for slug, jobtype_label in _JOB_TYPES.items():
            list_root = urllib.parse.urljoin(url, f"search/jobtype/{slug}/")
            page = 1
            max_page = 1

            while True:
                page_url = list_root if page == 1 else f"{list_root}page/{page}/"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                boxes = soup.select("div.result-box[data-id]")
                if not boxes:
                    break

                # wp-pagenavi の /page/N/ リンクから最終ページ番号を更新する
                pager = soup.select_one(".wp-pagenavi")
                if pager:
                    nums = [
                        int(m.group(1))
                        for a in pager.select("a[href]")
                        for m in [_PAGE_PATTERN.search(a["href"])]
                        if m
                    ]
                    if nums:
                        max_page = max(max_page, max(nums))

                for box in boxes:
                    job_id = (box.get("data-id") or "").strip()
                    if not job_id or job_id in seen:
                        continue
                    seen.add(job_id)

                    detail_url = urllib.parse.urljoin(url, f"job/{job_id}/")
                    try:
                        item = self._scrape_detail(detail_url, jobtype_label)
                    except Exception as e:
                        self.logger.warning(f"詳細取得失敗 {detail_url}: {e}")
                        continue
                    if item:
                        yield item

                if page >= max_page:
                    break
                page += 1

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url: str, jobtype_label: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        info = self._parse_info_table(soup)      # table.m-info__inner のサマリ
        company = self._parse_company_table(soup)  # table.company-table の会社概要
        dl = self._parse_definition_lists(soup)  # dl.m-offeringl_items 等の募集要項

        # 営業所名: サマリ → h1 ("〇〇株式会社 △△営業所(東京都品川区)の求人募集")
        office = info.get("営業所名", "")
        if not office:
            h1 = soup.select_one("h1")
            if h1:
                office = re.sub(r"\((?:[^()]*)\)の求人募集\s*$", "", _clean(h1.get_text())).strip()

        # 企業名: 会社概要の「会社名」を正とし、無ければ営業所名で代用する
        name = company.get("会社名", "") or office
        if not name:
            return None

        # 本社所在地 (通常掲載=「本社所在地」/ ハローワーク提供=「所在地」)
        hq = company.get("本社所在地", "") or company.get("所在地", "")
        pref, addr = _split_pref(hq)

        # 勤務地 (住所 + アクセス)。dl の該当行が無ければサマリの「勤務地」で代用
        work_addr, access = dl.get("_workplace", ("", ""))
        if not work_addr:
            work_addr = info.get("勤務地", "")

        rep_raw = company.get("代表者名", "")
        pos_match = _POSITION_PATTERN.match(rep_raw)
        position = pos_match.group(1) if pos_match else ""
        rep_name = rep_raw[pos_match.end():].strip() if pos_match else rep_raw

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.FAC_NAME: office,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.REP_NM: rep_name,
            Schema.POS_NM: position,
            Schema.EMP_NUM: company.get("従業員数", ""),
            Schema.CAP: self._blank_dash(company.get("資本金", "")),
            Schema.OPEN_DATE: _normalize_founded(company.get("設立", "")),
            # 職種: dl → サマリ → 巡回中の職種タクソノミ名
            Schema.CAT_SITE: dl.get(_DL_JOBTYPE, "") or info.get("職種", "") or jobtype_label,
            # 「定休日」に相当する項目は無いため、最も近い「休日・休暇」を格納する
            Schema.HOLIDAY: dl.get(_DL_HOLIDAY, ""),
            Schema.HP: self._company_site(soup),
            "営業所名": office,
            "勤務地住所": work_addr,
            "勤務地へのアクセス": access,
            "車両台数": self._blank_dash(company.get("車両台数", "")),
            "掲載日": self._posted_date(soup),
            "掲載サイト名": _SITE_NAME,
        }
        for label, column in _DL_EXTRA.items():
            item[column] = dl.get(label, "")
        return item

    # ------------------------------------------------------------------
    # 部分パーサ
    # ------------------------------------------------------------------
    @staticmethod
    def _blank_dash(value: str) -> str:
        """未掲載を表す "-" を空文字に落とす。"""
        return "" if value.strip() in ("-", "ー", "―", "−") else value

    def _parse_info_table(self, soup) -> dict:
        """table.m-info__inner (営業所名 / 職種 / 勤務地 / 給与 / 求人番号) を辞書化する。"""
        result: dict[str, str] = {}
        table = soup.select_one("table.m-info__inner")
        if not table:
            return result
        for tr in table.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())
            # 「▶同じ無線グループの求人を探す」等の付随リンクは値に含めない
            for junk in td.select("p.musen_link, .js-shard_tab"):
                junk.decompose()
            result[label] = _multiline(td)
        return result

    def _parse_company_table(self, soup) -> dict:
        """table.company-table (会社概要) を辞書化する。2レイアウト共通。"""
        result: dict[str, str] = {}
        for table in soup.select("table.company-table"):
            if table.select_one("th h3") and not table.select_one("tr th"):
                continue
            for tr in table.select("tr"):
                th, td = tr.find("th"), tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                if label not in (
                    "会社名", "本社所在地", "所在地", "設立", "資本金",
                    "代表者名", "従業員数", "車両台数",
                ):
                    continue
                # 住所欄に埋め込まれた Google Map iframe を除去する
                for junk in td.select("iframe, .m-offeringl_row__iframe, .js-shard_tab"):
                    junk.decompose()
                value = _multiline(td)
                if value and label not in result:
                    result[label] = value
        return result

    def _parse_definition_lists(self, soup) -> dict:
        """求人募集要項の dl (dt>h3 がラベル, dd が値) を辞書化する。

        勤務地だけは「住所」と「アクセス」に分割し `_workplace` キーに格納する。
        """
        result: dict[str, object] = {}
        for dl in soup.select("div.m-offeringl dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                label = _clean(dt.get_text())
                for junk in dd.select(".js-shard_tab, iframe, .m-offeringl_row__iframe"):
                    junk.decompose()

                if label in _DL_WORKPLACE and "_workplace" not in result:
                    text_box = dd.select_one(".m-offeringl_row__text") or dd
                    parts = _lines(text_box)
                    result["_workplace"] = (
                        parts[0] if parts else "",
                        " ".join(parts[1:]) if len(parts) > 1 else "",
                    )
                    continue

                if label not in result:
                    result[label] = _multiline(dd)
        return result

    def _company_site(self, soup) -> str:
        """JSON-LD JobPosting の hiringOrganization.sameAs から企業HPを取得する。"""
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text() or ""
            if "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            org = data.get("hiringOrganization") if isinstance(data, dict) else None
            if isinstance(org, dict):
                same_as = org.get("sameAs") or ""
                if isinstance(same_as, str) and same_as.startswith("http"):
                    return same_as
        return ""

    @staticmethod
    def _posted_date(soup) -> str:
        """「掲載日：2023年09月01日」を YYYY-MM-DD で返す。"""
        el = soup.select_one("p.m-job_about__date")
        if not el:
            return ""
        m = _POSTED_PATTERN.search(_clean(el.get_text()))
        if not m:
            return ""
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


if __name__ == "__main__":
    scraper = DriversWorkCrawler()
    scraper.execute("https://www.drivers-work.com/")

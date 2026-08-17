"""
工場ワークス (04510.jp) — 製造業求人クローラー (製造系職種 / TEL必須 / 直近3ヶ月)

既存の site_id `works` (jobs.works) との違い:
    - TEL カラムを必ず埋める (電話番号が一切取れない求人は yield しない)
    - 物流・軽作業・清掃・ドライバー・工場内事務系の職種を除外し、
      製造系職種 (加工・食品製造・機械等) の求人のみを対象にする
    - 求人掲載日が直近3ヶ月以内のものに絞り込む

取得カラム (依頼要件):
    会社名 / 募集職種 / 応募先電話番号 / 住所 / 従業員数 / 求人掲載日 / 求人URL

取得フロー:
    1. トップページ (sites.yml の url) から都道府県別求人一覧
       (/jobs/areas/{地方}/{県}/) を列挙
    2. 各県ページから職種別一覧 (/jobs/areas/{地方}/{県}/occupations/{slug}/) の
       リンクを拾い、製造系職種 (_MANUFACTURING_OCCUPATIONS) のみ残す
    3. 職種別一覧を ?page=N で全ページ巡回し、求人カードから詳細 URL を収集
    4. 求人詳細 (/jobs/{jobId}/?companyId={cid}) を 1 件取得するごとに即 yield
       - JSON-LD (JobPosting) から 掲載日 / 会社名 / 給与 / 勤務地 を取得
       - dl.p-informationListView__list の dt/dd から
         職種 / 業種 / 従業員数 / 所在地 / 電話番号 を取得
       - 会社詳細ページ (/app/m/fawp/a/S15C/clientId/{cid}/...) を companyId 単位で
         キャッシュ取得し、郵便番号 / 従業員数 / 資本金 / 設立 / HP / 連絡先TEL を補完

電話番号の扱い (STX 名寄せのため):
    求人詳細に載る 0037-641-8xxxxxxx は求人ごとに異なるコールトラッキング番号で、
    企業マスタとの名寄せには使えない。そのため
        Schema.TEL      … 企業の実電話番号 (会社詳細の連絡先TEL) を優先
        EXTRA 応募先電話番号 … 求人ページに掲載された電話応募番号 (0037-…) をそのまま
    とし、どちらも取れない求人はスキップする。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/04510_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id 04510_2
"""

import json
import logging
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 都道府県別求人一覧 (例: https://04510.jp/jobs/areas/kanto/tokyo/)
_AREA_PATTERN = re.compile(r"/jobs/areas/[a-z]+/[a-z]+/?$")
# 職種別求人一覧 (例: https://04510.jp/jobs/areas/kanto/tokyo/occupations/kako/)
_OCCUPATION_PATTERN = re.compile(r"/jobs/areas/[a-z]+/[a-z]+/occupations/([a-z]+)/?$")
# 求人詳細 (例: https://04510.jp/jobs/28316971/?companyId=1118440008)
_JOB_PATTERN = re.compile(r"/jobs/(\d+)/\?companyId=(\d+)")
# 一覧ページの求人カード
# (「その他エリアのおすすめ求人」ブロックが常時5件混ざるため wrap-body でスコープ必須。
#  スコープしないと求人0件のページでも5件返り、ページ送りの終了判定が壊れる)
_CARD_SELECTOR = (
    "div.p-contentSearchResult__wrap-body "
    "li.p-contentSearchResult__jobList__item a[href*='/jobs/']"
)
_TOTAL_PATTERN = re.compile(r"([\d,]+)\s*件")
_TEL_PATTERN = re.compile(r"0[\d\-]{8,}\d")
# 求人ごとに払い出されるコールトラッキング番号 (企業の実番号ではない)
_TRACKING_TEL_PREFIX = "0037"
_POST_CODE_PATTERN = re.compile(r"^(\d{3})-?(\d{4})\s*")
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_DATE_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# 対象とする製造系職種の slug (サイトの職種カテゴリ 30 種のうち 20 種)。
# 除外しているのは製造工程ではない職種:
#   shiwake(仕分け・梱包・ピッキング) / buhin(部品供給・充填・運搬) /
#   fokurifuto(フォークリフト) / tamakake(玉掛け) / kuren(クレーン) /
#   doraiba(ドライバー) / keisagyo(その他軽作業・物流・配送) /
#   seisou(清掃・洗浄) / kojonaijimu(工場内事務) / syuri(修理)
_MANUFACTURING_OCCUPATIONS = frozenset({
    "kako",             # 加工
    "syokuhinkako",     # 食品加工
    "kumitate",         # 組立・組付け
    "mashin",           # マシンオペレーター
    "seizou",           # その他製造・工場系
    "kenpin",           # 検品・検査・調整
    "baritori",         # バリ取り・研磨
    "puresu",           # プレス・板金・塗装
    "tanzo",            # 鋳造・鍛造
    "yosetsu",          # 溶接
    "handa",            # ハンダ付け
    "ncsenban",         # ＮＣ旋盤
    "oem",              # OEM
    "kanagatasekkei",   # 金型設計
    "seisangijyutsu",   # 生産技術
    "seizogijyutsu",    # 製造技術
    "seisanjimu",       # 生産管理・生産事務・工程管理
    "hinshitsukanri",   # 品質管理・品質保証
    "shiken",           # 試験・実験・評価
    "mentenansu",       # メンテナンス・保守・保全
})

# 求人詳細 / 会社詳細ページから拾うラベル (自由記述プロースのラベルは意図的に含めない)
_JOB_LABELS = (
    "求人掲載企業名", "所在地", "電話番号", "職種", "業種",
    "雇用形態", "勤務時間", "従業員数",
)
_COMPANY_LABELS = (
    "会社名", "所在地", "連絡先TEL", "電話番号", "従業員数", "資本金", "設立",
    "ホームページURL", "労働者派遣事業許可番号", "有料職業紹介許可番号",
)

# 給与の単位表記
_SALARY_UNIT = {"HOUR": "時給", "DAY": "日給", "MONTH": "月給", "YEAR": "年収"}
# JSON-LD employmentType のフォールバック表記
_EMPLOYMENT_TYPE = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣",
    "OTHER": "その他",
}


class KojoWorksManufacturingCrawler(StaticCrawler):
    """工場ワークス (製造系職種 / TEL必須 / 直近3ヶ月) スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    # 掲載日が何ヶ月以内の求人を取得するか (依頼: 直近3ヶ月以内掲載分)
    RECENT_MONTHS = 3
    # 1 職種一覧あたりのページ上限 (無限ループ防止のセーフティ)
    MAX_PAGES = 500

    EXTRA_COLUMNS = [
        "求人掲載日",
        "募集職種",
        "応募先電話番号",
        "雇用形態",
        "給与",
        "勤務地都道府県",
        "勤務地市区町村",
        "労働者派遣事業許可番号",
        "有料職業紹介許可番号",
        "求人ID",
        "企業ID",
    ]

    def prepare(self):
        # companyId 単位で会社詳細ページをキャッシュする (同一企業が多数の求人を出稿するため)
        self._company_cache: dict[str, dict] = {}
        # 同一求人が複数の職種カテゴリに載るため、求人URL 単位で重複排除する
        self._seen_jobs: set[str] = set()
        self._cutoff = self._months_ago(self.RECENT_MONTHS)
        self._skipped_old = 0
        self._skipped_no_tel = 0
        logger.info("掲載日の下限: %s 以降の求人のみ取得します", self._cutoff)

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        area_urls = self._collect_area_urls(url)
        logger.info("都道府県別一覧: %d 件", len(area_urls))

        estimated = 0
        for area_url in area_urls:
            for occ_url in self._collect_occupation_urls(area_url):
                for page in range(1, self.MAX_PAGES + 1):
                    list_url = occ_url if page == 1 else f"{occ_url}?page={page}"
                    soup = self.get_soup(list_url)
                    if soup is None:
                        break

                    if page == 1:
                        total = self._extract_total(soup)
                        if total:
                            # 職種は重複掲載があるため、あくまで進捗表示用の概算
                            estimated += total
                            self.total_items = estimated

                    job_urls = self._extract_job_urls(list_url, soup)
                    if not job_urls:
                        break

                    for job_url in job_urls:
                        if job_url in self._seen_jobs:
                            continue
                        self._seen_jobs.add(job_url)
                        try:
                            item = self._scrape_detail(job_url)
                        except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                            logger.warning(
                                "求人詳細の解析に失敗 (スキップ): %s — %s", job_url, e
                            )
                            continue
                        if item:
                            yield item

        logger.info(
            "除外件数: 掲載日が古い %d 件 / 電話番号なし %d 件",
            self._skipped_old, self._skipped_no_tel,
        )

    # -------------------------------------------------------------- 一覧ページ

    def _collect_area_urls(self, url: str) -> list[str]:
        """トップページから都道府県別一覧 URL を列挙する。"""
        soup = self.get_soup(url)
        if soup is None:
            return [url]

        area_urls: list[str] = []
        for a in soup.select("a[href]"):
            href = urljoin(url, a["href"])
            if _AREA_PATTERN.search(href) and href not in area_urls:
                area_urls.append(href if href.endswith("/") else href + "/")

        if not area_urls:
            # 引数 url 自体が一覧ページだった場合のフォールバック
            logger.warning(
                "都道府県別一覧が見つからないため、指定 URL を一覧として扱います: %s", url
            )
            return [url]
        return area_urls

    def _collect_occupation_urls(self, area_url: str) -> list[str]:
        """都道府県ページから製造系職種の一覧 URL だけを取り出す。"""
        soup = self.get_soup(area_url)
        if soup is None:
            return []

        occ_urls: list[str] = []
        for a in soup.select("a[href]"):
            href = urljoin(area_url, a["href"])
            m = _OCCUPATION_PATTERN.search(href)
            if m is None or m.group(1) not in _MANUFACTURING_OCCUPATIONS:
                continue
            full = href if href.endswith("/") else href + "/"
            if full not in occ_urls:
                occ_urls.append(full)

        if not occ_urls:
            logger.warning("製造系職種の一覧が見つかりません: %s", area_url)
        return occ_urls

    def _extract_job_urls(self, list_url: str, soup) -> list[str]:
        """一覧ページの求人カードから詳細 URL を取り出す。

        カード内には応募フォーム (/entry/jobs/?jobId=…) へのリンクも含まれるが、
        _JOB_PATTERN に一致しないため自然に除外される。
        """
        urls: list[str] = []
        for a in soup.select(_CARD_SELECTOR):
            href = a.get("href") or ""
            if not _JOB_PATTERN.search(href):
                continue
            full = urljoin(list_url, href)
            if full not in urls:
                urls.append(full)
        return urls

    @staticmethod
    def _extract_total(soup) -> int | None:
        """一覧ページの総件数表示 (例: 「91 件」) を取得する。"""
        node = soup.select_one(
            "div.p-contentSearchResult__wrap-head, div.p-contentSearchResult"
        )
        if node is None:
            return None
        m = _TOTAL_PATTERN.search(node.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else None

    # -------------------------------------------------------------- 詳細ページ

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        job_ld = self._find_job_posting(soup)
        labels = self._label_map(soup, "dl.p-informationListView__list", _JOB_LABELS)

        posted = self._parse_date(job_ld.get("datePosted", ""))
        if posted and posted < self._cutoff:
            # 依頼の指示: 直近3ヶ月以内の掲載分のみ取得する
            self._skipped_old += 1
            return None

        m = _JOB_PATTERN.search(url)
        job_id = m.group(1) if m else ""
        company_id = m.group(2) if m else ""
        company = self._get_company(url, soup, company_id)

        # ---- 電話番号。実番号 (名寄せ用) と応募先番号 (コールトラッキング) を分けて持つ
        apply_tel = self._pick_tel(labels.get("電話番号")) or self._pick_tel(
            company.get("電話番号")
        )
        real_tel = self._pick_tel(company.get("連絡先TEL"))
        if real_tel.startswith(_TRACKING_TEL_PREFIX):
            real_tel = ""
        # 実番号が取れなければ応募先番号で代用する (TEL は必須カラムのため)
        tel = real_tel or apply_tel
        if not tel:
            # 依頼の指示: STX 名寄せに使うため電話番号のない求人は出力しない
            self._skipped_no_tel += 1
            return None

        # ---- 会社名
        name = labels.get("求人掲載企業名") or company.get("会社名") or ""
        if not name:
            org = job_ld.get("hiringOrganization") or {}
            name = org.get("name", "") if isinstance(org, dict) else ""
        # 一部の企業名は末尾に社内管理コードが付く (例: 「◯◯株式会社　本社（66851821）」)
        name = re.sub(r"\s*[（(]\d{5,}[）)]\s*$", "", name).strip()

        # ---- 住所 (掲載企業の所在地)。会社詳細側は先頭に郵便番号が付く
        addr = labels.get("所在地") or ""
        post_code = ""
        co_addr = company.get("所在地") or ""
        pm = _POST_CODE_PATTERN.match(co_addr)
        if pm:
            post_code = f"{pm.group(1)}-{pm.group(2)}"
            co_addr = co_addr[pm.end():].strip()
        addr = addr or co_addr
        addr = addr.split("※")[0].strip()
        prefm = _PREF_PATTERN.search(addr)
        pref = prefm.group(1) if prefm else ""

        # ---- 募集職種 / 業種
        occupation = labels.get("職種") or job_ld.get("industry") or ""
        industry = self._last_line(labels.get("業種"))
        if not industry:
            # 期間工系のレイアウトには 職種/業種 の dl が無いため、
            # JSON-LD の title 末尾の括弧内 (例:「加工（医療・福祉・介護・製薬）」) で補う
            title = job_ld.get("title") or ""
            im = re.search(r"（([^（）]+)）\s*$", title)
            industry = im.group(1) if im else ""

        address_ld = (job_ld.get("jobLocation") or {}).get("address") or {}

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.EMP_NUM: labels.get("従業員数") or company.get("従業員数") or "",
            Schema.CAP: company.get("資本金") or "",
            Schema.OPEN_DATE: company.get("設立") or "",
            Schema.HP: company.get("ホームページURL") or "",
            Schema.CAT_SITE: industry,
            Schema.TIME: self._clean_hours(labels.get("勤務時間") or job_ld.get("workHours")),
            Schema.URL: url,
            "求人掲載日": posted.isoformat() if posted else "",
            "募集職種": occupation,
            "応募先電話番号": apply_tel,
            "雇用形態": labels.get("雇用形態") or self._employment_type(job_ld),
            "給与": self._format_salary(job_ld.get("baseSalary")),
            "勤務地都道府県": address_ld.get("addressRegion", ""),
            "勤務地市区町村": address_ld.get("addressLocality", ""),
            "労働者派遣事業許可番号": company.get("労働者派遣事業許可番号") or "",
            "有料職業紹介許可番号": company.get("有料職業紹介許可番号") or "",
            "求人ID": job_id,
            "企業ID": company_id,
        }

    def _get_company(self, job_url: str, soup, company_id: str) -> dict:
        """会社詳細ページのラベル値を取得する (companyId 単位でキャッシュ)。"""
        if company_id and company_id in self._company_cache:
            return self._company_cache[company_id]

        link = soup.select_one("a[href*='/clientId/']")
        if link is None:
            self._company_cache[company_id] = {}
            return {}

        company_url = urljoin(job_url, link["href"])
        csoup = self.get_soup(company_url)
        data = self._label_map(csoup, "dl", _COMPANY_LABELS) if csoup is not None else {}
        if company_id:
            self._company_cache[company_id] = data
        return data

    # ------------------------------------------------------------------ 補助

    @staticmethod
    def _label_map(soup, dl_selector: str, wanted: tuple[str, ...]) -> dict[str, str]:
        """dt をラベル、直後の dd を値として辞書化する (同名ラベルは最初の1件を採用)。"""
        out: dict[str, str] = {}
        for dl in soup.select(dl_selector):
            for dt in dl.select("dt"):
                label = dt.get_text(" ", strip=True)
                if label not in wanted or label in out:
                    continue
                dd = dt.find_next_sibling("dd")
                if dd is not None:
                    out[label] = dd.get_text(" ", strip=True)
        return out

    @staticmethod
    def _find_job_posting(soup) -> dict:
        for script in soup.select("script[type='application/ld+json']"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}

    @staticmethod
    def _months_ago(months: int) -> date:
        today = date.today()
        month = today.month - months
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        # 月末日の差異を吸収する (例: 5/31 の3ヶ月前 → 2/28)
        day = today.day
        while day > 0:
            try:
                return date(year, month, day)
            except ValueError:
                day -= 1
        return today

    @staticmethod
    def _parse_date(value: str) -> date | None:
        m = _DATE_PATTERN.search(value or "")
        if not m:
            return None
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _pick_tel(text: str | None) -> str:
        """注意書きを含む dd テキストから電話番号だけを取り出す。"""
        if not text:
            return ""
        m = _TEL_PATTERN.search(text.replace(" ", ""))
        return m.group(0) if m else ""

    @staticmethod
    def _last_line(text: str | None) -> str:
        """業種の dd は「勤務先の説明 <br> 業種名」となる場合があるため最終行を採用する。"""
        if not text:
            return ""
        parts = [p.strip() for p in re.split(r"[\n]+", text) if p.strip()]
        return parts[-1] if parts else ""

    @staticmethod
    def _clean_hours(text: str | None) -> str:
        """勤務時間から「備考：〜」以降 (自由記述) を落として時間帯だけ残す。"""
        if not text:
            return ""
        return re.split(r"備考[:：]", text)[0].strip()

    @staticmethod
    def _employment_type(job_ld: dict) -> str:
        types = job_ld.get("employmentType") or []
        if isinstance(types, str):
            types = [types]
        return "/".join(_EMPLOYMENT_TYPE.get(t, t) for t in types)

    @staticmethod
    def _format_salary(base_salary) -> str:
        if not isinstance(base_salary, dict):
            return ""
        value = base_salary.get("value") or {}
        if not isinstance(value, dict):
            return ""
        unit = _SALARY_UNIT.get(value.get("unitText", ""), "")
        lo = value.get("minValue")
        hi = value.get("maxValue")
        single = value.get("value")
        if single is not None:
            return f"{unit}{int(single):,}円"
        if lo is not None and hi is not None:
            return f"{unit}{int(lo):,}円～{int(hi):,}円"
        if lo is not None:
            return f"{unit}{int(lo):,}円～"
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KojoWorksManufacturingCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://04510.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

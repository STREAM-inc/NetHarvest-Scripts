"""
キレイビズ (kireibiz.jp) — 美容業界専門求人サイト (株式会社スタッフエージェント運営)

取得対象:
    - 全求人横断一覧 (/jobs/) 経由で巡回する各求人詳細ページ (/jobs/id-XXXXX/)
    - 2026-10-06 時点 25,211 件 / 10 件・頁 (全 2,522 頁)

取得フロー:
    1. 引数 url (= https://kireibiz.jp/jobs/) を 1 ページ目として取得し、
       ページャの `aria-label="pagination.last"` から総ページ数を確定する
    2. 一覧の求人リンク (/jobs/id-{id}/) を抽出
    3. 1 件ずつ詳細ページを取得し、JSON-LD (schema.org JobPosting) と
       `dl.uk-description-list` の募集情報からフィールドを抽出して即 yield
    4. `?page=N` で次ページへ進む

備考 (依頼):
    職種別・都道府県別 URL には分割せず、全求人横断一覧のみを起点とする。

実装メモ:
    - JSON-LD JobPosting は description 等に生の改行を含み json.loads が
      `Invalid control character` で失敗するため strict=False でパースする。
    - 電話番号はサイト共通の応募フリーダイヤル (0120-431-866) のみで、
      店舗固有番号は掲載されていない。全件同一値で名寄せに使えないため
      Schema.TEL は空にし、EXTRA の「応募窓口電話番号」に入れる。
    - 「採用予定人数 / 募集人数」はサイト上に項目が存在しない (常に空)。
    - 「定休日」も独立項目が無いため、「休日・休暇」内の "定休" を含む行を抽出する。
    - サロン紹介文 (JSON-LD description / 「仕事内容・アピール」の PR 本文) は
      長文の自由記述プロースのため著作権リスクで取得しない。
      「事業内容」は定型の短文である責務記述 (responsibilities / 仕事内容) を採用する。

実行方法:
    python scripts/sites/jobs/kireibiz.py
    docker compose exec worker python /app/bin/run_flow.py --site-id kireibiz
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 一覧ページ中の求人詳細リンク (/jobs/id-16306/)
_JOB_ID_PATTERN = re.compile(r"/jobs/id-(\d+)/")
# ページャ末尾の「>>」リンク (/jobs/?page=2522) から総ページ数を得る
_LAST_PAGE_PATTERN = re.compile(r"[?&]page=(\d+)")
# 検索結果件数 (例: 25211件)
_TOTAL_PATTERN = re.compile(r"検索結果:\s*</?[^>]*>?\s*([\d,]+)\s*件")
# 勤務時間テキスト中の【営業時間】行
_OPEN_HOURS_PATTERN = re.compile(r"【営業時間】\s*(.+)")

# 詳細ページ `dl.uk-description-list` のラベル → 内部キー
_DL_LABELS = {
    "店舗名": "shop",
    "雇用形態": "emp_type",
    "募集職種": "occupation",
    "給与": "salary",
    "休日・休暇": "holiday",
    "待遇": "benefits",
    "勤務時間": "work_hours",
    "仕事内容": "responsibilities",
    "応募資格": "qualifications",
    "勤務地住所": "address",
    "アクセス・交通手段": "access",
}

# JSON-LD employmentType コード → 日本語ラベル (dl が欠けた場合のフォールバック)
_EMP_TYPE_MAP = {
    "FULL_TIME": "正社員",
    "PART_TIME": "パート・アルバイト",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣社員",
    "INTERN": "インターン",
    "OTHER": "その他",
}

_SITE_NAME = "キレイビズ"


def _clean(text: str) -> str:
    """改行コードを LF に揃え、連続改行・前後の空白を詰める。"""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"\n{2,}", "\n", text).strip()

# 都道府県 (住所フォールバック時に先頭の都道府県を剥がす)
_PREFECTURES = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    "東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    "滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    "香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_PATTERN = re.compile(rf"^({_PREFECTURES})")


class Kireibiz(StaticCrawler):
    """キレイビズ スクレイパー (一覧 → 詳細)"""

    DELAY = 0.5

    EXTRA_COLUMNS = [
        "雇用形態",          # 正社員・業務委託 など
        "募集職種",          # スタイリスト(美容師) など
        "給与情報",          # 月給 / 時給レンジと手当
        "勤務時間",          # 営業時間・就業時間
        "休日・休暇",        # 休日制度
        "応募条件",          # 応募資格
        "勤務地へのアクセス",  # 最寄駅・交通手段
        "採用予定人数",       # ※サイトに項目が無く常に空
        "福利厚生",          # 待遇
        "応募窓口電話番号",   # サイト共通の応募フリーダイヤル
        "求人ID",
        "掲載日",
        "掲載サイト名",       # 固定値: キレイビズ
    ]

    def prepare(self):
        """日本語ページを確実に受け取るためのヘッダを補強する。"""
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )

    # ------------------------------------------------------------------
    # メイン: 一覧 (?page=N) → 詳細 → 1 件ずつ yield
    # ------------------------------------------------------------------
    def parse(self, url: str):
        page = 1
        last_page = None
        seen_ids: set[str] = set()

        while True:
            list_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした: %s", list_url)
                return

            if page == 1:
                html = str(soup)
                last_page = self._extract_last_page(html)
                total = self._extract_total(html)
                if total:
                    self.total_items = total
                logger.info("総件数=%s / 総ページ数=%s", total, last_page)

            # 一覧の求人リンクを出現順に重複排除して取得
            job_ids = []
            for a in soup.select('a[href*="/jobs/id-"]'):
                m = _JOB_ID_PATTERN.search(a.get("href", ""))
                if m and m.group(1) not in seen_ids:
                    seen_ids.add(m.group(1))
                    job_ids.append(m.group(1))

            if not job_ids:
                logger.info("求人リンクが無くなったため終了します: %s", list_url)
                return

            for job_id in job_ids:
                detail_url = urljoin(url, f"id-{job_id}/")
                item = self._parse_detail(detail_url, job_id)
                if item:
                    yield item

            if last_page and page >= last_page:
                return
            page += 1

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, job_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        dl_data = self._parse_description_list(soup)
        posting = self._parse_job_posting(soup)

        name = (
            dl_data.get("shop")
            or (posting.get("hiringOrganization") or {}).get("name", "")
        ).strip()
        if not name:
            logger.warning("店舗名を取得できませんでした: %s", detail_url)
            return None

        pref, post_code, addr = self._resolve_address(posting, dl_data)
        work_hours = _clean(dl_data.get("work_hours") or posting.get("workHours", ""))

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            # TEL はサイト共通の応募ダイヤルのみ (店舗固有番号なし) のため空にする
            Schema.TEL: "",
            # 事業内容: 定型の短文 (例: 美容室での業務を中心としたサロン業務全般)
            Schema.LOB: _clean(dl_data.get("responsibilities") or posting.get("responsibilities", "")),
            Schema.CAT_SITE: self._extract_category(soup),
            Schema.HOLIDAY: self._extract_regular_holiday(dl_data.get("holiday", "")),
            Schema.TIME: self._extract_open_hours(work_hours),
            "雇用形態": dl_data.get("emp_type") or self._emp_type_from_posting(posting),
            "募集職種": (dl_data.get("occupation") or self._occupation_from_title(posting)).replace("\n", " / "),
            "給与情報": dl_data.get("salary", ""),
            "勤務時間": work_hours,
            "休日・休暇": dl_data.get("holiday", ""),
            "応募条件": _clean(dl_data.get("qualifications") or posting.get("qualifications", "")),
            "勤務地へのアクセス": dl_data.get("access", ""),
            # サイト上に「採用予定人数 / 募集人数」項目が存在しないため常に空
            "採用予定人数": "",
            "福利厚生": _clean(dl_data.get("benefits") or posting.get("jobBenefits", "")),
            "応募窓口電話番号": self._extract_tel(soup),
            "求人ID": job_id,
            "掲載日": posting.get("datePosted", ""),
            "掲載サイト名": _SITE_NAME,
        }

    # ------------------------------------------------------------------
    # 抽出ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_last_page(html: str) -> int | None:
        """ページャの「>>」(aria-label="pagination.last") から総ページ数を得る。"""
        pages = [int(n) for n in _LAST_PAGE_PATTERN.findall(html)]
        return max(pages) if pages else None

    @staticmethod
    def _extract_total(html: str) -> int | None:
        m = _TOTAL_PATTERN.search(html)
        return int(m.group(1).replace(",", "")) if m else None

    @staticmethod
    def _parse_description_list(soup) -> dict:
        """募集情報の `dl.uk-description-list` をラベル → 値の辞書にする。"""
        data: dict[str, str] = {}
        for dl in soup.select("dl.uk-description-list"):
            for dt in dl.find_all("dt"):
                label = dt.get_text(" ", strip=True)
                key = _DL_LABELS.get(label)
                if not key or key in data:
                    continue
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                data[key] = _clean(dd.get_text("\n", strip=True))
        return data

    @staticmethod
    def _parse_job_posting(soup) -> dict:
        """JSON-LD の schema.org JobPosting を返す (無ければ空 dict)。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                # description 等に生の改行が入るため strict=False が必須
                data = json.loads(raw, strict=False)
            except ValueError as e:
                logger.debug("JSON-LD をパースできませんでした: %s", e)
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}

    @staticmethod
    def _resolve_address(posting: dict, dl_data: dict) -> tuple[str, str, str]:
        """JSON-LD の PostalAddress を優先し、無ければ「勤務地住所」から分解する。"""
        locations = posting.get("jobLocation") or []
        if isinstance(locations, dict):
            locations = [locations]
        if locations:
            address = (locations[0] or {}).get("address") or {}
            pref = (address.get("addressRegion") or "").strip()
            post_code = (address.get("postalCode") or "").strip()
            addr = " ".join(
                part.strip()
                for part in (address.get("addressLocality"), address.get("streetAddress"))
                if part and part.strip()
            )
            if pref or addr:
                return pref, post_code, addr

        # フォールバック: 「〒564-0053\n大阪府吹田市江の木町6-26 アカツキマンション1F」
        raw = dl_data.get("address", "")
        post_code = ""
        m = re.search(r"〒\s*([\d\-]+)", raw)
        if m:
            post_code = m.group(1)
            raw = raw.replace(m.group(0), "")
        raw = " ".join(raw.split())
        pref_match = _PREF_PATTERN.match(raw)
        if pref_match:
            return pref_match.group(1), post_code, raw[pref_match.end():].strip()
        return "", post_code, raw

    @staticmethod
    def _extract_category(soup) -> str:
        """パンくず (BreadcrumbList) の職種一覧から「エステティシャン」等を得る。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw or "BreadcrumbList" not in raw:
                continue
            try:
                data = json.loads(raw, strict=False)
            except ValueError:
                continue
            graph = data.get("@graph", [data]) if isinstance(data, dict) else []
            for node in graph:
                if not isinstance(node, dict) or node.get("@type") != "BreadcrumbList":
                    continue
                for element in node.get("itemListElement", []):
                    # position 3 が職種一覧 (例: "エステティシャン求人の一覧")
                    if element.get("position") == 3:
                        name = (element.get("name") or "").strip()
                        return re.sub(r"求人の一覧$", "", name)
        return ""

    @staticmethod
    def _extract_regular_holiday(holiday_text: str) -> str:
        """「休日・休暇」から "定休" を含む行だけを抜き出す (独立項目が無いため)。"""
        lines = [
            line.strip(" ・※")
            for line in holiday_text.splitlines()
            if "定休" in line
        ]
        return " / ".join(line for line in lines if line)

    @staticmethod
    def _extract_open_hours(work_hours: str) -> str:
        """勤務時間テキスト中の「【営業時間】...」行をサロンの営業時間として抽出する。"""
        m = _OPEN_HOURS_PATTERN.search(work_hours or "")
        return m.group(1).strip() if m else ""

    @staticmethod
    def _emp_type_from_posting(posting: dict) -> str:
        emp_types = posting.get("employmentType") or []
        if isinstance(emp_types, str):
            emp_types = [emp_types]
        return "・".join(_EMP_TYPE_MAP.get(code, code) for code in emp_types)

    @staticmethod
    def _occupation_from_title(posting: dict) -> str:
        """JSON-LD title (例: "エステティシャン【正社員】") から職種部分を取り出す。"""
        title = (posting.get("title") or "").strip()
        return re.sub(r"【[^】]*】", "", title).strip()

    @staticmethod
    def _extract_tel(soup) -> str:
        a = soup.select_one('a[href^="tel:"]')
        if not a:
            return ""
        return a["href"].replace("tel:", "").strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Kireibiz()
    scraper.execute("https://kireibiz.jp/jobs/")

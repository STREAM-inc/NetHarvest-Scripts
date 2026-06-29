"""
フロムエーナビ (froma.com) — アルバイト/パート求人の掲載企業情報スクレイパー

取得対象:
    - 全国47都道府県の求人掲載企業 (採用企業名・代表者名・所在地・電話番号・
      事業内容・企業HP・採用予定人数など、求人詳細ページの「企業情報」全カラム)

取得フロー:
    1. ルート (https://www.froma.com/?red=0) からホスト部を取り出し、
       都道府県別の求人一覧 /prefectures/{slug}/job_search/ を起点にする。
    2. 一覧ページの埋め込み JSON (__NEXT_DATA__) から jobCards を取得。
       ページ送りは pageInfo.nextPages[0].cursor を使ったカーソル方式。
    3. 各 jobCard の indeedJobKey から求人詳細 /viewjob/{key}/ を開き、
       詳細ページの __NEXT_DATA__ props.pageProps.data.jobData から企業情報を抽出。
       (JSON-LD は採用企業名・住所しか持たず、代表者名/TEL/HP/事業内容/採用予定人数
        などの「企業情報」カラムは jobData にのみ存在する)
    4. 詳細を 1 件取得するごとに即 yield する (Pattern B)。

jobData から取得する企業情報カラム:
    - employerName     → 採用企業名 (法人名)        → Schema.NAME
    - ceoName          → 代表者名                   → Schema.REP_NM
    - employerAddress  → 企業所在地 (本社住所)       → Schema.ADDR / Schema.PREF
    - phoneNumber      → 電話番号                   → Schema.TEL
    - contactPhone     → お問い合わせ先TEL           → EXTRA「お問い合わせ先TEL」
    - businessDetails  → 事業内容                   → Schema.LOB
    - corporateWebsite → 企業HP                     → Schema.HP
    - industry         → 業種                       → Schema.CAT_SITE
    - positionCount    → 採用予定人数               → EXTRA「採用予定人数」
    加えて求人コンテキスト (勤務先・勤務地・職種名・雇用形態・給与・掲載日・求人ID) を
    構造化された短いフィールドのみ EXTRA に格納する。

雇用形態の取得方針:
    名称の下に表示される求人単位の「雇用形態」を正確に取得する。本文中の
    「正社員登用制度」等の文言に引きずられないよう、求人固有の構造化フィールドのみを
    根拠とし、(1) 詳細ページ recruitmentForm → (2) 詳細ページ JSON-LD
    JobPosting.employmentType (schema.org enum を日本語化) → (3) 一覧カード jobType
    の順で決定する。一覧カードの値は最終フォールバックに留める。

備考対応:
    - 「北海道だけでなく全エリアを取得したい」 → PREFS に全47都道府県を登録し巡回。
      フィルター指定は無いため絞り込みは行わない (全件取得)。

著作権配慮:
    - 仕事内容/PRコメント/応募資格/待遇/選考プロセス等の自由記述プロースは取得しない。
      取得するのは構造化された企業情報カラムと、求人の短い構造化フィールドのみ。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/froma.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id froma
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 全47都道府県のURLスラッグ (froma.com の /prefectures/{slug}/ で確認済み)
PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 1都道府県あたりの巡回上限ページ数 (暴走防止のセーフティ。1ページ100件)
MAX_PAGES = 500

# employerAddress 先頭の都道府県を切り出す
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")

# JSON-LD JobPosting.employmentType (schema.org の controlled enum) →
# froma が画面に表示する「雇用形態」表記への対応。
# froma の recruitmentForm 語彙 (正社員 / アルバイト・パート / 業務委託 / 派遣社員) に揃える。
# この enum は froma の構造化データ生成が求人単位で付与する機械可読値で、名称の下に
# 表示される雇用形態と一致するため、recruitmentForm が欠落した際の確実な補完元になる。
_LD_EMPLOYMENT = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "CONTRACT": "業務委託",
    "TEMPORARY": "派遣社員",
    "INTERN": "インターン",
    "VOLUNTEER": "ボランティア",
    "PER_DIEM": "日雇い",
    "OTHER": "その他",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _normalize_tel(s) -> str:
    """電話番号を整形する。froma は区切り無しの数字列で持つことが多い。"""
    return re.sub(r"[^\d]", "", _clean(s))


class FromaScraper(StaticCrawler):
    """フロムエーナビ 求人掲載企業スクレイパー (froma.com)

    都道府県別の求人一覧を全件巡回し、各求人詳細の __NEXT_DATA__ jobData から
    採用企業名・代表者名・所在地・TEL・事業内容・企業HP・採用予定人数などの
    企業情報カラムを抽出する。
    """

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "お問い合わせ先TEL", "採用予定人数", "勤務先", "勤務地", "勤務地郵便番号",
        "職種名", "雇用形態", "給与", "掲載日", "求人ID",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルートURL (sites.yml の url = SSOT) からホスト部のみを派生させる。
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"

        seen: set[str] = set()  # 訪問済み求人キー (都道府県をまたぐ重複排除)

        for slug in PREFS:
            list_url = f"{base}/prefectures/{slug}/job_search/"
            self.logger.info("一覧巡回: %s", list_url)
            yield from self._scrape_prefecture(base, list_url, seen)

        self.logger.info("収集求人数: %d件", len(seen))

    # ------------------------------------------------------------------ #
    # 都道府県別一覧のカーソルページネーション巡回
    # ------------------------------------------------------------------ #
    def _scrape_prefecture(self, base: str, list_url: str,
                           seen: set) -> Generator[dict, None, None]:
        cursor = None
        for _ in range(MAX_PAGES):
            page_url = list_url if cursor is None else f"{list_url}?cursor={cursor}"
            data = self._get_page_data(page_url)
            if not data:
                break

            for card in data.get("jobCards", []):
                key = card.get("indeedJobKey")
                if not key or key in seen:
                    continue
                seen.add(key)
                item = self._scrape_detail(f"{base}/viewjob/{key}/", card)
                if item:
                    yield item

            # 次ページのカーソルを取得。無ければこの都道府県は終了。
            next_pages = (data.get("pageInfo") or {}).get("nextPages") or []
            if not next_pages:
                break
            cursor = next_pages[0].get("cursor")
            if not cursor:
                break

    def _get_page_data(self, url: str) -> dict | None:
        """ページの __NEXT_DATA__ から props.pageProps.data を取り出す。

        一覧ページなら data に jobCards / pageInfo、
        詳細ページなら data に jobData が含まれる。
        """
        soup = self.get_soup(url)
        if soup is None:
            return None
        return self._parse_next_data(soup)

    @staticmethod
    def _parse_next_data(soup) -> dict | None:
        """soup の __NEXT_DATA__ から props.pageProps.data を取り出す。"""
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag or not tag.string:
            return None
        try:
            payload = json.loads(tag.string)
            return payload["props"]["pageProps"]["data"]
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    # ------------------------------------------------------------------ #
    # 求人詳細ページ (__NEXT_DATA__ jobData) から企業情報を抽出
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, card: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        data = self._parse_next_data(soup)
        jd = data.get("jobData") if isinstance(data, dict) else None
        if not isinstance(jd, dict):
            return None
        # 名称の下に表示される「雇用形態」を確実に取るため、JSON-LD JobPosting の
        # employmentType (schema.org enum) も同じページから取得しておく。
        jsonld_emp = self._jsonld_employment(soup)

        # --- 企業情報 (会社カラム) ---
        name = _clean(jd.get("employerName")) or _clean(card.get("employerName"))
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: url,
        }

        rep = _clean(jd.get("ceoName"))
        if rep:
            item[Schema.REP_NM] = rep

        # 企業所在地 (本社住所)。先頭の都道府県を PREF に切り出し、残りを ADDR に。
        emp_addr = _clean(jd.get("employerAddress"))
        if emp_addr:
            m = _PREF_RE.match(emp_addr)
            if m:
                item[Schema.PREF] = m.group(1)
                rest = emp_addr[m.end():].strip()
                item[Schema.ADDR] = rest or emp_addr
            else:
                item[Schema.ADDR] = emp_addr

        tel = _normalize_tel(jd.get("phoneNumber"))
        if tel:
            item[Schema.TEL] = tel

        lob = _clean(jd.get("businessDetails"))
        if lob:
            item[Schema.LOB] = lob

        hp = _clean(jd.get("corporateWebsite"))
        if hp:
            item[Schema.HP] = hp

        industry = _clean(jd.get("industry"))
        if industry:
            item[Schema.CAT_SITE] = industry

        # --- EXTRA: 構造化された短いフィールドのみ (自由記述プロースは含めない) ---
        item["お問い合わせ先TEL"] = _normalize_tel(jd.get("contactPhone"))
        item["採用予定人数"] = _clean(jd.get("positionCount"))
        item["勤務先"] = _clean(jd.get("officeName")) or _clean(jd.get("companyName")) \
            or _clean(card.get("employerName"))
        item["勤務地"] = self._build_work_location(jd.get("jobLocation"))
        item["勤務地郵便番号"] = _clean((jd.get("jobLocation") or {}).get("postalCode"))
        item["職種名"] = _clean(jd.get("title")) or _clean(card.get("title"))
        # 雇用形態は「名称の下」に出る求人単位の値を正確に取る。
        # 一覧カードの jobType はあくまで最終フォールバックに留め、
        # 詳細ページの recruitmentForm / JSON-LD を優先する。
        item["雇用形態"] = self._select_employment(
            jd.get("recruitmentForm"), jsonld_emp, card.get("jobType"))
        # jobData.baseSalary は "時給1,226円以上" のような整形済み短文字列
        item["給与"] = _clean(jd.get("baseSalary"))
        item["掲載日"] = _clean(jd.get("datePublished"))
        item["求人ID"] = _clean(jd.get("indeedJobKey")) or _clean(card.get("indeedJobKey"))

        return item

    @staticmethod
    def _build_work_location(job_location) -> str:
        """jobLocation (勤務地) から都道府県・市区町村・番地を結合する。"""
        if not isinstance(job_location, dict):
            return ""
        region = _clean(job_location.get("addressRegion"))
        locality = _clean(job_location.get("addressLocality"))
        street = _clean(job_location.get("streetAddress"))
        # addressLocality は "東京 千代田区" のように都道府県名 (短縮形含む) を
        # 先頭トークンに含む場合がある。region と重複するので除去する。
        parts = locality.split()
        if parts and region and (region.startswith(parts[0]) or parts[0] == region):
            parts = parts[1:]
        locality = "".join(parts)
        return _clean(f"{region}{locality}{street}")

    @staticmethod
    def _select_employment(recruitment_form, jsonld_emp, job_type) -> str:
        """雇用形態を「名称の下に表示される求人単位の値」として正確に決定する。

        株式会社デジタルハーツのように、本文に「正社員登用制度」等の文言があっても
        その求人自体はアルバイト・パート、という取り違えを避けるため、求人固有の
        構造化フィールドのみを根拠にする。優先順位は:

            1. 詳細ページ __NEXT_DATA__ の recruitmentForm
               (画面で名称の下に出る「雇用形態」そのもの)
            2. 詳細ページ JSON-LD JobPosting.employmentType
               (schema.org enum を日本語化。recruitmentForm 欠落時の確実な補完)
            3. 一覧カードの jobType (最終フォールバック)
        """
        rf = FromaScraper._join_employment(recruitment_form)
        if rf:
            return rf
        if _clean(jsonld_emp):
            return _clean(jsonld_emp)
        return FromaScraper._join_employment(job_type)

    @staticmethod
    def _join_employment(value) -> str:
        """recruitmentForm / jobType (リスト or 文字列) を重複なく結合する。"""
        if isinstance(value, list):
            vals = [_clean(v) for v in value if _clean(v)]
            return "/".join(dict.fromkeys(vals))
        return _clean(value)

    @staticmethod
    def _jsonld_employment(soup) -> str:
        """詳細ページの JSON-LD JobPosting.employmentType を日本語表記で返す。

        schema.org の controlled enum (FULL_TIME / PART_TIME / CONTRACTOR /
        TEMPORARY 等) を froma の表示語彙に対応付ける。未知コードは原値のまま返す。
        """
        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            if not tag.string:
                continue
            try:
                obj = json.loads(tag.string)
            except (json.JSONDecodeError, TypeError):
                continue
            for node in (obj if isinstance(obj, list) else [obj]):
                if not isinstance(node, dict) or node.get("@type") != "JobPosting":
                    continue
                et = node.get("employmentType")
                codes = et if isinstance(et, list) else [et]
                labels = []
                for c in codes:
                    c = _clean(c)
                    if not c:
                        continue
                    key = re.sub(r"[\s-]+", "_", c).upper()
                    labels.append(_LD_EMPLOYMENT.get(key, c))
                if labels:
                    return "/".join(dict.fromkeys(labels))
        return ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = FromaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.froma.com/?red=0")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

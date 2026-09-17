"""
建職バンク (kenshoku-bank.com) — 建設業界特化型 転職・求人サイトの掲載企業情報

取得対象:
    - 公開中の求人詳細ページ (/jobs/{id}) 1 件 = 1 レコード (求人単位)。
      同一企業が複数の求人を出しているため会社情報は重複し得る (dedup しない)。
    - 会社名 / 住所 (郵便番号・都道府県・番地) / 代表者 / 従業員数 / 資本金 / 設立 /
      会社HP / 職種 / 雇用形態 / 年収レンジ / 掲載日・更新日 ほか

取得フロー:
    1. sites.yml の url (トップ) から sitemap インデックス
       (/sitemaps/sitemap_jobs_index.xml) を辿り、公開求人 URL (/jobs/{id}) を列挙
    2. 求人詳細を 1 件取得するごとに解析 → 即 yield (Pattern B)
       - JSON-LD (JobPosting) から 掲載日 / 更新日 / 年収レンジ を取得
       - 「会社概要」テーブル (.p-job-detail__information-table-row) から
         会社名 / 会社HP / 住所 / 従業員数 / 資本金 / 設立 を取得
       - ページ上部 (.p-job-info__tr) から 職種 / 勤務地都道府県 を取得
       - 会社ページ (/companies/{id}) を URL 単位でキャッシュ取得し、代表者 (役職+氏名)・
         売上高を取得。求人詳細に住所/従業員数/資本金/設立/会社HP が無い場合の補完にも使う
    3. TEL は本サイトに掲載が無いため常に空欄

備考:
    - 「仕事内容」「休日・休暇」「求める人物像」「事業内容」「企業からのメッセージ」等の
      長文の自由記述プロースは著作権リスクのため取得しない。
    - 従業員数 / 設立 / 会社HP / 代表者 / 売上高 は掲載が無い企業があり、その場合は空文字となる
      (セレクタの不備ではなく出典データの欠落)。
    - robots.txt は /jobs/search?* と /api/ を Disallow。列挙は sitemap のみを使い、
      検索結果ページ・内部 API は叩かない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/kenshoku_bank.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kenshoku_bank
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

# sitemap インデックス (robots.txt に記載) — トップ URL からの相対パスで組み立てる
_JOBS_SITEMAP_INDEX = "/sitemaps/sitemap_jobs_index.xml"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}\s*-?\s*\d{4})")
_JOB_URL_RE = re.compile(r"/jobs/(\d+)$")
_COMPANY_ID_RE = re.compile(r"/companies/(\d+)")
_ESTABLISHED_RE = re.compile(r"(\d{4})年(?:\s*(\d{1,2})月)?(?:\s*(\d{1,2})日)?")
# 会社概要テーブルのうちレコードに採用するラベル (長文プロースのラベルは含めない)
_COMPANY_LABELS = ("会社名", "会社HP", "住所", "従業員数", "資本金", "設立")


class KenshokuBankCrawler(StaticCrawler):
    """建職バンク 求人掲載企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "職種",
        "雇用形態",
        "勤務地都道府県",
        "年収下限(万円)",
        "年収上限(万円)",
        "資格カテゴリ",
        "特徴タグ",
        "求人の種類",
        "求人種別",
        "掲載日",
        "更新日",
        "企業ページURL",
    ]

    def parse(self, url: str):
        # 企業ページ (代表者) は求人間で使い回すためキャッシュする
        self._company_cache: dict[str, dict] = {}

        for job_url in self._iter_job_urls(url):
            try:
                record = self._scrape_job(job_url, url)
            except Exception as e:  # noqa: BLE001 — 1 件の失敗で全体を止めない
                self.logger.warning("求人の解析に失敗 (スキップ): %s — %s", job_url, e)
                continue
            if record:
                yield record

    # ------------------------------------------------------------------
    # 列挙
    # ------------------------------------------------------------------
    def _iter_job_urls(self, root_url: str):
        """sitemap インデックス → 各 sitemap → 求人 URL を順に返す。"""
        index_url = urllib.parse.urljoin(root_url, _JOBS_SITEMAP_INDEX)
        index_soup = self.get_soup(index_url)
        if index_soup is None:
            self.logger.error("sitemap インデックスを取得できません: %s", index_url)
            return

        child_urls = [
            loc.get_text(strip=True)
            for loc in index_soup.find_all("loc")
            if loc.get_text(strip=True)
        ]
        # インデックスではなく urlset が直接返る場合もあるので求人 URL を先に拾う
        direct = [u for u in child_urls if _JOB_URL_RE.search(u)]
        sitemaps = [u for u in child_urls if u.endswith(".xml")]

        seen: set[str] = set()
        for job_url in direct:
            if job_url not in seen:
                seen.add(job_url)
                yield job_url

        for sitemap_url in sitemaps:
            soup = self.get_soup(sitemap_url)
            if soup is None:
                continue
            for loc in soup.find_all("loc"):
                job_url = loc.get_text(strip=True)
                if _JOB_URL_RE.search(job_url) and job_url not in seen:
                    seen.add(job_url)
                    yield job_url

    # ------------------------------------------------------------------
    # 求人詳細
    # ------------------------------------------------------------------
    def _scrape_job(self, job_url: str, root_url: str) -> dict | None:
        soup = self.get_soup(job_url)
        if soup is None:
            return None

        info = self._label_map(soup)
        job_info = self._job_info_map(soup)
        posting = self._job_posting(soup)

        name = info.get("会社名", "")
        if not name:
            # 会社概要が無いページ (掲載終了等) は h1 内の企業名にフォールバック
            company_span = soup.select_one(".c-panel__header__title__company")
            name = company_span.get_text(strip=True) if company_span else ""
        if not name:
            return None  # NAME は必須

        _company_id, company_url = self._company_link(soup, root_url)
        company_info = self._fetch_company(company_url) if company_url else {}

        post_code, pref, addr = self._split_address(
            info.get("住所", "") or company_info.get("住所", "")
        )

        # 会社HP は href を優先 (テキストが省略表示される場合があるため)
        hp = info.get("会社HP", "")
        for row in soup.select(".p-job-detail__information-table-row"):
            header = row.select_one(".p-job-detail__information-table-header")
            if header and header.get_text(strip=True) == "会社HP":
                a = row.select_one(".p-job-detail__information-table-content a[href]")
                if a:
                    hp = a["href"]
                break
        if not hp:
            hp = company_info.get("会社HP", "")

        job_id_match = _JOB_URL_RE.search(job_url)
        job_id = job_id_match.group(1) if job_id_match else ""

        salary_min, salary_max = self._salary_range(posting)
        tags = [
            t.get_text(strip=True)
            for t in soup.select(".c-panel__header__title + div .c-keyword, "
                                 ".c-panel__header .c-keyword")
        ]
        tags = [t for t in dict.fromkeys(tags) if t and t != "NEW"]
        employment = [
            t.get_text(strip=True) for t in soup.select(".c-panel__header .c-keyword.-green")
        ]

        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: "",  # 本サイトに電話番号の掲載は無い
            Schema.REP_NM: company_info.get("代表者名", ""),
            Schema.POS_NM: company_info.get("役職", ""),
            Schema.EMP_NUM: info.get("従業員数", "") or company_info.get("従業員数", ""),
            Schema.CAP: info.get("資本金", "") or company_info.get("資本金", ""),
            Schema.OPEN_DATE: self._normalize_date(
                info.get("設立", "") or company_info.get("設立", "")
            ),
            Schema.SALES: company_info.get("売上高", ""),
            Schema.CAT_SITE: job_info.get("職種", ""),
            Schema.HP: hp,
            "求人ID": job_id,
            "求人タイトル": self._job_title(soup, name),
            "職種": job_info.get("職種", ""),
            "雇用形態": " / ".join(dict.fromkeys(employment)),
            "勤務地都道府県": self._work_pref(job_info, posting),
            "年収下限(万円)": salary_min,
            "年収上限(万円)": salary_max,
            "資格カテゴリ": self._license_from_breadcrumb(soup),
            "特徴タグ": " / ".join(tags),
            "求人の種類": info.get("求人の種類", ""),
            "求人種別": info.get("求人種別", ""),
            "掲載日": posting.get("datePosted", "") or "",
            "更新日": posting.get("dateModified", "") or self._updated_at(soup),
            "企業ページURL": company_url,
        }

    # ------------------------------------------------------------------
    # 企業ページ (代表者の補完)
    # ------------------------------------------------------------------
    def _fetch_company(self, company_url: str) -> dict:
        """企業ページの定義リスト (住所/代表者/設立/従業員数/資本金/売上高/会社HP) を返す。

        同一企業の求人が複数あるため URL 単位でキャッシュする。
        """
        cached = self._company_cache.get(company_url)
        if cached is not None:
            return cached

        result: dict[str, str] = {}
        soup = self.get_soup(company_url)
        if soup is not None:
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                label = dt.get_text(strip=True)
                if not label or label in result:
                    continue
                if label == "会社HP":
                    a = dd.find("a", href=True)
                    result[label] = a["href"] if a else dd.get_text(" ", strip=True)
                    continue
                value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()
                value = value.replace("Googleマップで開く", "").strip()
                result[label] = value

            position, rep_name = self._split_representative(result.get("代表者", ""))
            result["役職"] = position
            result["代表者名"] = rep_name

        self._company_cache[company_url] = result
        return result

    @staticmethod
    def _split_representative(raw: str) -> tuple[str, str]:
        """「代表取締役社長 瀧谷 善郎」→ ("代表取締役社長", "瀧谷 善郎")。"""
        if not raw:
            return "", ""
        m = re.match(r"^([^\s]*?(?:社長|代表|会長|理事長|取締役|所長|CEO))\s+(.+)$", raw)
        if m:
            return m.group(1), m.group(2).strip()
        return "", raw

    # ------------------------------------------------------------------
    # パーツ抽出
    # ------------------------------------------------------------------
    @staticmethod
    def _label_map(soup) -> dict:
        """会社概要・求人情報テーブルのラベル → 値 (長文ラベルは無視)。"""
        result: dict[str, str] = {}
        wanted = set(_COMPANY_LABELS) | {"求人の種類", "求人種別", "プラン"}
        for row in soup.select(".p-job-detail__information-table-row"):
            header = row.select_one(".p-job-detail__information-table-header")
            content = row.select_one(".p-job-detail__information-table-content")
            if header is None or content is None:
                continue
            label = header.get_text(strip=True)
            if label in wanted and label not in result:
                result[label] = re.sub(r"\s+", " ", content.get_text(" ", strip=True)).strip()
        return result

    @staticmethod
    def _job_info_map(soup) -> dict:
        """ページ上部サマリ (年収 / 職種 / 勤務地 / 資格) のラベル → 値。"""
        result: dict[str, str] = {}
        for tr in soup.select(".p-job-info__tr"):
            th = tr.select_one(".p-job-info__th")
            td = tr.select_one(".p-job-info__td")
            if th is None or td is None:
                continue
            label = th.get_text(strip=True)
            more = td.select_one("a.c-more-link")
            if more:
                more.extract()
            value = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
            if label and label not in result:
                result[label] = value
        return result

    @staticmethod
    def _job_posting(soup) -> dict:
        """JSON-LD の JobPosting を返す (無ければ空 dict)。"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}

    @staticmethod
    def _salary_range(posting: dict) -> tuple[str, str]:
        """JSON-LD baseSalary (年額 円) を万円単位の文字列にする。"""
        base = posting.get("baseSalary") or {}
        value = base.get("value") if isinstance(base, dict) else None
        if not isinstance(value, dict):
            return "", ""
        if value.get("unitText") != "YEAR":
            return "", ""

        def _man(v):
            if isinstance(v, (int, float)) and v:
                return str(int(v // 10000))
            return ""

        return _man(value.get("minValue")), _man(value.get("maxValue"))

    @staticmethod
    def _job_title(soup, company_name: str) -> str:
        h1 = soup.select_one("h1")
        if h1 is None:
            return ""
        company_span = h1.select_one(".c-panel__header__title__company")
        if company_span:
            company_span.extract()
        title = re.sub(r"\s+", " ", h1.get_text(" ", strip=True)).strip()
        if company_name and title.startswith(company_name):
            title = title[len(company_name):].strip()
        return title

    @staticmethod
    def _work_pref(job_info: dict, posting: dict) -> str:
        raw = job_info.get("勤務地", "")
        m = _PREF_PATTERN.search(raw)
        if m:
            return m.group(1)
        location = posting.get("jobLocation") or {}
        if isinstance(location, list):
            location = location[0] if location else {}
        address = location.get("address") if isinstance(location, dict) else None
        if isinstance(address, dict):
            return address.get("addressRegion", "") or ""
        return ""

    @staticmethod
    def _license_from_breadcrumb(soup) -> str:
        """パンくずの資格カテゴリ (例: 第一種電気工事士) を返す。"""
        for a in soup.select('ol.breadcrumb a[href^="/licenses/"]'):
            text = a.get_text(strip=True)
            if text and "・" not in text:
                return text
        return ""

    @staticmethod
    def _updated_at(soup) -> str:
        for span in soup.select("span"):
            text = span.get_text(" ", strip=True)
            if text.startswith("更新日"):
                m = re.search(r"(\d{4})[/年](\d{1,2})[/月](\d{1,2})", text)
                if m:
                    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return ""

    @staticmethod
    def _company_link(soup, root_url: str) -> tuple[str, str]:
        a = soup.select_one('a[href^="/companies/"]')
        if a is None:
            return "", ""
        m = _COMPANY_ID_RE.search(a["href"])
        if not m:
            return "", ""
        return m.group(1), urllib.parse.urljoin(root_url, f"/companies/{m.group(1)}")

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """「〒 463-0046 愛知県名古屋市…」→ (郵便番号, 都道府県, 市区町村以降)。"""
        if not raw:
            return "", "", ""
        text = re.sub(r"\s+", " ", raw).strip()
        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            digits = re.sub(r"[^\d]", "", m.group(1))
            post_code = f"{digits[:3]}-{digits[3:]}"
            text = (text[: m.start()] + text[m.end():]).strip()
        text = text.lstrip("〒").strip()

        pm = _PREF_PATTERN.search(text)
        if pm:
            return post_code, pm.group(1), text[pm.end():].strip()
        return post_code, "", text

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """「1951年2月12日」→ 1951-02-12 / 「1949年5月」→ 1949-05。"""
        if not raw:
            return ""
        m = _ESTABLISHED_RE.search(raw)
        if not m:
            return ""
        year, month, day = m.group(1), m.group(2), m.group(3)
        if month and day:
            return f"{year}-{int(month):02d}-{int(day):02d}"
        if month:
            return f"{year}-{int(month):02d}"
        return year


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KenshokuBankCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kenshoku-bank.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

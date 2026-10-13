"""
カーワク — 自動車業界特化型求人サイト (carworkassist.com / 株式会社アプティ)

取得対象:
    - 整備職のみ (職種: 自動車整備士 / 自動車検査員 / 鈑金・塗装) の掲載中求人。
      フロント・営業・事務・ガソリンスタンド・回送などは対象外 (備考の指示)。
      2026-10 時点で整備職は約 11,200 件 (全職種は約 17,400 件)。

取得フロー:
    一覧 /job に Drupal Views の絞り込みクエリ occupation_id[57|56|54] を付与し、
    ?page=N (0 始まり・50件/ページ) で全ページを巡回する。
    一覧カード (div.node--type-job.list) から職種で再フィルタしたうえで
    詳細ページ /job/{id}.html を 1 件ずつ取得して即 yield する (Pattern B)。

    詳細ページの取得元:
      - h1.title / div.comname / div.jobid (求人タイトル・企業名・求人ID)
      - div.map-col (郵便番号・勤務地住所)
      - table.job-table の th/td (職種・仕事内容・勤務時間・休日・雇用形態・給与 等)
      - table.com_table の th/td (企業名・業態・事業内容・設立年月・代表者名・
        従業員数・本社住所・企業URL)
      - JSON-LD JobPosting (datePosted / validThrough / 住所の補完)

    TEL: 求人・企業情報ともに企業個別の電話番号は掲載されておらず、
    ページ上の番号はカーワク共通の転職相談ダイヤル (03-6694-2095) のみ。
    共通番号は名寄せに使えないため Schema.TEL には入れず、
    com_table に電話番号行がある場合のみ採用する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/carworkassist.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id carworkassist
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
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})\s*-?\s*(\d{4})")
_JOB_ID_PATTERN = re.compile(r"/job/(\d+)\.html")
_NUM_PATTERN = re.compile(r"\d[\d,]*")

_LIST_PATH = "job"
_ITEMS_PER_PAGE = 50
_MAX_PAGES = 2000  # 無限ループ防止の上限 (実際は約225ページ)

# 整備職のみを対象とする (備考: フロント・営業・事務・洗車のみの求人は対象外)
# key = Drupal の occupation_id, value = サイト表記
_TARGET_OCCUPATIONS = {
    "57": "自動車整備士",
    "56": "自動車検査員",
    "54": "鈑金・塗装",
}
_TARGET_OCCUPATION_NAMES = set(_TARGET_OCCUPATIONS.values())

_MEDIA_NAME = "カーワク"

# 求人本文に結合する table.job-table の見出し (備考: 仕事内容・応募資格・会社の特徴・待遇)
_BODY_LABELS = ["仕事内容", "必要な経験", "必要な免許・資格", "学歴", "福利厚生・待遇"]


def _clean(text: str) -> str:
    """空白・改行を 1 スペースに正規化する。"""
    return re.sub(r"[\s　]+", " ", text or "").strip()


def _lookup(table: dict, *labels: str) -> str:
    """見出しを完全一致 → 前方一致の順で引く。

    com_table の見出しには「従業員数 会社全体および男女比率」のように
    補足説明が続くものがあるため、完全一致だけでは取りこぼす。
    """
    for label in labels:
        if table.get(label):
            return _clean(table[label])
    for label in labels:
        for key, value in table.items():
            if key.startswith(label) and value:
                return _clean(value)
    return ""


def _multiline(el) -> str:
    """<br> 区切りを改行として保ったままテキスト化する。"""
    if el is None:
        return ""
    text = el.get_text("\n")
    lines = [re.sub(r"[ \t　]+", " ", ln).strip() for ln in text.split("\n")]
    return "\n".join([ln for ln in lines if ln]).strip()


class CarWorkAssistCrawler(StaticCrawler):
    """カーワク (carworkassist.com) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "媒体名",
        "求人ID",
        "求人タイトル",
        "雇用形態",
        "年収",
        "給与",
        "年間休日",
        "勤務先名",
        "メーカー系列・チェーン店・FC",
        "業態",
        "こだわり条件",
        "アクセス",
        "試用期間",
        "本社住所",
        "掲載日",
        "掲載終了予定日",
        "求人本文",
    ]

    def parse(self, url: str):
        list_url = urllib.parse.urljoin(url, _LIST_PATH)
        query = "&".join(
            f"occupation_id%5B{oid}%5D={oid}" for oid in _TARGET_OCCUPATIONS
        )

        max_page = None
        seen: set[str] = set()

        for page in range(_MAX_PAGES):
            page_url = f"{list_url}?{query}&page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cards = soup.select("div.node--type-job.list")
            if not cards:
                break

            if page == 0:
                total_el = soup.select_one(".totalcnt .cnt")
                if total_el:
                    m = _NUM_PATTERN.search(total_el.get_text())
                    if m:
                        total = int(m.group(0).replace(",", ""))
                        self.total_items = total
                        max_page = (total + _ITEMS_PER_PAGE - 1) // _ITEMS_PER_PAGE

            for card in cards:
                try:
                    link = card.select_one('a[href*="/job/"]')
                    if not link:
                        continue
                    m = _JOB_ID_PATTERN.search(link.get("href", ""))
                    if not m:
                        continue
                    job_id = m.group(1)
                    if job_id in seen:
                        continue
                    seen.add(job_id)

                    # 一覧カードの職種で整備職以外を除外 (備考のフィルタ指示)
                    occ_el = card.select_one(
                        ".job-list-occupation .field__item"
                    ) or card.select_one(
                        ".field--name-field-job-occupation .field__item"
                    )
                    occupation = _clean(occ_el.get_text()) if occ_el else ""
                    if occupation and occupation not in _TARGET_OCCUPATION_NAMES:
                        continue

                    detail_url = urllib.parse.urljoin(url, f"/job/{job_id}.html")
                    item = self._scrape_detail(detail_url, job_id, occupation)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(f"一覧アイテム解析失敗: {e}")
                    continue

            if max_page is not None and page + 1 >= max_page:
                break

    # ------------------------------------------------------------------ detail

    def _scrape_detail(self, detail_url: str, job_id: str, list_occupation: str):
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        job_table = self._label_map(soup, "table.job-table")
        com_table = self._label_map(soup, "table.com_table")
        ld = self._job_posting_ld(soup)

        occupation = job_table.get("職種", "") or list_occupation
        occupation = _clean(occupation)
        # 詳細側でも整備職以外を除外する (一覧の職種表記が欠けている場合の保険)
        if occupation and occupation not in _TARGET_OCCUPATION_NAMES:
            return None

        title_el = soup.select_one("h1.title")
        title = _clean(title_el.get_text()) if title_el else ""

        company = _clean(com_table.get("企業名", ""))
        if not company:
            company = _clean(
                (ld.get("hiringOrganization") or {}).get("name", "")
            )
        if not company:
            comname = soup.select_one("div.comname")
            company = _clean(comname.get_text()) if comname else ""

        post_code, address = self._location(soup, job_table, ld)
        pref = ""
        m = _PREF_PATTERN.search(address)
        if m:
            pref = m.group(1)
        if not pref:
            pref = _clean((ld.get("jobLocation") or {}).get("address", {}).get("addressRegion", ""))

        # 企業個別の電話番号 (掲載があれば)。共通ダイヤルは採用しない。
        tel = _lookup(com_table, "電話番号", "TEL", "代表電話", "本社電話")
        hp = _lookup(com_table, "企業URL", "企業HP", "ホームページ", "URL")

        salary_year = self._year_salary(soup)
        salary_text = job_table.get("給与詳細", "") or job_table.get("給与", "")

        kodawari = [
            _clean(el.get_text())
            for el in soup.select(
                ".inkodawari .field--name-field-job-kodawari .field__item"
            )
        ]

        jobid_el = soup.select_one("div.jobid")
        jobid_text = ""
        if jobid_el:
            m = re.search(r"(\d+)", jobid_el.get_text())
            jobid_text = m.group(1) if m else ""

        return {
            Schema.URL: detail_url,
            Schema.NAME: company,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: occupation,
            Schema.REP_NM: _lookup(com_table, "代表者名"),
            # 見出しは「従業員数 会社全体および男女比率」のように補足が付く場合がある
            Schema.EMP_NUM: _lookup(com_table, "従業員数"),
            Schema.OPEN_DATE: _lookup(com_table, "設立年月"),
            Schema.LOB: _lookup(com_table, "事業内容"),
            Schema.TIME: _clean(job_table.get("勤務時間", "")),
            Schema.HOLIDAY: _clean(job_table.get("休日・休暇", "")),
            "媒体名": _MEDIA_NAME,
            "求人ID": jobid_text or job_id,
            "求人タイトル": title,
            "雇用形態": _clean(job_table.get("雇用形態", "")),
            "年収": salary_year,
            "給与": _clean(salary_text),
            "年間休日": _clean(job_table.get("年間休日", "")),
            "勤務先名": _clean(job_table.get("勤務先名", "")),
            "メーカー系列・チェーン店・FC": _clean(
                job_table.get("メーカー系列・チェーン店・FC", "")
            ),
            "業態": _lookup(com_table, "業態"),
            "こだわり条件": " / ".join(kodawari),
            "アクセス": _clean(job_table.get("アクセス・最寄駅等", "")),
            "試用期間": _clean(job_table.get("試用期間について", "")),
            "本社住所": _lookup(com_table, "本社住所"),
            "掲載日": _clean(ld.get("datePosted", "")),
            "掲載終了予定日": _clean(ld.get("validThrough", "")),
            "求人本文": self._body_text(soup, job_table),
        }

    # ----------------------------------------------------------------- helpers

    @staticmethod
    def _label_map(soup, table_selector: str) -> dict:
        """th / td 形式のテーブルを {見出し: 値} に畳む (先勝ち)。"""
        result: dict[str, str] = {}
        for tr in soup.select(f"{table_selector} tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())
            if not label or label in result:
                continue
            result[label] = _multiline(td)
        return result

    @staticmethod
    def _job_posting_ld(soup) -> dict:
        """JSON-LD の JobPosting ノードを返す (無ければ空 dict)。"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw or "JobPosting" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            nodes = data.get("@graph", data) if isinstance(data, dict) else data
            if isinstance(nodes, dict):
                nodes = [nodes]
            for node in nodes:
                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                    return node
        return {}

    @staticmethod
    def _location(soup, job_table: dict, ld: dict) -> tuple[str, str]:
        """勤務地の郵便番号と住所を返す。"""
        raw = job_table.get("勤務地", "")
        if not raw:
            map_col = soup.select_one(".map-row .map-col")
            if map_col:
                raw = _multiline(map_col).replace("勤務地", "", 1)

        post_code = ""
        address = ""
        if raw:
            m = _POST_PATTERN.search(raw)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"
                raw = raw[: m.start()] + raw[m.end():]
            address = _clean(raw)

        ld_addr = (ld.get("jobLocation") or {}).get("address", {}) or {}
        if not post_code:
            pc = _clean(str(ld_addr.get("postalCode", "")))
            m = _POST_PATTERN.search(pc)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"
            else:
                post_code = pc
        if not address:
            address = _clean(
                "{}{}{}".format(
                    ld_addr.get("addressRegion", "") or "",
                    ld_addr.get("addressLocality", "") or "",
                    ld_addr.get("streetAddress", "") or "",
                )
            )
        return post_code, address

    @staticmethod
    def _year_salary(soup) -> str:
        """ヘッダの年収レンジ (例: 300万円～500万円) を表記どおり組み立てる。"""
        money = soup.select_one(".money-row .money-col")
        if money is None:
            return ""
        low = money.select_one(".field--name-field-jobex-year-salary-l")
        high = money.select_one(".field--name-field-jobex-year-salary-h")
        low_text = _clean(low.get_text()) if low else ""
        high_text = _clean(high.get_text()) if high else ""
        if low_text and high_text:
            return f"{low_text}～{high_text}"
        return low_text or high_text

    @staticmethod
    def _body_text(soup, job_table: dict) -> str:
        """仕事内容・応募資格・会社の特徴・待遇を見出し付きで結合する (備考の指示)。"""
        parts = []

        back = soup.select_one(".field--name-field-job-recruit-back")
        back_text = _multiline(back)
        if back_text:
            parts.append(f"【募集背景】\n{back_text}")

        catch = soup.select_one(
            ".field--name-field-jobex-catch-copy .field__item"
        )
        catch_text = _multiline(catch)
        if catch_text:
            parts.append(f"【PRポイント】\n{catch_text}")

        for label in _BODY_LABELS:
            value = job_table.get(label, "")
            if value:
                parts.append(f"【{label}】\n{value}")

        return "\n\n".join(parts)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CarWorkAssistCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://carworkassist.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

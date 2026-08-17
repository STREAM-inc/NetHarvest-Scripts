"""
対象サイト: https://www.keibi-baito.com/

ケイサーチ！ (警備員のバイト・求人情報サイト)

構造メモ:
    - サイト全体をまたぐ求人一覧ページは存在せず、トップの地方タブ
      (ul.prefectureNavi) から各地方 (関東/関西/東海/北海道･東北/甲信越･北陸/
      中国･四国/九州･沖縄) に入って検索する導線しかない。
    - 地方ページには window.__initialState__ (React の初期 state) が埋め込まれて
      おり、そこに全地方の area id 一覧が入っている。
    - 求人一覧の実データは HTML には無く、POST /ajax/jobUtil/getJobList
      (application/x-www-form-urlencoded, 認証・CSRF 不要) が JSON で返す。
      1 ページ 15 件固定 (limit 指定は無視される)。
      pagination.count は地方別合計で、7 地方の合計 = 全件数と一致する。
    - 詳細は /detail/{job_id}。会社概要・連絡先住所は #detailArticle 内の
      th/td テーブル、勤務地の都道府県/市区町村は JSON-LD JobPosting にある。
    - 電話番号は詳細ページでは JS 描画のため静的 HTML に出ない。一覧 API の
      Company.tel (会社ごとに異なる 050 番号) を採用する。
      ヘッダーの 0120-167-771 はサイト共通番号なので採用しない。

除外フィールド (著作権リスクのため取得しない):
    仕事内容 / 給与 / 待遇 / 応募資格 / シフト / 面接地 / 応募と今後の流れ /
    求人キャッチコピー — いずれも企業が書いた長文の自由記述のため。
"""

import json
import re
from typing import Generator
from urllib.parse import urljoin

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class KeibiBaitoScraper(StaticCrawler):
    """ケイサーチ！ スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "求人ID",
        "会社ID",
        "地方",
        "雇用形態",
        "勤務地",
        "勤務地都道府県",
        "勤務地市区町村",
        "本社所在地",
        "給与額",
        "給与単位",
        "掲載日",
        "電話受付時間",
    ]

    # 一覧 API / 詳細ページのパス (ルート URL からの相対)
    LIST_API_PATH = "ajax/jobUtil/getJobList"
    DETAIL_PATH = "detail/{job_id}"

    # 一覧 API のページ取得の最大リトライ回数
    MAX_LIST_RETRY = 3
    # 暴走防止のページ数上限 (実際は地方ごとに 500 ページ弱)
    MAX_PAGE = 2000

    # サイト共通の問い合わせ番号 (企業固有ではないので TEL に採用しない)
    COMMON_TEL = "0120-167-771"

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url if url.endswith("/") else url + "/"

        for area_id, area_name in self._discover_areas(base):
            self.logger.info("地方を巡回: %s (area_id=%s)", area_name, area_id)
            page = 1
            while page <= self.MAX_PAGE:
                data = self._fetch_job_list(base, area_id, page)
                if data is None:
                    break

                jobs = data.get("jobs") or []
                if not jobs:
                    break

                for job in jobs:
                    item = self._build_item(base, job, area_name)
                    if item:
                        # 詳細 1 件ごとに即 yield する (全件バッファしない)
                        yield item

                if not (data.get("pagination") or {}).get("nextPage"):
                    break
                page += 1

    # ------------------------------------------------------------------ #
    # 地方 (area) の発見
    # ------------------------------------------------------------------ #
    def _discover_areas(self, base: str) -> list[tuple[str, str]]:
        """トップの地方タブ → 地方ページの __initialState__ から area id 一覧を得る。

        Returns:
            [(area_id, area_name), ...]
        """
        soup = self.get_soup(base)
        region_links: list[str] = []
        if soup:
            for a in soup.select("ul.prefectureNavi a[href]"):
                href = urljoin(base, a["href"])
                if href not in region_links:
                    region_links.append(href)

        # 地方ページの初期 state には全地方の id/名称が入っているので 1 枚読めば足りる
        for href in region_links:
            state = self._get_initial_state(href)
            areas = ((state or {}).get("searchConditionsState") or {}).get("areas") or []
            found = [(str(a.get("id")), a.get("name", "")) for a in areas if a.get("id")]
            if found:
                return found

        self.logger.warning("地方一覧を取得できませんでした。フィルタ無しで全件巡回します。")
        return [("", "")]

    def _get_initial_state(self, url: str) -> dict | None:
        """ページに埋め込まれた window.__initialState__ を JSON として取り出す。"""
        soup = self.get_soup(url)
        if soup is None:
            return None
        for script in soup.find_all("script"):
            text = script.string or script.get_text() or ""
            idx = text.find("window.__initialState__")
            if idx < 0:
                continue
            start = text.find("{", idx)
            if start < 0:
                continue
            try:
                obj, _ = json.JSONDecoder().raw_decode(text[start:])
                return obj
            except ValueError:
                self.logger.warning("__initialState__ の JSON 解析に失敗: %s", url)
        return None

    # ------------------------------------------------------------------ #
    # 一覧 API
    # ------------------------------------------------------------------ #
    def _fetch_job_list(self, base: str, area_id: str, page: int) -> dict | None:
        """POST /ajax/jobUtil/getJobList を叩いて data 部を返す。"""
        api_url = urljoin(base, self.LIST_API_PATH)
        payload = {"sort": "1", "page": str(page)}
        if area_id:
            payload.update({"search_type": "area", "area_ids[0]": area_id, "area_id": area_id})

        last_error: Exception | None = None
        for attempt in range(self.MAX_LIST_RETRY):
            if attempt:
                # 指数バックオフ (上限 30 秒)
                import time

                time.sleep(min(2 ** attempt, 30))
            try:
                res = self.session.post(
                    api_url,
                    data=payload,
                    headers={
                        "Accept": "application/json, text/plain, */*",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": base,
                    },
                    timeout=self.TIMEOUT,
                )
                res.raise_for_status()
                body = res.json()
            except Exception as e:  # 通信・JSON 解析エラーのみリトライ
                last_error = e
                self.logger.warning(
                    "一覧APIの取得に失敗 (area=%s page=%s, %s回目): %s", area_id, page, attempt + 1, e
                )
                continue

            if body.get("status") != "success":
                last_error = RuntimeError(f"status={body.get('status')}")
                self.logger.warning("一覧APIが success を返しません: %s", body.get("status"))
                continue
            return body.get("data") or {}

        if self.CONTINUE_ON_ERROR:
            self.error_count += 1
            self.logger.warning("一覧APIを諦めます (area=%s page=%s): %s", area_id, page, last_error)
            return None
        raise RuntimeError(f"一覧APIの取得に失敗しました (area={area_id}, page={page}): {last_error}")

    # ------------------------------------------------------------------ #
    # 1 件分の組み立て
    # ------------------------------------------------------------------ #
    def _build_item(self, base: str, job: dict, area_name: str) -> dict | None:
        job_info = job.get("Job") or {}
        company = job.get("Company") or {}
        category = job.get("JobCategory") or {}

        job_id = str(job_info.get("id") or "").strip()
        if not job_id:
            return None

        detail_url = urljoin(base, self.DETAIL_PATH.format(job_id=job_id))
        detail = self._parse_detail(detail_url)

        tel = (company.get("tel") or "").strip()
        if tel == self.COMMON_TEL:
            tel = ""

        name = detail.get("会社名") or (company.get("name") or "").strip()

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: detail.get("都道府県", ""),
            Schema.ADDR: detail.get("住所", ""),
            Schema.TEL: tel,
            Schema.REP_NM: detail.get("代表者", ""),
            Schema.CAP: detail.get("資本金", ""),
            Schema.EMP_NUM: detail.get("従業員数", ""),
            Schema.LOB: detail.get("事業内容", ""),
            Schema.OPEN_DATE: detail.get("設立", ""),
            Schema.CAT_SITE: detail.get("募集職種") or (category.get("name") or "").strip(),
            Schema.HP: detail.get("HP", ""),
            Schema.TIME: detail.get("勤務時間", ""),
            "求人ID": job_id,
            "会社ID": str(company.get("id") or ""),
            "地方": area_name,
            "雇用形態": detail.get("雇用形態", ""),
            "勤務地": detail.get("勤務地", ""),
            "勤務地都道府県": detail.get("勤務地都道府県", ""),
            "勤務地市区町村": detail.get("勤務地市区町村", ""),
            "本社所在地": detail.get("本社所在地", ""),
            "給与額": detail.get("給与額", ""),
            "給与単位": detail.get("給与単位", ""),
            "掲載日": detail.get("掲載日") or self._normalize_date(job_info.get("updated") or ""),
            "電話受付時間": (company.get("receipt_time") or "").strip(),
        }

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> dict:
        soup = self.get_soup(detail_url)
        if soup is None:
            return {}

        result: dict[str, str] = {}
        result.update(self._parse_json_ld(soup))

        for tr in soup.select("#detailArticle tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)

            if label == "会社名":
                result["会社名"] = td.get_text(" ", strip=True)
            elif label == "URL":
                result["HP"] = td.get_text(" ", strip=True)
            elif label == "募集職種":
                result["募集職種"] = td.get_text(" ", strip=True)
            elif label == "雇用形態":
                result["雇用形態"] = td.get_text(" ", strip=True)
            elif label == "勤務時間":
                result["勤務時間"] = td.get_text(" ", strip=True)
            elif label == "代表者":
                result["代表者"] = td.get_text(" ", strip=True)
            elif label == "資本金":
                result["資本金"] = td.get_text(" ", strip=True)
            elif label == "従業員数":
                result["従業員数"] = td.get_text(" ", strip=True)
            elif label == "設立":
                result["設立"] = self._normalize_date(td.get_text(" ", strip=True))
            elif label == "勤務地":
                span = td.find("span")
                result["勤務地"] = span.get_text(" ", strip=True) if span else ""
            elif label == "連絡先住所":
                pref, addr = self._parse_contact_address(td)
                if pref:
                    result["都道府県"] = pref
                if addr:
                    result["住所"] = addr
            elif label == "概要":
                result.update(self._parse_overview(td))

        # 連絡先住所が無い場合は勤務地 (JSON-LD) を住所として使う
        if not result.get("都道府県"):
            result["都道府県"] = result.get("勤務地都道府県", "")
        if not result.get("住所"):
            result["住所"] = result.get("勤務地市区町村", "")

        return result

    @staticmethod
    def _parse_contact_address(td: bs4.element.Tag) -> tuple[str, str]:
        """連絡先住所セル (会社名 <br> 都道府県/市区町村/番地/建物の span 群) を分解する。"""
        parts = [s.get_text(" ", strip=True) for s in td.find_all("span")]
        parts = [p for p in parts if p]
        if not parts:
            return "", ""
        pref = parts[0] if parts[0].endswith(("都", "道", "府", "県")) else ""
        rest = parts[1:] if pref else parts
        return pref, "".join(rest)

    @staticmethod
    def _parse_overview(td: bs4.element.Tag) -> dict:
        """概要セルを 【見出し】 単位に割り、事業内容 (箇条書き) と本社所在地を取り出す。"""
        text = td.get_text("\n", strip=True)
        sections: dict[str, list[str]] = {}
        current = ""
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            m = re.fullmatch(r"【(.+?)】", line)
            if m:
                current = m.group(1)
                sections.setdefault(current, [])
                continue
            if current:
                sections[current].append(line)

        out: dict[str, str] = {}
        for key, values in sections.items():
            joined = [re.sub(r"^[■●・◆□▲\-]\s*", "", v) for v in values]
            joined = [v for v in joined if v]
            if "事業内容" in key:
                out["事業内容"] = "／".join(joined)
            elif "所在地" in key:
                out["本社所在地"] = " ".join(v.replace("住所：", "") for v in joined)
        return out

    # JSON-LD の単位表記 → 日本語
    _PAY_UNITS = {"HOUR": "時給", "DAY": "日給", "MONTH": "月給", "YEAR": "年収", "WEEK": "週給"}

    @classmethod
    def _parse_json_ld(cls, soup: bs4.BeautifulSoup) -> dict:
        """JSON-LD JobPosting から勤務地・掲載日・給与額を取り出す。

        description に生の HTML (未エスケープの改行・引用符) が入っており
        json.loads が失敗するページが多いため、失敗時は正規表現で拾い直す。
        """
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text() or ""
            if "JobPosting" not in raw:
                continue

            try:
                data = json.loads(raw)
            except ValueError:
                return cls._parse_json_ld_by_regex(raw)

            if not isinstance(data, dict) or data.get("@type") != "JobPosting":
                continue

            address = ((data.get("jobLocation") or {}).get("address")) or {}
            salary = ((data.get("baseSalary") or {}).get("value")) or {}
            unit = str(salary.get("unitText") or "").strip()
            return {
                "勤務地都道府県": str(address.get("addressRegion") or "").strip(),
                "勤務地市区町村": str(address.get("addressLocality") or "").strip(),
                "掲載日": str(data.get("datePosted") or "").strip(),
                "給与額": str(salary.get("value") or "").strip(),
                "給与単位": cls._PAY_UNITS.get(unit, unit),
            }
        return {}

    @classmethod
    def _parse_json_ld_by_regex(cls, raw: str) -> dict:
        """壊れた JSON-LD から必要なキーだけを正規表現で拾う。"""

        def pick(key: str) -> str:
            m = re.search(rf'"{key}"\s*:\s*"([^"]*)"', raw)
            return m.group(1).strip() if m else ""

        out = {
            "勤務地都道府県": pick("addressRegion"),
            "勤務地市区町村": pick("addressLocality"),
            "掲載日": pick("datePosted"),
            "給与額": "",
            "給与単位": "",
        }
        m = re.search(
            r'"baseSalary"\s*:\s*\{.*?"value"\s*:\s*\{.*?"value"\s*:\s*"([^"]*)".*?'
            r'"unitText"\s*:\s*"([^"]*)"',
            raw,
            re.S,
        )
        if m:
            out["給与額"] = m.group(1).strip()
            out["給与単位"] = cls._PAY_UNITS.get(m.group(2).strip(), m.group(2).strip())
        return out

    @staticmethod
    def _normalize_date(text: str) -> str:
        """「1971年1月6日」「1996年6月」→ YYYY-MM-DD (欠けた部分は 01 で補う)。"""
        m = re.search(r"(\d{4})\s*年(?:\s*(\d{1,2})\s*月)?(?:\s*(\d{1,2})\s*日)?", text)
        if m:
            year, month, day = m.group(1), m.group(2) or "1", m.group(3) or "1"
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        m = re.search(r"(\d{4})[/-](\d{1,2})(?:[/-](\d{1,2}))?", text)
        if m:
            year, month, day = m.group(1), m.group(2), m.group(3) or "1"
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return text.strip()


if __name__ == "__main__":
    scraper = KeibiBaitoScraper()
    scraper.execute("https://www.keibi-baito.com/")

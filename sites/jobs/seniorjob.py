"""
対象サイト: https://seniorjob.jp/tokyo/security-services/cleaning/
シニアジョブ（清掃 1都3県）

構造メモ:
- Nuxt SSR。一覧の HTML カードは先頭3件のみで残りは遅延描画のため、
  一覧・詳細とも <script id="__NUXT_DATA__"> (devalue 形式) をデコードして読む。
- 一覧: {pref}/security-services/cleaning/ → page-N/ (範囲外は 404)。1ページ25カード、
  各カードの collapsedJobs に「この企業の他の求人」が畳まれている (件数表示はこれを含む)。
- 求人URL: 通常 /company-{cid}/job-{id}/ 、ハローワーク転載 /job-{rawId}/
- status=private は「募集停止」表示 (一覧にも掲載され続ける)。
- 詳細の Nuxt データに企業情報 (代表者/資本金/従業員数/業界/所在地) が埋め込まれている。
  郵便番号だけは企業ページ /company-{cid}/ にしか無いため企業ごとに1回だけ取得してキャッシュ。
- 電話番号はサイト上に掲載なし (応募はサイト経由)。
"""
import json
import re
import time
import urllib.parse
from typing import Generator

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 備考指定の 1都3県 (引数 url の都道府県セグメントを差し替えて巡回する)
_TARGET_PREFS = ["tokyo", "kanagawa", "chiba", "saitama"]
_PREF_RE = re.compile(r"^(.{2,3}?[都道府県])")
_MAX_PAGES = 200

_DEVALUE_TAGS = {"Reactive", "ShallowReactive", "Ref", "ShallowRef", "EmptyRef", "EmptyShallowRef"}


def _decode_nuxt(soup: bs4.BeautifulSoup) -> list | None:
    """__NUXT_DATA__ (devalue) を読み、生配列と解決関数を返すための生配列を返す。"""
    tag = soup.find("script", id="__NUXT_DATA__")
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        return None


def _resolve(data: list, idx, depth: int = 0):
    """devalue の参照インデックスを再帰的に実体化する。"""
    if not isinstance(idx, int) or isinstance(idx, bool):
        return idx
    if idx < 0 or idx >= len(data) or depth > 30:
        return None
    x = data[idx]
    if isinstance(x, list):
        if len(x) == 2 and isinstance(x[0], str):
            if x[0] in _DEVALUE_TAGS:
                return _resolve(data, x[1], depth + 1)
            if x[0] == "Date":
                return x[1]
        if x and isinstance(x[0], str) and x[0] in ("Set", "Map"):
            return [_resolve(data, j, depth + 1) for j in x[1:]]
        return [_resolve(data, j, depth + 1) for j in x]
    if isinstance(x, dict):
        return {k: _resolve(data, v, depth + 1) for k, v in x.items()}
    return x


def _find_objects(data: list, required_keys: tuple) -> list[dict]:
    """必要キーをすべて持つ dict を実体化して返す。"""
    out = []
    for i, x in enumerate(data):
        if isinstance(x, dict) and all(k in x for k in required_keys):
            out.append(_resolve(data, i))
    return out


def _s(v) -> str:
    if v is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(v)).strip()


def _name(v) -> str:
    return _s(v.get("name")) if isinstance(v, dict) else ""


def _date(v) -> str:
    """ISO日時 → YYYY-MM-DD。和暦風 (2026年12月31日) もそのまま正規化。"""
    v = _s(v)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        return "-".join(m.groups())
    m = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", v)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return v


def _zip(v) -> str:
    d = re.sub(r"\D", "", _s(v))
    return f"{d[:3]}-{d[3:]}" if len(d) == 7 else ""


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _s(addr)
    m = _PREF_RE.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return "", addr


class SeniorjobScraper(StaticCrawler):
    """シニアジョブ（清掃 1都3県） スクレイパー"""
    DELAY = 3.0  # 待機時間（秒）。get_soup 前に毎回待機する
    ITEM_DELAY = 0  # 待機は通信側に寄せる

    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "職種",
        "雇用形態",
        "給与",
        "仕事内容",
        "勤務地都道府県",
        "勤務地郵便番号",
        "勤務地住所",
        "最寄り駅",
        "掲載状態",
        "掲載開始日",
        "更新日",
        "募集状況確認日",
        "掲載期限",
        "求人区分",
        "ハローワーク求人番号",
        "企業ページURL",
    ]

    def _get(self, url: str) -> bs4.BeautifulSoup | None:
        time.sleep(self.DELAY)
        return self.get_soup(url)

    # ------------------------------------------------------------------ 一覧
    def _list_urls(self, url: str) -> list[str]:
        """引数 url の都道府県セグメントを 1都3県 に差し替えた一覧URLを返す。"""
        parts = urllib.parse.urlsplit(url)
        segs = [s for s in parts.path.split("/") if s]
        # page-N/ 付きで渡された場合は落とす
        segs = [s for s in segs if not re.fullmatch(r"page-\d+", s)]
        if not segs:
            return [url]
        rest = "/".join(segs[1:])
        urls = [url.split("page-")[0] if "page-" in url else url]
        for pref in _TARGET_PREFS:
            path = f"/{pref}/{rest}/" if rest else f"/{pref}/"
            u = urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, "", ""))
            if u not in urls:
                urls.append(u)
        return urls

    @staticmethod
    def _cards(soup: bs4.BeautifulSoup) -> list[dict]:
        data = _decode_nuxt(soup)
        if not data:
            return []
        return _find_objects(data, ("collapsedJobsTotal", "isHelloworkJob", "rawId"))

    @staticmethod
    def _job_url(base: str, job: dict, company_id) -> str:
        if job.get("isHelloworkJob") or (job.get("rawId") and "-" in str(job.get("rawId"))):
            return urllib.parse.urljoin(base, f"/job-{job.get('rawId') or job.get('id')}/")
        cid = (job.get("company") or {}).get("id") if isinstance(job.get("company"), dict) else None
        cid = cid or company_id
        return urllib.parse.urljoin(base, f"/company-{cid}/job-{job.get('id')}/")

    # ------------------------------------------------------------------ main
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        self._company_zip: dict[str, str] = {}

        for list_url in self._list_urls(url):
            for page in range(1, _MAX_PAGES + 1):
                page_url = list_url if page == 1 else urllib.parse.urljoin(list_url, f"page-{page}/")
                soup = self._get(page_url)
                if soup is None:
                    break  # 範囲外 page は 404
                cards = self._cards(soup)
                if not cards:
                    break
                for card in cards:
                    company = card.get("company") if isinstance(card.get("company"), dict) else {}
                    jobs = [card] + [j for j in (card.get("collapsedJobs") or []) if isinstance(j, dict)]
                    for job in jobs:
                        if not job.get("id"):
                            continue
                        job_url = self._job_url(list_url, job, company.get("id"))
                        if job_url in seen:
                            continue
                        seen.add(job_url)
                        item = self._detail(job_url, job, card)
                        if item:
                            yield item

    # ------------------------------------------------------------------ 詳細
    def _detail(self, job_url: str, card_job: dict, card: dict) -> dict | None:
        soup = self._get(job_url)
        if soup is None:
            return None
        data = _decode_nuxt(soup) or []
        if "/company-" not in job_url:
            return self._hellowork(job_url, soup, data, card_job)

        jobs = [o for o in _find_objects(data, ("status", "workLocations", "endDate"))
                if _s(o.get("id")) == _s(card_job.get("id"))]
        job = jobs[0] if jobs else {}
        comps = _find_objects(data, ("representativeName", "capitalStock"))
        comp = comps[0] if comps else {}
        if not job and not comp:
            return None

        card_company = card.get("company") if isinstance(card.get("company"), dict) else {}
        cid = _s(comp.get("id") or card_company.get("id"))
        m = re.search(r"/company-(\d+)/", job_url)
        cid = cid or (m.group(1) if m else "")
        company_url = urllib.parse.urljoin(job_url, f"/company-{cid}/") if cid else ""

        # 会社所在地 (企業の会社概要)
        city = comp.get("city") if isinstance(comp.get("city"), dict) else {}
        c_pref = _name(city.get("prefecture"))
        c_addr = (_name(city) + _s(comp.get("addressDetail"))).strip()

        # 勤務地
        w_prefs, w_zips, w_addrs, stations = [], [], [], []
        for wl in job.get("workLocations") or []:
            if not isinstance(wl, dict):
                continue
            wcity = wl.get("city") if isinstance(wl.get("city"), dict) else {}
            wp = _name(wcity.get("prefecture"))
            if wp and wp not in w_prefs:
                w_prefs.append(wp)
            z = _zip(wl.get("zipcode"))
            if z and z not in w_zips:
                w_zips.append(z)
            a = (wp + _name(wcity) + _s(wl.get("addressDetail"))).strip()
            if a and a not in w_addrs:
                w_addrs.append(a)
            for st in wl.get("nearestStations") or []:
                if isinstance(st, dict):
                    sn = _name(st.get("station"))
                    if sn:
                        sn = f"{sn}駅 {_s(st.get('detail'))}".strip()
                        if sn not in stations:
                            stations.append(sn)
        if not w_addrs:
            w_addrs = [_s(card_job.get("workLocationsText"))]
            p, _ = _split_pref(w_addrs[0])
            if p:
                w_prefs = [p]

        # 業界: 大分類 ＞ 小分類
        industries = []
        for si in comp.get("subIndustries") or []:
            if isinstance(si, dict):
                big = _name(si.get("industry"))
                industries.append(f"{big} ＞ {_s(si.get('name'))}" if big else _s(si.get("name")))

        status = _s(job.get("status") or card_job.get("status"))
        cap = _s(comp.get("capitalStock"))
        emp = _s(comp.get("employeeNumber"))

        return {
            Schema.URL: job_url,
            Schema.NAME: _s(comp.get("name") or card_company.get("name")),
            Schema.PREF: c_pref,
            Schema.POST_CODE: self._company_postcode(company_url) if company_url else "",
            Schema.ADDR: c_addr,
            Schema.TEL: "",
            Schema.REP_NM: _s(comp.get("representativeName")),
            Schema.EMP_NUM: f"{emp}人" if emp else "",
            Schema.CAP: f"{cap}円" if cap.isdigit() else cap,
            Schema.CAT_SITE: " / ".join(industries),
            Schema.OPEN_DATE: _date(comp.get("establishmentDate")),
            "求人ID": _s(job.get("id") or card_job.get("id")),
            "求人タイトル": _s(job.get("title") or card_job.get("title")),
            "職種": _name(job.get("occupation") or card_job.get("occupation")),
            "雇用形態": _name(job.get("employmentType") or card_job.get("employmentType")),
            "給与": _s(card_job.get("salaryText")),
            "仕事内容": (job.get("detail") or card_job.get("detail") or "").strip(),
            "勤務地都道府県": " / ".join(w_prefs),
            "勤務地郵便番号": " / ".join(w_zips),
            "勤務地住所": " / ".join(w_addrs),
            "最寄り駅": " / ".join(stations),
            "掲載状態": self._status_label(status, soup),
            "掲載開始日": _date(job.get("createdDate") or card_job.get("createdDate")),
            "更新日": _date(job.get("updatedDate") or card_job.get("updatedDate")),
            "募集状況確認日": _date(job.get("confirmedDate")),
            "掲載期限": _date(job.get("endDate") or card_job.get("endDate")),
            "求人区分": "シニアジョブ掲載",
            "ハローワーク求人番号": "",
            "企業ページURL": company_url,
        }

    @staticmethod
    def _status_label(status: str, soup: bs4.BeautifulSoup) -> str:
        if status == "private" or soup.find(string=re.compile("この求人は募集停止中です")):
            return "募集停止"
        if status == "public":
            return "募集中"
        return status

    def _company_postcode(self, company_url: str) -> str:
        """企業ページの郵便番号 (詳細ページの埋め込み企業情報には無い)。企業ごとに1回だけ取得。"""
        if company_url in self._company_zip:
            return self._company_zip[company_url]
        zipcode = ""
        soup = self._get(company_url)
        if soup is not None:
            data = _decode_nuxt(soup) or []
            for o in _find_objects(data, ("zipcode", "representativeName")):
                zipcode = _zip(o.get("zipcode"))
                if zipcode:
                    break
        self._company_zip[company_url] = zipcode
        return zipcode

    # ------------------------------------------------------- ハローワーク転載
    def _hellowork(self, job_url: str, soup: bs4.BeautifulSoup, data: list, card_job: dict) -> dict | None:
        hws = _find_objects(data, ("jgshMei", "kjNo"))
        hw = hws[0] if hws else {}
        name = _s(hw.get("jgshMei") or _name(card_job.get("company")))
        if not name:
            return None
        c_pref, c_addr = _split_pref(hw.get("szci"))
        w_addr = _s(hw.get("shgBsJusho")) or _s(card_job.get("workLocationsText"))
        w_pref, _ = _split_pref(w_addr)
        if not w_pref:
            w_pref = " / ".join(dict.fromkeys(
                _s(wl.get("prefectureName")) for wl in hw.get("workLocations") or []
                if isinstance(wl, dict) and wl.get("prefectureName")))
        emp = _s(hw.get("jgisKigyoZentai"))
        status = _s(card_job.get("status"))
        station = _s(hw.get("shgBsMyorEki"))
        if station and hw.get("shgBsShyoJn"):
            station = f"{station} {_s(hw.get('shgBsKotsuShudan'))}{_s(hw.get('shgBsShyoJn'))}"
        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: c_pref,
            Schema.POST_CODE: "",
            Schema.ADDR: c_addr,
            Schema.TEL: "",
            Schema.REP_NM: "",
            Schema.EMP_NUM: emp,
            Schema.CAP: "",
            Schema.CAT_SITE: "",
            Schema.OPEN_DATE: "",
            "求人ID": _s(card_job.get("rawId") or hw.get("kjNo")),
            "求人タイトル": _s(hw.get("title") or card_job.get("title")),
            "職種": _name(hw.get("occupation") or card_job.get("occupation")),
            "雇用形態": _s(hw.get("koyoKeitai")) or _name(card_job.get("employmentType")),
            "給与": _s(card_job.get("salaryText")),
            "仕事内容": (hw.get("shigotoNy") or "").strip(),
            "勤務地都道府県": w_pref,
            "勤務地郵便番号": _zip(hw.get("shgBsYubinNo")),
            "勤務地住所": w_addr,
            "最寄り駅": station,
            "掲載状態": self._status_label(status, soup),
            "掲載開始日": _date(card_job.get("createdDate")),
            "更新日": _date(card_job.get("updatedDate")),
            "募集状況確認日": _date(hw.get("crawledDate")),
            "掲載期限": _date(hw.get("shkiKigenHi") or card_job.get("endDate")),
            "求人区分": f"ハローワーク転載（{_s(hw.get('juriAtsh') or card_job.get('hwSource'))}）",
            "ハローワーク求人番号": _s(hw.get("kjNo")),
            "企業ページURL": "",
        }


if __name__ == "__main__":
    scraper = SeniorjobScraper()
    scraper.execute("https://seniorjob.jp/tokyo/security-services/cleaning/")

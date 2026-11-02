"""
ゼヒトモ（Zehitomo）— プロフィール全件 (関西・四国の 8 府県在住のプロのみ)

対象サイト: https://www.zehitomo.com/sitemap-pro-profile-index.xml

取得対象:
    - sitemap index に並ぶ sitemap-pro-profile-{0..5}.xml.gz の全プロフィール (/profile/{slug}/pro、約 82,800 件)
    - カテゴリページ・エリアページ・service-profile / case-study / review の sitemap は対象外
    - 住所 (所在地) の都道府県が以下 8 府県のプロのみ出力する (それ以外は保存しない):
      滋賀・京都・大阪・兵庫・奈良・和歌山・徳島・高知

取得フロー:
    1. 引数 url (sitemap index) から sitemap-pro-profile-*.xml.gz だけを抜き出す
    2. 各 .xml.gz を gunzip して <loc> のプロフィール URL を列挙する
    3. URL をパーセントエンコードしてから 1 件ずつ取得し、__NEXT_DATA__ の
       props.pageProps (initialState.users.profiles.pro) から項目を取り出す
    3'. sitemap は所在地の塊で並ぶため、全 shard を読んでからラウンドロビンで巡回する
    4. 都道府県が対象 8 府県なら即 yield する

このサイト固有の注意点 (Phase 1 の実測で判明):
    - sitemap の <loc> は日本語 slug のまま。未エンコードで送ると HTTP 400 になるため quote してから取得する。
    - requests (Chrome UA) で 200 が返るが 504 / 502 / 接続切断が起きるため、
      1 件ごとに最大 MAX_ATTEMPTS 回まで指数バックオフで再試行し、回復しない URL はスキップする。
    - .xml.gz は Content-Encoding ではなく gzip ファイルそのものが返るので、自前で gunzip する。
    - 一部のプロフィールは無限リダイレクト (TooManyRedirects) になる。待っても直らないので即スキップ。
    - location.prefecture が欠落したプロフィールがあるので、住所文字列の先頭から都道府県を復元する。
    - TEL は構造化項目に無い。自己紹介文 (description / skills / interest / title / jobProfiles[].description /
      seoDescription) から正規表現で抽出する (ヒット率は 1 割未満で空欄が普通)。
      自己紹介文そのものは著作権リスクのためカラムとして出力しない。
    - HP (website) は zehitomo.com と res.cloudinary.com (画像 CDN) を除外する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/zehitomo_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zehitomo_5
"""

import gzip
import json
import logging
import re
import sys
import time
import unicodedata
import warnings
from itertools import zip_longest
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.DOTALL)
_PRO_SITEMAP_RE = re.compile(r"sitemap-pro-profile-\d+\.xml(?:\.gz)?$")
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)
_ZIP_RE = re.compile(r"^(\d{3})(\d{4})$")
_PREF_RE = re.compile(r"^(北海道|東京都|(?:京都|大阪)府|.{2,3}県)")

# 出力対象の 8 府県 (住所の都道府県で判定)
_TARGET_PREFS = frozenset({
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "徳島県", "高知県",
})

# 掲載終了 jobType の日本語名はページに無く、この公開 API でのみ引ける ({id} = jobType slug)
_JOB_TYPE_API = "https://api.zehitomo.com/api/job-types/{id}"

# HP から除外するドメイン (自サイト・画像 CDN)
_HP_EXCLUDE_HOSTS = ("zehitomo.com", "res.cloudinary.com")

# TEL 抽出: NFKC + ダッシュ正規化後のテキストに適用する。
# 法人番号 13 桁や UNIX 時刻が混ざるので前後の数字境界は必須
_DASHES_RE = re.compile(r"[‐‑‒–—―−ーｰ─━]")
_TEL_RE = re.compile(
    r"(?<![\d-])(0\d{1,4}[-\s(]{0,2}\d{1,4}[-\s)]{0,2}\d{3,4})(?![\d-])"
)

# sitemap index (XML) を get_soup (html.parser) で読むための警告抑止。<loc> の抽出は正規表現で行う
warnings.filterwarnings("ignore", category=bs4.XMLParsedAsHTMLWarning)


def _encode_url(url: str) -> str:
    """マルチバイト文字をパーセントエンコードする (既存の %xx は二重エンコードしない)。"""
    return quote(url, safe=":/?#[]@!$&'()*+,;=%~")


class Zehitomo5Scraper(StaticCrawler):
    """ゼヒトモ スクレイパー — sitemap からプロフィール全件を巡回し、対象 8 府県 (関西・四国) のみ出力する版"""

    DELAY = 1.5  # 最低リクエスト間隔 (秒)。約 8 万ページを巡回するため短め。実際の間隔制御は _throttle で行う
    ITEM_DELAY = 0.0  # 待機は _throttle に一本化する (失敗/スキップ時も間隔を守るため)
    TIMEOUT = 20
    MAX_ATTEMPTS = 4  # 1 URL あたりの最大試行回数
    MAX_BACKOFF = 30  # バックオフ上限 (秒)
    CONTINUE_ON_ERROR = False  # get_soup の例外を自前で分類するため、例外をそのまま受け取る

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = ["事業者ユーザーID", "取得URL_ゼヒトモ"]

    _last_request_at: float = 0.0
    _job_type_names: dict[str, str] | None = None

    # ------------------------------------------------------------------
    # 取得ユーティリティ
    # ------------------------------------------------------------------
    def _disable_session_retry(self):
        """基盤セッションの urllib3 自動リトライを無効化する。

        再試行は _fetch で 1 URL 最大 MAX_ATTEMPTS 回に制御するため、二重リトライを避ける。
        """
        no_retry = Retry(total=0, raise_on_status=False)
        self.session.mount("https://", HTTPAdapter(max_retries=no_retry))
        self.session.mount("http://", HTTPAdapter(max_retries=no_retry))

    def _throttle(self):
        """直前の実通信から DELAY 秒空ける。"""
        wait = self.DELAY - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)

    def _with_retry(self, url: str, fetch_once):
        """fetch_once(url) を最大 MAX_ATTEMPTS 回試す。回復しなければ None を返す。"""
        for attempt in range(self.MAX_ATTEMPTS):
            self._throttle()
            try:
                result = fetch_once(url)
                if not self._last_fetch_from_cache:
                    self._last_request_at = time.monotonic()
                return result
            except requests.exceptions.TooManyRedirects:
                self._last_request_at = time.monotonic()
                logger.warning("無限リダイレクトのためスキップ: %s", url)
                return None
            except requests.exceptions.RequestException as e:
                self._last_request_at = time.monotonic()
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status in (404, 410):
                    logger.warning("ページが存在しないためスキップ (%s): %s", status, url)
                    return None
                if status == 403:
                    # 旧 Cloudflare 対策: Cookie を持ち越すと 2 回目以降が弾かれることがあった
                    self.session.cookies.clear()
                logger.warning(
                    "取得失敗 (%d/%d): %s — %s", attempt + 1, self.MAX_ATTEMPTS, url, e
                )
            if attempt + 1 < self.MAX_ATTEMPTS:
                time.sleep(min(2 ** (attempt + 1), self.MAX_BACKOFF))
        logger.error("再試行上限に達したためスキップ: %s", url)
        self.error_count += 1
        return None

    def _fetch_bytes_once(self, url: str) -> bytes:
        """gzip サイトマップ用。get_soup は text 化してしまうためバイト列で取得する。"""
        self._last_fetch_from_cache = False
        logger.info("取得中: %s", url)
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        return response.content

    @staticmethod
    def _next_data(soup: bs4.BeautifulSoup) -> dict:
        """__NEXT_DATA__ の JSON を辞書で返す (無ければ空辞書)。"""
        tag = soup.find("script", id="__NEXT_DATA__")
        raw = tag.string if tag is not None else None
        if not raw:
            m = _NEXT_DATA_RE.search(str(soup))
            raw = m.group(1) if m else None
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning("__NEXT_DATA__ の解析に失敗: %s", e)
            return {}

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        """引数 url (= sites.yml の url, sitemap index) を唯一の起点として巡回する。"""
        self._disable_session_retry()

        index_soup = self._with_retry(url, self.get_soup)
        if index_soup is None:
            raise RuntimeError(f"sitemap index を取得できませんでした: {url}")

        sitemap_urls = [
            urljoin(url, loc)
            for loc in _LOC_RE.findall(str(index_soup))
            if _PRO_SITEMAP_RE.search(loc.strip())
        ]
        if not sitemap_urls:
            raise RuntimeError(f"sitemap-pro-profile が見つかりません: {url}")
        logger.info("プロフィール sitemap %d 本を巡回します", len(sitemap_urls))

        # sitemap 内は所在地ごとの塊で並ぶ (shard0 先頭は東京都が数千件続く) ため、
        # 全 shard を先に読み (計 6 通信) ラウンドロビンで巡回して対象府県の出現を平準化する
        shards: list[list[str]] = []
        for sitemap_url in sitemap_urls:
            profile_urls = self._read_sitemap(sitemap_url)
            logger.info("%s: %d 件", sitemap_url, len(profile_urls))
            shards.append(profile_urls)

        seen: set[str] = set()
        for profile_urls in zip_longest(*shards):
            for profile_url in profile_urls:
                if profile_url is None:
                    continue
                # 日本語 slug のまま送ると HTTP 400 になることがあるため、必ずパーセントエンコードする
                profile_url = _encode_url(profile_url)
                if profile_url in seen:
                    continue
                seen.add(profile_url)
                item = self._scrape_detail(profile_url)
                if item is not None:
                    yield item

    def _read_sitemap(self, sitemap_url: str) -> list[str]:
        """.xml.gz を gunzip して <loc> のプロフィール URL を返す。"""
        data = self._with_retry(sitemap_url, self._fetch_bytes_once)
        if data is None:
            return []
        if data[:2] == b"\x1f\x8b":
            try:
                data = gzip.decompress(data)
            except OSError as e:
                logger.error("sitemap の展開に失敗: %s — %s", sitemap_url, e)
                return []
        text = data.decode("utf-8", errors="replace")
        return [loc for loc in _LOC_RE.findall(text) if "/profile/" in loc]

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self._with_retry(url, self.get_soup)
        if soup is None:
            return None

        page_props = self._next_data(soup).get("props", {}).get("pageProps", {})
        pro = (
            page_props.get("initialState", {})
            .get("users", {})
            .get("profiles", {})
            .get("pro")
        )
        if not isinstance(pro, dict):
            logger.warning("プロフィール情報が見つかりません: %s", url)
            return None

        profile = pro.get("profile") or {}
        pro_profile = pro.get("proProfile") or {}
        location = pro.get("location") or {}

        pref = ((location.get("prefecture") or {}).get("name") or "").strip()
        if not pref:
            title = ((pro_profile.get("location") or {}).get("title") or location.get("name") or "")
            m = _PREF_RE.match(title.strip())
            pref = m.group(1) if m else ""
        if pref not in _TARGET_PREFS:
            logger.debug("対象外の都道府県 (%s): %s", pref or "不明", url)
            return None

        name = (
            pro.get("name")
            or profile.get("companyNameKanji")
            or self._join_name(profile.get("fullNameKanji"), sep="")
        )
        if not name:
            logger.warning("名称が無いためスキップ: %s", url)
            return None

        job_titles = self._enabled_job_titles(pro)
        main_job = self._main_job_title(pro, pro_profile) or (job_titles[0] if job_titles else "")
        if not job_titles:
            # 古いアカウントは enabledJobTypes / jobProfiles が空で、proProfile.jobTypes の slug しか無い
            job_titles = self._fallback_job_titles(pro, pro_profile)
            main_job = main_job or (job_titles[0] if job_titles else "")

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: self._format_zip(
                (pro_profile.get("location") or {}).get("zipCode") or location.get("zipCode")
            ),
            Schema.ADDR: self._build_address(location, pro_profile, pref),
            Schema.REP_NM: self._join_name(profile.get("fullNameKanji")),
            Schema.TEL: self._extract_tel(pro, pro_profile),
            Schema.HP: self._pick_website(pro_profile.get("website")),
            Schema.LOB: "、".join(job_titles),
            Schema.CAT_SITE: main_job,
            Schema.URL: url,
            "事業者ユーザーID": pro.get("id") or page_props.get("proUserId") or "",
            "取得URL_ゼヒトモ": url,
        }

    # ------------------------------------------------------------------
    # 値の整形
    # ------------------------------------------------------------------
    @staticmethod
    def _build_address(location: dict, pro_profile: dict, pref: str) -> str:
        """住所 (市区町村以降) を組み立てる。

        location の city + townArea を優先し、無ければ
        proProfile.location.title (「大阪府大阪市北区…」) から都道府県を除いて使う。
        町名未登録時に "undefined" が入ることがあるので除外する。
        """
        city = ((location.get("city") or {}).get("name") or "").strip()
        town = ((location.get("townArea") or {}).get("name") or "").strip()
        if town == "undefined":
            town = ""
        if city and city != "undefined":
            return f"{city}{town}"
        title = ((pro_profile.get("location") or {}).get("title") or "").strip()
        title = title.replace("undefined", "").strip()
        if pref and title.startswith(pref):
            return title[len(pref):].strip()
        return title

    @staticmethod
    def _join_name(name: dict | None, sep: str = " ") -> str:
        """{"last": "長南", "first": "芳昭"} を「長南 芳昭」に整形する。"""
        if not isinstance(name, dict):
            return ""
        last = (name.get("last") or "").strip()
        first = (name.get("first") or "").strip()
        return sep.join(p for p in (last, first) if p)

    @staticmethod
    def _format_zip(value: str | None) -> str:
        """0030831 → 003-0831。想定外の形式はそのまま返す。"""
        raw = re.sub(r"\D", "", value or "")
        m = _ZIP_RE.match(raw)
        return f"{m.group(1)}-{m.group(2)}" if m else (value or "")

    @staticmethod
    def _enabled_job_titles(pro: dict) -> list[str]:
        """掲載中のサービス名 (enabledJobTypes の title) を重複なしで返す。"""
        titles: list[str] = []
        for job in pro.get("enabledJobTypes") or []:
            title = (job or {}).get("title")
            if title and title not in titles:
                titles.append(title)
        return titles

    def _fallback_job_titles(self, pro: dict, pro_profile: dict) -> list[str]:
        """jobProfiles / reviews / proProfile.jobTypes の slug からサービス名を復元する。"""
        titles: list[str] = []
        for job in pro.get("jobProfiles") or []:
            title = (job or {}).get("label") or (job or {}).get("title")
            if title and title not in titles:
                titles.append(title)
        for review in (pro.get("reviews") or {}).get("list") or []:
            title = (review or {}).get("jobTypeName")
            if title and title not in titles:
                titles.append(title)
        if titles:
            return titles
        job_type_ids = list(pro_profile.get("enabledJobTypesArray") or [])
        job_type_ids += [k for k in (pro_profile.get("jobTypes") or {}) if k not in job_type_ids]
        for job_type_id in job_type_ids:
            title = self._job_type_name(job_type_id)
            if title and title not in titles:
                titles.append(title)
        return titles

    def _job_type_name(self, job_type_id: str) -> str:
        """jobType slug → 日本語名 (公開 API。結果はキャッシュする)。"""
        if self._job_type_names is None:
            self._job_type_names = {}
        if job_type_id in self._job_type_names:
            return self._job_type_names[job_type_id]
        data = self._with_retry(_JOB_TYPE_API.format(id=quote(job_type_id, safe="")), self._fetch_json_once)
        translations = (data or {}).get("translations") or {}
        name = (
            ((translations.get("requestType") or {}).get("ja"))
            or ((translations.get("seoName") or {}).get("ja"))
            or ""
        )
        self._job_type_names[job_type_id] = name
        return name

    def _fetch_json_once(self, url: str) -> dict:
        self._last_fetch_from_cache = False
        logger.info("取得中: %s", url)
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _main_job_title(pro: dict, pro_profile: dict) -> str:
        """メインの jobType (proProfile.mainJobType) の日本語名。"""
        main_id = pro_profile.get("mainJobType")
        if not main_id:
            return ""
        for job in pro.get("enabledJobTypes") or []:
            if (job or {}).get("id") == main_id and job.get("title"):
                return job["title"]
        for job in pro.get("jobProfiles") or []:
            if (job or {}).get("jobTypeId") == main_id:
                return job.get("label") or job.get("title") or ""
        return ""

    @staticmethod
    def _pick_website(value) -> str:
        """website 欄。ゼヒトモ自身・画像 CDN の URL は HP ではないので除外する。"""
        website = (value or "").strip() if isinstance(value, str) else ""
        if not website:
            return ""
        host = urlsplit(website if "://" in website else f"http://{website}").netloc.lower()
        if any(host == h or host.endswith("." + h) for h in _HP_EXCLUDE_HOSTS):
            return ""
        return website

    @staticmethod
    def _extract_tel(pro: dict, pro_profile: dict) -> str:
        """自己紹介文から電話番号を 1 件抽出する (無ければ空欄)。本文自体は出力しない。"""
        texts: list[str] = []
        for key in ("title", "description", "skills", "interest"):
            value = pro_profile.get(key)
            if isinstance(value, str):
                texts.append(value)
        for job in pro.get("jobProfiles") or []:
            value = (job or {}).get("description")
            if isinstance(value, str):
                texts.append(value)
        if isinstance(pro.get("seoDescription"), str):
            texts.append(pro["seoDescription"])

        for text in texts:
            normalized = _DASHES_RE.sub("-", unicodedata.normalize("NFKC", text))
            for m in _TEL_RE.finditer(normalized):
                digits = re.sub(r"\D", "", m.group(1))
                if len(digits) == 10 or (len(digits) == 11 and re.match(r"0(?:[5789]0|120|800)", digits)):
                    return re.sub(r"[\s()]+", "-", m.group(1)).strip("-").replace("--", "-")
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Zehitomo5Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.zehitomo.com/sitemap-pro-profile-index.xml")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
イツザイ（it'szai） — 採用サイト制作サービス「イツザイ」導入企業（関東エリア）

取得対象:
    イツザイ（サングローブ株式会社が提供する採用サイト制作・応募獲得サービス）を
    導入し、採用サイトが公開されている企業のうち **関東 1 都 6 県**
    （東京都 / 神奈川県 / 埼玉県 / 千葉県 / 茨城県 / 栃木県 / 群馬県）に
    勤務地がある企業のみ。全国の掲載総数は約 570 社。

取得フロー:
    1. 正規 URL (https://www.itszai.jp/) の「イツザイ制作事例」リンクから
       制作実績ギャラリー (sungrove.co.jp/itszai/) を発見する。
       ※ ギャラリー URL はハードコードせず、必ず引数 url から辿って解決する。
    2. ギャラリーを業種フィルタ (?industry[0]=NN) 単位で巡回する。
       これにより各社の「サイト定義業種」を確定できる。1 ページ 30 件、
       ページ送りは /itszai/page/N/。最後に業種無指定の一覧も巡回し、
       業種が付いていない取りこぼしを拾う。
    3. 各カードのリンク先 = 採用サイトのトップ求人ページ
       (clients.itszai.jp/{customer}/job/{job}) は SSR 済みの静的 HTML。
       <title> が「{社名} - {都道府県} {最寄駅} {職種} 採用サイト」形式なので
       ここで都道府県を判定し、**関東以外はこの時点で除外**する
       (募集要項ページのレンダリングを行わないため大幅に高速)。
       社名・社名カナは __NEXT_DATA__ の companyInfo から取得する。
    4. 関東の企業のみ、募集要項ページ (/{customer}/recruitments/{id}) を
       Playwright でレンダリングして住所・郵便番号・TEL・勤務時間等を取得し、
       1 件取得するごとに即 yield する (Pattern B)。

備考:
    - 募集要項ページの本文は __NEXT_DATA__ 内で AES 暗号化されており、
      静的取得では読めない。JS 実行後に描画されるため DynamicCrawler が必須。
      ただし描画は networkidle 到達で完了するので待機は get_soup に任せる。
    - 一覧・求人ページ (手順 2-3) は静的 HTML のため requests で取得し、
      重い Playwright 描画は関東の企業に絞って行う。
    - 仕事内容・アピールポイント・福利厚生・応募について等の長文
      (自由記述プロース) は著作権リスクを避けるためカラム化しない。
      TEL のみ「応募について」欄から電話番号を正規表現で抽出する。
    - 掲載企業は採用サイトのため代表者名・資本金・従業員数・法人番号は
      サイト上に存在せず取得できない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/itszai.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id itszai
"""

import json
import re
import sys
import time
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

# --- 関東 1 都 6 県 (備考「関東エリアのみ」のフィルタ条件) ---
KANTO_PREFS = (
    "東京都",
    "神奈川県",
    "埼玉県",
    "千葉県",
    "茨城県",
    "栃木県",
    "群馬県",
)

_ALL_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
# <title> は「{社名} - {都道府県} {最寄駅} {職種} 採用サイト」形式。
# 社名自体に県名が含まれる誤検出を防ぐため、区切り「 - 」の直後のみを見る。
_TITLE_PREF_RE = re.compile(r"[-–—]\s*(" + _ALL_PREFS + r")(?=\s|$)")
_ADDR_PREF_RE = re.compile(r"(" + _ALL_PREFS + r")")
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3})[-ー－]?\s*(\d{4})")
_TEL_RE = re.compile(r"0\d{1,4}[-(（]\d{1,4}[-)）]\d{3,4}|0\d{9,10}")
_TIME_RANGE_RE = re.compile(r"\d{1,2}:\d{2}\s*[~〜～\-–]\s*\d{1,2}:\d{2}")
_DATE_RE = re.compile(r"(\d{4})[/年.-](\d{1,2})[/月.-](\d{1,2})")
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)
_SUBDOMAIN_RE = re.compile(r"https://([a-z0-9][a-z0-9\-]*)\.itszai\.jp/")

# 募集要項ページで取得する見出し (h3) ラベル。
# 長文の自由記述 (仕事内容 / アピールポイント / 福利厚生 / 備考 等) は
# 著作権リスクを避けるため意図的に対象外とする。
_SECTION_LABELS = (
    "勤務地",
    "最寄駅",
    "勤務時間",
    "雇用形態",
    "契約期間",
    "給与",
    "休日休暇",
    "加入保険",
    "受動喫煙防止措置",
    "応募について",
)


class Itszai(DynamicCrawler):
    """イツザイ（it'szai）導入企業スクレイパー（関東エリア限定）"""

    DELAY = 0.3          # 1 アイテム書き出しごとの待機 (execute 側)
    ITEM_DELAY = 0.0
    MIN_INTERVAL = 0.4   # 静的リクエストの最低間隔 (秒)
    LIST_PAGE_SIZE = 30  # ギャラリー 1 ページあたりのカード数
    MAX_PAGES = 40       # 暴走防止 (実際は最大 19 ページ)

    EXTRA_COLUMNS = [
        "募集職種",
        "雇用形態",
        "給与",
        "最寄駅",
        "契約期間",
        "加入保険",
        "受動喫煙防止措置",
        "特徴タグ",
        "最終更新日",
        "求人ページURL",
    ]

    # ギャラリーの業種フィルタ (industry[0] の値 → 表示名)。
    # 掲載件数の多い業種を先に巡回し、早期に 1 件目を yield する。
    INDUSTRIES = [
        ("56", "建築/⼟⽊/建設"),
        ("63", "医療/介護/福祉"),
        ("59", "ドライバー"),
        ("67", "⼯場/製造"),
        ("49", "美容師/アイデザイナー/ネイリスト"),
        ("79", "接客/販売/サービス系"),
        ("87", "飲食/フード"),
        ("53", "その他"),
        ("70", "鍼灸/あん摩/マッサージ/エステ"),
        ("82", "IT/コンピューター系"),
        ("46", "警備"),
        ("81", "農業/造園/酪農"),
        ("85", "営業"),
        ("88", "動物関連"),
        ("89", "士業"),
        ("230", "電気工事関係"),
        ("362", "事務"),
        ("76", "教育"),
        ("72", "漁業系"),
    ]

    _last_request_at = 0.0

    # ------------------------------------------------------------------
    # セットアップ
    # ------------------------------------------------------------------
    def _setup(self):
        """Playwright に加えて、静的ページ取得用の requests セッションを用意する。

        一覧ページと求人ページ (SSR 済み HTML) は requests で十分なうえ高速で、
        Playwright での描画は暗号化された募集要項ページだけに限定できる。
        """
        super()._setup()

        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ja,en;q=0.8",
            }
        )

    def _teardown_resources(self):
        if getattr(self, "session", None):
            self.session.close()
        super()._teardown_resources()

    # ------------------------------------------------------------------
    # 静的取得ヘルパー
    # ------------------------------------------------------------------
    def _get_static(self, url: str) -> str | None:
        """requests で HTML を取得する。404 等はスキップ扱い (None) で返す。"""
        wait = self.MIN_INTERVAL - (time.monotonic() - Itszai._last_request_at)
        if wait > 0:
            time.sleep(wait)
        try:
            res = self.session.get(url, timeout=30)
        except Exception as e:  # noqa: BLE001 - 通信断はスキップして継続する
            self.logger.warning("取得失敗: %s — %s", url, e)
            return None
        finally:
            Itszai._last_request_at = time.monotonic()

        if res.status_code >= 400:
            self.logger.info("スキップ (HTTP %s): %s", res.status_code, url)
            return None
        res.encoding = res.apparent_encoding or res.encoding
        return res.text

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        """引数 url (イツザイ公式サイト) を唯一の起点として関東の導入企業を収集する。"""
        gallery_url = self._find_gallery_url(url)
        if not gallery_url:
            self.logger.error("制作事例ギャラリーのリンクが見つかりません: %s", url)
            return

        self.logger.info("制作事例ギャラリー: %s", gallery_url)
        seen: set[str] = set()

        # 業種フィルタ毎に巡回 → 最後に業種無指定で取りこぼしを回収
        targets: list[tuple[dict | None, str]] = [
            ({"industry[0]": code}, name) for code, name in self.INDUSTRIES
        ]
        targets.append((None, ""))

        for params, industry_name in targets:
            for page in range(1, self.MAX_PAGES + 1):
                cards = self._fetch_cards(gallery_url, page, params)
                if not cards:
                    break

                for job_url in cards:
                    if job_url in seen:
                        continue
                    seen.add(job_url)
                    try:
                        item = self._build_item(job_url, industry_name)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("解析失敗: %s — %s", job_url, e)
                        continue
                    if item:
                        yield item

                if len(cards) < self.LIST_PAGE_SIZE:
                    break

    # ------------------------------------------------------------------
    # 手順 1: ギャラリー URL の発見
    # ------------------------------------------------------------------
    def _find_gallery_url(self, url: str) -> str | None:
        """公式サイトトップから「イツザイ制作事例」ギャラリーの URL を解決する。"""
        html = self._get_static(url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("a[href]"):
            if "制作事例" in a.get_text(" ", strip=True):
                return urllib.parse.urljoin(url, a["href"])

        # 予備: ホスト違いで /itszai/ に向かうリンク
        host = urllib.parse.urlparse(url).netloc
        for a in soup.select("a[href]"):
            href = urllib.parse.urljoin(url, a["href"])
            parsed = urllib.parse.urlparse(href)
            if parsed.netloc != host and parsed.path.rstrip("/").endswith("/itszai"):
                return href
        return None

    # ------------------------------------------------------------------
    # 手順 2: ギャラリー一覧
    # ------------------------------------------------------------------
    def _fetch_cards(
        self, gallery_url: str, page: int, params: dict | None
    ) -> list[str]:
        """ギャラリー 1 ページ分の求人ページ URL を返す。"""
        page_url = gallery_url if page == 1 else urllib.parse.urljoin(
            gallery_url.rstrip("/") + "/", f"page/{page}/"
        )
        if params:
            page_url = f"{page_url}?{urllib.parse.urlencode(params)}"

        html = self._get_static(page_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")

        urls: list[str] = []
        for a in soup.select("li.c-results__list-item a[href]"):
            urls.append(urllib.parse.urljoin(page_url, a["href"]))
        return urls

    # ------------------------------------------------------------------
    # 手順 3-4: 求人ページ → (関東のみ) 募集要項ページ
    # ------------------------------------------------------------------
    def _build_item(self, job_url: str, industry_name: str) -> dict | None:
        html = self._get_static(job_url)
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        title = soup.title.get_text(strip=True) if soup.title else ""
        name, name_kana = self._company_from_next_data(html)
        if not name:
            # 予備: <title> の「 - 」より前を社名とみなす
            name = title.split(" - ")[0].strip()
        if not name:
            return None

        pref = ""
        m = _TITLE_PREF_RE.search(title)
        if m:
            pref = m.group(1)
            if pref not in KANTO_PREFS:
                # 関東以外は募集要項をレンダリングせずに除外する
                return None

        # 募集要項ページ (Playwright で描画)
        detail_a = soup.select_one('a[href*="/recruitments/"]')
        detail: dict[str, str] = {}
        detail_url = ""
        if detail_a:
            detail_url = urllib.parse.urljoin(job_url, detail_a["href"])
            detail = self._parse_recruitment(detail_url)

        # 勤務地から都道府県を確定 (<title> に県名が無い場合の判定もここで行う)
        location = detail.get("勤務地", "")
        addr_pref = ""
        m = _ADDR_PREF_RE.search(location)
        if m:
            addr_pref = m.group(1)
        pref = addr_pref or pref
        if pref and pref not in KANTO_PREFS:
            return None
        if not pref:
            # 都道府県を特定できない求人は関東と断定できないため除外する
            self.logger.info("都道府県不明のためスキップ: %s", job_url)
            return None

        post_code, addr = self._split_address(location, pref)
        job_title = self._job_title(soup, title)

        item = {
            Schema.URL: detail_url or job_url,
            Schema.NAME: name,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._pick_tel(detail.get("応募について", "")),
            Schema.CAT_SITE: industry_name,
            Schema.HP: self._site_url(html, job_url),
            Schema.TIME: self._working_time(detail.get("勤務時間", "")),
            Schema.HOLIDAY: self._first_line(detail.get("休日休暇", "")),
            "募集職種": job_title,
            "雇用形態": self._first_line(detail.get("雇用形態", "")),
            "給与": self._first_line(detail.get("給与", "")),
            "最寄駅": self._first_line(detail.get("最寄駅", "")),
            "契約期間": self._first_line(detail.get("契約期間", "")),
            "加入保険": self._first_line(detail.get("加入保険", "")),
            "受動喫煙防止措置": self._first_line(detail.get("受動喫煙防止措置", "")),
            "特徴タグ": detail.get("_tags", ""),
            "最終更新日": detail.get("_updated", ""),
            "求人ページURL": job_url,
        }
        return item

    def _parse_recruitment(self, detail_url: str) -> dict[str, str]:
        """募集要項ページを Playwright で描画し、見出し単位で値を取り出す。

        本文は __NEXT_DATA__ 内で暗号化されており、JS 実行後にのみ DOM へ現れる。
        networkidle まで待てば描画が完了するため、追加の待機は行わない。
        """
        soup = self.get_soup(detail_url, wait_until="networkidle")
        if soup is None:
            return {}

        out: dict[str, str] = {}
        for h3 in soup.find_all("h3"):
            label = h3.get_text(" ", strip=True)
            if label not in _SECTION_LABELS:
                continue
            body = h3.find_next_sibling("div")
            if body is None:
                continue
            out[label] = body.get_text("\n", strip=True)

        # 特徴タグ (正社員 / 交通費支給 / 寮・社宅あり …) は短い定型ラベル
        tags = []
        for span in soup.find_all("span"):
            classes = span.get("class") or []
            if "rounded-md" in classes and "border-2" in classes:
                text = span.get_text(" ", strip=True)
                if text and text not in tags:
                    tags.append(text)
        out["_tags"] = " / ".join(tags)

        node = soup.find(string=re.compile("最終更新日"))
        if node:
            m = _DATE_RE.search(str(node))
            if m:
                out["_updated"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return out

    # ------------------------------------------------------------------
    # 値の整形
    # ------------------------------------------------------------------
    @staticmethod
    def _company_from_next_data(html: str) -> tuple[str, str]:
        """__NEXT_DATA__ の companyInfo から社名・社名カナを取り出す。"""
        m = _NEXT_DATA_RE.search(html)
        if not m:
            return "", ""
        try:
            data = json.loads(m.group(1))
        except (ValueError, TypeError):
            return "", ""
        info = (data.get("props", {}).get("pageProps", {}) or {}).get("companyInfo") or {}
        return (info.get("name") or "").strip(), (info.get("name_kana") or "").strip()

    @staticmethod
    def _site_url(html: str, job_url: str) -> str:
        """採用サイト独自ドメイン ({slug}.itszai.jp) があればそれを HP とする。"""
        for m in _SUBDOMAIN_RE.finditer(html):
            slug = m.group(1)
            if slug in ("www", "clients", "dev"):
                continue
            return f"https://{slug}.itszai.jp/"
        return job_url

    @staticmethod
    def _job_title(soup: BeautifulSoup, title: str) -> str:
        """募集職種。求人ページ <title> の「{県} {駅} {職種} 採用サイト」部分から取る。"""
        h2 = soup.find("h2")
        m = _TITLE_PREF_RE.search(title)
        if m:
            rest = title[m.end():].strip()
            rest = re.sub(r"採用サイト\s*$", "", rest).strip()
            # 先頭に最寄駅 (「○○駅」) が付く場合は除去する
            rest = re.sub(r"^\S*?駅\s+", "", rest).strip()
            if rest:
                return rest
        return h2.get_text(" ", strip=True) if h2 else ""

    @staticmethod
    def _split_address(location: str, pref: str) -> tuple[str, str]:
        """「〒246-0006 神奈川県 横浜市瀬谷区 上瀬谷町 37-22」を郵便番号と住所に分解する。"""
        if not location:
            return "", ""
        line = location.split("\n")[0].strip()
        post_code = ""
        m = _POST_CODE_RE.search(line)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            line = line[m.end():]
        if pref and pref in line:
            line = line.split(pref, 1)[1]
        addr = re.sub(r"\s+", "", line).strip("　 ")
        return post_code, addr

    @staticmethod
    def _pick_tel(text: str) -> str:
        """「応募について」欄の電話番号。フリーダイヤルより実番号を優先する。"""
        if not text:
            return ""
        cands = [
            re.sub(r"[（）(]", "-", t).replace("--", "-").strip("-")
            for t in _TEL_RE.findall(text)
        ]
        for tel in cands:
            if not tel.startswith(("0120", "0800", "0078")):
                return tel
        return cands[0] if cands else ""

    @staticmethod
    def _working_time(text: str) -> str:
        """勤務時間欄から時間帯 (8:00～17:00) のみを取り出す。"""
        if not text:
            return ""
        m = _TIME_RANGE_RE.search(text)
        if m:
            return m.group(0)
        return Itszai._first_line(text)

    @staticmethod
    def _first_line(text: str) -> str:
        if not text:
            return ""
        for line in text.split("\n"):
            line = line.strip()
            if line:
                return line
        return ""


if __name__ == "__main__":
    scraper = Itszai()
    scraper.execute("https://www.itszai.jp/")

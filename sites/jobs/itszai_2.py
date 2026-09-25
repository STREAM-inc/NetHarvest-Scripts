"""
イツザイ（it'szai） — 採用サイト制作サービス「イツザイ」導入企業（長野県）

取得対象:
    イツザイ（サングローブ株式会社が提供する採用サイト制作・応募獲得サービス）を
    導入し、採用サイトが公開されている企業のうち **勤務地が長野県** の求人のみ。
    全国の掲載総数は約 495 件で、そのうち長野県は 7 件（2026-09 時点）。
    関東版 (site_id: itszai) の姉妹クローラーで、対象都道府県のみが異なる。

取得フロー:
    1. 正規 URL (https://www.itszai.jp/) の「イツザイ制作事例」リンクから
       制作実績ギャラリー (sungrove.co.jp/itszai/) を発見する。
       ※ ギャラリー URL はハードコードせず、必ず引数 url から辿って解決する。
    2. ギャラリーを業種フィルタ (?industry[0]=NN) 単位で巡回する。
       これにより各社の「サイト定義業種」を確定できる。1 ページ 30 件、
       ページ送りは /itszai/page/N/。最後に業種無指定の一覧も巡回する。
       ※ 業種フィルタ無しの一覧は 269 件 (9 ページ) で打ち止めになるが、
          業種別に巡回すると和集合で 495 件になる。長野県の該当数も
          3 件 → 7 件に増えるため、業種別巡回は網羅性に必須。
    3. 各カードのリンク先 = 採用サイトのトップ求人ページ
       (clients.itszai.jp/{customer}/job/{job}?ref=true) は SSR 済みの静的 HTML。
       ※ クエリ ?ref=true が無いと /not-found へ 307 リダイレクトされるため、
          ギャラリーの href をそのまま使う（クエリを落とさない）。
       <title> が「{社名} - {都道府県} {最寄駅} {職種} 採用サイト」形式なので
       ここで都道府県を判定し、**長野県以外はこの時点で除外**する。
       この絞り込みだけスレッドプールで並列化し、長野県が見つかり次第
       その場で詳細を取得して即 yield する (Pattern B)。
    4. 長野県の求人のみ、募集要項ページ (/{customer}/recruitments/{id}) を取得する。
       ※ こちらは逆に ?ref=true を付けると /not-found になるため付けない。

募集要項ページのデータ構造 (重要):
    募集要項の本文は __NEXT_DATA__ の props.pageProps.recruitment に
    CryptoJS (AES / OpenSSL "Salted__" 形式) で格納されており、素の HTML には
    一切現れない。ブラウザは同梱 JS の decryptJSONData() —
    `JSON.parse(CryptoJS.AES.decrypt(e, "5ac").toString(enc.Utf8))` — で
    復号して描画している。本クローラーも同じ合鍵 "5ac" で復号するため、
    **Playwright は不要**（関東版は Playwright で描画していたが、復号方式なら
    静的取得のみで完結し、高速かつ描画待ちの不安定さが無い）。
    復号後の JSON は描画済み DOM より整形されており、都道府県・郵便番号・
    電話番号・雇用形態などを構造化された値のまま取得できる。

    なお復号後の JSON には応募受付メールアドレス・営業担当者・課金額など
    ページ上に表示されない内部情報も含まれるが、これらは取得対象外とし、
    **ブラウザ上で実際に公開表示される項目のみ**をカラム化する。
    また published_flag が False の求人はサイト上でも「お探しのページは
    見つかりませんでした」と表示される非公開求人のため除外する。

備考:
    - 仕事内容・アピールポイント・福利厚生・求める人材・応募について等の長文
      (自由記述プロース) は著作権リスクを避けるためカラム化しない。
    - 掲載企業は採用サイトのため代表者名・資本金・従業員数はカラム化しない
      (復号 JSON には含まれるが、関東版と同じカラム構成に揃えている)。
    - clients.itszai.jp は短時間に多数リクエストすると HTTP 429 を返す。
      グローバルな最小リクエスト間隔 + 429 バックオフで回避する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/itszai_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id itszai_2
"""

import base64
import concurrent.futures as futures
import hashlib
import json
import re
import sys
import threading
import time
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- 備考「長野県のみ」のフィルタ条件 ---
TARGET_PREFS = ("長野県",)

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
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3})-?\s*(\d{4})(?!\d)")
_TEL_RE = re.compile(r"0\d{1,4}[-(（]\d{1,4}[-)）]\d{3,4}|0\d{9,10}")
_TIME_RANGE_RE = re.compile(r"\d{1,2}:\d{2}\s*[~〜～\-–]\s*\d{1,2}:\d{2}")
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)
_SUBDOMAIN_RE = re.compile(r"https://([a-z0-9][a-z0-9\-]*)\.itszai\.jp/")

# 同梱 JS の decryptJSONData() が使う合鍵 (CryptoJS AES のパスフレーズ)
_CRYPTO_PASSPHRASE = b"5ac"

# 募集要項 JSON の列挙値 (JS バンドルの定数定義より)
_CONTRACT_TYPES = {
    1: "正社員",
    2: "アルバイト・パート",
    3: "契約社員",
    4: "派遣社員",
    5: "新卒",
    6: "インターン",
    7: "業務委託",
}
_SALARY_TYPES = {1: "時給", 2: "日給", 3: "週給", 4: "月給", 5: "年俸"}


class Itszai2(StaticCrawler):
    """イツザイ（it'szai）導入企業スクレイパー（長野県限定）"""

    DELAY = 0.0
    ITEM_DELAY = 0.0
    MIN_INTERVAL = 0.3   # 全スレッド共通の最小リクエスト間隔 (429 対策)
    SCREEN_WORKERS = 4   # 求人ページ絞り込みの並列数
    SCREEN_BATCH = 8     # 1 バッチの絞り込み件数 (小さいほど早く yield できる)
    MAX_RETRY = 4        # 429 等のリトライ上限
    LIST_PAGE_SIZE = 30  # ギャラリー 1 ページあたりのカード数
    MAX_PAGES = 40       # 暴走防止 (実際は最大 9 ページ)

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
    # 長野県の掲載が多い業種を先に巡回し、早期に 1 件目を yield する。
    INDUSTRIES = [
        ("56", "建築/⼟⽊/建設"),
        ("63", "医療/介護/福祉"),
        ("87", "飲食/フード"),
        ("81", "農業/造園/酪農"),
        ("59", "ドライバー"),
        ("67", "⼯場/製造"),
        ("49", "美容師/アイデザイナー/ネイリスト"),
        ("79", "接客/販売/サービス系"),
        ("53", "その他"),
        ("70", "鍼灸/あん摩/マッサージ/エステ"),
        ("82", "IT/コンピューター系"),
        ("46", "警備"),
        ("85", "営業"),
        ("88", "動物関連"),
        ("89", "士業"),
        ("230", "電気工事関係"),
        ("362", "事務"),
        ("76", "教育"),
        ("72", "漁業系"),
    ]

    def _setup(self):
        super()._setup()
        self.session.headers.update({"Accept-Language": "ja,en;q=0.8"})
        self._rate_lock = threading.Lock()
        self._last_request_at = 0.0

    # ------------------------------------------------------------------
    # 取得ヘルパー
    # ------------------------------------------------------------------
    def _get(self, url: str) -> str | None:
        """レート制限を守って HTML を取得する。404 等はスキップ扱い (None)。

        clients.itszai.jp はバースト時に HTTP 429 を返すため、全スレッド共通の
        最小間隔を挟み、429/通信エラーは上限付きの指数バックオフで再試行する。
        上限に達したら None を返してスキップする (無限リトライはしない)。
        """
        for attempt in range(self.MAX_RETRY):
            with self._rate_lock:
                wait = self.MIN_INTERVAL - (time.monotonic() - self._last_request_at)
                if wait > 0:
                    time.sleep(wait)
                self._last_request_at = time.monotonic()
            try:
                # session.get はテストランナーが時間切れ監視用にラップしている
                res = self.session.get(url, timeout=self.TIMEOUT)
            except Exception as e:  # noqa: BLE001 - 通信断は再試行し、駄目ならスキップ
                self.logger.warning("取得失敗 (%d/%d): %s — %s",
                                    attempt + 1, self.MAX_RETRY, url, e)
                time.sleep(min(2 ** attempt, 10))
                continue

            if res.status_code == 429:
                time.sleep(min(2 ** attempt, 10))
                continue
            if res.status_code >= 400:
                self.logger.info("スキップ (HTTP %s): %s", res.status_code, url)
                return None
            res.encoding = res.apparent_encoding or res.encoding
            return res.text

        self.logger.warning("リトライ上限に達したためスキップ: %s", url)
        return None

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        """引数 url (イツザイ公式サイト) を唯一の起点として長野県の導入企業を収集する。"""
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

                fresh = [u for u in cards if u not in seen]
                seen.update(fresh)

                # 絞り込み (求人ページの <title> 判定) だけ並列化する。
                # 小さめのバッチ単位で回し、長野県が見つかり次第 即 yield する。
                for start in range(0, len(fresh), self.SCREEN_BATCH):
                    batch = fresh[start:start + self.SCREEN_BATCH]
                    for job_url, html in self._screen(batch):
                        try:
                            item = self._build_item(job_url, html, industry_name)
                        except Exception as e:  # noqa: BLE001
                            self.logger.warning("解析失敗: %s — %s", job_url, e)
                            continue
                        if item:
                            yield item

                if len(cards) < self.LIST_PAGE_SIZE:
                    break

    def _screen(self, job_urls: list[str]) -> list[tuple[str, str]]:
        """求人ページを並列取得し、対象都道府県のものだけ (url, html) で返す。

        <title> に都道府県が出るので、募集要項ページを引く前にここで捨てられる。
        都道府県を読み取れなかったページは、募集要項側の prefecture_name で
        判定するため通過させる (取りこぼし防止)。
        """
        if not job_urls:
            return []
        with futures.ThreadPoolExecutor(self.SCREEN_WORKERS) as ex:
            # map は入力順を保つので、ギャラリー掲載順のまま yield できる
            pages = list(ex.map(self._get, job_urls))

        hits: list[tuple[str, str]] = []
        for job_url, html in zip(job_urls, pages):
            if not html:
                continue
            title = self._title_of(html)
            m = _TITLE_PREF_RE.search(title)
            if m and m.group(1) not in TARGET_PREFS:
                continue
            hits.append((job_url, html))
        return hits

    # ------------------------------------------------------------------
    # 手順 1: ギャラリー URL の発見
    # ------------------------------------------------------------------
    def _find_gallery_url(self, url: str) -> str | None:
        """公式サイトトップから「イツザイ制作事例」ギャラリーの URL を解決する。"""
        html = self._get(url)
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
    def _fetch_cards(self, gallery_url: str, page: int, params: dict | None) -> list[str]:
        """ギャラリー 1 ページ分の求人ページ URL を返す。"""
        page_url = gallery_url if page == 1 else urllib.parse.urljoin(
            gallery_url.rstrip("/") + "/", f"page/{page}/"
        )
        if params:
            page_url = f"{page_url}?{urllib.parse.urlencode(params)}"

        html = self._get(page_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")

        urls: list[str] = []
        for a in soup.select("li.c-results__list-item a[href]"):
            # href のクエリ (?ref=true) は必須なので落とさずそのまま結合する
            urls.append(urllib.parse.urljoin(page_url, a["href"]))
        return urls

    # ------------------------------------------------------------------
    # 手順 3-4: 求人ページ → 募集要項ページ
    # ------------------------------------------------------------------
    def _build_item(self, job_url: str, job_html: str, industry_name: str) -> dict | None:
        soup = BeautifulSoup(job_html, "html.parser")
        name, name_kana = self._company_from_next_data(job_html)
        title = self._title_of(job_html)
        if not name:
            name = title.split(" - ")[0].strip()

        detail_a = soup.select_one('a[href*="/recruitments/"]')
        if not detail_a:
            return None
        # 募集要項ページは ?ref=true を付けると /not-found になるため、パスのみ使う
        detail_url = urllib.parse.urljoin(job_url, detail_a["href"]).split("?")[0]

        rec = self._fetch_recruitment(detail_url)
        if not rec:
            return None
        # サイト上でも非表示の求人 (published_flag=False) は対象外
        if not rec.get("published_flag"):
            self.logger.info("非公開求人のためスキップ: %s", detail_url)
            return None

        basic = rec.get("recruitment_basic") or {}
        customer = rec.get("customer") or {}

        # 都道府県は復号 JSON の prefecture_name が最も正確
        pref = (basic.get("prefecture_name") or "").strip()
        if not pref:
            m = _TITLE_PREF_RE.search(title)
            pref = m.group(1) if m else ""
        if pref not in TARGET_PREFS:
            return None

        if not name:
            name = (customer.get("name") or customer.get("source_name") or "").strip()
        if not name:
            return None

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref,
            Schema.POST_CODE: self._post_code(basic, customer),
            Schema.ADDR: self._addr(basic),
            Schema.TEL: self._pick_tel(basic, customer),
            Schema.CAT_SITE: industry_name,
            Schema.HP: self._site_url(basic, job_html, job_url),
            Schema.TIME: self._working_time(basic.get("working_time")),
            Schema.HOLIDAY: self._first_line(self._text(basic.get("holiday_policy"))),
            "募集職種": (basic.get("job_name") or "").strip(),
            "雇用形態": _CONTRACT_TYPES.get(basic.get("contract_type"), ""),
            "給与": self._salary(basic),
            "最寄駅": (basic.get("nearest_station") or "").strip(),
            "契約期間": self._first_line(self._text(basic.get("contract_period"))),
            "加入保険": self._first_line(self._text(basic.get("insurance"))),
            "受動喫煙防止措置": self._first_line(self._text(basic.get("smoking_prevent_method"))),
            "特徴タグ": self._tags(basic),
            "最終更新日": self._date(rec.get("updated_at")),
            "求人ページURL": job_url,
        }

    def _fetch_recruitment(self, detail_url: str) -> dict | None:
        """募集要項ページの暗号化ペイロードを取得・復号して dict で返す。"""
        html = self._get(detail_url)
        if not html:
            return None
        m = _NEXT_DATA_RE.search(html)
        if not m:
            return None
        try:
            page_props = json.loads(m.group(1))["props"]["pageProps"]
        except (ValueError, KeyError, TypeError):
            return None
        blob = page_props.get("recruitment")
        if not blob:
            # /not-found へ飛ばされた場合は pageProps が空になる
            return None
        try:
            return self._decrypt(blob)
        except Exception as e:  # noqa: BLE001 - 鍵の変更等は握り潰さずログに残す
            self.logger.warning("復号に失敗: %s — %s", detail_url, e)
            return None

    @staticmethod
    def _decrypt(blob: str) -> dict:
        """CryptoJS (AES-CBC / OpenSSL "Salted__" 形式) のペイロードを復号する。

        CryptoJS はパスフレーズから MD5 ベースの EVP_BytesToKey で
        鍵 32 バイト + IV 16 バイトを導出する。
        """
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        raw = base64.b64decode(blob)
        if raw[:8] != b"Salted__":
            raise ValueError("想定外のペイロード形式です")
        salt, ciphertext = raw[8:16], raw[16:]

        derived, block = b"", b""
        while len(derived) < 48:
            block = hashlib.md5(block + _CRYPTO_PASSPHRASE + salt).digest()
            derived += block

        decryptor = Cipher(
            algorithms.AES(derived[:32]), modes.CBC(derived[32:48])
        ).decryptor()
        plain = decryptor.update(ciphertext) + decryptor.finalize()
        plain = plain[: -plain[-1]]  # PKCS#7 パディング除去
        return json.loads(plain.decode("utf-8"))

    # ------------------------------------------------------------------
    # 値の整形
    # ------------------------------------------------------------------
    @staticmethod
    def _title_of(html: str) -> str:
        m = re.search(r"<title[^>]*>(.*?)</title>", html, re.DOTALL)
        return re.sub(r"\s+", " ", m.group(1)).strip() if m else ""

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
    def _text(value) -> str:
        """値が HTML 断片 (<p>…</p>) の場合があるのでプレーンテキストに均す。"""
        if not value:
            return ""
        text = str(value)
        if "<" in text:
            text = BeautifulSoup(text, "html.parser").get_text("\n", strip=True)
        return re.sub(r"\n{2,}", "\n", text).strip()

    @staticmethod
    def _first_line(text: str) -> str:
        for line in (text or "").split("\n"):
            line = line.strip()
            if line:
                return line
        return ""

    @classmethod
    def _post_code(cls, basic: dict, customer: dict) -> str:
        """郵便番号。求人側の入力ミスがあるため 3 つの出典の多数決で決める。

        例: 株式会社SORAIRO は求人側 zipcode が "899-8304" (鹿児島県の番号) だが、
        会社情報と本社所在地表記はいずれも正しい "399-8304" を持つ。
        同数の場合は 求人側 → 会社側 → 本社表記 の優先順で採用する。
        """
        candidates: list[str] = []
        for raw in (
            basic.get("zipcode"),
            customer.get("postcode"),
            cls._text(basic.get("head_office_location")),
        ):
            m = _POST_CODE_RE.search(str(raw or ""))
            if m:
                candidates.append(f"{m.group(1)}-{m.group(2)}")
        if not candidates:
            return ""
        return max(
            candidates,
            key=lambda z: (candidates.count(z), -candidates.index(z)),
        )

    @staticmethod
    def _addr(basic: dict) -> str:
        """市区町村以降の住所を組み立てる (都道府県は PREF カラムに分離)。"""
        parts = [
            (basic.get("city_name") or "").strip(),
            (basic.get("address1") or "").strip(),
            (basic.get("address2") or "").strip(),
        ]
        return "".join(p for p in parts if p)

    @classmethod
    def _pick_tel(cls, basic: dict, customer: dict) -> str:
        """求人の連絡先 → 会社代表番号 → 応募案内文中の番号 の順で採用する。"""
        for value in (basic.get("contact_phone"), customer.get("phone")):
            tel = (value or "").strip()
            if tel:
                return tel
        found = _TEL_RE.findall(cls._text(basic.get("about_application_content")))
        for tel in found:
            tel = re.sub(r"[（）(]", "-", tel).replace("--", "-").strip("-")
            if not tel.startswith(("0120", "0800", "0078")):
                return tel
        return ""

    @staticmethod
    def _site_url(basic: dict, job_html: str, job_url: str) -> str:
        """企業 HP。独自ドメインが無ければ採用サイト ({slug}.itszai.jp) を使う。"""
        site = (basic.get("company_website") or "").strip()
        if site:
            return site
        for m in _SUBDOMAIN_RE.finditer(job_html):
            slug = m.group(1)
            if slug not in ("www", "clients", "dev"):
                return f"https://{slug}.itszai.jp/"
        return job_url

    @classmethod
    def _working_time(cls, value) -> str:
        """勤務時間。時間帯 (8:00〜17:00) が書かれていればそれを優先する。"""
        text = cls._text(value)
        if not text:
            return ""
        m = _TIME_RANGE_RE.search(text)
        return m.group(0) if m else cls._first_line(text)

    @staticmethod
    def _salary(basic: dict) -> str:
        """「日給8,500円〜12,000円」形式に組み立てる。"""
        unit = _SALARY_TYPES.get(basic.get("salary_type"), "")
        low, high = basic.get("salary_min"), basic.get("salary_max")
        if not low and not high:
            return ""
        if low and high and low != high:
            return f"{unit}{int(low):,}円〜{int(high):,}円"
        return f"{unit}{int(low or high):,}円〜"

    @staticmethod
    def _tags(basic: dict) -> str:
        """特徴タグ (残業なし / 未経験者歓迎 …)。短い定型ラベルのみ。"""
        tags: list[str] = []
        for key in (
            "indeed_keyword_categories",
            "indeed_insurance_keywords",
            "indeed_worksystem_keywords",
        ):
            for entry in basic.get(key) or []:
                label = (entry.get("keyword") or "").strip()
                if label and label not in tags:
                    tags.append(label)
        return " / ".join(tags)

    @staticmethod
    def _date(value) -> str:
        """"2024-05-17 02:20:29" → "2024-05-17"。"""
        m = re.match(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", str(value or ""))
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else ""


if __name__ == "__main__":
    scraper = Itszai2()
    scraper.execute("https://www.itszai.jp/")

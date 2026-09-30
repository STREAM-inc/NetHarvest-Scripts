"""
RNAV アルゼンチン旅行代理店登録簿 (Registro Nacional de Agencias de Viajes) — rnav_2

取得対象:
    アルゼンチン全国の登録旅行代理店 (本店 + 支店) 約 8,400 件。
    依頼の列指定に合わせて 会社名 / 住所 / 都市 / 国 / TEL / メールアドレス /
    URL / CUIT (作業用) / 取得元URL を取得する。

サイト構造 (Phase 1 調査結果):
    - トップ (https://agenciasdeviajes.ar/) の「Directorio de agencias」(#buscador)
      が唯一の一覧導線。素の HTML には検索結果が 1 件も含まれず、Livewire v3
      コンポーネント `buscador-de-agencias` が POST /livewire/update で
      結果 HTML (components[0].effects.html) を返す。
    - ルート HTML から csrf="..." と wire:snapshot="..." を取り出し、
      {"_token": csrf, "components": [{"snapshot": …, "updates": {"buscar": 語},
       "calls": [gotoPage(N, 'page')]}]} を JSON POST する。
      毎回まっさらな初期スナップショットを送れば任意のページに直接飛べる。
    - 検索語は 3 文字以上が必須。全件を無条件に返す手段は無い。
    - `buscar` は 商号 / 法人名 / CUIT を横断検索する。アルゼンチンの CUIT は
      必ず 20/23/24/25/26/27/30/33/34 のいずれかで始まるため、"20-" 〜 "34-" の
      9 プレフィックスの和集合で登録簿全体を覆える。
      実測件数: 20-=1891 / 23-=490 / 24-=110 / 25-=84 / 26-=90 / 27-=2424 /
      30-=2891 / 33-=351 / 34-=68 → 合計 8,399 件 (公称 約 8,200 件と整合)。
      州名で引く `ciudad` 軸は州が空欄のレコードを取りこぼすため採用しない。
    - 1 ページ 15 件固定。"Mostrando 1 a 15 de 1891 resultados" から総件数が読める。
      範囲外ページはカード 0 件で返るので停止条件に使える。
    - 詳細ページは存在せず、全項目が検索結果カード (div.m-4) に載る。
      したがって Schema.URL (取得元URL) はルート URL 固定。

備考 (依頼指定) の反映:
    - 国: 全件「アルゼンチン」固定 (EXTRA カラム「国」)。
    - TEL: 掲載は "(351) 3109541" 形式。国番号付き "+54 351 3109541" に整形する。
      携帯 (市内番号が 15 始まり) は国際表記の "+54 9 {市外局番} {番号}" に直す。
      原文は EXTRA カラム「TEL(原文)」に残す。
    - 絞り込み「旅行代理店のみ」: RNAV は旅行代理店の登録簿そのもので、
      旅行代理店以外の業種は掲載されない。よって業種による除外は不要。
      代わりに Legajo (登録番号) を持たないカード = 登録代理店ではない行を
      除外するガードを parse() に入れている。支店 (Sucursal) は
      独立した営業所なので除外せず、本支店区分を EXTRA カラムに残す。

備考 (技術):
    - 素の requests 既定 UA は 403 ("Prohibido el acceso desde su ubicacion")。
      StaticCrawler 既定の Chrome UA なら 200 で通る。
    - robots.txt は `User-agent: * / Disallow:` (全面許可)。
      /reglamento・/legales を確認したが、スクレイピング・クローリング・
      自動取得を禁ずる条項は無い。
    - CP (郵便番号) はアルゼンチン式 4 桁。Schema.POST_CODE は日本の 7 桁を
      前提に正規化されるため EXTRA カラムに退避する。
    - Domicilio 欄は住所非公開のレコードでは Web / Instagram の URL が入る
      (<strong> が無い)。その場合 住所は空にし、リンクは URL 側で拾う。
    - 支店 (Sucursal) は本店と Legajo / CUIT が同一なので、
      Legajo だけで重複除去すると支店が丸ごと落ちる。住所・連絡先まで
      含めた複合キーで重複判定する。
    - 長文の自由記述 (紹介文等) は掲載が無く、取得対象にも含めない。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/rnav_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id rnav_2
"""

import html as html_mod
import logging
import math
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# ルート HTML から Livewire の CSRF トークンと初期スナップショットを拾う
_CSRF_PATTERN = re.compile(r'csrf="([^"]+)"')
_SNAPSHOT_PATTERN = re.compile(r'wire:snapshot="([^"]+)"')
_UPDATE_URI_PATTERN = re.compile(r'data-update-uri="([^"]+)"')

# "Mostrando 16 a 30 de 2.793 resultados" → 総件数
_TOTAL_PATTERN = re.compile(r"de\s+([\d.,]+)\s+resultados")

# 品質認証の有効期間 "07/2026 - 06/2028"
_PERIOD_PATTERN = re.compile(r"^\d{2}/\d{4}\s*-\s*\d{2}/\d{4}$")

# カード内 <p> のラベル
_LABEL_RAZON = re.compile(r"^razon\s+social\s*:", re.I)
_LABEL_CUIT = re.compile(r"^cuit\s*:", re.I)
_LABEL_LEGAJO = re.compile(r"^legajo\s*:", re.I)
_LABEL_CP = re.compile(r"^cp\s*:", re.I)
_LABEL_DOMICILIO = re.compile(r"^domicilio\s*:", re.I)
_LABEL_SECTUR = re.compile(r"turismo\s+estudiantil", re.I)
_LABEL_SUCURSAL = re.compile(r"^sucursal\b", re.I)

# 連絡先 <p> の判定
_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PHONE_CHARS = re.compile(r"^[\d\s()\-+./]+$")

# 国 (依頼により固定値)
_COUNTRY = "アルゼンチン"
# 国番号
_COUNTRY_CODE = "+54"

# 州 (provincia) が空欄で都市名だけのレコード (CABA など) を救済するための一覧
_PROVINCES = [
    "Buenos Aires", "CABA", "Catamarca", "Chaco", "Chubut", "Cordoba",
    "Corrientes", "Entre Rios", "Formosa", "Jujuy", "La Pampa", "La Rioja",
    "Mendoza", "Misiones", "Neuquen", "Rio Negro", "Salta", "San Juan",
    "San Luis", "Santa Cruz", "Santa Fe", "Santiago del Estero",
    "Tierra del Fuego", "Tucuman",
]


def _fold(value: str) -> str:
    """アクセントと大小文字を落として比較用キーにする (Córdoba → cordoba)。"""
    decomposed = unicodedata.normalize("NFKD", value or "")
    return "".join(c for c in decomposed if not unicodedata.combining(c)).strip().lower()


_PROVINCE_KEYS = {_fold(p) for p in _PROVINCES}


def _clean(value: str) -> str:
    """連続空白を 1 個に潰し、前後の空白と区切り記号を落とす。"""
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip(" ,\t")


def _text(node) -> str:
    """要素のテキストを区切り文字なしで連結して返す。

    検索語は結果 HTML 内で <span> に包まれて強調されるため、区切り文字を入れて
    連結すると値が割れてしまう (CUIT "20-12876018-1" → "20- 12876018-1")。
    要素間の空白は元々独立したテキストノードとして残っているので、
    区切りなしで連結してから _clean() に空白を整えさせる。
    """
    return _clean(node.get_text("")) if node is not None else ""


class Rnav2(StaticCrawler):
    """RNAV アルゼンチン旅行代理店登録簿 スクレイパー (rnav_2)"""

    # ページ取得 (POST) の間隔。1 ページ = 15 件なので item 単位では待たない
    DELAY = 0.8
    ITEM_DELAY = 0.0
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "国",
        "都市(Ciudad)",
        "番地・所在地(Domicilio)",
        "郵便番号(CP)",
        "CUIT",
        "登録番号(Legajo)",
        "法人名・登録名義(Razon Social)",
        "TEL(原文)",
        "学生旅行取扱許可(SECTUR)",
        "品質認証(Sello de Calidad)",
        "品質認証有効期間",
        "本支店区分(Sucursal)",
    ]

    # Livewire ページャの固定表示件数
    PER_PAGE = 15
    # CSRF / セッション切れからの復帰リトライ上限 (無限リトライ禁止)
    MAX_ATTEMPTS = 3
    # 暴走防止のページ数上限
    MAX_PAGE = 2000

    # CUIT の先頭 3 文字。20/23/24/27 = 個人, 30/33/34 = 法人, 25/26 = 稀
    CUIT_PREFIXES = ["20-", "23-", "24-", "25-", "26-", "27-", "30-", "33-", "34-"]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """CUIT プレフィックス 9 種を順に検索し、カード 1 件ごとに即 yield する。"""
        self._bootstrap(url)

        seen: set[tuple] = set()

        for prefix in self.CUIT_PREFIXES:
            page = 1
            last_page = None

            while page <= self.MAX_PAGE:
                result_html = self._search(url, prefix, page)
                if result_html is None:
                    break

                soup = bs4.BeautifulSoup(result_html, "html.parser")
                cards = self._extract_cards(soup)
                if not cards:
                    logger.info("CUIT %s: %d ページ目で結果なし", prefix, page)
                    break

                if last_page is None:
                    total = self._extract_total(soup)
                    if total:
                        last_page = math.ceil(total / self.PER_PAGE)
                        self.total_items = (self.total_items or 0) + total
                        logger.info("CUIT %s: %d 件 (%d ページ)", prefix, total, last_page)

                for card in cards:
                    item = self._build_item(card, url)
                    if item is None:
                        continue
                    # 支店 (Sucursal) は本店と Legajo / CUIT が同一なので、
                    # 所在地・連絡先まで含めた複合キーで重複判定する。
                    key = (
                        item["登録番号(Legajo)"],
                        item["CUIT"],
                        _fold(item[Schema.NAME]),
                        _fold(item["番地・所在地(Domicilio)"]),
                        _fold(item["都市(Ciudad)"]),
                        item["TEL(原文)"],
                        _fold(item[Schema.EMAIL]),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    yield item

                if last_page is not None and page >= last_page:
                    break
                page += 1

        logger.info("取得ユニーク件数: %d", len(seen))

    # ------------------------------------------------------------------ #
    # Livewire 通信
    # ------------------------------------------------------------------ #
    def _bootstrap(self, url: str) -> None:
        """ルート HTML から CSRF トークン / スナップショット / 更新 URI を取り出す。

        CSRF トークンはセッション cookie と対になっているため、キャッシュ済み
        HTML ではなく毎回 self.session で実取得する。
        """
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        page_html = response.text

        csrf = _CSRF_PATTERN.search(page_html)
        snapshot = _SNAPSHOT_PATTERN.search(page_html)
        if not csrf or not snapshot:
            raise RuntimeError(f"Livewire の CSRF / snapshot を取得できませんでした: {url}")

        self._csrf = csrf.group(1)
        self._snapshot = html_mod.unescape(snapshot.group(1))

        update_uri = _UPDATE_URI_PATTERN.search(page_html)
        self._update_url = urljoin(url, update_uri.group(1) if update_uri else "/livewire/update")
        logger.debug("Livewire 初期化: %s", self._update_url)

    def _search(self, url: str, keyword: str, page: int) -> str | None:
        """検索コンポーネントを 1 ページ分叩き、結果 HTML を返す。

        毎回まっさらな初期スナップショットに `buscar` と gotoPage を載せて送るため、
        ページ間で状態を引き継ぐ必要がない (任意のページに直接飛べる)。
        リトライは MAX_ATTEMPTS 回で打ち切る (自己再帰によるリトライはしない)。
        """
        calls = []
        if page > 1:
            calls.append({"path": "", "method": "gotoPage", "params": [page, "page"]})

        for attempt in range(self.MAX_ATTEMPTS):
            if self.DELAY:
                time.sleep(self.DELAY)

            payload = {
                "_token": self._csrf,
                "components": [
                    {
                        "snapshot": self._snapshot,
                        "updates": {"buscar": keyword},
                        "calls": calls,
                    }
                ],
            }
            try:
                response = self.session.post(
                    self._update_url,
                    json=payload,
                    headers={
                        "X-Livewire": "true",
                        "Referer": url,
                        "Accept": "text/html, application/xhtml+xml",
                    },
                    timeout=self.TIMEOUT,
                )
            except OSError as exc:
                logger.warning(
                    "Livewire 通信エラー (%s p%d, 試行 %d/%d): %s",
                    keyword, page, attempt + 1, self.MAX_ATTEMPTS, exc,
                )
                self._backoff(attempt)
                continue

            # 419 = CSRF / セッション切れ。トークンを取り直して再試行する
            if response.status_code in (401, 403, 419):
                logger.warning(
                    "セッション切れ (%s p%d, HTTP %d)。トークンを再取得します",
                    keyword, page, response.status_code,
                )
                self._backoff(attempt)
                self._bootstrap(url)
                continue

            if response.status_code != 200:
                logger.warning(
                    "予期しない HTTP %d (%s p%d, 試行 %d/%d)",
                    response.status_code, keyword, page, attempt + 1, self.MAX_ATTEMPTS,
                )
                self._backoff(attempt)
                continue

            try:
                return response.json()["components"][0]["effects"]["html"]
            except (ValueError, KeyError, IndexError, TypeError) as exc:
                logger.warning(
                    "レスポンス解析に失敗 (%s p%d, 試行 %d/%d): %s",
                    keyword, page, attempt + 1, self.MAX_ATTEMPTS, exc,
                )
                self._backoff(attempt)
                continue

        if self.CONTINUE_ON_ERROR:
            self.error_count += 1
            logger.warning(
                "%d 回試行しても取得できずスキップ: %s p%d", self.MAX_ATTEMPTS, keyword, page
            )
            return None
        raise RuntimeError(f"検索結果を取得できませんでした: keyword={keyword} page={page}")

    @staticmethod
    def _backoff(attempt: int) -> None:
        time.sleep(min(2 ** attempt, 8))

    # ------------------------------------------------------------------ #
    # HTML 解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_total(soup: bs4.BeautifulSoup) -> int | None:
        """"Mostrando 16 a 30 de 2.793 resultados" から総件数を取り出す。"""
        match = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
        if not match:
            return None
        digits = re.sub(r"\D", "", match.group(1))
        return int(digits) if digits else None

    @staticmethod
    def _extract_cards(soup: bs4.BeautifulSoup) -> list[bs4.Tag]:
        """検索結果カード (h3 を持つ div.m-4) を並び順どおりに返す。"""
        cards = [card for card in soup.select("div.m-4") if card.find("h3") is not None]
        if cards:
            return cards
        # クラス名が変わった場合のフォールバック: h3 の親をカードとみなす
        return [h.parent for h in soup.select("h3") if h.parent is not None]

    def _build_item(self, card: bs4.Tag, url: str) -> dict | None:
        """カード 1 枚を 1 レコードに変換する。登録代理店以外は None を返す。"""
        name = _text(card.find("h3"))
        if not name:
            return None

        razon = cuit = legajo = sectur = ""
        domicilio = postal = tel_raw = email = ""
        location = periodo = sucursal = ""

        for paragraph in card.select("p"):
            text = _text(paragraph)
            if not text or text.lower() == "web":
                continue

            value = _text(paragraph.find("strong"))

            if _LABEL_RAZON.match(text):
                razon = value
            elif _LABEL_CUIT.match(text):
                cuit = value
            elif _LABEL_LEGAJO.match(text):
                legajo = value
            elif _LABEL_SECTUR.search(text):
                sectur = value
            elif _LABEL_CP.match(text):
                postal = value
            elif _LABEL_DOMICILIO.match(text):
                # 住所非公開のレコードでは代わりに Web サイトのリンクが入る。
                # <strong> がある場合だけ住所として採用する。
                domicilio = value
            elif _LABEL_SUCURSAL.match(text):
                sucursal = text
            elif _PERIOD_PATTERN.match(text):
                periodo = text
            elif _EMAIL_PATTERN.search(text):
                email = _EMAIL_PATTERN.search(value or text).group(0)
            elif _PHONE_CHARS.match(text) and len(re.sub(r"\D", "", text)) >= 6:
                tel_raw = value or text
            elif not location:
                # ラベルの無い "都市, 州" 行
                location = value or text

        # 旅行代理店のみ: 登録番号を持たない行 (広告・案内等) は採らない
        if not legajo:
            return None

        city, pref = self._split_location(location)
        website = self._extract_website(card)
        lowered = website.lower()
        is_sns = "instagram.com" in lowered or "facebook.com" in lowered

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: self._compose_address(city, domicilio),
            Schema.TEL: self._to_international(tel_raw),
            Schema.EMAIL: email,
            Schema.HP: "" if is_sns else website,
            Schema.INSTA: website if "instagram.com" in lowered else "",
            Schema.FB: website if "facebook.com" in lowered else "",
            Schema.CO_NUM: cuit,
            "国": _COUNTRY,
            "都市(Ciudad)": city,
            "番地・所在地(Domicilio)": domicilio,
            "郵便番号(CP)": postal,
            "CUIT": cuit,
            "登録番号(Legajo)": legajo,
            "法人名・登録名義(Razon Social)": razon,
            "TEL(原文)": tel_raw,
            "学生旅行取扱許可(SECTUR)": sectur,
            "品質認証(Sello de Calidad)": self._extract_seal(card),
            "品質認証有効期間": periodo,
            "本支店区分(Sucursal)": sucursal,
        }

    @staticmethod
    def _compose_address(city: str, domicilio: str) -> str:
        """Schema.ADDR は「市区町村以降」なので 都市 + 番地 の形に組み立てる。"""
        return " ".join(part for part in (city, domicilio) if part)

    @staticmethod
    def _to_international(tel_raw: str) -> str:
        """"(351) 3109541" を国番号付き "+54 351 3109541" に整形する。

        掲載は「(市外局番) 市内番号」形式。市内番号が 15 で始まるものは
        アルゼンチン国内の携帯表記なので、国際表記の "+54 9 市外局番 番号" に直す。
        括弧が無く分割できない場合は全桁をまとめて国番号だけ付ける。
        """
        tel_raw = _clean(tel_raw)
        if not tel_raw:
            return ""

        match = re.match(r"^\s*\(\s*(\d+)\s*\)\s*(.*)$", tel_raw)
        if match:
            area = match.group(1).lstrip("0")
            local = re.sub(r"\D", "", match.group(2))
        else:
            area = ""
            local = re.sub(r"\D", "", tel_raw)

        if not local and not area:
            return ""

        # 国内携帯表記の 15 プレフィックスは国際表記では 9 が国番号直後に付く。
        # アルゼンチンの国内番号は 市外局番 + 加入者番号 = 10 桁なので、
        # 15 を外して 10 桁ちょうどになるときだけ携帯と判定する
        # ("(11) 15xxxxxx" のような 15 始まりの固定電話を誤変換しないため)。
        mobile = ""
        if local.startswith("15") and len(area) + len(local) - 2 == 10:
            mobile = "9"
            local = local[2:]

        parts = [_COUNTRY_CODE, mobile, area, local]
        return " ".join(part for part in parts if part)

    @staticmethod
    def _split_location(location: str) -> tuple[str, str]:
        """"Ushuaia, Tierra Del Fuego" のような「都市, 州」表記を分割する。

        州名自体がカンマを含む場合があるため最初のカンマだけで切る。
        州が空欄で都市名だけのレコード (CABA など) は、その値が州名と一致する
        場合に限り州にも埋める。
        """
        location = _clean(location)
        if not location:
            return "", ""
        city, _, pref = location.partition(",")
        city, pref = _clean(city), _clean(pref)
        if not pref and _fold(city) in _PROVINCE_KEYS:
            pref = city
        return city, pref

    @staticmethod
    def _extract_website(card: bs4.Tag) -> str:
        """カード内の外部リンク (Web サイト / SNS) を 1 本取り出す。"""
        for anchor in card.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "#")):
                continue
            # "//www.example.com" 形式はスキームを補う
            if href.startswith("//"):
                href = "https:" + href
            return href
        return ""

    @staticmethod
    def _extract_seal(card: bs4.Tag) -> str:
        """品質認証シール (Sello de Calidad Bronce など) の名称を img から取り出す。"""
        for img in card.select("img"):
            label = _clean(img.get("title") or img.get("alt") or "")
            if "sello" in label.lower():
                return label
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Rnav2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://agenciasdeviajes.ar/")

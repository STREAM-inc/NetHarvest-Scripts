"""
SERNATUR (チリ国家観光庁) 観光サービス提供者登録 — 旅行代理店 (Agencia de viajes) — sernatur_2

取得対象:
    チリ国家観光庁 SERNATUR の「観光サービス提供者登録 (Registro de Prestadores de
    Servicios Turísticos)」のうち、区分 "Agencia de viajes" (旅行代理店) の全登録事業者。
    全 Región (地域) が対象で、2026-09 時点で 1,665 件 (20 件 / ページ × 84 ページ)。

取得フロー:
    1. 引数 url (ポータル https://portalserviciosturisticos.sernatur.cl/) を唯一の起点として
       取得し、ページ内リンクから公開検索サイト (Buscador de Servicios Turísticos =
       serviciosturisticos.sernatur.cl) を導出する。
       リンクが見つからない場合はホスト名の先頭 "portal" を落として導出する
       (ポータルと検索サイトはサブドメインが対になっている)。
    2. 検索結果 `resultado.php?page=N&nombre=&tipo_servicio=3&comuna_nombre=` を
       1 ページずつ取得する。`tipo_servicio=3` が区分「Agencia de viajes」のフィルタで、
       `comuna_nombre` を空にすることで全 Región が対象になる (依頼の絞り込み指示)。
    3. 一覧カード (div.caja-resultado を包む a[href="/{登録ID}-{slug}"]) から詳細 URL を
       1 件取り出すたびに詳細ページを取得して即 yield する (Pattern B)。
    4. カードが 0 件になったページで終了する (範囲外ページは 0 件を返す)。

詳細ページの構造 (3 件 + 10 件サンプリングで確認):
    h4.tituloperfil        … 事業者名                        (10/10)
    直前の兄弟 h5          … サービス分類 (Receptiva / Emisiva / Receptiva y emisiva)
    span.region            … 地域 (Región Metropolitana 等)
    div.textos > p         … "ラベル: <b>値</b>" 形式
                             Dirección (住所)      10/10
                             Localidad (都市)      10/10
                             Teléfono (電話)       10/10
                             Email (メール)        10/10
                             Calificación (等級)    1/10 ← 低頻度だが実装する
    a.btn-web[href]        … 事業者の Web サイト (無い事業者あり → 空文字)
    "Registro vigente" / "Registro no vigente" … 登録状態

備考 (依頼指示の反映):
    - 出力カラムは 会社名 / 住所 / 都市 / 国 / TEL / メールアドレス / URL / 取得元URL。
    - 国は「チリ」で固定 (COUNTRY 定数)。
    - TEL はサイト表記 "(56) 228289500" を "+56 228289500" の国際表記に正規化する。
    - 区分は「Agencia de viajes」(tipo_servicio=3) のみ、Región による絞り込みは行わず全地域。
    - チリの事業者のため Schema.PREF (日本の都道府県) は使わず、EXTRA カラム
      "地域" (Región) / "都市" (Localidad) / "国" に格納する。
    - Localidad の末尾に付く "(p)" 等の内部区分記号は都市名から除去する
      (例: "Talca (p)" → "Talca")。
    - 詳細ページの "Sellos de Distinción" 画像は認証の有無に関わらず表示される
      テンプレート画像で実態と一致しないため取得しない。
    - 自由記述の長文フィールドはサイト上に存在しない (掲載は構造化項目のみ)。
      よって著作権リスクのあるプロース列は EXTRA_COLUMNS に含めていない。

利用規約:
    portalserviciosturisticos.sernatur.cl/robots.txt は `User-agent: * / Disallow:` で全許可。
    ポータル・検索サイトともに利用規約 / 利用条件ページは存在せず (terminos, condiciones-de-uso,
    aviso-legal 等はいずれも 404)、スクレイピングを禁止する条項は確認されなかった。
    公的機関の公開登録簿のため、過負荷を避ける目的で DELAY = 0.5 とする。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/sernatur_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sernatur_2
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 詳細ページのパス (/{登録ID}-{slug})
_DETAIL_PATH_PATTERN = re.compile(r"^/(\d+)-[^/]+/?$")

# 検索結果ヘッダの総件数 ("1665 Servicios")
_TOTAL_PATTERN = re.compile(r"([\d.,]+)\s*Servicios")

# div.textos の "ラベル: 値" のラベル抽出
_LABEL_PATTERN = re.compile(r"^\s*([^:]+?)\s*:\s*")

# 電話番号の国番号表記 "(56) 228289500" / "+(56) 9..." の先頭部
_TEL_CC_PATTERN = re.compile(r"^\+?\s*\(\s*(\d{1,4})\s*\)\s*")

# Localidad 末尾の内部区分記号 ("Talca (p)" の "(p)")
_LOCALIDAD_SUFFIX_PATTERN = re.compile(r"\s*\([a-zA-Z]\)\s*$")

# 登録状態 ("Registro vigente" / "Registro no vigente")
_STATUS_PATTERN = re.compile(r"Registro\s+(?:no\s+)?vigente", re.IGNORECASE)


class Sernatur2(StaticCrawler):
    """SERNATUR 観光サービス提供者登録 (旅行代理店) スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    DELAY = 0.5
    TIMEOUT = 45

    EXTRA_COLUMNS = [
        "都市",
        "国",
        "地域",
        "サービス分類",
        "等級",
        "登録状態",
        "登録ID",
    ]

    # 国はチリで固定 (依頼指示)
    COUNTRY = "チリ"

    # 国際表記の既定国番号 (チリ)
    COUNTRY_CALLING_CODE = "56"

    # 絞り込み: 区分「Agencia de viajes」(検索フォーム select の value=3)
    SERVICE_TYPE_ID = "3"
    SERVICE_TYPE_NAME = "Agencia de viajes"

    # 検索結果ページのパス
    LIST_PATH = "resultado.php"

    # ページ巡回の安全上限 (2026-09 時点で 84 ページ)
    MAX_PAGES = 300

    # div.textos のラベル (小文字) → 出力先キー
    _LABEL_MAP = {
        "dirección": Schema.ADDR,
        "direccion": Schema.ADDR,
        "localidad": "都市",
        "teléfono": Schema.TEL,
        "telefono": Schema.TEL,
        "email": Schema.EMAIL,
        "e-mail": Schema.EMAIL,
        "correo": Schema.EMAIL,
        "calificación": "等級",
        "calificacion": "等級",
    }

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 引数 url (ポータル) が唯一のルート。検索サイトはここから導出する。
        search_base = self._resolve_search_base(url)
        logger.info("検索サイト: %s", search_base)

        seen: set[str] = set()
        for page in range(1, self.MAX_PAGES + 1):
            list_url = self._build_list_url(search_base, page)
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした (打ち切り): %s", list_url)
                break

            if page == 1:
                total = self._extract_total(soup)
                if total:
                    self.total_items = total
                    logger.info("総件数: %d 件 (%s)", total, self.SERVICE_TYPE_NAME)

            detail_urls = self._extract_detail_urls(soup, list_url)
            if not detail_urls:
                logger.info("カードが 0 件のため終了します (page=%d)", page)
                break
            logger.info("page=%d: %d 件", page, len(detail_urls))

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 1 件の失敗で全体を止めない
                    self.error_count += 1
                    logger.warning("詳細ページの取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

    # ------------------------------------------------------------------ #
    # 検索サイトの導出 / 一覧ページ
    # ------------------------------------------------------------------ #
    def _resolve_search_base(self, portal_url: str) -> str:
        """ポータル (引数 url) から公開検索サイトのベース URL を導出する。"""
        fallback = self._strip_portal_prefix(portal_url)

        soup = self.get_soup(portal_url)
        if soup is None:
            logger.warning("ポータルを取得できないためホスト名から導出します: %s", fallback)
            return fallback

        portal_host = urlsplit(portal_url).netloc.lower()
        for anchor in soup.select("a[href]"):
            parts = urlsplit(urljoin(portal_url, anchor["href"].strip()))
            host = parts.netloc.lower()
            # ポータル自身を除く sernatur.cl 配下で "serviciosturisticos" を含むホスト
            if (
                host
                and host != portal_host
                and host.endswith("sernatur.cl")
                and "serviciosturisticos" in host
            ):
                return f"{parts.scheme}://{parts.netloc}/"

        logger.info("検索サイトのリンクが見つからないためホスト名から導出します: %s", fallback)
        return fallback

    @staticmethod
    def _strip_portal_prefix(portal_url: str) -> str:
        """ホスト名の先頭 "portal" を落として検索サイトの URL を組み立てる。"""
        parts = urlsplit(portal_url)
        host = parts.netloc
        if host.lower().startswith("portal"):
            host = host[len("portal"):]
        return f"{parts.scheme}://{host}/"

    def _build_list_url(self, search_base: str, page: int) -> str:
        """検索結果ページ URL (区分 = Agencia de viajes / 全 Región) を組み立てる。"""
        return urljoin(
            search_base,
            f"{self.LIST_PATH}?page={page}&nombre="
            f"&tipo_servicio={self.SERVICE_TYPE_ID}&comuna_nombre=",
        )

    @staticmethod
    def _extract_total(soup: bs4.BeautifulSoup) -> int | None:
        m = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
        if not m:
            return None
        try:
            return int(re.sub(r"[.,]", "", m.group(1)))
        except ValueError:
            return None

    @staticmethod
    def _extract_detail_urls(soup: bs4.BeautifulSoup, page_url: str) -> list[str]:
        """一覧カードから詳細ページ URL を掲載順を保って取り出す。"""
        urls: list[str] = []
        for box in soup.select("div.caja-resultado"):
            anchor = box.find_parent("a", href=True)
            if anchor is None:
                continue
            absolute = urljoin(page_url, anchor["href"].strip())
            if not _DETAIL_PATH_PATTERN.match(urlsplit(absolute).path):
                continue
            if absolute not in urls:
                urls.append(absolute)
        return urls

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        title = soup.select_one("h4.tituloperfil")
        if title is None:
            logger.warning("事業者名が見つかりません (スキップ): %s", detail_url)
            return None

        path_match = _DETAIL_PATH_PATTERN.match(urlsplit(detail_url).path)
        item = {
            Schema.URL: detail_url,                       # 取得元URL
            Schema.NAME: self._clean(title.get_text(" ", strip=True)),  # 会社名
            Schema.ADDR: "",                              # 住所
            Schema.TEL: "",                               # TEL (+56 国際表記)
            Schema.EMAIL: "",                             # メールアドレス
            Schema.HP: "",                                # URL (事業者サイト)
            Schema.CAT_SITE: self.SERVICE_TYPE_NAME,      # 区分 (Agencia de viajes)
            "都市": "",
            "国": self.COUNTRY,
            "地域": "",
            "サービス分類": "",
            "等級": "",
            "登録状態": "",
            "登録ID": path_match.group(1) if path_match else "",
        }

        # --- サービス分類 (事業者名の直前の兄弟 h5) ---
        # ページ冒頭のプリローダー内にも h5 ("Cargando...") があるため兄弟に限定する。
        clase = title.find_previous_sibling("h5")
        if clase is not None:
            item["サービス分類"] = self._clean(clase.get_text(" ", strip=True))

        # --- 地域 (Región) ---
        region = soup.select_one("span.region")
        if region is not None:
            item["地域"] = self._clean(region.get_text(" ", strip=True))

        # --- 住所 / 都市 / 電話 / メール / 等級 ---
        for p in soup.select("div.textos p"):
            text = self._clean(p.get_text(" ", strip=True))
            label_match = _LABEL_PATTERN.match(text)
            if not label_match:
                continue
            key = self._LABEL_MAP.get(label_match.group(1).strip().lower())
            if not key:
                continue
            value = self._clean(text[label_match.end():])
            if not value:
                continue
            if key == Schema.TEL:
                value = self._normalize_tel(value)
            elif key == "都市":
                value = _LOCALIDAD_SUFFIX_PATTERN.sub("", value)
            if value and not item[key]:
                item[key] = value

        # --- 事業者の Web サイト (掲載の無い事業者あり) ---
        web = soup.select_one("a.btn-web[href]")
        if web is not None:
            href = web["href"].strip()
            if href and not href.lower().startswith(("javascript:", "#")):
                item[Schema.HP] = urljoin(detail_url, href)

        # --- 登録状態 ---
        status = _STATUS_PATTERN.search(soup.get_text(" ", strip=True))
        if status:
            item["登録状態"] = self._clean(status.group(0))

        return item

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """連続する空白 (改行・NBSP 含む) を 1 個の半角スペースにまとめる。"""
        return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()

    def _normalize_tel(self, raw: str) -> str:
        """"(56) 228289500" → "+56 228289500" の国際表記に正規化する。"""
        text = self._clean(raw)
        if not text:
            return ""

        # 先頭の "(56)" / "+(56)" を国番号として取り込む
        cc_match = _TEL_CC_PATTERN.match(text)
        if cc_match:
            country_code = cc_match.group(1)
            rest = text[cc_match.end():]
        elif text.startswith("+"):
            # 既に国際表記 ("+56 9...") の場合は国番号を切り出す
            digits = re.sub(r"\D", "", text)
            if digits.startswith(self.COUNTRY_CALLING_CODE):
                country_code = self.COUNTRY_CALLING_CODE
                rest = digits[len(self.COUNTRY_CALLING_CODE):]
            else:
                return text
        else:
            country_code = self.COUNTRY_CALLING_CODE
            rest = text

        number = re.sub(r"\D", "", rest)
        if not number:
            return ""
        # 国内表記の先頭 0 (市外局番のトランクプレフィックス) は国際表記では落とす
        number = number.lstrip("0")
        if not number:
            return ""
        return f"+{country_code} {number}"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    scraper = Sernatur2()
    scraper.execute("https://portalserviciosturisticos.sernatur.cl/")

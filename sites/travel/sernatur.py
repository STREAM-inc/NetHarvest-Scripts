"""
SERNATUR チリ旅行代理店登録 (Agencia de viajes) — sernatur

取得対象:
    チリ国家観光庁 SERNATUR の「観光サービス提供者登録 (Registro de Prestadores de
    Servicios Turísticos)」のうち、区分 "Agencia de viajes" (旅行代理店) の全登録事業者。
    全 Región (地域) が対象で、2026-09 時点で 1,662 件 (20 件 / ページ × 84 ページ)。

取得フロー:
    1. ルート (引数 url = ポータル https://portalserviciosturisticos.sernatur.cl/) を取得し、
       ページ内のリンクから公開検索サイト (Buscador de Servicios Turísticos /
       serviciosturisticos.sernatur.cl) の URL を導出する。
       リンクが見つからない場合はホスト名の先頭 "portal" を落として導出する
       (= ポータルとサブドメインが対になっている)。
    2. 検索結果 `resultado.php?page=N&nombre=&tipo_servicio=3&comuna_nombre=` を
       1 ページずつ取得する。`tipo_servicio=3` が「Agencia de viajes」区分のフィルタで、
       comuna (市区町村) を空にすることで全地域が対象になる。
    3. 一覧カード (div.caja-resultado を包む a) から詳細 URL (/{id}-{slug}) を 1 件取り出す
       たびに詳細ページを取得し、即 yield する (Pattern B)。
    4. カードが 0 件になったページで終了する (範囲外ページは 0 件を返す)。

詳細ページの構造:
    h4.tituloperfil            … 事業者名
    直前の h5                  … サービス分類 (Receptiva / Emisiva / Receptiva y emisiva)
    span.region                … 地域 (Región de Tarapacá 等)
    div.textos > p             … "ラベル: <b>値</b>" 形式。
                                 Dirección (住所) / Localidad (都市) /
                                 Teléfono (電話) / Email (メール) /
                                 Calificación (等級。出現率は低いが実装する)
    a.btn-web[href]            … 事業者の Web サイト
    .vertical-align .textos    … "Registro vigente" / "Registro no vigente" (登録状態)

備考:
    - 依頼の指示どおり「Agencia de viajes」区分のみを対象とし、全 Región を巡回する。
      取得項目は 会社名 / 住所 / 都市 / TEL / メール / URL を中心に据える。
    - チリの事業者のため Schema.PREF (日本の都道府県) は使用せず、
      EXTRA カラム "地域" (Región) / "都市" (Localidad) / "国" に入れる。
    - 詳細ページの "Sellos de Distinción" 画像 (sello-calidad-turistica.png 等) は
      認証の有無に関わらず表示されるテンプレート画像で、認証実態と一致しないため取得しない。
    - 自由記述の長文フィールドはサイト上に存在しない (掲載は構造化項目のみ)。
    - 電話は "(56) 945520631" 表記のため "+56 945520631" に正規化する。
    - robots.txt は `User-agent: * / Disallow:` で全許可。過負荷を避けるため DELAY = 0.5。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/sernatur.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sernatur
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

# 詳細ページ URL (/{登録ID}-{slug}) の判定
_DETAIL_PATH_PATTERN = re.compile(r"^/(\d+)-[^/]+/?$")

# 検索結果ヘッダの総件数 ("1662 Servicios")
_TOTAL_PATTERN = re.compile(r"([\d.,]+)\s*Servicios")

# 詳細ページ div.textos の "ラベル: 値" 判定
_LABEL_PATTERN = re.compile(r"^\s*([^:]+?)\s*:\s*")

# 電話番号の国番号表記 "(56) 9xxxxxxxx"
_TEL_CC_PATTERN = re.compile(r"^\(\s*(\d+)\s*\)\s*")


class Sernatur(StaticCrawler):
    """SERNATUR 観光サービス登録 (旅行代理店) スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    DELAY = 0.5
    TIMEOUT = 45

    EXTRA_COLUMNS = [
        "地域",
        "都市",
        "国",
        "サービス分類",
        "等級",
        "登録ID",
    ]

    # 国は固定 (チリ国家観光庁の登録簿)
    COUNTRY = "チリ"

    # 取得対象の区分。resultado.php の tipo_servicio に渡す値。
    # 3 = Agencia de viajes (旅行代理店)
    TARGET_SERVICE_ID = "3"
    TARGET_SERVICE_NAME = "Agencia de viajes"

    # ページ巡回の安全上限 (2026-09 時点で 84 ページ)
    MAX_PAGES = 300

    # 詳細ページ div.textos のラベル → 出力先キー
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
        root_soup = self.get_soup(url)
        if root_soup is None:
            raise RuntimeError(f"ルートページを取得できませんでした: {url}")

        search_root = self._resolve_search_root(root_soup, url)
        logger.info("検索サイト: %s", search_root)

        seen: set[str] = set()
        for page in range(1, self.MAX_PAGES + 1):
            list_url = self._build_list_url(search_root, page)
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした (打ち切り): %s", list_url)
                break

            if page == 1:
                total = self._extract_total(soup)
                if total:
                    self.total_items = total
                    logger.info("総件数: %d 件 (%s)", total, self.TARGET_SERVICE_NAME)

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
    # ルート URL から検索サイトを導出
    # ------------------------------------------------------------------ #
    def _resolve_search_root(self, soup: bs4.BeautifulSoup, url: str) -> str:
        """ポータル (引数 url) 内のリンクから公開検索サイトのルート URL を導出する。"""
        portal_host = urlsplit(url).netloc.lower()
        for a in soup.select("a[href]"):
            absolute = urljoin(url, (a.get("href") or "").strip())
            parts = urlsplit(absolute)
            host = parts.netloc.lower()
            if not host or host == portal_host:
                continue
            # "serviciosturisticos.sernatur.cl" (ポータルは "portal" 付きの別ホスト)
            if host.split(".")[0] == "serviciosturisticos" or host == portal_host.removeprefix("portal"):
                return f"{parts.scheme}://{parts.netloc}/"

        # リンクが見つからない場合はホスト名から導出する
        fallback_host = portal_host.removeprefix("portal")
        if fallback_host and fallback_host != portal_host:
            scheme = urlsplit(url).scheme or "https"
            logger.warning("検索サイトへのリンクが見つからないためホスト名から導出します: %s", fallback_host)
            return f"{scheme}://{fallback_host}/"
        raise RuntimeError(f"検索サイトの URL を導出できませんでした: {url}")

    def _build_list_url(self, search_root: str, page: int) -> str:
        """区分 "Agencia de viajes" / 全地域 の検索結果 URL を組み立てる。"""
        return urljoin(
            search_root,
            f"resultado.php?page={page}&nombre=&tipo_servicio={self.TARGET_SERVICE_ID}&comuna_nombre=",
        )

    # ------------------------------------------------------------------ #
    # 一覧ページ
    # ------------------------------------------------------------------ #
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
        """一覧カードから詳細ページ URL を順序を保って取り出す。"""
        urls: list[str] = []
        for box in soup.select("div.caja-resultado"):
            anchor = box.find_parent("a", href=True)
            if not anchor:
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

        m = _DETAIL_PATH_PATTERN.match(urlsplit(detail_url).path)
        item = {
            Schema.URL: detail_url,
            Schema.NAME: self._clean(title.get_text(" ", strip=True)),
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMAIL: "",
            Schema.HP: "",
            Schema.CAT_SITE: self.TARGET_SERVICE_NAME,
            Schema.STS_NM: "",
            "地域": "",
            "都市": "",
            "国": self.COUNTRY,
            "サービス分類": "",
            "等級": "",
            "登録ID": m.group(1) if m else "",
        }

        # --- サービス分類 (事業者名の直前の h5) ---
        clase = title.find_previous("h5")
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
            if key is None:
                continue
            value_node = p.find("b")
            value = self._clean(
                value_node.get_text(" ", strip=True) if value_node else text[label_match.end():]
            )
            if value and not item[key]:
                item[key] = value

        # --- 連絡先リンクからの補完 (ラベルが欠けている場合の保険) ---
        if not item[Schema.EMAIL]:
            mail = soup.select_one("a[href^='mailto:']")
            if mail:
                item[Schema.EMAIL] = self._clean(mail["href"].split(":", 1)[1])
        if not item[Schema.TEL]:
            tel = soup.select_one("a[href^='tel:']")
            if tel:
                item[Schema.TEL] = self._clean(tel["href"].split(":", 1)[1].lstrip("+"))

        item[Schema.TEL] = self._format_tel(item[Schema.TEL])
        item[Schema.EMAIL] = item[Schema.EMAIL].lower()

        # --- Web サイト ---
        web = soup.select_one("a.btn-web[href]")
        if web:
            item[Schema.HP] = self._normalize_url(web["href"])

        # --- 登録状態 (Registro vigente / no vigente) ---
        status = soup.select_one("div.vertical-align div.textos")
        if status is not None:
            item[Schema.STS_NM] = self._clean(status.get_text(" ", strip=True))

        return item

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _format_tel(text: str) -> str:
        """"(56) 945520631" → "+56 945520631" に正規化する。"""
        if not text:
            return ""
        text = _TEL_CC_PATTERN.sub(r"+\1 ", text).strip()
        return re.sub(r"\s+", " ", text)

    @staticmethod
    def _normalize_url(href: str) -> str:
        """Web サイト URL を正規化する (スキーム欠落・空値に対応)。"""
        href = (href or "").strip()
        if not href or href.lower() in {"#", "http://", "https://"}:
            return ""
        if not re.match(r"^https?://", href, re.IGNORECASE):
            href = "http://" + href.lstrip("/")
        parts = urlsplit(href)
        if not parts.netloc or "." not in parts.netloc:
            return ""
        return href

    @staticmethod
    def _clean(text: str) -> str:
        """NBSP・改行・重複空白を畳み、前後の区切り文字を落とす。"""
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text.strip(" ;,")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Sernatur()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://portalserviciosturisticos.sernatur.cl/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

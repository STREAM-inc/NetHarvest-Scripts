"""
ANTA会員一覧 (ナミビア旅行代理店協会) — anta_3

取得対象:
    Association of Namibian Travel Agents (ANTA) 公式サイト
    https://anta.com.na/ の「MEMBERS」セクション (アンカー #Members) に
    掲載されている会員企業 (2026-09 時点で 12 社)。

    ANTA は旅行代理店 (travel agent) の業界団体であり、本サイトに掲載される
    会員は全て travel agent 区分。ツアーオペレーター (tour operator) は
    FENATA 傘下の別協会 (TASA 等) の所管で本サイトには掲載されていないため、
    区分による除外フィルタは「MEMBERS セクション内の会員のみを採用する」
    形で実装している (_member_sections が BECOME A MEMBER セクション以降を
    打ち切るため、装飾画像やツアーオペレーターの混入は起こらない)。

サイト構造 (Phase 1 調査結果):
    - WordPress + Elementor の 1 ページ完結サイト (Static / JS 不要)。
      wp-json/wp/v2/pages が返す公開ページはトップ 1 件のみ、
      wp-sitemap.xml も記事 2 本とトップのみで、会員ディレクトリページや
      会員詳細ページはサイト内に存在しない (探索済み)。
    - ページネーション無し。ルート URL を 1 回取得すれば全件揃う。
    - robots.txt は Disallow: /wp-admin/ のみ。トップページの取得は許可。
    - 利用規約ページはサイト内に存在しない (公開ページがトップ 1 枚のみのため)。
      → スクレイピングを禁止する記載は確認できなかった。

⚠ 会社名がテキストとして存在しない:
    会員セクションは Elementor の画像ウィジェット (ロゴ) のみで構成され、
    <img> の alt は全て空、リンク (<a>) も張られていない。
    そのため会社名は次の優先順で解決する:
        1. ロゴを目視確認して作成した対応表 (_MEMBER_NAMES / 添付ID・ファイル名)
        2. img の alt 属性 (将来入力された場合)
        3. WordPress メディア REST API (/wp-json/wp/v2/media/{id}) の title
        4. 画像ファイル名から生成した暫定名
    どの経路で解決したかは EXTRA カラム「名称の出典」に記録する。

⚠ 住所 / TEL / 会員企業 HP / メールアドレス:
    本サイトには会員ごとの住所・電話番号・自社サイト URL・メールアドレスが
    一切掲載されていない (トップページ全文 / wp-json / wp-sitemap で確認済)。
    したがって Schema.ADDR / TEL / HP / EMAIL は常に空文字となる。
    唯一の連絡先情報である CONTACT セクションの ANTA 役員 (氏名・役職・
    メール・所属会員企業) のみ、所属会員企業名が会員名と一致する場合に
    EXTRA カラムへ補完する。役員メールは大半が協会ドメイン (@anta.com.na) の
    ため、会員企業の連絡先と誤認されないよう Schema.EMAIL には入れない。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/anta_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id anta_3
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 画像 URL 末尾の WordPress リサイズ接尾辞 (-768x410 等)
_SIZE_SUFFIX_PATTERN = re.compile(r"-\d+x\d+$")

# <img class="... wp-image-766"> から添付 ID を取り出す
_WP_IMAGE_ID_PATTERN = re.compile(r"^wp-image-(\d+)$")

# 社名突き合わせ用の正規化 (英数字のみ・小文字)
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")


class Anta3(StaticCrawler):
    """ANTA会員一覧 (ナミビア旅行代理店協会) スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "会員区分",
        "ロゴ画像URL",
        "WordPress添付ID",
        "名称の出典",
        "ANTA役員",
        "ANTA役員役職",
        "ANTA役員メール",
    ]

    # 取得対象の会員区分 (備考: travel agent のみ。tour operator は除外)
    _MEMBER_CATEGORY = "Travel Agent"

    # ロゴ画像 → 会社名の対応表。
    # キーは WordPress 添付 ID と、リサイズ接尾辞・拡張子を除いたファイル名 (小文字) の両方。
    # 社名はロゴ画像の表記をそのまま採用している (サイト上にテキストとしての社名が無いため)。
    _MEMBER_NAMES: dict[str, str] = {
        "766": "Advanced Travel & Tours",
        "logo-002": "Advanced Travel & Tours",
        "515": "Blueberry Travel",
        "blueberry-logo": "Blueberry Travel",
        "761": "Kaoko Travel & Tours",
        "kaoko": "Kaoko Travel & Tours",
        "512": "Rennies BCD Travel Namibia",
        "rennies-bcd-namibia-logoredgrey": "Rennies BCD Travel Namibia",
        "762": "Satguru Travel",
        "satguru": "Satguru Travel",
        "689": "Seasons Travel & Tours Namibia",
        "unnamed-3": "Seasons Travel & Tours Namibia",
        "696": "Travel Hub Namibia",
        "travel-hub-logo": "Travel Hub Namibia",
        "510": "Travel Magic cc",
        "tm-logo-resize": "Travel Magic cc",
        "516": "Trip Travel",
        "triptravel_slogan2020": "Trip Travel",
        "511": "Ultra Travel",
        "ultratravel_logo": "Ultra Travel",
        "513": "XL The Travel Professionals",
        "xl-the-travel-professionals-logo-": "XL The Travel Professionals",
        "773": "Elite Travel Planners",
        "elite-logo-1": "Elite Travel Planners",
    }

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """ルート URL (= sites.yml の url) を唯一の起点として会員を 1 件ずつ yield する。"""
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"トップページを取得できませんでした: {url}")

        # CONTACT セクションの ANTA 役員 (所属会員企業 → 氏名/役職/メール)
        officers = self._extract_officers(soup)

        sections = self._member_sections(soup)
        logger.info("会員ロゴのセクション数: %d", len(sections))

        seen: set[str] = set()
        for section in sections:
            for img in section.select("div.elementor-widget-image img[src]"):
                logo_url = urljoin(url, (img.get("src") or "").strip())
                if not logo_url or logo_url in seen:
                    continue
                seen.add(logo_url)

                attachment_id = self._attachment_id(img)
                name, source = self._resolve_name(img, logo_url, attachment_id, url)
                if not name:
                    logger.warning("社名を解決できない画像 (スキップ): %s", logo_url)
                    continue

                officer = officers.get(self._normalize(name), ("", "", ""))
                # 1 件取得するごとに即 yield する (Pattern B)
                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.ADDR: "",    # サイトに掲載が無い
                    Schema.TEL: "",     # サイトに掲載が無い
                    Schema.HP: "",      # ロゴにリンクが張られていない
                    Schema.EMAIL: "",   # サイトに掲載が無い
                    Schema.CAT_SITE: "ANTA会員 (旅行代理店)",
                    "会員区分": self._MEMBER_CATEGORY,
                    "ロゴ画像URL": logo_url,
                    "WordPress添付ID": attachment_id,
                    "名称の出典": source,
                    "ANTA役員": officer[0],
                    "ANTA役員役職": officer[1],
                    "ANTA役員メール": officer[2],
                }

        self.total_items = len(seen)

    # ------------------------------------------------------------------ #
    # MEMBERS セクションの切り出し
    # ------------------------------------------------------------------ #
    @staticmethod
    def _member_sections(soup: bs4.BeautifulSoup) -> list[bs4.Tag]:
        """#Members アンカー以降の「画像だけのセクション」を順に集める。

        テキストを持つセクション ("BECOME A MEMBER" 等) に当たった時点で打ち切る。
        これにより BECOME A MEMBER の装飾画像 (飛行機アイコン) を会員と誤検出しない。
        """
        anchor = soup.select_one("div.elementor-menu-anchor#Members, #Members")
        if anchor is None:
            return []

        # アンカーを含む top-section まで遡る
        heading_section = None
        node = anchor
        while node is not None:
            if getattr(node, "name", None) == "section" and "elementor-top-section" in (
                node.get("class") or []
            ):
                heading_section = node
                break
            node = node.parent
        if heading_section is None:
            return []

        sections: list[bs4.Tag] = []
        for sibling in heading_section.find_next_siblings("section"):
            if sibling.get_text(strip=True):
                break  # 会員ロゴ以外のセクション (BECOME A MEMBER / NEWS ...)
            if not sibling.select("div.elementor-widget-image img[src]"):
                break
            sections.append(sibling)
        return sections

    # ------------------------------------------------------------------ #
    # 会社名の解決
    # ------------------------------------------------------------------ #
    @staticmethod
    def _attachment_id(img: bs4.Tag) -> str:
        """<img class="wp-image-766"> から WordPress 添付 ID を取り出す。"""
        for cls_name in img.get("class") or []:
            m = _WP_IMAGE_ID_PATTERN.match(cls_name)
            if m:
                return m.group(1)
        return ""

    def _resolve_name(
        self, img: bs4.Tag, logo_url: str, attachment_id: str, root_url: str
    ) -> tuple[str, str]:
        """ロゴ画像から会社名を解決し、(社名, 出典) を返す。"""
        # 1. 対応表 (添付 ID → ファイル名の順で照合)
        if attachment_id and attachment_id in self._MEMBER_NAMES:
            return self._MEMBER_NAMES[attachment_id], "ロゴ画像 (目視確認)"

        stem = self._file_stem(logo_url)
        if stem in self._MEMBER_NAMES:
            return self._MEMBER_NAMES[stem], "ロゴ画像 (目視確認)"

        # 2. alt 属性 (現状は全て空だが、将来入力される可能性に備える)
        alt = (img.get("alt") or "").strip()
        if alt:
            return alt, "img alt 属性"

        # 3. WordPress メディア REST API の title (将来追加される会員向け)
        media_title = self._media_title(attachment_id, root_url)
        if media_title:
            return media_title, "WordPress メディア title (要確認)"

        # 4. フォールバック: ファイル名から暫定名を作る
        if not stem:
            return "", ""
        provisional = re.sub(r"[-_]+", " ", stem).strip()
        provisional = re.sub(r"\s+", " ", provisional)
        return provisional.title(), "画像ファイル名 (暫定・要確認)"

    def _media_title(self, attachment_id: str, root_url: str) -> str:
        """/wp-json/wp/v2/media/{id} の title を返す。取得できなければ空文字。

        ルート URL から派生させた同一ホストの REST API のみを参照する。
        """
        if not attachment_id:
            return ""
        api_url = urljoin(root_url, f"wp-json/wp/v2/media/{attachment_id}")
        try:
            response = self.session.get(api_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:  # noqa: BLE001 — 補助的な情報源なので落とさない
            logger.warning("メディア API を取得できませんでした: %s — %s", api_url, e)
            return ""
        title = (payload.get("title") or {}).get("rendered") or ""
        return bs4.BeautifulSoup(title, "html.parser").get_text(strip=True)

    @staticmethod
    def _file_stem(logo_url: str) -> str:
        """画像 URL からリサイズ接尾辞・拡張子を除いたファイル名 (小文字) を返す。"""
        filename = logo_url.split("?")[0].rstrip("/").split("/")[-1]
        stem = filename.rsplit(".", 1)[0].lower()
        return _SIZE_SUFFIX_PATTERN.sub("", stem)

    # ------------------------------------------------------------------ #
    # CONTACT セクション (ANTA 役員)
    # ------------------------------------------------------------------ #
    @classmethod
    def _extract_officers(
        cls, soup: bs4.BeautifulSoup
    ) -> dict[str, tuple[str, str, str]]:
        """CONTACT セクションから {正規化した所属会員企業名: (氏名, 役職, メール)} を作る。

        1 名分は 1 カラム (div.elementor-column) にテキストウィジェットが 4 つ並ぶ
        「氏名 / 役職 / メールアドレス / 所属会員企業名」の構成。
        同じ会員企業に複数名いる場合は " / " で連結する。
        所属が "ANTA" (協会自体) の役員は会員企業に紐づかないため除外する。
        """
        collected: dict[str, list[tuple[str, str, str]]] = {}
        for column in soup.select("div.elementor-column"):
            lines = [
                line.strip()
                for line in column.get_text("\n", strip=True).split("\n")
                if line.strip()
            ]
            # 「3 行目だけがメールアドレスの 4 行カラム」を役員ブロックとみなす
            if len(lines) != 4 or "@" not in lines[2] or "@" in lines[3]:
                continue
            name, position, email, company = lines
            key = cls._normalize(company)
            if not key or key == "anta":
                continue
            collected.setdefault(key, []).append((name, position, email))

        officers = {
            key: tuple(" / ".join(v for v in values if v) for values in zip(*entries))
            for key, entries in collected.items()
        }
        logger.info("CONTACT の役員 (会員企業所属): %d 社分", len(officers))
        return officers

    @staticmethod
    def _normalize(text: str) -> str:
        """社名突き合わせ用の正規化 (小文字・英数字のみ)。"""
        return _NON_ALNUM_PATTERN.sub("", text.lower())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Anta3()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://anta.com.na/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

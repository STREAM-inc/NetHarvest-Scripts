"""
【STREAMREQ-17785】政府認可旅行代理店一覧（ニューカレドニア）
DECAT (Direction des Entreprises, de la Consommation, de l'Attractivité et
des Télécommunications / ニューカレドニア政府) が認可した
「agences de tourisme et de voyage (観光・旅行代理店)」一覧クローラー。

取得対象:
    ニューカレドニア政府が認可・登録した観光代理店 (agences de tourisme) と
    旅行代理店 (agences de voyages) の全社。
    会社名 (RAISON SOCIALE) / 住所 (ADRESSE) / 認可アレテ番号 (Référence arrêté) /
    ライセンス番号 (N° de licence) / 金融保証機関 (GARANT FINANCIER) /
    保険会社 (ASSUREUR)。

取得フロー:
    引数 url (= sites.yml の url = DECAT の一覧ページ) が唯一のルート。
    1. url を get_soup() で取得する。
       ⚠ このページの HTML には代理店データが 1 件も無い。実データは
         「Documents à télécharger」ブロックに置かれた 2 本の PDF
         (tourisme_liste_diffusion.pdf / voyages_liste_diffusion.pdf) の中にある。
    2. ページ内の PDF リンクを urljoin(url, href) で絶対化して列挙する。
    3. 各 PDF を self.session.get でダウンロードし (get_soup と同経路で
       テストランナーのソフトタイムアウト対象)、pdfplumber で表を抽出する。
       ヘッダに「RAISON SOCIALE」を持つ表だけを採用するため、
       将来ページに別種の PDF が増えても誤取得しない。
    4. 1 社ぶんの行が確定するたびに即 yield する (Pattern B。全件バッファしない)。
       PDF の表は 1 社が複数行に折り返されることがあり、先頭列 (社名) が空の行は
       直前の社の住所などの続き行として結合する。

備考 (呼び出し指示への対応):
    - 指示された取得カラムのうち「TEL」「URL (企業サイト)」は出典 PDF に列自体が
      存在しない (掲載項目は社名/住所/アレテ番号/ライセンス番号/保証機関/保険会社の
      6 列のみ)。該当カラムは空文字で出力する。
    - 対象は海外 (ニューカレドニア) 事業者のため STX 名寄せ対象外。
      Schema.PREF (都道府県) は日本の行政区分のため空とし、国名は EXTRA 列へ出す。
    - フィルタ指示は無いため、認可済みの全社 (観光代理店 + 旅行代理店) を取得する。
    - EXTRA_COLUMNS は短い構造化ラベル・番号・日付のみ。自由記述の文章は
      出典 PDF に存在せず、著作権リスクのある列は含めていない。
    - robots.txt は Crawl-delay: 10 のみ (Disallow 対象外のパス)。
      /mentions-legales も「複製 (reproduction)」に関する著作権条項のみで、
      スクレイピング/クローリング/自動収集を明示的に禁止する記述は無い。
      Crawl-delay を尊重し PDF ダウンロード間に DELAY 秒の間隔を空ける。
      HTTP リクエストは 1 (HTML) + PDF 本数 のみで、アイテム単位の通信は無いため
      ITEM_DELAY = 0 とする。

実行方法:
    # ローカルテスト
    python scripts/sites/government/streamreq_17785.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_17785
"""

import io
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 表ヘッダ (PDF 側の列見出し) → 内部キー
_HEADER_KEYS = {
    "RAISON SOCIALE": "name",
    "ADRESSE": "addr",
    "REFERENCE ARRETE": "arrete",
    "N° DE LICENCE": "licence",
    "N DE LICENCE": "licence",
    "GARANT FINANCIER": "garant",
    "ASSUREUR": "assureur",
}

# PDF 表題から業種を判定 ("AGENCES DE TOURISME ..." / "AGENCES DE VOYAGES ...")
_KIND_RE = re.compile(r"AGENCES?\s+DE\s+(TOURISME|VOYAGES?)", re.I)
_KIND_LABEL = {
    "TOURISME": "観光代理店 (agence de tourisme)",
    "VOYAGE": "旅行代理店 (agence de voyage)",
}

# 一覧の基準日 ("Mise à jour : 07/02/2023")
_MAJ_RE = re.compile(r"Mise\s*à\s*jour\s*:?\s*(\d{1,2})/(\d{1,2})/(\d{4})", re.I)


class Streamreq17785(StaticCrawler):
    """ニューカレドニア政府 DECAT 認可 観光・旅行代理店一覧 (PDF) スクレイパー"""

    # robots.txt の Crawl-delay: 10 に合わせた PDF ダウンロード間隔。
    DELAY = 10.0
    # 1 社ごとの追加通信は無い (PDF は一括取得済み) ためアイテム間ウェイトは不要。
    ITEM_DELAY = 0.0

    EXTRA_COLUMNS = [
        "国",
        "認可アレテ番号",
        "ライセンス番号",
        "金融保証機関",
        "保険会社",
        "一覧基準日",
        "出典PDF",
    ]

    # ---- ユーティリティ -----------------------------------------------------

    @staticmethod
    def _cell(value) -> str:
        """セル内改行・連続空白を 1 個の半角スペースに畳んで整形する。"""
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    @staticmethod
    def _norm_header(value) -> str:
        """ヘッダ照合用の正規化 (アクセント除去・大文字化・改行/空白畳み込み)。

        PDF の見出しは "Référence arrêté" のようにアクセント付きなので、
        NFD 分解して結合文字を落としてから照合する。
        """
        text = unicodedata.normalize("NFD", str(value or ""))
        text = "".join(c for c in text if not unicodedata.combining(c))
        return re.sub(r"\s+", " ", text).strip().upper()

    @classmethod
    def _kind_label(cls, text: str) -> str:
        """PDF 本文の表題から業種ラベルを決める。"""
        m = _KIND_RE.search(text or "")
        if not m:
            return ""
        head = m.group(1).upper().rstrip("S")  # VOYAGES → VOYAGE
        return _KIND_LABEL.get(head, "")

    @staticmethod
    def _maj_date(text: str) -> str:
        """"Mise à jour : 07/02/2023" (dd/mm/yyyy) → "2023-02-07"。"""
        m = _MAJ_RE.search(text or "")
        if not m:
            return ""
        day, month, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{int(month):02d}-{int(day):02d}"

    # ---- メイン -------------------------------------------------------------

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", url)
            return

        pdf_urls = self._collect_pdf_urls(soup, url)
        if not pdf_urls:
            logger.error("一覧ページに PDF リンクが見つかりませんでした: %s", url)
            return
        logger.info("PDF %d 本を検出しました", len(pdf_urls))

        for index, pdf_url in enumerate(pdf_urls):
            # robots.txt の Crawl-delay: 10 を尊重 (2 本目以降のみ待機)
            if index > 0 and self.DELAY > 0:
                time.sleep(self.DELAY)
            try:
                yield from self._parse_pdf(pdf_url)
            except Exception as e:  # 1 本の PDF の失敗で他を巻き込まない
                self.error_count += 1
                logger.warning("PDF の解析に失敗しskip: %s — %s", pdf_url, e)
                continue

    @staticmethod
    def _collect_pdf_urls(soup, url: str) -> list[str]:
        """「Documents à télécharger」ブロックの PDF リンクを絶対 URL で列挙する。"""
        # 本文領域 (region-content) に限定し、サイドバー/フッターの資料を拾わない
        scope = soup.select_one("div.region-content") or soup
        urls: list[str] = []
        for a in scope.select('a[href]'):
            href = (a.get("href") or "").strip()
            if not href or ".pdf" not in href.lower():
                continue
            absolute = urljoin(url, href)
            if absolute not in urls:
                urls.append(absolute)
        return urls

    def _parse_pdf(self, pdf_url: str):
        """PDF 1 本を解析し、1 社ぶん確定するたびに即 yield する。"""
        # session.get はテストランナーのソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(pdf_url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        try:
            import pdfplumber
        except ImportError:
            logger.error(
                "pdfplumber が未導入のため PDF を解析できません。"
                "worker に pdfplumber を導入してください (pyproject.toml に記載済み)"
            )
            return

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            full_text = "\n".join((p.extract_text() or "") for p in pdf.pages)
            kind = self._kind_label(full_text)
            maj = self._maj_date(full_text)

            current: dict[str, str] | None = None
            for page in pdf.pages:
                for table in page.extract_tables():
                    if not table:
                        continue
                    header = [self._norm_header(c) for c in table[0]]
                    if "RAISON SOCIALE" not in header:
                        continue  # 表題・更新日だけのレイアウト表はスキップ
                    # 列見出し → 列位置 (列順が変わっても追随できるようヘッダ駆動)
                    cols = {
                        _HEADER_KEYS[h]: i
                        for i, h in enumerate(header)
                        if h in _HEADER_KEYS
                    }
                    for row in table[1:]:
                        name = self._cell(row[cols["name"]]) if "name" in cols else ""
                        if name:
                            # 新しい社が始まったので、直前の社を確定して yield
                            if current:
                                yield current
                            current = self._build_item(row, cols, kind, maj, pdf_url)
                        elif current:
                            # 社名が空の行 = 直前の社の折り返し (住所の続き等)
                            self._merge_row(current, row, cols)
            if current:
                yield current

    def _build_item(self, row, cols, kind: str, maj: str, pdf_url: str) -> dict:
        def val(key: str) -> str:
            i = cols.get(key)
            if i is None or i >= len(row):
                return ""
            return self._cell(row[i])

        return {
            Schema.URL: pdf_url,
            Schema.NAME: val("name"),
            # 都道府県は日本の行政区分のため海外事業者では空 (国名は EXTRA 列へ)
            Schema.PREF: "",
            Schema.ADDR: val("addr"),
            # 出典 PDF に電話番号・企業サイト URL の列は存在しない
            Schema.TEL: "",
            Schema.HP: "",
            Schema.CAT_SITE: kind,
            "国": "ニューカレドニア",
            "認可アレテ番号": val("arrete"),
            "ライセンス番号": val("licence"),
            "金融保証機関": val("garant"),
            "保険会社": val("assureur"),
            "一覧基準日": maj,
            "出典PDF": pdf_url,
        }

    def _merge_row(self, item: dict, row, cols) -> None:
        """社名が空の折り返し行の各セルを、直前の社の対応項目へ追記する。"""
        targets = {
            "addr": Schema.ADDR,
            "arrete": "認可アレテ番号",
            "licence": "ライセンス番号",
            "garant": "金融保証機関",
            "assureur": "保険会社",
        }
        for key, column in targets.items():
            i = cols.get(key)
            if i is None or i >= len(row):
                continue
            extra = self._cell(row[i])
            if not extra:
                continue
            item[column] = f"{item[column]} {extra}".strip() if item[column] else extra


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Streamreq17785()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute(
        "https://decat.gouv.nc/professionnels-professions-reglementees-agent-de-tourisme-et-de-voyage/les-agences-de-tourisme-et-de"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
なっぷ (nap-camp.com) — 全国キャンプ場情報スクレイパー

取得対象:
    - 全国のキャンプ場詳細ページ (約5,900件)
    - 名称 / 住所(都道府県分割) / 定休日 / カード決済 などの構造化フィールド

取得フロー:
    1. サイトの campsite 用 sitemap (sitemap-dynamic-campsite.xml) を取得
    2. ルート詳細URL (/{region}/{id}/) のみを抽出 (images / topics / plans 等の
       サブページは除外)
    3. 詳細ページを1件ずつ取得し、取得即 yield (Pattern B)

備考対応:
    「すべてのエリアで一括検索できない」問題への対策として、エリア横断のフォーム検索
    ではなく公式 sitemap を起点に全エリアの詳細URLを列挙する。これにより 47 都道府県
    すべてのキャンプ場を漏れなく取得できる。特定エリアへの絞り込み指示は無いため
    フィルタは掛けず全件対象とする。

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/nap_camp.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nap_camp
"""

import re
import sys
import warnings
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# sitemap を html.parser で読む際の警告を抑制 (loc 抽出には十分)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# 詳細ページのルートURL (/{region romaji}/{id}/) だけにマッチさせる。
# /{region}/{id}/images, /plans などのサブページや検索ページを除外する。
_DETAIL_PATTERN = re.compile(r"^https?://www\.nap-camp\.com/[a-z]+/\d+/$")

# 都道府県を住所文字列の先頭から切り出す
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 詳細ページの定義リスト (label div → value div) のうち、長文の自由記述
# (道案内・料金注意書き・定型注意文・名称の重複) は著作権リスク回避のため取得しない。
_PROSE_EXCLUDE = {
    "キャンプ場詳細",        # 名称の重複
    "アクセス案内",          # 道順の自由記述
    "料金情報",              # 料金に関する注意書き (自由記述)
    "領収書（インボイス制度対応）",  # 定型注意文
}

# Schema にマッピングする日本語ラベル
_LABEL_HOLIDAY = "定休日"
_LABEL_PAYMENTS = "カード決済"

# 電話番号は構造化テーブルには無く、本文の「お問い合わせ」自由記述ブロック内に
# 「tel:090-7431-1140」のような形で混在する。まず tel: プレフィックス付きを優先し、
# 無ければ日本の電話番号パターン(市外局番始まり)を fallback で拾う。
_TEL_PREFIX_PATTERN = re.compile(r"tel[:：\s]*([0０][\d０-９][\d０-９\-－\s]{6,12}[\d０-９])", re.IGNORECASE)
_TEL_FALLBACK_PATTERN = re.compile(r"(?<![\d\-])(0\d{1,4}-\d{1,4}-\d{3,4})(?![\d\-])")

# EXTRA_COLUMNS として保持する構造化ラベル (短い・列挙形式)
_EXTRA_LABELS = [
    "乗り入れ可能車両",
    "立地環境",
    "施設タイプ",
    "駐車場",
    "場内共有設備",
    "レンタル可能用品",
    "営業期間",
    "チェックイン",
    "チェックアウト",
    "利用タイプ",
]


class NapCampScraper(StaticCrawler):
    """なっぷ (nap-camp.com) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = list(_EXTRA_LABELS)

    def parse(self, url: str):
        # 引数 url を唯一のルートとして sitemap URL を導出する (SSOT = sites.yml の url)
        sitemap_url = urljoin(url, "sitemap-dynamic-campsite.xml")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return

        detail_urls = []
        seen = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if _DETAIL_PATTERN.match(href) and href not in seen:
                seen.add(href)
                detail_urls.append(href)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item  # 取得即 yield (Pattern B)

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        h1 = soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        fields = self._extract_fields(soup)

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.CAT_SITE: "キャンプ場",
            Schema.TEL: self._extract_tel(soup),
            Schema.HOLIDAY: fields.get(_LABEL_HOLIDAY, ""),
            Schema.PAYMENTS: fields.get(_LABEL_PAYMENTS, ""),
        }

        # 住所 → 都道府県 + 住所(以降)
        address = fields.get("住所", "")
        if address:
            m = _PREF_PATTERN.match(address)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = address[m.end():].strip()
            else:
                item[Schema.ADDR] = address

        # EXTRA_COLUMNS (出現しないものは空文字)
        for label in _EXTRA_LABELS:
            item[label] = fields.get(label, "")

        return item

    @staticmethod
    def _extract_tel(soup) -> str:
        """ページ本文から電話番号を抽出する。

        電話番号は構造化テーブルには無く「お問い合わせ」自由記述ブロック内に
        「tel:090-7431-1140」のような形で含まれる。この自由記述は Next.js の
        <script> ペイロード(JSON)に格納され get_text() では拾えないため、
        tel: プレフィックス付きはレンダリング済みマークアップ全体を対象に抽出する。
        無ければ可視テキストから一般的な電話番号パターンを fallback で拾う。
        記載が無いページも多いため、見つからなければ空文字を返す。
        """
        # tel: 付きは <script> JSON 内にもあるためマークアップ全体を対象にする
        m = _TEL_PREFIX_PATTERN.search(str(soup))
        if not m:
            # fallback は誤検出を避けるため可視テキストのみを対象にする
            m = _TEL_FALLBACK_PATTERN.search(soup.get_text(" ", strip=True))
        if not m:
            return ""

        # 全角→半角、区切りをハイフンへ正規化
        tel = m.group(1).translate(str.maketrans("０１２３４５６７８９－", "0123456789-"))
        tel = re.sub(r"\s+", "-", tel.strip())
        tel = re.sub(r"-+", "-", tel).strip("-")
        return tel

    @staticmethod
    def _extract_fields(soup) -> dict:
        """詳細ページの label/value 定義リストを {ラベル: 値} で返す。

        value 側は通常テキストの場合と、Declarative Shadow DOM の <template> に
        値が格納されている場合があるため両対応する。長文の自由記述ラベルは除外する。
        """
        fields = {}
        for label_div in soup.find_all("div"):
            cls = " ".join(label_div.get("class") or [])
            if "md:max-w-[220px]" not in cls or "font-semibold" not in cls:
                continue
            key = label_div.get_text(strip=True)
            if not key or key in _PROSE_EXCLUDE:
                continue
            value_div = label_div.find_next_sibling("div")
            if value_div is None:
                continue
            template = value_div.find("template")
            if template is not None:
                text = template.get_text(" ", strip=True)
            else:
                # 「地図を見る」等のボタンUIテキストを除去してから抽出
                for btn in value_div.find_all("button"):
                    btn.extract()
                text = value_div.get_text(" ", strip=True)
            fields[key] = re.sub(r"\s+", " ", text).strip()
        return fields


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NapCampScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.nap-camp.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

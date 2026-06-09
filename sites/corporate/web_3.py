"""
優良WEB — ホームページ制作会社ディレクトリ (yuryoweb.com) ／全国版

取得対象:
    - 全国のホームページ制作会社 (/company_info/ を起点に全ページ巡回)

取得フロー (一覧 → 詳細, Pattern B = 詳細を1件取得するごとに即 yield):
    1. /company_info/[page/{N}/] を巡回し、各ページのカード (li.tax_company_list_li)
       から詳細URL (h3.tax_company_title a, /company_info/{slug}/) を収集する
    2. 収集した詳細URLごとに即座に詳細ページを取得し、企業情報を抽出して yield する
       (途中で中断しても無駄な通信が起きないよう、1件ずつ yield する)
    3. ページまたぎの重複は URL ベースで排除する

サイト構成 (2026-06 時点):
    - WordPress 製の静的サイト (wp-pagenavi, AIOSEO)。requests で取得可能なため Static。
    - 1ページ 12件 × 949ページ ≈ 約 11,400 件
    - ページネーション: /company_info/page/{N}/ (1ページ目は /company_info/)
    - 詳細ページの会社概要は dl.about_list (会社名・代表者名・設立・資本金など) と
      dl.service_list (得意分野などのタグ集合) に分かれて格納されている
    - 「設⽴」「資本⾦」「⼝コミ」等のラベルは康熙部首の異体字を含むため NFKC 正規化で吸収する

著作権配慮:
    - 自由記述の紹介文 (tax_description / 「続きを読む」/ オンライン対応の説明文) は取得しない
    - 構造化された短いラベル・タグ集合・数値・URL のみを取得する

実行方法:
    python scripts/sites/corporate/web_3.py
    docker compose exec worker python /app/bin/run_flow.py --site-id web_3
"""

import re
import sys
import unicodedata
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://yuryoweb.com"
START_URL = f"{BASE_URL}/company_info/"

_POST_RE = re.compile(r"(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"(0\d{1,4}-\d{1,4}-\d{3,4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    """全角空白・連続空白を整理した文字列を返す。"""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _nlabel(s: str) -> str:
    """dt ラベルを NFKC 正規化する (設⽴→設立 / 資本⾦→資本金 / ⼝コミ→口コミ 等の異体字対策)。"""
    return unicodedata.normalize("NFKC", _clean(s))


class YuryoWebScraper(StaticCrawler):
    """優良WEB 全国版 ホームページ制作会社スクレイパー"""

    DELAY = 1.5

    # サイト固有カラム (構造化された短いラベル・タグ集合・URL・数値のみ。自由記述の紹介文は除外)
    EXTRA_COLUMNS = [
        "支社所在地",
        "対応エリア",
        "実績紹介ページ",
        "口コミ・評判ページ",
        "主要取引先",
        "公認パートナー",
        "資格・認証",
        "制作価格帯",
        "得意サイトタイプ",
        "得意業種",
        "制作系の特徴",
        "集客系の特徴",
        "デザイン系の特徴",
        "その他の特徴",
        "得意とするCMS",
        "得意とするECシステム",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧ページを巡回し、詳細URLを見つけ次第すぐに詳細を取得して yield する (Pattern B)。"""
        seen: set[str] = set()
        page = 1

        while True:
            page_url = START_URL if page == 1 else f"{START_URL}page/{page}/"
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("%d ページ目の取得に失敗。終了します", page)
                break

            cards = soup.select("h3.tax_company_title a[href]")
            if not cards:
                self.logger.info("%d ページ目にカードが無いため終了 (総ページ巡回完了)", page)
                break

            self.logger.info("%d ページ目: カード %d 件", page, len(cards))

            for a in cards:
                href = a.get("href", "")
                if not href or "/company_info/" not in href:
                    continue
                detail_url = href if href.startswith("http") else BASE_URL + href
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            page += 1

    def _collect_fields(self, soup) -> dict:
        """詳細ページの dl.about_list / dl.service_list から {正規化ラベル: 値} を収集する。

        about_list を先に処理することで、本社所在地などは郵便番号・番地を含む完全な値が優先される
        (service_list 側は「都道府県 市区」だけの簡略版のため)。
        """
        fields: dict[str, str] = {}
        for selector in ("dl.about_list", "dl.service_list"):
            for dl in soup.select(selector):
                for dt in dl.find_all("dt"):
                    dd = dt.find_next_sibling("dd")
                    if dd is None:
                        continue
                    key = _nlabel(dt.get_text())
                    value = _clean(dd.get_text(" ", strip=True))
                    if key and key not in fields:
                        fields[key] = value
        return fields

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        f = self._collect_fields(soup)

        name = f.get("会社名", "")
        if not name:
            h1 = soup.select_one("h1")
            name = _clean(h1.get_text()) if h1 else ""
        if not name:
            return None

        item: dict = {Schema.URL: url, Schema.NAME: name}

        rep = f.get("代表者名", "")
        if rep:
            item[Schema.REP_NM] = rep

        established = f.get("設立", "")
        if established:
            item[Schema.OPEN_DATE] = established

        cap = f.get("資本金", "")
        if cap:
            item[Schema.CAP] = cap

        emp = f.get("スタッフ数", "")
        if emp:
            item[Schema.EMP_NUM] = emp

        hp = f.get("URL", "")
        if hp:
            item[Schema.HP] = hp

        # 事業内容: 提供サービスの列挙 (構造化された項目の集合)。自由記述の紹介文ではない。
        lob = f.get("事業内容", "")
        if lob:
            item[Schema.LOB] = lob

        # 電話番号: 「03-xxxx-xxxx ※優良WEBを見たとお伝えください」等。電話番号らしき部分のみ抽出。
        # 「非公開」等の場合は空のままとする (Pipeline が全角→半角を正規化する)。
        tel_raw = f.get("電話番号", "")
        m_tel = _TEL_RE.search(tel_raw)
        if m_tel:
            item[Schema.TEL] = m_tel.group(1)

        # 本社所在地: 〒郵便番号 + 都道府県 + 住所 を分解する。
        addr_raw = f.get("本社所在地", "")
        if addr_raw:
            m_post = _POST_RE.search(addr_raw)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
            body = re.sub(r"〒?\s*\d{3}-?\d{4}\s*", "", addr_raw).strip()
            m_pref = _PREF_RE.search(body)
            if m_pref:
                item[Schema.PREF] = m_pref.group(1)
                item[Schema.ADDR] = body[m_pref.end():].strip()
            else:
                item[Schema.ADDR] = body

        # EXTRA カラム (構造化された短いラベル・タグ集合・URL・数値のみ)
        for col in self.EXTRA_COLUMNS:
            value = f.get(_nlabel(col), "")
            if value:
                item[col] = value

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = YuryoWebScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

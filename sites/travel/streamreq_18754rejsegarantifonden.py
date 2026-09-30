"""
【STREAMREQ-18754】Rejsegarantifonden (デンマーク旅行保証基金) 会員名簿 — streamreq_18754rejsegarantifonden

取得対象:
    デンマーク旅行保証基金 (Rejsegarantifonden) に登録されている旅行提供者
    (rejseudbyder) の全件。2026-09 時点で 87 ページ × 10 件 = 約 864 件。

取得フロー:
    1. ルート (引数 url = https://www.rejsegarantifonden.dk/medlem/) を取得する。
    2. WordPress アーカイブの記事 (article.rgf_member) を 1 件ずつ即 yield する。
    3. ページ送りは `page/{N}/` (url からの相対派生)。ページャの
       a.next.page-numbers を辿り、リンクが無くなる / 記事 0 件で終了する。

一覧ページの構造 (1 件 = article.rgf_member):
    h2.entry-title                      … 会社名
    .register-div h3                    … 区分 (Medlem / Formidler / Udenlandsk dækning)
    table tr > th[scope=row] + td       … ラベル / 値の対。実在するラベルは
        CVR nr.      … デンマーク法人登録番号 (外国法人は "SE5592347438" 等)
        Nr.          … 会員番号 (Rejsegarantifonden 側の登録番号)
        Hjemmeside   … 会社 Web サイト
        Binavne      … 副称 (ul > li の company 名リスト)
        Formidlere   … この会員の仲介業者 (ul > li)
        Formidler for… この仲介業者が仲介する会員 (ul > li)
        Garantiland  … 保証国 (海外の旅行保証制度に加入している場合のみ)

住所 / 都市 / TEL について (重要):
    本サイトの一覧ページ・詳細ページ (/medlem/{slug}/) のいずれにも
    住所・都市・電話番号は掲載されていない。詳細ページは本文が空で
    タイトルと公開日しか描画されない (全 87 ページ・複数サンプルで確認済)。
    そのため備考の「詳細ページに住所・TEL・URL がある場合は詳細ページも辿る」
    という条件は成立せず、詳細ページは巡回しない (無駄な 864 リクエストを回避)。
    住所 / 都市 / TEL のカラムは空文字で出力する。
    ただし将来サイト側に "Adresse:" / "By:" / "Telefon:" 行が追加されても
    拾えるよう、テーブルはラベル駆動でパースし TEL は +45 表記へ正規化する。

備考:
    - 「国」カラムは依頼指示どおり "デンマーク" 固定。
      海外の保証制度に加入している会員の保証国は EXTRA "保証国" に入れる。
    - 区分は Medlem=正会員 / Formidler=仲介業者 / Udenlandsk dækning=海外保証
      に読み替えて EXTRA "区分(作業用)" に入れる (未知の値は原文のまま)。
    - 自由記述の長文 (eu-alert の注意文等) はサイト共通の定型文かつ著作物のため取得しない。
    - robots.txt は Disallow: /wp-admin/ のみ。/medlem/ の取得は許可されている。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18754rejsegarantifonden.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18754rejsegarantifonden
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

# ページャ最終ページ番号 (".../medlem/page/87/")
_PAGE_NUM_PATTERN = re.compile(r"/page/(\d+)/?$")

# 電話番号から数字以外を除去する際に残す先頭 "+"
_TEL_CLEAN_PATTERN = re.compile(r"[^\d+]")

# 区分 (デンマーク語) → 作業用の日本語ラベル
_KIND_MAP = {
    "Medlem": "正会員",
    "Formidler": "仲介業者",
    "Udenlandsk dækning": "海外保証",
}

# 区分バッジの CSS クラス修飾子 → 事業者の所在区分 (作業用の短いラベル)
#   dk         … デンマーク国内に設立され、本基金の保証対象
#   eu         … EU/EEA 域内の他国の旅行保証制度に加入 (デンマークの保証対象外)
#   outside_eu … EU 域外に設立 (保証条件が異なる)
#   (修飾子なし) … 仲介業者 (Formidler)
_ORIGIN_MAP = {
    "dk": "デンマーク国内",
    "eu": "EU域内(他国保証)",
    "outside_eu": "EU域外",
}

# ページ送りの暴走防止 (2026-09 時点で 87 ページ)
_MAX_PAGES = 300


class Streamreq18754Rejsegarantifonden(StaticCrawler):
    """Rejsegarantifonden 会員名簿 スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "区分(作業用)",
        "設立地区分",
        "都市",
        "国",
        "会員番号",
        "保証国",
        "副称",
        "仲介業者",
        "仲介先会員",
    ]

    def parse(self, url: str):
        page_url = url
        seen_pages: set[str] = set()

        for page_no in range(1, _MAX_PAGES + 1):
            if page_url in seen_pages:
                break
            seen_pages.add(page_url)

            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("ページ取得に失敗しました: %s", page_url)
                break

            if page_no == 1:
                self.total_items = self._estimate_total(soup)

            articles = soup.select("article.rgf_member")
            if not articles:
                logger.info("記事が 0 件のため終了します: %s", page_url)
                break

            for article in articles:
                item = self._build_item(article, page_url)
                if item:
                    yield item

            next_url = self._next_page_url(soup, url)
            if not next_url:
                break
            page_url = next_url

    # ------------------------------------------------------------------ 内部処理

    def _estimate_total(self, soup: bs4.BeautifulSoup) -> int | None:
        """ページャの最終ページ番号から概算の総件数を求める (ETA 表示用)。"""
        last_page = 0
        for a in soup.select("nav.pagination a.page-numbers"):
            m = _PAGE_NUM_PATTERN.search(a.get("href") or "")
            if m:
                last_page = max(last_page, int(m.group(1)))
        per_page = len(soup.select("article.rgf_member"))
        if last_page and per_page:
            return last_page * per_page
        return per_page or None

    def _next_page_url(self, soup: bs4.BeautifulSoup, root_url: str) -> str | None:
        """ページャの「次へ」リンク (root_url 基準の絶対 URL) を返す。無ければ None。"""
        next_a = soup.select_one("nav.pagination a.next.page-numbers")
        if next_a and next_a.get("href"):
            return urljoin(root_url, next_a["href"])
        return None

    def _build_item(self, article: bs4.Tag, page_url: str) -> dict | None:
        name_el = article.select_one("h2.entry-title")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        fields = self._parse_table(article)

        kind_el = article.select_one(".register-div h3")
        kind_raw = kind_el.get_text(strip=True) if kind_el else ""
        kind = _KIND_MAP.get(kind_raw, kind_raw)

        # バッジ (.register-div-inner-*) の修飾クラスに事業者の所在区分が入る。
        # 「Medlem」表記でも EU 域外設立のものがあり、区分だけでは判別できないため
        # 後段クレンジング用に別カラムとして保持する。
        origin = ""
        badge = article.select_one(".register-div div[class*=register-div-inner-]")
        if badge:
            for cls in badge.get("class") or []:
                if cls in _ORIGIN_MAP:
                    origin = _ORIGIN_MAP[cls]
                    break

        return {
            Schema.NAME: name,
            # 住所 / TEL はサイトに掲載が無いため通常は空文字になる
            Schema.ADDR: fields.get("Adresse", ""),
            Schema.TEL: self._normalize_tel(fields.get("Telefon", "")),
            Schema.CO_NUM: fields.get("CVR nr.", ""),
            Schema.HP: self._normalize_hp(article),
            Schema.URL: page_url,
            "区分(作業用)": kind,
            "設立地区分": origin,
            "都市": fields.get("By", ""),
            "国": "デンマーク",
            "会員番号": fields.get("Nr.", ""),
            "保証国": fields.get("Garantiland", ""),
            "副称": fields.get("Binavne", ""),
            "仲介業者": fields.get("Formidlere", ""),
            "仲介先会員": fields.get("Formidler for", ""),
        }

    def _parse_table(self, article: bs4.Tag) -> dict[str, str]:
        """article 内の th/td テーブルを {ラベル: 値} に変換する。

        値が ul > li のリスト (Binavne 等) の場合は " / " 区切りで連結する。
        """
        fields: dict[str, str] = {}
        for tr in article.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).rstrip(":").strip()
            lis = td.find_all("li")
            if lis:
                value = " / ".join(li.get_text(strip=True) for li in lis if li.get_text(strip=True))
            else:
                value = td.get_text(strip=True)
            if label:
                fields[label] = value
        return fields

    def _normalize_hp(self, article: bs4.Tag) -> str:
        """Hjemmeside 行の URL を絶対 URL で返す。"""
        for tr in article.select("table tr"):
            th = tr.find("th")
            if not th or th.get_text(strip=True).rstrip(":").strip() != "Hjemmeside":
                continue
            td = tr.find("td")
            if not td:
                return ""
            a = td.find("a", href=True)
            raw = (a["href"] if a else td.get_text(strip=True)).strip()
            if not raw:
                return ""
            if not raw.startswith(("http://", "https://")):
                raw = "https://" + raw.lstrip("/")
            return raw
        return ""

    def _normalize_tel(self, tel: str) -> str:
        """デンマークの電話番号を +45 を含む国際表記に正規化する。"""
        if not tel:
            return ""
        digits = _TEL_CLEAN_PATTERN.sub("", tel)
        if not digits:
            return ""
        if digits.startswith("+"):
            return digits
        if digits.startswith("0045"):
            return "+" + digits[2:]
        if digits.startswith("45") and len(digits) == 10:
            return "+" + digits
        return "+45" + digits


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Streamreq18754Rejsegarantifonden()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.rejsegarantifonden.dk/medlem/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

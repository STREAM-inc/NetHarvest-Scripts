"""
（一社）日本ロボット工業会（JARA） — 会員名簿（公開）スクレイパー

取得対象:
    - 会員社名 (NAME) / 会員企業URL (HP) / 会員区分 (EXTRA: 正会員 / 賛助会員（法人）)
    - 所在地 … 会員企業の本社ページ (= HP) にアクセスして住所を抽出
      (PREF=都道府県 / POST_CODE=郵便番号 / ADDR=住所)

取得フロー:
    引数 url (= https://www.jara.jp/) を唯一の起点とし、公開されている 2 つの
    会員名簿ページを urljoin で派生させる:
        - about/member_regular.html … 正会員 名簿
        - about/member_support.html … 賛助会員（法人）名簿
    ※ /member/index.html 配下は会員専用 (401) だが、上記 2 ページは about 配下の
      公開名簿。各ページの本文は五十音見出し (h2) ごとに
      `dl.list-links-first > dd > a` で 1 社 1 リンクが並ぶ。
      リンクテキスト=会員社名、href=会員企業の公式サイト (本社ページ) URL。
      リンクを持たない dd (URL 未登録の会員) は社名のみ取得する。

    所在地 (住所) は JARA の公開名簿に掲載が無いため、会員企業の本社ページ (HP) に
    アクセスして抽出する。まず HP トップを取得し、郵便番号+都道府県で始まる住所表記を
    探す。トップに無ければ「会社概要 / 企業情報 / 会社案内 / アクセス」等の
    プロフィール系リンクを数件だけ辿って抽出する (1 社あたりの外部アクセスは時間・
    件数の両面で上限を設けて過負荷を防ぐ)。住所が取れなくても社名・URL・区分は必ず
    yield する (best-effort)。

    各 dd を 1 件取得するたびに即 yield する (取得即 yield なので途中 break しても
    無駄な通信が起きず、テスト実行が早期に最初の 1 件を返せる)。

    サイトポリシー上「無許可の複製・転載・第三者提供」を禁じる著作権条項はあるが、
    スクレイピング/自動アクセスを明示的に禁止する記載は無く、robots.txt も未設置。
    取得値は社名・公開 URL・住所の事実情報のみで自由記述プロースは含めない。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jara.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jara
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 会員区分ごとの公開名簿ページ (相対パス, url からの派生用) と区分ラベル
_MEMBER_PAGES = [
    ("about/member_regular.html", "正会員"),
    ("about/member_support.html", "賛助会員（法人）"),
]

_MEMBER_TYPE = "会員区分"

# ---- 住所抽出用 定数 --------------------------------------------------------
_PREF = (
    r"(?:北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 郵便番号 + 都道府県始まりの連続した住所表記 (最も信頼できる)
_ADDR_POSTAL = re.compile(
    r"〒?\s*(\d{3})[-‐‑–—ー－]?\s*(\d{4})\s*(" + _PREF + r"[^\s　]{1,40})"
)
# 郵便番号が無い場合のフォールバック: 都道府県〜市区郡 + 番地数字を含む連続表記
_ADDR_PREF = re.compile(
    r"(" + _PREF + r"[^\s　、。]{0,20}?[市区郡][^\s　、。]{0,25}?\d[^\s　、。]{0,12})"
)
_PREF_HEAD = re.compile(r"^(" + _PREF + r")")

# 本社ページ内で辿るプロフィール系リンクの手掛かり
_PROFILE_TEXT_KW = (
    "会社概要", "企業情報", "会社案内", "会社情報", "会社紹介",
    "アクセス", "所在地", "概要", "企業概要", "会社データ",
)
_PROFILE_HREF_KW = (
    "company", "about", "profile", "corporate", "outline",
    "access", "overview", "gaiyo", "kaisya", "info",
)

# 1 社あたりの外部アクセス制限 (過負荷・タイムアウト防止)
_EXT_TIMEOUT = 10          # 本社ページ 1 リクエストのタイムアウト(秒)
_EXT_PROFILE_LIMIT = 3     # トップに無いとき辿るプロフィール候補の上限
_EXT_TIME_BUDGET = 12.0    # 1 社の住所探索にかける最大秒数


class Jara(StaticCrawler):
    """（一社）日本ロボット工業会 会員名簿 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_MEMBER_TYPE]  # 構造化された短ラベル (正会員 / 賛助会員（法人）)

    def parse(self, url: str) -> Generator[dict, None, None]:
        total = 0
        for rel_path, member_type in _MEMBER_PAGES:
            page_url = urljoin(url, rel_path)
            try:
                soup = self.get_soup(page_url)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("会員名簿ページ取得失敗 %s: %s", page_url, exc)
                continue
            if soup is None:
                continue

            entries = soup.select("dl.list-links-first dd")
            total += len(entries)
            self.total_items = total

            for dd in entries:
                try:
                    a = dd.find("a", href=True)
                    name = (a.get_text(strip=True) if a else dd.get_text(strip=True))
                    if not name:
                        continue
                    hp = a["href"].strip() if a else ""

                    item = {
                        Schema.NAME: name,
                        Schema.HP: hp,
                        Schema.URL: page_url,
                        _MEMBER_TYPE: member_type,
                    }
                    # 本社ページ (HP) にアクセスして所在地を抽出 (best-effort)
                    if hp:
                        addr = self._fetch_address(hp)
                        if addr:
                            pref, post, full = addr
                            item[Schema.ADDR] = full
                            if pref:
                                item[Schema.PREF] = pref
                            if post:
                                item[Schema.POST_CODE] = post
                    yield item
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("会員エントリ解析失敗 (%s): %s", member_type, exc)
                    continue

    # ------------------------------------------------------------------ #
    # 本社ページからの住所抽出
    # ------------------------------------------------------------------ #
    def _fetch_address(self, hp_url: str):
        """会員企業の本社ページ (HP) から所在地を抽出する。

        トップページ → プロフィール系リンクの順に探索し、最初に見つかった
        (都道府県, 郵便番号, 住所全文) を返す。見つからなければ None。
        外部サイトは構造がバラバラなので取得失敗・住所不在は許容する。
        """
        deadline = time.monotonic() + _EXT_TIME_BUDGET
        try:
            soup = self._fetch_external(hp_url)
        except Exception:  # noqa: BLE001
            return None
        if soup is None:
            return None

        # 1) トップページ本文から抽出
        found = self._extract_address(soup.get_text(" ", strip=True))
        if found:
            return found

        # 2) プロフィール系リンクを数件だけ辿る
        for link in self._profile_links(soup, hp_url)[:_EXT_PROFILE_LIMIT]:
            if time.monotonic() >= deadline:
                break
            try:
                sub = self._fetch_external(link)
            except Exception:  # noqa: BLE001
                continue
            if sub is None:
                continue
            found = self._extract_address(sub.get_text(" ", strip=True))
            if found:
                return found
        return None

    def _fetch_external(self, url: str):
        """外部サイトを短いタイムアウトで取得し soup を返す (失敗時 None)。

        get_soup() はエラー伝播やキャッシュ/ビーコン記録の対象になるため、
        第三者サイトへの補助アクセスは self.session を直接使う軽量取得にする。
        """
        if not url.lower().startswith(("http://", "https://")):
            return None
        try:
            resp = self.session.get(url, timeout=_EXT_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
        except Exception:  # noqa: BLE001
            return None
        ctype = resp.headers.get("Content-Type", "")
        if ctype and "html" not in ctype.lower():
            return None
        if "charset=" not in ctype.lower():
            resp.encoding = resp.apparent_encoding
        return bs4.BeautifulSoup(resp.text, "html.parser")

    def _profile_links(self, soup: bs4.BeautifulSoup, base_url: str):
        """会社概要/企業情報 等のプロフィール系リンク URL を優先順に列挙する。"""
        seen = set()
        ordered = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            text = a.get_text(strip=True)
            hl = href.lower()
            hit = any(k in text for k in _PROFILE_TEXT_KW) or any(
                k in hl for k in _PROFILE_HREF_KW
            )
            if not hit:
                continue
            full = urljoin(base_url, href)
            if full in seen or full == base_url:
                continue
            seen.add(full)
            ordered.append(full)
        return ordered

    @staticmethod
    def _extract_address(text: str):
        """空白連結済みの本文から (都道府県, 郵便番号, 住所全文) を抽出する。"""
        m = _ADDR_POSTAL.search(text)
        if m:
            post = f"{m.group(1)}-{m.group(2)}"
            full = m.group(3).strip("　 、。・")
            pref_m = _PREF_HEAD.match(full)
            pref = pref_m.group(1) if pref_m else ""
            return (pref, post, full)

        m = _ADDR_PREF.search(text)
        if m:
            full = m.group(1).strip("　 、。・")
            pref_m = _PREF_HEAD.match(full)
            pref = pref_m.group(1) if pref_m else ""
            return (pref, "", full)
        return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Jara()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jara.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

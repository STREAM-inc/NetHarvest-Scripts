"""
【STREAMREQ-18920】ITTAA イスラエル旅行業協会 会員一覧（イスラエル）
 — streamreq_18920ittaa

取得対象:
    ITTAA (התאחדות משרדי הנסיעות ויועצי התיירות בישראל /
    イスラエル旅行業協会) の「חברי ההתאחדות (会員一覧)」ページに掲載されている
    全会員行 (2026-09-30 実測 130 行 / URL ドメインで数えると 64 社)。

サイト構造 (Phase 1 調査結果 / 2026-09-30 実測):
    - 起点 URL 1 ページで完結。ページネーション・内部 API・遅延読み込みは一切無い
      (画面の「130 משרדים」と HTML 内の <tr> 130 行が一致)。
    - HTML は WordPress が返す静的 HTML。Content-Type に charset=UTF-8 があるため
      ヘブライ語は文字化けしない。Playwright は不要 → StaticCrawler。
    - 会員 1 件ごとの詳細ページは存在しない (行内の <a> はいずれも会員の自社 HP への
      外部リンク)。→ Schema.URL には起点 URL (= sites.yml の url) を全行に入れる。
    - テーブルは 1 つだけ。thead は #/לוגו/שם משרד הנסיעות/טלפון の 4 列で、
      tbody の各 <tr> も 4 つの <td> を持つ (130 行すべて 4 列で例外なし)。
        td[0] : 掲載番号 (1〜130)
        td[1] : ロゴ (会員 HP への <a> + favicon 画像。名称列と同じリンク先)
        td[2] : 会員名。HP がある行は <a>会員名</a><br><span>ドメイン</span>、
                無い行は <span>会員名</span> のみ
        td[3] : 電話。<a href="tel:...">☎ 03-5260900</a>、無い行は <span>&mdash;</span>
    - 充足率 (全 130 行): 会員名 130/130 (100%)、HP 86/130 (66%)、
      電話 48/130 (37%、うち 5 件は *XXXX 形式の短縮番号)。住所は掲載されていない
      (依頼工程⑬で会員の公式 HP から補完する前提)。
    - HP リンクは全て https。1 件だけ広告パラメータ付き
      (https://www.skygini.com/?utm_source=...&gclid=...) → 除去する。
    - <span> のドメイン表記と <a href> のホストは 86 行すべて一致 (不整合なし)。

依頼 (備考) の反映:
    - 国カラムは「イスラエル」で固定。
    - 会員名はヘブライ語表記のまま保持する (UTF-8。英語社名は工程⑬で HP から補完)。
    - TEL は +972 を含む国際表記に正規化する。ただし Schema.TEL は共通正規化で
      数字とハイフン以外が落ちる (src/utils/normalizer.py::_normalize_tel) ため、
        Schema.TEL   : 国内表記 (例 03-5260900 / 054-4292745)
        Schema.PHONE : 国際表記 (例 +972-3-5260900 / +972-54-4292745)
      の 2 本立てにする。原文は EXTRA「電話番号(原文)」に必ず残す。
        * 1-700 / 1-800 / 1-599 等のイスラエル国内サービス番号は国際発信できないため
          +972 を付けず国内表記のまま (実測 2 件: 1-700-700-078 / 1700-705-770)。
        * *XXXX 形式の短縮番号 (実測 5 件) も国際発信できない。そのまま Schema.TEL に
          入れると共通正規化で "*" が落ちて別番号 (例 *2250 → 2250) に化けるため、
          Schema.TEL / Schema.PHONE は空にし、EXTRA「電話番号(原文)」にのみ残す
          (依頼どおり工程⑬で HP の通常番号に置き換える)。
    - 同一社の支店行 (例 Ophir Tours 8 行、ISSTA 5 行、Lachish Tours 5 行) の統合は
      依頼工程⑪で行うため、parse() では統合せず 130 行すべてを出力する。
      統合のキーとして EXTRA「ドメイン」(www. を除いたホスト) を付与する。

取得しないフィールド (除外):
    - ロゴ画像 URL: unavatar.io / Google favicon サービスの生成 URL であり
      会員自身のデータではないため。
    - ページ本文の案内文・注意書き: 自由記述プロースのため著作権リスクで除外。
    - 住所・メールアドレス: 一覧に掲載が無い (工程⑬で HP から補完する前提)。

名寄せ:
    海外所在の事業者のため STX 名寄せは対象外 (依頼指示)。

利用規約 / robots.txt (2026-09-30 確認):
    - https://ittaa.org.il/robots.txt : "User-agent: * / Disallow:" (全ページ許可)。
    - 規約 https://ittaa.org.il/%d7%aa%d7%a7%d7%a0%d7%95%d7%9f-%d7%90%d7%aa%d7%a8/ :
      「קניין רוחני (知的財産)」節に一般的な複製・頒布の禁止条項があるのみで、
      スクレイピング / クローリング / 自動取得を禁じる文言は無い
      (scrap / crawl / robot / רובוט / סריקה / אוטומט / כרייה のいずれも本文に無し)。
    → 収集継続可能と判断。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18920ittaa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18920ittaa
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, urlsplit, urlunsplit, urlencode

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 広告・計測用のクエリパラメータ (会員 HP の URL から除去する)
_TRACKING_PARAM_PATTERN = re.compile(
    r"^(?:utm_[a-z_]+|gclid|gclsrc|gbraid|wbraid|gad_source|gad_campaignid|dclid"
    r"|fbclid|msclkid|yclid|ttclid|twclid|igshid|mc_cid|mc_eid|_ga|_gl|ref|ref_src)$",
    re.IGNORECASE,
)

# 電話リンクの href (tel:035260900 / tel:*2250)
_TEL_HREF_PATTERN = re.compile(r"^tel:", re.IGNORECASE)

# *XXXX 形式の短縮番号 (国際発信不可)
_SHORT_CODE_PATTERN = re.compile(r"^\*\d+$")

# イスラエル国内サービス番号 (1-700 / 1-800 / 1-599 等。国際発信不可)
_SERVICE_NUMBER_PATTERN = re.compile(r"^1(?:700|800|599|801|802|809)")

# 携帯・VoIP の 3 桁プレフィックス (0 を除いた先頭 2 桁)
_TWO_DIGIT_AREA_PATTERN = re.compile(r"^(?:5\d|7[2-9])")

# 会員名として無効なプレースホルダ
_PLACEHOLDER_NAMES = {"", "-", "—", "–", "&mdash;"}


class StreamReq18920Ittaa(StaticCrawler):
    """ITTAA (イスラエル旅行業協会) 会員一覧のスクレイパー"""

    DELAY = 1.0
    # 1 ページ取得で全件が揃うため、アイテムごとの待機は不要
    ITEM_DELAY = 0
    TIMEOUT = 60

    COUNTRY = "イスラエル"

    EXTRA_COLUMNS = [
        "国",
        "掲載番号",
        "ドメイン",
        "電話番号(原文)",
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """起点 URL (= sites.yml の url) の会員一覧テーブルを 1 行ずつ yield する。

        ページネーションは存在しないため、取得は起点 URL 1 回のみ。
        行をパースした直後にその場で yield する (全件バッファはしない)。
        """
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("会員一覧ページを取得できませんでした: %s", url)
            return

        rows = self._select_member_rows(soup)
        if not rows:
            self.logger.error("会員一覧テーブルの行が見つかりませんでした: %s", url)
            return

        self.total_items = len(rows)
        self.logger.info("会員一覧: %d 行", len(rows))

        count = 0
        skipped = 0
        for row in rows:
            try:
                item = self._build_item(row, url)
            except Exception as exc:  # 1 行の失敗で全体を止めない
                self.logger.warning("行のパースに失敗しました (スキップ): %s", exc)
                skipped += 1
                continue
            if item is None:
                skipped += 1
                continue
            count += 1
            yield item

        self.logger.info("取得件数: %d 件 (スキップ %d 件)", count, skipped)

    # ------------------------------------------------------------------ #
    # テーブルの特定
    # ------------------------------------------------------------------ #
    def _select_member_rows(self, soup) -> list:
        """会員一覧テーブルの <tr> を返す。

        ページ内のテーブルのうち「4 列構成で最も行数が多いもの」を会員一覧とみなす
        (将来テーブルが増えても誤検出しないようにするため)。
        """
        best: list = []
        for table in soup.find_all("table"):
            rows = [
                tr for tr in table.select("tbody tr") if len(tr.find_all("td")) >= 4
            ]
            if len(rows) > len(best):
                best = rows
        return best

    # ------------------------------------------------------------------ #
    # 1 行のパース
    # ------------------------------------------------------------------ #
    def _build_item(self, row, url: str) -> dict | None:
        cells = row.find_all("td")
        if len(cells) < 4:
            return None

        no_cell, _logo_cell, name_cell, tel_cell = cells[0], cells[1], cells[2], cells[3]

        name = self._extract_name(name_cell)
        if name in _PLACEHOLDER_NAMES:
            return None

        hp = self._extract_hp(name_cell)
        domain = self._extract_domain(hp)
        tel_raw = self._extract_tel_raw(tel_cell)
        tel_national, tel_international = self._normalize_tel(tel_raw)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            # Schema.TEL は共通正規化で数字とハイフン以外が落ちるため国内表記を入れ、
            # "+972-" 付きの国際表記は Schema.PHONE 側に保持する
            Schema.TEL: tel_national,
            Schema.PHONE: tel_international,
            Schema.HP: hp,
            "国": self.COUNTRY,
            "掲載番号": self._clean(no_cell.get_text()),
            "ドメイン": domain,
            "電話番号(原文)": tel_raw,
        }

    @classmethod
    def _extract_name(cls, cell) -> str:
        """会員名を取り出す。

        HP がある行は <a>会員名</a><br><span>ドメイン</span> なので <a> のテキスト、
        無い行は <span>会員名</span> のテキストを使う (セル全体だとドメインが混ざる)。
        """
        link = cell.find("a")
        if link is not None:
            name = cls._clean(link.get_text(" "))
            if name:
                return name
        span = cell.find("span")
        if span is not None:
            name = cls._clean(span.get_text(" "))
            if name:
                return name
        return cls._clean(cell.get_text(" "))

    @classmethod
    def _extract_hp(cls, cell) -> str:
        """会員の HP URL を取り出し、広告・計測パラメータを除去する。"""
        link = cell.find("a", href=True)
        if link is None:
            return ""
        return cls._strip_tracking_params(link["href"])

    @staticmethod
    def _strip_tracking_params(href: str) -> str:
        """utm_* / gclid 等の広告パラメータを URL から取り除く。"""
        href = (href or "").strip()
        if not href.lower().startswith(("http://", "https://")):
            return ""
        parts = urlsplit(href)
        kept = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if not _TRACKING_PARAM_PATTERN.match(k)
        ]
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(kept), "")
        )

    @staticmethod
    def _extract_domain(hp: str) -> str:
        """HP URL のホスト (www. を除く) を返す。工程⑪の支店統合キーに使う。"""
        if not hp:
            return ""
        host = urlsplit(hp).netloc.lower()
        return host[4:] if host.startswith("www.") else host

    @classmethod
    def _extract_tel_raw(cls, cell) -> str:
        """電話セルから原文の番号を取り出す (☎ 記号や "—" は除く)。"""
        link = cell.find("a", href=_TEL_HREF_PATTERN)
        if link is None:
            return ""
        text = cls._clean(link.get_text(" "))
        # 先頭の受話器記号 (☎) や装飾文字を落とす
        text = re.sub(r"^[^\d*+]+", "", text)
        if text:
            return text
        # 表示テキストが空の場合は href 側 (tel:035260900) を使う
        return cls._clean(link["href"].split(":", 1)[1])

    # ------------------------------------------------------------------ #
    # 値の整形
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(value) -> str:
        """None / 余分な空白 (NBSP・ゼロ幅スペース含む) を落として文字列にする。"""
        if value is None:
            return ""
        text = str(value).replace("\xa0", " ").replace("​", "")
        # 書字方向制御文字 (RTL サイトで混入する) を除去
        text = re.sub(r"[‎‏‪-‮⁦-⁩]", "", text)
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def _normalize_tel(cls, raw: str) -> tuple[str, str]:
        """イスラエルの電話番号を (国内表記, 国際表記) に正規化する。

            "03-5260900"      → ("03-5260900",  "+972-3-5260900")
            "054-4292745"     → ("054-4292745", "+972-54-4292745")
            "1-700-700-078"   → ("1700-700-078", "")  国内サービス番号 (国際発信不可)
            "*2250"           → ("", "")              短縮番号 (工程⑬で HP の番号に置換)
        """
        text = cls._clean(raw)
        if not text:
            return "", ""

        # *XXXX 形式の短縮番号: Schema.TEL に入れると "*" が落ちて別番号に化けるため空にする
        if _SHORT_CODE_PATTERN.match(text.replace("-", "").replace(" ", "")):
            return "", ""

        digits = re.sub(r"\D", "", text)
        if not digits:
            return "", ""

        # 国番号付き表記 (00972... / 972...) は国番号を落として国内表記に戻す
        if digits.startswith("00972"):
            digits = "0" + digits[5:]
        elif digits.startswith("972") and len(digits) >= 11:
            digits = "0" + digits[3:]

        # 国内サービス番号 (1-700 / 1-800 等) は国際発信できないため国内表記のみ
        if _SERVICE_NUMBER_PATTERN.match(digits):
            return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}" if len(digits) == 10 else digits, ""

        subscriber = digits.lstrip("0")
        if not subscriber:
            return "", ""

        # 市外局番の桁数: 携帯/VoIP (05x・07x) は 2 桁、地域番号 (2/3/4/8/9) は 1 桁
        area_len = 2 if _TWO_DIGIT_AREA_PATTERN.match(subscriber) else 1
        area, rest = subscriber[:area_len], subscriber[area_len:]
        if not rest:
            return digits, ""

        national = f"0{area}-{rest}"
        international = f"+972-{area}-{rest}"
        return national, international


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = StreamReq18920Ittaa()
    scraper.site_id = "streamreq_18920ittaa"
    scraper.site_name = "【STREAMREQ-18920】ITTAA イスラエル旅行業協会 会員一覧（イスラエル）"
    scraper.execute(
        "https://ittaa.org.il/%d7%97%d7%91%d7%a8%d7%99-%d7%94%d7%94%d7%aa%d7%90%d7%97%d7%93%d7%95%d7%aa/"
    )

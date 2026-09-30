"""
【STREAMREQ-18914】JNTOインド公式サイト Tour Operators in India（インド）
 — streamreq_18914jnto_tour_operators_in_india

取得対象:
    日本政府観光局 (JNTO) インド向け公式サイトの "Tour Operators in India"
    (https://www.japan.travel/en/in/travel-agency/) に掲載された、
    日本旅行を取り扱うインドの旅行会社一覧 (2026-09 時点 33 行 / 32 社)。

サイト構造 (Phase 1 調査結果):
    - 1 ページ完結。ページネーション・詳細ページは無い。
    - テーブルは <table id="data-table"> (DataTables) が 1 個だけ。JS は
      並べ替え/絞り込みのみで追加データ取得はしない → StaticCrawler で全件取得可。
      (フレームワーク既定の Chrome UA でも 302 リダイレクト無しで 200 / 33 行取得できる
       ことを確認済み。robots.txt だけは言語判定で /pt/ に飛ぶが本ページは影響なし)
    - 行の種類は 3 つ:
        * <th> 3 個 … テーブル見出し (Tour Operators / Contact Information / Tour Language)
        * <th colspan=3> 1 個 … 地域見出し行
          (PAN India / Delhi / Mumbai / Ahmedabad / Bengaluru / Chennai /
           Coimbatore / Kolkata / Women Only Travel Agency)
        * <td> 3 個 … 1 社のデータ行
              td.sorting_1 … 会社名
              td (2 列目)  … 連絡先。<br> 区切りで
                             「Phone: …」or「Toll Free: …」/「Email: <a>…</a>」/
                             日本ツアーページの <a href>
              td (3 列目)  … Tour Language (全行 "English")
    - 住所の掲載は無い (Schema.ADDR は空。依頼工程⑬で公式 HP から補完される前提)。

データ上の注意点 (実データで確認済み):
    - メールの <a href> に mailto: が付いていない (href="india@mercurytravels.in")。
      さらに href とテキストが食い違う行がある
      (Footprint Holidays: href=sales@charutravels.in / 表示=info@footprintholidays.com)
      → 依頼の指示どおり **テキスト側** を採用する。
    - 日本ツアーページの href も壊れている行がある
      (Amatra: href="https://https://amatratravel.com/services/japan/")
      → リンクテキスト側を優先し、テキストが URL に見えない場合のみ href を使う。
    - Club7 Holidays Ltd. が Ahmedabad と Kolkata の 2 か所に重複掲載 (33 行 = 32 社)。
      → 会社名で名寄せし、都市は「Ahmedabad / Kolkata」のように連結して 1 件にする。
    - 電話が無い行が 2 行ある (Amatra / PickYourTrail)。メールが無い行も 2 行ある
      (Thomas Cook India / Abercrombie & Kent / PickYourTrail)。
      出現率が低いフィールドも実装し、無い場合は空文字を入れる。

依頼 (備考) の反映:
    - 国カラムは「インド」で固定。
    - 都市は地域見出し。ただし "PAN India" と "Women Only Travel Agency" は
      都市ではないため「都市」は空欄にし、見出し原文は EXTRA「掲載区分」に残す。
    - 納品用 URL (Schema.HP) は日本ツアーページのドメイントップ
      (例 https://www.sotc.in/)。日本ツアーページ URL 自体は EXTRA と備考に残す。
    - 取得元 URL (Schema.URL) を必ず保持する。JNTO の当該一覧に掲載されていること
      自体が日本旅行取扱の根拠になるため、EXTRA「日本取扱」は "該当" 固定。
    - TEL は +91 を含む国際表記に正規化する (先頭 0 の市外局番は除去)。
      "Toll Free: 1800 …" はインド国内フリーダイヤルのため +91 を付けずに
      そのまま残し、EXTRA「電話番号種別」で区別する。原文は
      EXTRA「電話番号(原文)」に保持する。

取得しないフィールド (除外):
    - 無し (このページは構造化された短い項目のみで、長文の自由記述カラムは存在しない)。

名寄せ:
    海外所在の事業者のため STX 名寄せは対象外 (依頼指示)。

利用規約 / robots.txt (2026-09-30 確認):
    - https://www.japan.travel/robots.txt : "Allow: /"、Disallow は /admin/ ・
      /*/travel-directory/* ・ /jp/ のみ。本ページ /en/in/travel-agency/ は対象外。
    - https://www.japan.travel/en/terms-of-use/ : scraping / crawling / robot /
      spider / data mining 等の禁止文言は無し (著作権条項とリンク条件のみ)。
    → 収集継続可能と判断。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18914jnto_tour_operators_in_india.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py \
        --site-id streamreq_18914jnto_tour_operators_in_india
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 地域見出しのうち「都市」ではないもの (依頼指示により「都市」は空欄にする)
_NON_CITY_HEADINGS = {"pan india", "women only travel agency"}

# 連絡先セル内の電話ラベル
_TEL_LABEL_PATTERN = re.compile(r"^\s*(Toll\s*Free|Phone|Tel)\s*[:：]\s*(.+)$", re.IGNORECASE)
# 連絡先セル内のメールラベル
_EMAIL_LABEL_PATTERN = re.compile(r"^\s*Email\s*[:：]\s*", re.IGNORECASE)
# メールアドレスらしき文字列
_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# URL らしき文字列 (スキーム有無どちらも)
_URL_LIKE_PATTERN = re.compile(r"^(?:https?://|www\.)", re.IGNORECASE)
# インド国内フリーダイヤル (1800 / 1860 など 18xx・186x で始まる番号)
_TOLL_FREE_PATTERN = re.compile(r"^(?:1800|1860|1861)")


class StreamReq18914JntoTourOperatorsInIndia(StaticCrawler):
    """JNTO インド版 Tour Operators in India の旅行会社一覧スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "国",
        "都市",
        "掲載区分",
        "日本取扱",
        "日本ツアーページURL",
        "電話番号種別",
        "電話番号(原文)",
        "対応言語",
        "備考",
    ]

    COUNTRY = "インド"

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """ルート URL (= sites.yml の url) の一覧テーブルを 1 行ずつ処理する。

        ページネーション・詳細ページは無く、この 1 ページで完結する。
        追加の通信は一切発生しないため、解析できた行はその場で即 yield する。
        """
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("一覧ページを取得できませんでした: %s", url)
            return

        table = soup.select_one("table#data-table") or soup.select_one("table")
        if table is None:
            self.logger.warning("一覧テーブルが見つかりません: %s", url)
            return

        # DataTables は最初の地域見出し行 (PAN India) を <thead> 側に置き、
        # 残りの地域見出し行は <tbody> に入れる。tbody だけを見ると先頭の地域見出しを
        # 取りこぼすため、テーブル全体の <tr> を文書順で走査する。
        rows = table.find_all("tr")
        self.logger.info("一覧 %d 行 (見出し行含む) を検出: %s", len(rows), url)

        # Club7 Holidays Ltd. のように複数の地域見出しに重複掲載される会社があるため、
        # 先に「会社名 → 掲載都市/区分」の対応表を作ってから 1 社 1 行で出力する。
        # (同一 HTML 内の走査のみで追加通信は無いので待ち時間は発生しない)
        headings_by_name = self._collect_headings(rows)

        seen: set[str] = set()
        count = 0
        for tr in rows:
            item = self._parse_row(tr, url, headings_by_name, seen)
            if item:
                count += 1
                yield item

        self.logger.info("取得件数: %d 件 (重複掲載は名寄せ済み)", count)

    # ------------------------------------------------------------------ #
    # 重複掲載のための事前走査
    # ------------------------------------------------------------------ #
    @classmethod
    def _collect_headings(cls, rows: list) -> dict[str, list[str]]:
        """会社名 (正規化済み) ごとに、掲載されている地域見出しを出現順に集める。"""
        result: dict[str, list[str]] = {}
        heading = ""
        for tr in rows:
            new_heading = cls._heading_of(tr)
            if new_heading is not None:
                heading = new_heading
                continue
            name = cls._name_of(tr)
            if not name:
                continue
            key = cls._key(name)
            bucket = result.setdefault(key, [])
            if heading and heading not in bucket:
                bucket.append(heading)
        return result

    # ------------------------------------------------------------------ #
    # 1 行のパース
    # ------------------------------------------------------------------ #
    def _parse_row(
        self,
        tr: bs4.Tag,
        url: str,
        headings_by_name: dict[str, list[str]],
        seen: set[str],
    ) -> dict | None:
        """データ行を辞書化する。見出し行・2 回目以降の重複掲載は None を返す。"""
        if self._heading_of(tr) is not None:
            return None

        name = self._name_of(tr)
        if not name:
            return None

        key = self._key(name)
        if key in seen:
            # 同一社が別の地域見出しにも掲載されている 2 行目以降
            # (都市は 1 行目にまとめて出力済み)
            self.logger.info("重複掲載のためスキップ: %s", name)
            return None
        seen.add(key)

        tds = tr.find_all("td", recursive=False)
        contact = tds[1] if len(tds) > 1 else None
        language = self._clean(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else ""

        tel_raw, tel_kind = self._extract_tel(contact)
        email = self._extract_email(contact)
        tour_url = self._extract_tour_url(contact, url)

        headings = headings_by_name.get(key, [])
        cities = [h for h in headings if h.lower() not in _NON_CITY_HEADINGS]

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.ADDR: "",  # 当ページに住所の掲載は無い (工程⑬で公式 HP から補完)
            Schema.TEL: self._normalize_tel(tel_raw),
            Schema.EMAIL: email,
            Schema.HP: self._site_top(tour_url),
            "国": self.COUNTRY,
            "都市": " / ".join(cities),
            "掲載区分": " / ".join(headings),
            "日本取扱": "該当",
            "日本ツアーページURL": tour_url,
            "電話番号種別": tel_kind,
            "電話番号(原文)": tel_raw,
            "対応言語": language,
            "備考": self._build_note(tour_url, headings),
        }

    # ------------------------------------------------------------------ #
    # 行種別の判定
    # ------------------------------------------------------------------ #
    @classmethod
    def _heading_of(cls, tr: bs4.Tag) -> str | None:
        """地域見出し行なら見出し文字列を返す。データ行なら None。

        テーブル見出し行 (<th> が 3 個) も見出し扱いだが、地域見出しではないので
        空文字を返して「見出し行だが地域は更新しない」ことを表す。
        """
        ths = tr.find_all("th", recursive=False)
        if not ths:
            return None
        if len(ths) == 1:
            return cls._clean(ths[0].get_text(" ", strip=True))
        return ""

    @classmethod
    def _name_of(cls, tr: bs4.Tag) -> str:
        """会社名セル (td.sorting_1、無ければ 1 列目の td) のテキストを返す。"""
        td = tr.select_one("td.sorting_1")
        if td is None:
            tds = tr.find_all("td", recursive=False)
            if not tds:
                return ""
            td = tds[0]
        return cls._clean(td.get_text(" ", strip=True))

    # ------------------------------------------------------------------ #
    # 連絡先セルの抽出
    # ------------------------------------------------------------------ #
    @classmethod
    def _contact_lines(cls, contact: bs4.Tag | None) -> list[str]:
        """連絡先 td を <br> 区切りの行リストに分解する。"""
        if contact is None:
            return []
        lines: list[str] = []
        buf: list[str] = []
        for node in contact.descendants:
            if isinstance(node, bs4.Tag):
                if node.name == "br":
                    lines.append(cls._clean(" ".join(buf)))
                    buf = []
                continue
            if isinstance(node, bs4.NavigableString):
                text = cls._clean(str(node))
                if text:
                    buf.append(text)
        lines.append(cls._clean(" ".join(buf)))
        return [line for line in lines if line]

    @classmethod
    def _extract_tel(cls, contact: bs4.Tag | None) -> tuple[str, str]:
        """連絡先セルから電話番号 (原文) と種別を取り出す。

        Returns:
            (電話番号の原文, 種別) — 種別は "フリーダイヤル" / "代表番号" / ""
        """
        for line in cls._contact_lines(contact):
            m = _TEL_LABEL_PATTERN.match(line)
            if not m:
                continue
            label, value = m.group(1).lower(), cls._clean(m.group(2))
            if not value:
                continue
            kind = "フリーダイヤル" if label.replace(" ", "") == "tollfree" else "代表番号"
            return value, kind
        return "", ""

    @classmethod
    def _extract_email(cls, contact: bs4.Tag | None) -> str:
        """連絡先セルからメールアドレスを取り出す。

        <a href> は mailto: が無いうえ表示テキストと食い違う行があるため、
        依頼の指示どおり **表示テキスト側** を採用する。
        """
        for line in cls._contact_lines(contact):
            text = _EMAIL_LABEL_PATTERN.sub("", line)
            m = _EMAIL_PATTERN.search(text.replace(" ", ""))
            if m:
                return m.group(0)
        return ""

    @classmethod
    def _extract_tour_url(cls, contact: bs4.Tag | None, base_url: str) -> str:
        """連絡先セルから日本ツアーページの URL を取り出す。

        href が壊れている行 ("https://https://…") があるため、リンクテキストが
        URL に見える場合はテキストを優先し、そうでない場合のみ href を使う。
        """
        if contact is None:
            return ""
        for a in contact.find_all("a"):
            text = cls._clean(a.get_text(" ", strip=True))
            href = cls._clean(a.get("href") or "")
            # メールリンク (mailto: 無しの生アドレス) は対象外
            if "@" in text or "@" in href:
                continue
            if _URL_LIKE_PATTERN.match(text):
                return cls._normalize_url(text)
            if href:
                return cls._normalize_url(cls._fix_double_scheme(urljoin(base_url, href)))
        # <a> が無い行のために素のテキストからも拾う
        for line in cls._contact_lines(contact):
            if _URL_LIKE_PATTERN.match(line):
                return cls._normalize_url(line)
        return ""

    # ------------------------------------------------------------------ #
    # 正規化ヘルパー
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """ノーブレークスペース等を通常空白にし、連続空白を 1 つに畳む。"""
        return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()

    @staticmethod
    def _key(name: str) -> str:
        """重複掲載の名寄せキー (大小文字・空白・末尾のピリオドを無視)。"""
        return re.sub(r"[\s.,]+", "", name).lower()

    @staticmethod
    def _fix_double_scheme(url: str) -> str:
        """"https://https://example.com/…" のような二重スキームを 1 つに直す。"""
        return re.sub(r"^(https?://)+(?=https?://)", "", url, flags=re.IGNORECASE)

    @classmethod
    def _normalize_url(cls, url: str) -> str:
        """スキームが欠けた URL ("www.example.com") に https を補う。"""
        url = cls._fix_double_scheme(cls._clean(url))
        if not url:
            return ""
        if url.startswith(("http://", "https://")):
            return url
        return f"https://{url}"

    @classmethod
    def _site_top(cls, url: str) -> str:
        """日本ツアーページ URL から納品用のドメイントップ URL を作る。

        例: https://www.sotc.in/international-tour-packages/japan-tour-packages
            → https://www.sotc.in/
        """
        url = cls._normalize_url(url)
        if not url:
            return ""
        parts = urlsplit(url)
        if not parts.netloc:
            return ""
        return f"{parts.scheme}://{parts.netloc}/"

    @classmethod
    def _normalize_tel(cls, raw: str) -> str:
        """インドの電話番号を +91 を含む国際表記に正規化する。

        - "1800 …" 等の国内フリーダイヤルは国際表記にできないため原文のまま残す
          (依頼指示: 代表の固定電話が取れなければ 1800 番号を残す)。
        - 先頭の国内プレフィックス 0 は除去し、国番号 91 の重複付与も避ける。
        """
        raw = cls._clean(raw)
        if not raw:
            return ""

        digits = re.sub(r"\D", "", raw)
        if not digits:
            return ""

        if _TOLL_FREE_PATTERN.match(digits):
            # インド国内フリーダイヤル (日本の 0120/0800 とは別物) はそのまま
            return raw

        # 国番号・国内プレフィックスを剥がす
        if digits.startswith("0091"):
            digits = digits[4:]
        elif digits.startswith("91") and len(digits) > 10:
            digits = digits[2:]
        digits = digits.lstrip("0")

        if not digits:
            return ""
        return f"+91-{digits}"

    @classmethod
    def _build_note(cls, tour_url: str, headings: list[str]) -> str:
        """備考を組み立てる (掲載されている日本ツアーページ URL を必ず残す)。"""
        notes = ["JNTO インド向け公式サイト「Tour Operators in India」掲載"]
        if tour_url:
            notes.append(f"日本ツアーページ: {tour_url}")
        if len(headings) > 1:
            notes.append("複数の地域見出しに重複掲載: " + " / ".join(headings))
        return " / ".join(notes)


if __name__ == "__main__":
    scraper = StreamReq18914JntoTourOperatorsInIndia()
    scraper.execute("https://www.japan.travel/en/in/travel-agency/")

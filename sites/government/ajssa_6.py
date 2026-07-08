"""
一般社団法人 福島県警備業協会（AJSSA 会員名簿・福島県）— 会員企業一覧

取得対象:
    - 福島県警備業協会の会員企業（警備会社・約117社）
    - 会社名 / 代表者名 / 役職 / 設立 / 主な業務(号) / 方部 / 郵便番号 /
      住所 / TEL / FAX / HP / 他拠点(【他】)
    ※賛助会員（警備業者以外の支援企業・ソフト会社等）は対象外とする。

取得フロー:
    /pages/13/ は goope 系の静的な単一ページ。本文にタブ状の会員一覧があり、
    方部(県北/県南/会津/いわき/相双)ごとに `div.dataArea` が並ぶ。末尾の
    `div.dataArea` は「賛助会員」で、対応するタブラベル(.record)に「賛助」を
    含む。賛助会員の dataArea は除外する。
    各 dataArea 内の `table.type007Table > tr.type007Tr` が 1 社を表し、2 つの
    td で構成される:
      - 左 td: 会社名(strong) / 代表者行(役職＋氏名) / 設立 / 主な業務(号) /
               【他】(他拠点の市名等)
      - 右 td: 〒郵便番号 / 住所(複数行) / TEL / FAX / ホームページ(<a href>)
    行を 1 件ずつ即 yield する。ページネーション無し。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_6.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_6
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF = "福島県"

# 代表者行の役職。長い順に照合して先頭の役職を切り出す。
_POSITIONS = [
    "代表取締役社長", "代表取締役会長", "代表取締役専務", "代表取締役常務",
    "代表取締役", "取締役社長", "取締役会長", "専務取締役", "常務取締役",
    "代表理事", "理事長", "支社長", "支店長", "営業所長", "代表者",
    "所長", "社長", "会長", "代表", "館長", "局長",
]
_POS_RE = re.compile(r"^\s*(" + "|".join(_POSITIONS) + r")")
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class Ajssa6(StaticCrawler):
    """一般社団法人 福島県警備業協会 会員企業一覧 スクレイパー"""

    DELAY = 1.5
    # 方部(地域区分) / FAX / 他拠点 は Schema に収まらない短い構造化カラム。
    EXTRA_COLUMNS = ["方部", "FAX", "他拠点"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        tabs = [t.get_text(" ", strip=True) for t in soup.select(".record")]
        areas = soup.select(".dataArea")

        # 賛助会員タブに対応する dataArea を除外し、方部ラベルを対応付ける
        targets = []  # (方部ラベル, dataArea)
        for i, area in enumerate(areas):
            label = tabs[i] if i < len(tabs) else ""
            if "賛助" in label:
                continue
            targets.append((label, area))

        rows = []  # (方部, tr)
        for label, area in targets:
            for tr in area.select("tr.type007Tr"):
                rows.append((label, tr))
        self.total_items = len(rows)

        for label, tr in rows:
            try:
                item = self._parse_row(tr, label, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, houbu: str, source_url: str) -> dict | None:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            return None
        left, right = tds[0], tds[1]

        # --- 左 td: 会社名・代表者・設立・主な業務・他拠点 ---
        strong = left.find("strong")
        name = self._norm(strong.get_text(" ", strip=True)) if strong else ""
        if not name:
            return None

        rep_nm = pos_nm = open_date = cat_site = other = ""
        for div in left.find_all("div"):
            text = self._norm(div.get_text(" ", strip=True))
            if not text or text == name:
                continue
            if text.startswith("設立"):
                open_date = self._norm(text[len("設立"):])
            elif text.startswith("主な業務"):
                cat_site = self._norm(text[len("主な業務"):])
            elif text.startswith("【他】"):
                other = self._norm(text[len("【他】"):])
            elif not pos_nm and _POS_RE.match(text):
                m = _POS_RE.match(text)
                pos_nm = m.group(1)
                rep_nm = self._norm(text[m.end():])

        # --- 右 td: 郵便番号・住所・TEL・FAX・HP ---
        post_code = addr = tel = fax = ""
        addr_parts = []
        for div in right.find_all("div"):
            text = self._norm(div.get_text(" ", strip=True))
            if not text:
                continue
            if text.startswith("〒") or _POST_RE.match(text):
                m = _POST_RE.search(text)
                if m:
                    post_code = m.group(1)
                rest = self._norm(_POST_RE.sub("", text, count=1))
                if rest:
                    addr_parts.append(rest)
            elif text.startswith("TEL"):
                tel = self._norm(text[3:])
            elif text.startswith("FAX"):
                fax = self._norm(text[3:])
            elif "ホームページ" in text:
                continue  # リンクは href から取得
            else:
                addr_parts.append(text)
        addr = " ".join(addr_parts).strip()

        a = right.find("a", href=True)
        hp = a["href"].strip() if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.NAME_KANA: "",
            Schema.PREF: _PREF,  # 福島県警備業協会の会員 = 全て福島県
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep_nm,
            Schema.POS_NM: pos_nm,
            Schema.OPEN_DATE: open_date,
            Schema.CAT_SITE: cat_site,
            Schema.HP: hp,
            "方部": houbu,
            "FAX": fax,
            "他拠点": other,
        }

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa6()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.fukukeikyo.or.jp/pages/13/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

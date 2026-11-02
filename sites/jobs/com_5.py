"""
線路を守る仕事.com（みちのく軌道会） 会員企業スクレイパー
対象サイト: https://www.senro-shigoto.com/index.html

取得フロー:
    1. ルート (index.html) の会員企業カードから個別ページ
       ({aomori|iwate|miyagi|fukushima}/NN.html、計23社) を列挙する。
       県別索引 ({pref}/index.html) は除外する。
    2. 各個別ページの 2 つ目以降の table.contact-table (企業情報) から
       会社名・代表者・従業員数・会社住所・電話番号・FAX番号・ホームページ・事業内容を、
       1 つ目 (応募要項) から職種 (=募集職種) を取得する。
    3. 1 社取得するごとに即 yield する (Pattern B)。

備考:
    - TEL は「企業情報」欄の電話番号 (応募要項の担当者携帯番号ではない)。
    - 都道府県は住所先頭から導出 (掲載県と所在地県は一致しない場合あり: 例 miyagi/01 は山形県)。
    - 会社住所に支店住所が併記される場合 (miyagi/07) は先頭 (本社) を採用。
    - 利用規約 (web.html) にスクレイピングの明示的禁止条項は無い。

実行方法:
    python scripts/sites/jobs/com_5.py
    docker compose exec worker python /app/bin/run_flow.py --site-id com_5
"""

import re
import sys
import logging
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 会員企業の個別ページ (県別索引 index.html は除外)
_DETAIL_HREF_RE = re.compile(r"^(?:\./)?(aomori|iwate|miyagi|fukushima)/\d+\.html$")
_POST_RE = re.compile(r"〒?\s*(\d{3})\s*[-－‐−ー]\s*(\d{4})")
_TEL_RE = re.compile(r"0\d{1,4}[-－‐−₋]\d{1,4}[-－‐−₋]\d{3,4}")
_POSITION_RE = re.compile(
    r"^((?:代表取締役|取締役|代表社員|代表理事|代表)?(?:社長|会長|専務|常務)?)[\s　]+(.+)$"
)


def _clean(text: str) -> str:
    return re.sub(r"[\s　]+", " ", text or "").strip()


def _normalize_tel(text: str) -> str:
    m = _TEL_RE.search(text or "")
    if not m:
        return ""
    return re.sub(r"[－‐−₋]", "-", m.group(0))


class Com5Scraper(StaticCrawler):
    """線路を守る仕事.com（みちのく軌道会） スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["FAX", "募集職種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"トップページを取得できません: {url}")

        detail_urls = []
        for a in soup.select("a[href]"):
            href = a["href"].strip()
            if _DETAIL_HREF_RE.match(href):
                full = urljoin(url, href)
                if full not in detail_urls:
                    detail_urls.append(full)
        logger.info("会員企業ページ: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            item = self._parse_detail(detail_url)
            if item:
                yield item

    def _parse_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("詳細ページ取得失敗: %s", url)
            return None

        tables = soup.select("table.contact-table")
        recruit = self._table_rows(tables[:1])
        company = self._table_rows(tables[1:])

        name = company.get("会社名") or recruit.get("企業名", "")
        if not name:
            h3 = soup.select_one(".Temp-title02 h3")
            name = _clean(h3.get_text()) if h3 else ""
        if not name:
            logger.warning("会社名なし: %s", url)
            return None

        # 住所: 支店併記 (本社住所 (〒… ／宮城支店)) の場合、名称が支店なら
        # 括弧内の支店住所 (TEL も支店番号) を、それ以外は先頭 (本社) を採用
        addr_full = company.get("会社住所", "")
        parts = re.split(r"[（(]", addr_full, maxsplit=1)
        addr_raw = parts[0]
        branch = parts[1] if len(parts) > 1 else ""
        if branch and "支店" in name and "〒" in branch:
            addr_raw = re.split(r"[／/)）]", branch, maxsplit=1)[0]
        post_code = ""
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            addr_raw = addr_raw[m.end():]
        addr = _clean(addr_raw)
        pm = _PREF_RE.match(addr)
        pref = pm.group(1) if pm else ""

        rep_raw = company.get("代表者", "")
        position, rep_name = "", rep_raw
        rm = _POSITION_RE.match(rep_raw)
        if rm and rm.group(1):
            position, rep_name = rm.group(1), rm.group(2)

        return {
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.PREF: pref,
            Schema.TEL: _normalize_tel(company.get("電話番号", "")),
            "FAX": _normalize_tel(company.get("FAX番号", "")),
            Schema.HP: company.get("ホームページアドレス") or company.get("ホームページ", ""),
            Schema.REP_NM: rep_name,
            Schema.POS_NM: position,
            Schema.EMP_NUM: company.get("従業員数", ""),
            Schema.LOB: company.get("事業内容", ""),
            "募集職種": recruit.get("職種", ""),
            Schema.URL: url,
        }

    @staticmethod
    def _table_rows(tables) -> dict:
        rows = {}
        for table in tables:
            for tr in table.select("tr"):
                th, td = tr.find("th"), tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                a = td.find("a", href=re.compile(r"^https?://"))
                value = a["href"].strip() if a and "ホームページ" in label else _clean(td.get_text(" "))
                rows.setdefault(label, value)
        return rows


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Com5Scraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.senro-shigoto.com/index.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

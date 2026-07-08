"""
日本アミューズメント産業協会 (JAIA / jaia.jp) — 会員企業一覧スクレイパー

取得対象:
    - 会員企業一覧 (https://jaia.jp/list/) の各社について、
      会社名・所属事業部の会員区分・企業ホームページ (HP) を取得し、
      さらに各社の HP をたどって所在地・郵便番号・電話番号を補完する。

取得フロー:
    1. 一覧ページ (単一テーブル / 約191社) を取得
    2. 各行から 会社名・会員区分(AMマシン/施設営業/遊園施設)・HP リンクを抽出
    3. HP がある場合は HP トップ → 会社概要/会社案内/所在地/アクセス 等の
       プロフィールページ (最大2ホップ) をたどり、
       ページ本文から 〒郵便番号 / 住所 / TEL を正規表現で補完
    4. 1社取得するごとに即 yield (Pattern B)

備考対応:
    「first get the company name and HP, then enter the HP to get further info
      like TEL, address, post」→ 上記フロー 2→3 で実装。

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/jaia.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jaia
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# ---- 正規表現 ---------------------------------------------------------------
_PREF = (
    r"(?:北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_PREF_PATTERN = re.compile(_PREF)
_POST_PATTERN = re.compile(r"〒\s*(\d{3})[-\s]?(\d{4})")
_TEL_PATTERN = re.compile(
    r"(?:TEL|Tel|tel|℡|電話|お電話|代表)[)）]?\s*[:：]?\s*(0\d{1,3}[\-\(\)0-9\s]{6,13}\d)"
)
# 住所: 都道府県 + 市区郡 から始まる構造化された1行
_ADDR_PATTERN = re.compile(_PREF + r"[^\n、。｜|]{0,4}?[市区郡][^\n、。｜|]{2,28}")

# プロフィールページ探索キーワード (優先度順の2ティア)
_PROFILE_TIERS = [
    ["会社概要", "会社案内", "所在地", "アクセス", "access"],
    ["企業概要", "会社情報", "企業情報", "company", "about", "概要"],
]
# 補助 URL に付与しないスキーム
_SKIP_HREF = ("mailto", "tel:", "#", "javascript")


class Jaia(StaticCrawler):
    """日本アミューズメント産業協会 会員企業一覧スクレイパー"""

    DELAY = 1.5
    TIMEOUT = 15  # 外部企業HPが遅い場合に備えて短めに
    # 会員区分は「正会員」等の短い構造化ラベルのみ (自由記述プロースは含めない)
    EXTRA_COLUMNS = [
        "会員区分_AMマシン事業部",
        "会員区分_施設営業事業部",
        "会員区分_遊園施設事業部",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページの取得に失敗しました: %s", url)
            return

        rows = soup.select("table tr")
        # 先頭行はヘッダ (会社名 / 各事業部 / ウェブサイト)
        data_rows = [r for r in rows if r.find(["th", "td"])][1:]
        self.total_items = len(data_rows)

        for row in data_rows:
            try:
                item = self._parse_row(row, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーは握りつぶさずログして継続
                logger.warning("行の解析に失敗 (スキップ): %s", e)
                continue

    def _parse_row(self, row, list_url: str) -> dict | None:
        cells = row.find_all(["th", "td"])
        if len(cells) < 4:
            return None
        name = cells[0].get_text(strip=True)
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: list_url,
            "会員区分_AMマシン事業部": cells[1].get_text(strip=True),
            "会員区分_施設営業事業部": cells[2].get_text(strip=True),
            "会員区分_遊園施設事業部": cells[3].get_text(strip=True),
            Schema.HP: "",
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
        }

        # HP リンク (一覧の「ウェブサイト »」)
        hp = ""
        link = row.select_one("a[href]")
        if link:
            href = (link.get("href") or "").strip()
            if href and not href.startswith(_SKIP_HREF):
                hp = urljoin(list_url, href)
        item[Schema.HP] = hp

        # HP をたどって 郵便番号 / 住所 / TEL を補完
        if hp:
            post, tel, addr = self._enrich_from_hp(hp)
            item[Schema.POST_CODE] = post
            item[Schema.TEL] = tel
            if addr:
                m = _PREF_PATTERN.match(addr)
                if m:
                    item[Schema.PREF] = m.group(0)
                    item[Schema.ADDR] = addr[m.end():].strip()
                else:
                    item[Schema.ADDR] = addr

        return item

    # ---- HP 補完ロジック ----------------------------------------------------
    def _enrich_from_hp(self, hp: str) -> tuple[str, str, str]:
        """HP トップ → プロフィールページ (最大2ホップ) をたどり (post, tel, addr) を返す。"""
        soup = self.get_soup(hp)
        if soup is None:
            return "", "", ""

        post, tel, addr = self._extract(soup)
        visited = {hp}

        for prof_url in self._profile_links(soup, hp)[:4]:
            if post and tel and addr:
                break
            if prof_url in visited:
                continue
            visited.add(prof_url)
            s2 = self.get_soup(prof_url)
            if s2 is None:
                continue
            p2, t2, a2 = self._extract(s2)
            post = post or p2
            tel = tel or t2
            addr = addr or a2
            # プロフィールページ内の更に深い会社概要リンクを1段だけ追う
            if not (post and tel and addr):
                for deep_url in self._profile_links(s2, prof_url)[:2]:
                    if post and tel and addr:
                        break
                    if deep_url in visited:
                        continue
                    visited.add(deep_url)
                    s3 = self.get_soup(deep_url)
                    if s3 is None:
                        continue
                    p3, t3, a3 = self._extract(s3)
                    post = post or p3
                    tel = tel or t3
                    addr = addr or a3

        return post, tel, addr

    @staticmethod
    def _extract(soup) -> tuple[str, str, str]:
        """ページ本文から 郵便番号 / 電話番号 / 住所 を抽出する。"""
        txt = soup.get_text("\n", strip=True)
        pm = _POST_PATTERN.search(txt)
        post = f"{pm.group(1)}-{pm.group(2)}" if pm else ""

        tm = _TEL_PATTERN.search(txt)
        if tm:
            tel = re.sub(r"\s", "", tm.group(1))
        else:
            tel_link = soup.select_one("a[href^='tel:']")
            tel = tel_link.get("href").split(":", 1)[1].strip() if tel_link else ""

        am = _ADDR_PATTERN.search(txt)
        addr = am.group(0).strip() if am else ""
        return post, tel, addr

    @staticmethod
    def _profile_links(soup, base: str) -> list[str]:
        """会社概要/所在地/アクセス 等のプロフィールページ候補 URL を優先度順に返す。"""
        out: list[str] = []
        for tier in _PROFILE_TIERS:
            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href or href.startswith(_SKIP_HREF):
                    continue
                blob = (a.get_text(" ", strip=True) + " " + href).lower()
                if any(kw.lower() in blob for kw in tier):
                    full = urljoin(base, href)
                    if full not in out:
                        out.append(full)
        return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jaia()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://jaia.jp/list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

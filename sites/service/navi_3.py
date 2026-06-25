"""
便利屋さん NAVI (benriyanavi.com) — 全国の便利屋さん検索ポータル 掲載店情報スクレイパー

取得対象:
    - 都道府県別ページ (/prefact/{pref}.html) に掲載された全国の便利屋さん
    - 各掲載店の 名称 / 住所 / 都道府県 / 郵便番号 / 代表者名 / TEL / HP /
      メールアドレス (掲載がある店舗のみ)

取得フロー:
    1. ルート URL (トップページ) から /prefact/*.html の都道府県別ページ URL を全て列挙
    2. 各都道府県ページ内で <h3>店名</h3> + 直後の <p>住所/代表者/TEL/HP</p> の
       ブロックを 1 件取得するごとに即 yield (途中中断に強い Pattern B)

注意:
    - 各店舗の自由記述 PR 文 (<p class="int">) は著作権リスクのため取得しない。
    - 住所は「所在地：」等の接頭辞と先頭の 〒郵便番号 を分離して格納する。
    - 当サイトは便利屋さんの専門ポータルのため、サイト定義業種は一律「便利屋」とする。

実行方法:
    # ローカルテスト
    python scripts/sites/service/navi_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id navi_3
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 〒郵便番号 (例: 〒550-0025 / 〒5500025)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})[-－‐]?(\d{4})")

# メールアドレス
_EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# <p> 内のラベル行を判別する接頭辞 (これらで始まる行は住所ではない)
_LABEL_PREFIXES = (
    "代表者", "TEL", "ＴＥＬ", "Tel", "電話",
    "HP", "URL", "FAX", "ＦＡＸ", "Fax",
    "Mail", "mail", "E-mail", "Email", "メール",
)

# 住所行の先頭に付く接頭辞 (除去対象)
_ADDR_PREFIX_PATTERN = re.compile(r"^(所在地|住所)[\s　]*[:：]?[\s　]*")


class Navi3Scraper(StaticCrawler):
    """便利屋さん NAVI (benriyanavi.com) 掲載店スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # 🔒 引数 url を唯一のルート(SSOT)として全 URL を派生させる。
        soup = self.get_soup(url)
        if not soup:
            self.logger.error("トップページの取得に失敗しました: %s", url)
            return

        # トップページから都道府県別ページ (/prefact/*.html) を列挙
        pref_urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "/prefact/" not in href or not href.endswith(".html"):
                continue
            full = urljoin(url, href)
            if full in seen:
                continue
            seen.add(full)
            pref_urls.append(full)

        self.total_items = len(pref_urls)
        self.logger.info("都道府県別ページ数: %d", len(pref_urls))

        for pref_url in pref_urls:
            pref_soup = self.get_soup(pref_url)
            if not pref_soup:
                continue
            container = pref_soup.select_one("div#prefact") or pref_soup
            for record in self._iter_shops(container, pref_url):
                # 1 件取得ごとに即 yield (全件収集してから一括 yield しない)
                yield record
            time.sleep(self.DELAY)

    def _iter_shops(self, container, page_url: str):
        for h3 in container.find_all("h3"):
            p = h3.find_next_sibling("p")
            if p is None:
                continue
            text = p.get_text("\n", strip=True)
            # 外部リンク (HP) を持つ <a> を収集
            ext_links = [
                a.get("href")
                for a in p.find_all("a", href=True)
                if a["href"].startswith("http") and "benriyanavi.com" not in a["href"]
            ]
            # 店舗ブロックの判定: ラベル行か外部リンクを持つもののみ
            if not (any(kw in text for kw in ("TEL", "代表者", "電話")) or ext_links):
                continue

            name = h3.get_text(strip=True)
            if not name:
                continue

            try:
                record = self._build_record(name, text, ext_links, p, page_url)
            except Exception as e:
                self.logger.warning("店舗解析失敗: %s — %s", name, e)
                continue
            if record:
                yield record

    def _build_record(self, name, text, ext_links, p, page_url) -> dict | None:
        item: dict = {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.CAT_SITE: "便利屋",
        }

        # 先頭のラベルが付かない行 = 住所 (最初のラベル行が現れるまで)
        addr_lines: list[str] = []
        addr_done = False
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("代表者"):
                addr_done = True
                rep = re.sub(r"^代表者[\s　:：]*", "", line).strip()
                if rep:
                    item[Schema.REP_NM] = rep
            elif line.startswith(("TEL", "ＴＥＬ", "Tel", "電話")):
                addr_done = True
                tel = re.sub(r"^(TEL|ＴＥＬ|Tel|電話)[\s　:：]*", "", line).strip()
                if tel:
                    item[Schema.TEL] = tel
            elif line.startswith(("Mail", "mail", "E-mail", "Email", "メール")):
                addr_done = True
                m = _EMAIL_PATTERN.search(line)
                if m:
                    item[Schema.EMAIL] = m.group(0)
            elif line.startswith(_LABEL_PREFIXES):
                # HP / URL / FAX 等のラベル行 (HP はアンカーから取得するためここでは無視)
                addr_done = True
            elif not addr_done:
                addr_lines.append(line)

        if addr_lines:
            self._apply_address(item, " ".join(addr_lines))

        # HP (外部リンクの最初の 1 件)。メール mailto は除く。
        if ext_links:
            item[Schema.HP] = ext_links[0]

        # mailto アンカーからメール補完
        if not item.get(Schema.EMAIL):
            for a in p.find_all("a", href=True):
                if a["href"].lower().startswith("mailto:"):
                    item[Schema.EMAIL] = a["href"][7:].strip()
                    break

        return item

    def _apply_address(self, item: dict, addr: str):
        addr = _ADDR_PREFIX_PATTERN.sub("", addr).strip()
        # 郵便番号を分離
        pm = _POST_PATTERN.search(addr)
        if pm:
            item[Schema.POST_CODE] = f"{pm.group(1)}-{pm.group(2)}"
            addr = (addr[: pm.start()] + addr[pm.end():]).strip()
        addr = addr.lstrip("　 ").strip()
        if addr:
            item[Schema.ADDR] = addr
            prefm = _PREF_PATTERN.search(addr)
            if prefm:
                item[Schema.PREF] = prefm.group(1)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Navi3Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://benriyanavi.com/?utm_source=chatgpt.com")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

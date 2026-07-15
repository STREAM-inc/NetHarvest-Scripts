"""
号外NET — 地域ニュースサイト (goguynet.jp) 店舗・イベント情報スクレイパー

取得対象:
    - トップの最新記事フィード (全国横断・?page=N で最大40ページ / 1ページ10件)
    - 各記事詳細ページの .shop-info（店舗名・住所・営業時間・定休日・最寄り駅・関連リンク）

取得フロー:
    一覧 (a.itemTitle01: タイトル/カテゴリ/掲載日時/詳細URL)
      → 詳細ページ (.shop-info の dt/dd) を1件ずつ取得して即 yield (Pattern B)

利用規約:
    https://goguynet.jp/about/privacy/ を確認済み。スクレイピング/クローリングを
    明示的に禁止する条項は無し (個人情報・Cookie の取り扱いが中心)。

著作権配慮:
    記事本文 (自由記述プロース) は取得しない。構造化された店舗情報のみを対象とする。

実行方法:
    python scripts/sites/portal/goguynet.py
    docker compose exec worker python /app/bin/run_flow.py --site-id goguynet
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47 都道府県（住所・パンくずからの都道府県抽出用。京都府/大阪府を切らないよう完全指定）
_PREF_NAMES = (
    "北海道|東京都|京都府|大阪府|"
    "青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    "神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    "滋賀県|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|"
    "愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile("(" + _PREF_NAMES + ")")
# 郵便番号 〒751-0805 / 7510805
_POST_RE = re.compile(r"〒?\s*(\d{3})[-－‐\s]?(\d{4})")
# 日本の電話番号（区切り無しの誤登録 0832561171 にも対応）
_TEL_RE = re.compile(r"0\d{1,3}[-‐−ー－(]?\d{2,4}[-‐−ー－)]?\d{3,4}")
# タイトル先頭の 【新潟市中央区】等の角括弧プレフィックスを除去
_BRACKET_PREFIX = re.compile(r"^[【\[（(][^】\]）)]*[】\]）)]\s*")

# 一覧・詳細のページ送り安全上限（実測は約40ページ。取り逃し防止に余裕を持たせる）
_MAX_PAGES = 60


class GoguynetScraper(StaticCrawler):
    """号外NET (goguynet.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["掲載日時", "地域", "最寄り駅"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        estimated_set = False
        while page <= _MAX_PAGES:
            list_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            anchors = soup.select("a.itemTitle01")
            if not anchors:
                break

            if not estimated_set:
                self.total_items = self._estimate_total(soup, len(anchors))
                estimated_set = True

            for a in anchors:
                href = a.get("href")
                if not href:
                    continue
                detail_url = urljoin(url, href)

                # 一覧側で確実に取れる情報（カテゴリ・タイトル・掲載日時）を先取り
                title_el = a.select_one("h1.itemTitle01In")
                list_title = title_el.get_text(" ", strip=True) if title_el else ""
                cat_el = a.select_one("span.label-default")
                category = cat_el.get_text(strip=True) if cat_el else ""
                date_el = a.select_one(".listDate01")
                post_date = date_el.get_text(" ", strip=True) if date_el else ""

                try:
                    item = self._scrape_detail(detail_url, list_title, category, post_date)
                except Exception as e:  # 個別記事の失敗で全体を止めない
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    @staticmethod
    def _estimate_total(soup, per_page: int) -> int:
        """ページャの最大ページ番号 × 1ページ件数で総件数を概算する。"""
        max_page = 1
        for a in soup.select('a[href*="page="]'):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page * per_page

    def _scrape_detail(
        self, url: str, list_title: str, category: str, post_date: str
    ) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        if category:
            data[Schema.CAT_SITE] = category
        if post_date:
            data["掲載日時"] = post_date

        # --- パンくず（都道府県・地域） ---
        crumbs = [
            a.get_text(strip=True)
            for a in soup.select('[class*="bread"] a, [class*="crumb"] a')
        ]
        pref = ""
        region = ""
        for c in crumbs:
            pm = _PREF_RE.search(c)
            if pm and not pref:
                pref = pm.group(1)
                continue
            # 「○○市記事一覧」→ 地域名（都道府県・全国トップ・カテゴリは除く）
            if c.endswith("記事一覧") and "全国" not in c and not pref_only(c):
                region = c.replace("記事一覧", "").strip()
        if region:
            data["地域"] = region

        # --- 店舗情報ブロック ---
        info = soup.select_one(".shop-info")
        name = ""
        if info is not None:
            name_el = info.select_one(".shop-info-name")
            if name_el:
                name = name_el.get_text(" ", strip=True)
            self._parse_shop_info(info, data)

        # 名称: 店舗名を優先、無ければ記事タイトル（角括弧の地域プレフィックスを除去）
        if not name:
            name = _BRACKET_PREFIX.sub("", list_title).strip()
        if name:
            data[Schema.NAME] = name

        # 都道府県: 住所から導出を優先、無ければパンくずの都道府県
        if data.get(Schema.ADDR):
            am = _PREF_RE.search(data[Schema.ADDR])
            if am:
                pref = am.group(1)
        if pref:
            data[Schema.PREF] = pref

        # TEL 補完: ラベルで取れなかった場合、店舗情報内の電話番号らしき文字列を拾う
        if info is not None and not data.get(Schema.TEL):
            tm = _TEL_RE.search(info.get_text(" ", strip=True))
            if tm:
                data[Schema.TEL] = tm.group(0)

        if not data.get(Schema.NAME):
            return None
        return data

    def _parse_shop_info(self, info, data: dict) -> None:
        """.shop-info-list の dt/dd を走査して各フィールドへ振り分ける。"""
        for row in info.select(".shop-info-row"):
            dt = row.find("dt")
            dd = row.find("dd")
            if dt is None or dd is None:
                continue
            key = dt.get_text(" ", strip=True)
            val = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()

            if key in ("住所", "所在地"):
                self._set_address(data, val)
            elif key in ("電話番号", "電話", "TEL", "Tel"):
                tm = _TEL_RE.search(val)
                if tm:
                    data[Schema.TEL] = tm.group(0)
                elif val:
                    data[Schema.TEL] = val
            elif key in ("営業時間", "時間"):
                if val:
                    data[Schema.TIME] = val
            elif key == "定休日":
                if val:
                    data[Schema.HOLIDAY] = val
            elif key in ("最寄り駅", "最寄駅", "アクセス"):
                if val:
                    data["最寄り駅"] = val
            elif key in ("関連リンク", "リンク", "URL", "HP", "ホームページ"):
                self._parse_links(dd, data)

    @staticmethod
    def _parse_links(dd, data: dict) -> None:
        """関連リンクの各 href を SNS / 公式サイトに振り分ける。"""
        for a in dd.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("tel:", "mailto:", "#")):
                continue
            low = href.lower()
            if "instagram.com" in low:
                data.setdefault(Schema.INSTA, href)
            elif "twitter.com" in low or "//x.com" in low or ".x.com" in low:
                data.setdefault(Schema.X, href)
            elif "facebook.com" in low:
                data.setdefault(Schema.FB, href)
            elif "tiktok.com" in low:
                data.setdefault(Schema.TIKTOK, href)
            elif "line.me" in low or "lin.ee" in low:
                data.setdefault(Schema.LINE, href)
            else:
                data.setdefault(Schema.HP, href)  # 公式サイト等

    @staticmethod
    def _set_address(data: dict, val: str) -> None:
        m = _POST_RE.search(val)
        if m:
            data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            val = _POST_RE.sub("", val).strip()
        if val:
            data[Schema.ADDR] = val


def pref_only(text: str) -> bool:
    """「新潟県最新記事一覧」のような都道府県のみのパンくずか判定（地域名から除外する）。"""
    return bool(_PREF_RE.fullmatch(text.replace("記事一覧", "").strip()))


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GoguynetScraper()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://goguynet.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

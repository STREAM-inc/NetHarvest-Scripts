"""
エミーオ(EMEAO) — 掲載業者(発注先企業)情報スクレイパー

取得対象 (一覧=ジャンル別検索ページ → 詳細ページで完結):
    - 会社名 / 詳細URL / 住所 (郵便番号・都道府県分割) / 電話番号 / HP
    - 創業年 / 社員数 / 資本金
    - サイト定義ジャンル (CAT_SITE, 例: アプリ開発 / 清掃業者 / 翻訳会社)
    - 顧客規模 / 実績数 / 対応エリア / 得意分野 (EXTRA, いずれも短い構造化値)

取得フロー:
    EMEAO! には全業者を横断する単一の一覧が存在しない。検索はジャンル別
    `/search-{genre}/` ページに分かれている。ルート `/search/` の SSR HTML には
    全ジャンルへのリンク (`/search-{slug}/`) が含まれるので、そこから genre slug を
    抽出して列挙ソースとする。
    各ジャンル一覧 `/search-{slug}/` は (ページ送りは見かけ上存在するが) 1ページ目に
    そのジャンルの全業者カードが並ぶ (page/2 以降は同じ集合の並べ替えなので使わない)。
    各カードの詳細 `/search-{slug}/{id}/` を 1 件取得するたびに即 yield する
    (Pattern B: 取得即 yield なので途中で中断しても無駄な通信が起きない)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/emeao.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id emeao
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


# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# /search/ ページから genre slug を抽出 (page 等の非ジャンルは除外)
_GENRE_SLUG_RE = re.compile(r"/search-([a-z][a-z0-9-]*)/")
# 一覧タイトルの 【ジャンル名】 を抽出
_GENRE_NAME_RE = re.compile(r"【([^】]+)】")

# kihonTable の th ラベル → Schema 定数 (構造化された短い値のみ)
_SCHEMA_LABELS = {
    "会社名": Schema.NAME,
    "住所": Schema.ADDR,
    "電話番号": Schema.TEL,
    "URL": Schema.HP,
    "創業年": Schema.OPEN_DATE,
    "設立年": Schema.OPEN_DATE,
    "社員数": Schema.EMP_NUM,
    "従業員数": Schema.EMP_NUM,
    "資本金": Schema.CAP,
}

# kihonTable の th ラベル → EXTRA カラム名 (短い数値・区分のみ。プロースは含めない)
_EXTRA_LABELS = {
    "顧客規模": "顧客規模",
    "実績数": "実績数",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


class EmeaoScraper(StaticCrawler):
    """エミーオ(EMEAO) 掲載業者スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["顧客規模", "実績数", "対応エリア", "得意分野"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url (= sites.yml の url, 例: https://emeao.jp/search/) を唯一のルートとする
        root_soup = self.get_soup(url)
        if root_soup is None:
            self.logger.error("ルートページ取得失敗: %s", url)
            return

        # ルートページの SSR HTML から genre slug を列挙
        slugs: list[str] = []
        seen_slug: set[str] = set()
        for slug in _GENRE_SLUG_RE.findall(str(root_soup)):
            if slug == "page" or slug in seen_slug:
                continue
            seen_slug.add(slug)
            slugs.append(slug)
        self.logger.info("ジャンル %d 件を抽出: %s", len(slugs), slugs)

        # url ("https://emeao.jp/search/") から各ジャンル一覧URLを派生
        base = url.rstrip("/")  # -> https://emeao.jp/search

        seen_detail: set[str] = set()
        for slug in slugs:
            list_url = f"{base}-{slug}/"
            try:
                list_soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧取得失敗: %s — %s", list_url, e)
                continue
            if list_soup is None:
                continue

            # ジャンル表示名 (CAT_SITE) を一覧タイトルから取得
            title = list_soup.title.get_text(strip=True) if list_soup.title else ""
            gm = _GENRE_NAME_RE.search(title)
            genre_name = gm.group(1).strip() if gm else slug

            # 結果カードの詳細リンク (新着業者サイドバー等は .resultItem__btn 以外なので除外)
            detail_urls = []
            for a in list_soup.select(".resultItem__btn a[href]"):
                href = urljoin(list_url, a["href"])
                if re.search(rf"/search-{re.escape(slug)}/\d+/?$", href):
                    detail_urls.append(href)
            self.logger.info("[%s] 業者 %d 件", genre_name, len(detail_urls))

            for detail_url in detail_urls:
                if detail_url in seen_detail:  # ジャンル横断の重複を排除
                    continue
                seen_detail.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url, genre_name)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

    def _scrape_detail(self, detail_url: str, genre_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {
            Schema.NAME: "",
            Schema.URL: detail_url,
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            Schema.OPEN_DATE: "",
            Schema.EMP_NUM: "",
            Schema.CAP: "",
            Schema.CAT_SITE: genre_name,
        }
        for col in self.EXTRA_COLUMNS:
            item[col] = ""

        # 基本情報テーブル (th=ラベル / td=値)
        table = soup.select_one("table.kihonTable")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text(" ", strip=True))
                value = _clean(td.get_text(" ", strip=True))
                if not value:
                    continue
                if label in _SCHEMA_LABELS:
                    key = _SCHEMA_LABELS[label]
                    if not item.get(key):
                        item[key] = value
                elif label in _EXTRA_LABELS:
                    col = _EXTRA_LABELS[label]
                    if not item.get(col):
                        item[col] = value

        # 会社名フォールバック (タイトル "{会社名}｜...の業者探しならEMEAO!")
        if not item[Schema.NAME] and soup.title:
            item[Schema.NAME] = _clean(soup.title.get_text().split("｜")[0])
        if not item[Schema.NAME]:
            return None

        # 住所から 郵便番号 / 都道府県 を分割
        addr = item.get(Schema.ADDR, "")
        if addr:
            pm = _POST_RE.search(addr)
            if pm:
                item[Schema.POST_CODE] = pm.group(1)
                addr = _clean(addr[pm.end():])
            pref = _PREF_RE.search(addr)
            if pref:
                item[Schema.PREF] = pref.group(0)
                addr = addr[pref.start():].strip()
            item[Schema.ADDR] = addr

        # 対応エリア (都道府県等の短いラベルのリスト)
        areas = [_clean(li.get_text(strip=True)) for li in soup.select("ul.area__wrap li")]
        item["対応エリア"] = " / ".join([a for a in areas if a])

        # 対応業務 / 得意分野 (短いフレーズのリスト, 重複除去)
        taiou, seen_t = [], set()
        for li in soup.select("ul.taiou__wrap li"):
            t = _clean(li.get_text(strip=True))
            if t and t not in seen_t:
                seen_t.add(t)
                taiou.append(t)
        item["得意分野"] = " / ".join(taiou)

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EmeaoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://emeao.jp/search/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

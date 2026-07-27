"""
PRfree (無料プレスリリース「PR-FREE」) — 掲載企業 企業情報スクレイパー

取得対象 (一覧=サイトマップ → 各プレスリリース詳細ページで完結):
    - 発表元企業名 (NAME) / 詳細URL
    - サイト定義ジャンル (CAT_SITE) / 掲載日 (EXTRA)
    - 記事内に「会社概要」ブロックが記載されている場合のみ:
        代表者名 (REP_NM) / 役職 (POS_NM) / 本社所在地 (ADDR, 都道府県分割) /
        設立年月日 (OPEN_DATE) / 公式サイトURL (HP)

取得フロー:
    企業一覧ページは存在しない。WordPress (Yoast SEO) の
    サイトマップインデックス `/sitemap.xml` → `post-sitemapN.xml` を列挙ソースとする。
    各プレスリリース詳細ページ /{year}/{id}/ を 1 件取得するたびに即 yield する
    (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。
    新しい記事から処理するためサイトマップは番号降順・URL は逆順で巡回する。

    「会社概要」ブロック内の `ラベル：値` 形式から構造化された短い値のみを抽出する。
    プレスリリース本文・見出し・コメント等の自由記述プロースは
    著作権リスク回避のため取得しない (リリースタイトル / 本文も同様に除外)。

    ※ 利用規約 (https://pr-free.jp/terms/) 第2条にスクレイピングを明示的に禁止する
      条項は無い (複製・二次利用に関する一般条項のみ)。robots.txt は名前付き AI ボット
      (ClaudeBot 等) のみ Disallow で `User-agent: *` の全面禁止は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/prfree.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id prfree
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


# サイトマップインデックス (ルート URL から派生させる)
_SITEMAP_INDEX_PATH = "/sitemap.xml"
# プレスリリース記事のサイトマップ (post-sitemapN.xml)
_POST_SITEMAP_RE = re.compile(r"/post-sitemap(\d+)\.xml$")
# プレスリリース詳細 URL: /{year}/{id}/
_ARTICLE_RE = re.compile(r"^https?://[^/]+/20\d{2}/\d+/?$")

# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)

# 会社概要ブロックの見出し (【○○会社概要】等) と ラベル：値 行
_HEADER_RE = re.compile(r"^【(.+?)】$")
_LINE_RE = re.compile(
    r"^(会社名|社名|代表取締役|代表者|代表|本社|所在地|住所|設立|創業|創立"
    r"|ＵＲＬ|URL|ホームページ|ＨＰ|HP)\s*[：:]\s*(.+)"
)
# 代表者値から役職を切り出す
_POS_RE = re.compile(
    r"^(代表取締役社長兼CEO|代表取締役社長|代表取締役会長|代表取締役CEO"
    r"|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|代表)\s*(.*)"
)

# ラベル → フィールド種別
_LABELS = {
    "会社名": "name", "社名": "name",
    "代表者": "rep", "代表": "rep", "代表取締役": "rep",
    "本社": "addr", "所在地": "addr", "住所": "addr",
    "設立": "open", "創業": "open", "創立": "open",
    "URL": "hp", "ＵＲＬ": "hp", "ホームページ": "hp", "ＨＰ": "hp", "HP": "hp",
}

# EXTRA カラム
_COL_DATE = "掲載日"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t]+", " ", str(s).replace("\r", "")).strip()


def _norm_date(v: str) -> str:
    """「1992年4月1日」→「1992-04-01」等に正規化。失敗時は原文を返す。"""
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", v)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", v)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"
    m = re.search(r"(\d{4})\s*年", v)
    if m:
        return f"{int(m.group(1)):04d}"
    return _clean(v)


class PrFreeScraper(StaticCrawler):
    """PRfree 掲載企業 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_DATE]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルート URL (= sites.yml の url) からサイトマップインデックスを派生
        index_url = urljoin(url, _SITEMAP_INDEX_PATH)
        index_soup = self.get_soup(index_url)
        if index_soup is None:
            self.logger.error("サイトマップインデックス取得失敗: %s", index_url)
            return

        # post-sitemapN.xml を番号降順 (新しい記事群) で並べる
        post_maps = []
        for loc in index_soup.find_all("loc"):
            u = loc.get_text(strip=True)
            m = _POST_SITEMAP_RE.search(u)
            if m:
                post_maps.append((int(m.group(1)), u))
        post_maps.sort(key=lambda t: t[0], reverse=True)
        self.logger.info("記事サイトマップ %d 本を検出", len(post_maps))

        seen: set[str] = set()
        for _, sm_url in post_maps:
            sm_soup = self.get_soup(sm_url)
            if sm_soup is None:
                self.logger.warning("サイトマップ取得失敗: %s", sm_url)
                continue
            # 各サイトマップ内も新しい記事 (末尾) から
            article_urls = [
                loc.get_text(strip=True)
                for loc in sm_soup.find_all("loc")
                if _ARTICLE_RE.match(loc.get_text(strip=True))
            ]
            for detail_url in reversed(article_urls):
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別エラーはスキップして継続
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 発表元企業名: h1 内の span (「会社名／」) を優先、無ければ #postcontent h2
        span = soup.select_one("main.content_text h1 span") or soup.select_one(
            "#postcontent h2"
        )
        raw_name = _clean(span.get_text(" ", strip=True)) if span else ""
        # 末尾の「／」やキャッチコピー (｜以降) を除去して社名部分を得る
        name = re.split(r"[／/]", raw_name)[0]
        name = re.split(r"[｜|]", name)[0].strip()
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.REP_NM: "",
            Schema.POS_NM: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.OPEN_DATE: "",
            Schema.HP: "",
            Schema.CAT_SITE: "",
            _COL_DATE: "",
        }

        cat = soup.select_one("div.post-info a[rel~=category]")
        if cat:
            item[Schema.CAT_SITE] = _clean(cat.get_text(strip=True))
        tm = soup.select_one("div.post-info time")
        if tm:
            item[_COL_DATE] = _clean(tm.get("datetime") or tm.get_text(strip=True))

        content = soup.select_one("#postcontent")
        if content:
            self._apply_profile(item, content.get_text("\n", strip=True), name)

        return item

    def _apply_profile(self, item: dict, body: str, name: str) -> None:
        """本文中の「会社概要」ブロックから構造化された短い値のみを抽出する。

        複数社の会社概要が併記される場合があるため、発表元 (name) を含む見出しの
        ブロックを優先し、単一ブロックのみの場合はそれを採用する。
        """
        blocks: list[dict] = []
        cur: dict | None = None
        for line in body.split("\n"):
            line = line.strip()
            hm = _HEADER_RE.match(line)
            if hm:
                cur = {"h": hm.group(1), "kv": {}}
                blocks.append(cur)
                continue
            if cur is None:
                continue
            # 見出し外・コメント (■) / 注記 (※) に入ったらブロック終了
            if line.startswith("※") or line.startswith("■"):
                cur = None
                continue
            lm = _LINE_RE.match(line)
            if lm:
                key = _LABELS.get(lm.group(1).strip())
                if key and key not in cur["kv"]:
                    cur["kv"][key] = _clean(lm.group(2))

        profiles = [
            b
            for b in blocks
            if b["kv"]
            and any(k in b["h"] for k in ("会社概要", "会社情報", "企業概要", "会社データ"))
        ]
        if not profiles:
            return
        if len(profiles) == 1:
            kv = profiles[0]["kv"]
        else:
            matched = [b for b in profiles if name and name in b["h"]]
            if not matched:
                return
            kv = matched[0]["kv"]

        rep = kv.get("rep", "")
        if rep:
            pm = _POS_RE.match(rep)
            if pm and pm.group(2):
                item[Schema.POS_NM] = pm.group(1)
                item[Schema.REP_NM] = _clean(pm.group(2))
            else:
                item[Schema.REP_NM] = rep

        addr = kv.get("addr", "")
        if addr:
            p = _PREF_RE.search(addr)
            if p:
                item[Schema.PREF] = p.group(0)
                item[Schema.ADDR] = addr[p.start():].strip()
            else:
                item[Schema.ADDR] = addr

        if kv.get("open"):
            item[Schema.OPEN_DATE] = _norm_date(kv["open"])
        if kv.get("hp"):
            item[Schema.HP] = kv["hp"]


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PrFreeScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://pr-free.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

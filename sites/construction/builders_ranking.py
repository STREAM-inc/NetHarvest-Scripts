# scripts/sites/construction/builders_ranking.py
"""
ビルダーランキング (builders-ranking.com) — 住宅会社・工務店・ハウスメーカーのランキング

取得対象:
    - 全国47都道府県 × 3ランキング種類(Instagramフォロワー数 / YouTubeチャンネル登録数 /
      Google口コミ評価) の各上位ランキング掲載会社
    - 名称・都道府県・サイト定義業種(タグ)・Instagramアカウント・公式HP・口コミ採点・
      ランキング種類・ランキング順位・Instagramフォロワー数・YouTubeチャンネル登録数・
      YouTubeチャンネルURL

取得フロー:
    ルート(url) から各ランキング種類のインデックスページ (ig-index/ yt-index/ g-index/) を取得
      → ページ内の都道府県セレクト (.detail-hero__select の <option>) から
        都道府県別ランキングページ URL と都道府県名を取得
      → 各都道府県ランキングページの .ranking-item を解析し 1 件ずつ即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/construction/builders_ranking.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id builders_ranking
"""

import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class BuildersRankingScraper(StaticCrawler):
    """ビルダーランキング スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "ランキング種類",
        "ランキング順位",
        "Instagramフォロワー数",
        "YouTubeチャンネル登録数",
        "YouTubeチャンネル",
    ]

    # ランキング種類ごとの (インデックスページのパス, ランキング種類ラベル)
    # インデックスページ内の都道府県セレクトから個別ページ URL を取得する
    RANKING_TYPES = [
        ("ig-index/", "Instagramフォロワー数"),
        ("yt-index/", "YouTubeチャンネル登録数"),
        ("g-index/", "Google口コミ評価"),
    ]

    def parse(self, url: str):
        """
        ルート url を起点に、3 ランキング種類 × 47 都道府県のランキングページを巡回する。

        Args:
            url: サイトのルート URL (例: https://builders-ranking.com/)
        """
        for index_path, rank_label in self.RANKING_TYPES:
            index_url = urljoin(url, index_path)
            index_soup = self.get_soup(index_url)
            if index_soup is None:
                self.logger.warning("インデックス取得失敗: %s", index_url)
                continue

            # 都道府県セレクト: <option value="/{slug}/{slug}_{type}">{都道府県名}</option>
            # 先頭の「一覧」(value に index/ranking を含む) は除外する
            options = []
            select = index_soup.select_one(".detail-hero__select")
            if select:
                for opt in select.select("option"):
                    value = (opt.get("value") or "").strip()
                    pref_name = opt.get_text(strip=True)
                    if not value:
                        continue
                    if "index" in value or "/ranking/" in value:
                        continue  # 全国「一覧」ページはスキップ
                    options.append((value, pref_name))

            for value, pref_name in options:
                # value 例: /tokyo/tokyo_ig → 末尾スラッシュ付きの絶対 URL に正規化
                pref_url = urljoin(url, value.strip("/") + "/")
                pref_soup = self.get_soup(pref_url)
                if pref_soup is None:
                    self.logger.warning("都道府県ページ取得失敗: %s", pref_url)
                    continue

                items = [
                    it
                    for it in pref_soup.select(".ranking-item")
                    if "ranking-item__none" not in it.get("class", [])
                ]
                for item in items:
                    try:
                        row = self._parse_item(item, pref_url, pref_name, rank_label)
                        if row:
                            yield row
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("アイテム解析失敗: %s (%s)", pref_url, e)
                        continue

    def _parse_item(self, item, page_url: str, pref_name: str, rank_label: str) -> dict | None:
        """1 件の .ranking-item から行データを構築する"""
        name_box = item.select_one(".ranking-item__name")
        if name_box is None:
            return None

        # ランキング順位: .ranking-item__name 直下の最初の <p> (class は hold/up/down と変動)
        rank = ""
        p_rank = name_box.find("p")
        if p_rank:
            rank = p_rank.get_text(strip=True)

        # 名称: h2 から地域 span を除いたテキスト
        name = ""
        h2 = name_box.select_one("h2")
        if h2:
            span = h2.select_one("span")
            if span:
                span.extract()
            name = h2.get_text(strip=True)
        if not name:
            return None

        # アカウントフォロワー情報 (ラベルでマッピング。掲載が無い種類は欠落する)
        insta_followers = ""
        yt_subscribers = ""
        review_score = ""
        for li in item.select(".account-follower li"):
            label_el = li.select_one("p")
            value_el = li.select_one("strong")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True)
            value = value_el.get_text(strip=True)
            if "Instagram" in label:
                insta_followers = value
            elif "YouTube" in label or "Youtube" in label:
                yt_subscribers = value
            elif "Google" in label or "口コミ" in label:
                review_score = value

        # サイト定義業種・ジャンル (タグの羅列)
        tags = [t.get_text(strip=True) for t in item.select(".tag-list li")]
        site_category = " / ".join(t for t in tags if t)

        # リンク群
        hp = ""
        insta = ""
        youtube = ""
        hp_a = item.select_one(".links__list a.btn")
        if hp_a and hp_a.get("href"):
            hp = hp_a["href"].strip()
        ig_a = item.select_one(".links__list a.ig")
        if ig_a and ig_a.get("href"):
            insta = ig_a["href"].strip()
        yt_a = item.select_one(".links__list a.yt")
        if yt_a and yt_a.get("href"):
            youtube = yt_a["href"].strip()

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.PREF: pref_name,
            Schema.CAT_SITE: site_category,
            Schema.INSTA: insta,
            Schema.HP: hp,
            Schema.SCORES: review_score,
            "ランキング種類": rank_label,
            "ランキング順位": rank,
            "Instagramフォロワー数": insta_followers,
            "YouTubeチャンネル登録数": yt_subscribers,
            "YouTubeチャンネル": youtube,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BuildersRankingScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://builders-ranking.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

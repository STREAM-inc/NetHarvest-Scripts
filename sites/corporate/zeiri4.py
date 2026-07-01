"""
税理士ドットコム — 税理士・会計事務所 検索一覧スクレイパー

取得対象 (一覧 → 詳細ページで完結):
    - 事務所名 / 詳細URL
    - 所在地 (郵便番号・都道府県を分割) / 代表 (所属税理士) 名
    - 最寄り駅 / 所属税理士数 / 所属税理士会 / 税理士登録年 (EXTRA)
    - 得意分野 / 得意業種 / 取り扱い分野 / 取り扱い業種 (EXTRA)

取得フロー:
    検索一覧ページ (?...&page=N) を 1 ページ 50 件で巡回し、各パネルから
    事務所名・詳細URL を取り出す。詳細ページを 1 件取得するたびに即 yield する
    (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。
    一覧末尾 (page=1131 付近, 約 56,518 件) まで panel が無くなったら停止する。

    詳細ページには 2 つのレイアウトがある:
      - 税理士事務所 (/f_N/)  : dl.b-firmTabContent__sectionCardTableDl
      - 税理士法人   (/nf_N/) : dl.b-nfProfile__dl / dl.b-nfProfile__zeirishi
    どちらも dt(ラベル)→dd(値) のペアなので、ホワイトリストのラベルだけを
    プロフィール用 dl から先勝ちで拾う (関連事務所パネルの datalist は除外)。

    一覧パネルの紹介キャッチコピー (profileHeading) は自由記述の長文プロースのため
    著作権リスク回避により取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/zeiri4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zeiri4
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
_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")

# 詳細プロフィールの dl (関連事務所一覧 b-firmSearchPanel__datalist は除外する)
_PROFILE_DL = (
    "dl.b-firmTabContent__sectionCardTableDl, "
    "dl.b-nfProfile__dl, dl.b-nfProfile__zeirishi"
)

# dt ラベル → 取り込みキー (先勝ち)。代表者名は 名前 / 所属税理士 の両レイアウトに対応。
_REP_LABELS = ("名前", "所属税理士")
_STATION_LABELS = ("アクセス", "最寄り駅")
_EXTRA_LABELS = (
    "所属税理士数",
    "所属税理士会",
    "税理士登録年",
    "得意分野",
    "得意業種",
    "取り扱い分野",
    "取り扱い業種",
)


class Zeiri4(StaticCrawler):
    """税理士ドットコム スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "最寄り駅",
        "所属税理士数",
        "所属税理士会",
        "税理士登録年",
        "得意分野",
        "得意業種",
        "取り扱い分野",
        "取り扱い業種",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして使い、ページ送りは &page=N で派生させる。
        page = 1
        while True:
            list_url = f"{url}&page={page}"
            soup = self.get_soup(list_url)
            if soup is None:  # 取得失敗 (WAF/タイムアウト等) は末尾扱いで停止
                break
            panels = soup.select("li.b-firmlistPanel")
            if not panels:
                break

            for panel in panels:
                link = panel.select_one(".b-firmlistPanel__title a[href]")
                if not link:
                    continue
                name = link.get_text(strip=True)
                detail_url = urljoin(url, link.get("href"))
                try:
                    item = self._scrape_detail(detail_url, name)
                except Exception as exc:  # 個別ページの失敗はスキップして継続
                    self.logger.warning("detail failed %s: %s", detail_url, exc)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:  # 詳細取得失敗時は最低限 名前・URL のみ返す
            return {Schema.NAME: name, Schema.URL: url}

        # プロフィール dl からラベル→値を先勝ちで収集
        fields: dict[str, str] = {}
        for dl in soup.select(_PROFILE_DL):
            for dt in dl.select("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = dt.get_text(" ", strip=True)
                if label and label not in fields:
                    value = re.sub(r"\s*地図\s*$", "", dd.get_text(" ", strip=True)).strip()
                    fields[label] = value

        item = {
            Schema.NAME: name,
            Schema.URL: url,
        }

        addr = fields.get("所在地", "")
        if addr:
            m_post = _POST_RE.search(addr)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
                addr = _POST_RE.sub("", addr).strip()
            m_pref = _PREF_RE.match(addr)
            if m_pref:
                item[Schema.PREF] = m_pref.group(0)
                addr = addr[m_pref.end():].strip()
            item[Schema.ADDR] = addr

        for lbl in _REP_LABELS:
            if fields.get(lbl):
                item[Schema.REP_NM] = fields[lbl]
                break

        for lbl in _STATION_LABELS:
            if fields.get(lbl):
                item["最寄り駅"] = fields[lbl]
                break

        for lbl in _EXTRA_LABELS:
            if lbl in fields:
                item[lbl] = fields[lbl]

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Zeiri4()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute(
        "https://www.zeiri4.com/firm/search/?FirmSearchForm%5BPrefecture_id%5D=&FirmSearchForm%5BAutonomy_id%5D="
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

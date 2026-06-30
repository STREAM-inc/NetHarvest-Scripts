"""
キャリタス就活 (job_3) — 新卒採用・インターンシップ情報サイトの企業検索 スクレイパー

取得対象:
    - 企業検索結果一覧 (condition-search/result) に掲載される全企業 (約25,733社)
    - 企業名・所在地(都道府県/郵便番号/住所)・代表者(役職/氏名)・設立/創業・
      資本金・従業員数・サイト定義業種、および売上高/上場区分/評価/フォロワー数

取得フロー:
    1. ルート URL (= sites.yml の url) を起点に ?p=N でページ送り (40社/ページ)
    2. 一覧の各企業パネルから 都道府県・業種・企業名・評価・フォロワー数 を取得し、
       企業詳細 (/corp/{id}/) を 1 件ごとに取得して即 yield (Pattern B)
    3. 詳細ページの th→td テーブルから 設立・住所・代表者・資本金・従業員数・
       売上高・上場区分 を抽出してマージする

備考:
    - 事業所/事業内容・沿革・子会社/関連会社・主要取引先・検索用キーワード等の
      長文の自由記述 (プロース) は著作権リスク回避のため取得対象から除外している。
    - 電話番号・HP URL は詳細ページに掲載が無いため取得しない。

実行方法:
    python scripts/sites/jobs/job_3.py
    docker compose exec worker python /app/bin/run_flow.py --site-id job_3
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


# 都道府県切り出し (一覧の "{都道府県}{業種…}" 文字列や住所の先頭から)
PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 郵便番号 (〒103-0021 / 1030021)
POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 総件数 ("40 / 25733社" の分母)
TOTAL_PATTERN = re.compile(r"/\s*([\d,]+)\s*社")

# 代表者の役職プレフィックス判定
POS_PREFIX = re.compile(
    r"^(代表|取締役|社長|会長|理事|院長|園長|店長|頭取|学長|校長|CEO|社主|理事長)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class CaritasJobScraper(StaticCrawler):
    """キャリタス就活 企業検索スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "売上高",
        "上場区分",
        "評価",
        "フォロワー数",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        seen: set[str] = set()
        while True:
            page_url = f"{url}&p={page}" if "?" in url else f"{url}?p={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            # 初回ページで総件数を確定し進捗表示を有効化
            if page == 1:
                m = TOTAL_PATTERN.search(soup.get_text(" "))
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))
                    self.logger.info("キャリタス就活 総企業数: %d 社", self.total_items)

            anchors = soup.select('a.c_panelLink[href^="/corp/"]')
            # 一覧パネル (c_panelCompanyInfoMain) を持つものだけが検索結果。
            anchors = [a for a in anchors if a.select_one(".c_panelCompanyInfoMain")]
            if not anchors:
                break

            new_on_page = 0
            for a in anchors:
                href = a.get("href", "")
                # /corp/{id}/default/ は /corp/{id}/ にリダイレクトするため正規化
                href = href.replace("default/", "")
                detail_url = urljoin(url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                new_on_page += 1

                list_data = self._parse_list_panel(a, detail_url)
                try:
                    item = self._scrape_detail(detail_url, list_data)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                    item = list_data if list_data.get(Schema.NAME) else None
                if item:
                    yield item

            if new_on_page == 0:
                break
            page += 1

    def _parse_list_panel(self, anchor, detail_url: str) -> dict:
        """一覧パネルから 都道府県・業種・企業名・評価・フォロワー数 を取得。"""
        data: dict = {Schema.URL: detail_url}

        ttl = anchor.select_one(".c_panelCompanyInfoMain__ttl")
        if ttl:
            data[Schema.NAME] = _clean(ttl.get_text())

        # "{都道府県}{業種1｜業種2｜…}" (区切り無し) → 先頭の都道府県を切り出し残りを業種に
        txt_el = anchor.select_one(".c_panelCompanyInfoMain__txt")
        if txt_el:
            txt = _clean(txt_el.get_text())
            pm = PREF_PATTERN.match(txt)
            if pm:
                data[Schema.PREF] = pm.group(1)
                data[Schema.CAT_SITE] = txt[pm.end():].strip()
            else:
                data[Schema.CAT_SITE] = txt

        rating = anchor.select_one(".c_panelCompanyInfoMain__starRatingNum")
        if rating:
            data["評価"] = _clean(rating.get_text())
        follower = anchor.select_one(".c_panelCompanyInfoMain__followerNum")
        if follower:
            data["フォロワー数"] = _clean(follower.get_text())

        return data

    def _scrape_detail(self, url: str, list_data: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return list_data if list_data.get(Schema.NAME) else None

        data = dict(list_data)

        # 企業名 (一覧で取れていなければ h1 から補完)
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())
        if not data.get(Schema.NAME):
            return None

        # 詳細テーブル (th→td)。同一ラベルが複数表に出る場合は最初の非空値を採用。
        rows: dict[str, str] = {}
        for tr in soup.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))
                if key and key not in rows and val:
                    rows[key] = val

        # 住所 (本社所在地1)。末尾 "MAP" を除去し、郵便番号・都道府県を切り出す。
        addr = rows.get("本社所在地1", "")
        addr = re.sub(r"\s*MAP\s*$", "", addr).strip()
        pc = POST_PATTERN.search(addr)
        if pc:
            data[Schema.POST_CODE] = pc.group(1)
            addr = POST_PATTERN.sub("", addr, count=1).strip()
        if not data.get(Schema.PREF):
            apm = PREF_PATTERN.search(addr)
            if apm:
                data[Schema.PREF] = apm.group(1)
        data[Schema.ADDR] = addr

        # 代表者 ("代表取締役社長　関口 晃介" → 役職 + 氏名)
        rep = rows.get("代表者", "")
        pos, name = "", rep
        if "　" in rep:
            pos, name = rep.split("　", 1)
        elif " " in rep and POS_PREFIX.match(rep):
            pos, name = rep.split(" ", 1)
        data[Schema.POS_NM] = pos.strip()
        data[Schema.REP_NM] = name.strip()

        # 設立/創業
        data[Schema.OPEN_DATE] = rows.get("創業/設立", "")
        # 資本金 / 従業員数
        data[Schema.CAP] = rows.get("資本金", "")
        data[Schema.EMP_NUM] = rows.get("従業員数", "")

        # EXTRA: 構造化された短い値のみ (売上高・上場区分)
        data["売上高"] = rows.get("売上高", "")
        data["上場区分"] = rows.get("上場区分", "")

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CaritasJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://job.career-tasu.jp/condition-search/result/?keyword=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

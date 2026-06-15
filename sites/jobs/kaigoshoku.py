# scripts/sites/jobs/kaigoshoku.py
"""
マイナビ介護職 (kaigoshoku.mynavi.jp) — 介護求人スクレイパー

取得対象:
    - 全国の介護職求人（約99,748件 / 2,494ページ × 40件）
    - 一覧ページ → 詳細ページ の2段階取得

取得フロー:
    /r/ (page1) → /r/pg_N (page2以降)
      → /d/{求人番号} (各求人詳細)

備考:
    法人名フィールドから法人種別（社会福祉法人・医療法人・NPO法人 等）を
    自動抽出。後段で法人種別ごとのフィルタリングが可能。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/kaigoshoku.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kaigoshoku
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 法人名フィールドから法人種別を抽出する（ユーザー備考: 社会福祉法人・医療法人・NPO法人の区別）
_HOJIN_TYPE_PATTERN = re.compile(
    r"(社会福祉法人|医療法人|NPO法人|特定非営利活動法人|"
    r"学校法人|宗教法人|公益財団法人|公益社団法人|"
    r"一般財団法人|一般社団法人|財団法人|社団法人|"
    r"独立行政法人|地方独立行政法人|"
    r"株式会社|有限会社|合同会社|合名会社|合資会社)"
)


class KaigoshokuScraper(StaticCrawler):
    """マイナビ介護職 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人番号",
        "法人名",
        "法人種別",
        "雇用形態",
        "最寄り駅",
        "本社所在地",
    ]

    def parse(self, url: str):
        """一覧ページをページネーションしながら各求人詳細を取得・即yield"""
        page = 1

        while True:
            # 🔒 url を起点にページ URL を構築 (/r/pg_N は1始まりで統一)
            list_url = urljoin(url, f"/r/pg_{page}")
            self.logger.info("一覧ページ取得: page=%d (%s)", page, list_url)

            soup = self.get_soup(list_url)

            if page == 1:
                total_el = soup.select_one('[class*="total"]')
                if total_el:
                    m = re.search(r"[\d,]+", total_el.get_text())
                    if m:
                        self.total_items = int(m.group().replace(",", ""))

            # class が ['resultItem'] のみの要素（広告バナー等を除外）
            items = [
                el for el in soup.select("div.resultItem")
                if el.get("class") == ["resultItem"]
            ]

            if not items:
                break

            for item in items:
                detail_a = item.select_one('a[href^="/d/"]')
                if not detail_a:
                    continue
                detail_url = urljoin(url, detail_a["href"])

                # 一覧側のサービス種別（詳細に無い場合の補完用）
                svc_el = item.select_one(".resultItem_head-label p")
                list_service = svc_el.get_text(strip=True) if svc_el else ""

                try:
                    record = self._scrape_detail(detail_url, list_service)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                    continue

            # 次ページリンクが無ければ終了
            next_links = soup.select('.pager_bt a[href^="/r/pg_"]')
            if not next_links:
                break

            page += 1

    def _scrape_detail(self, url: str, list_service: str = "") -> dict | None:
        """求人詳細ページから全フィールドを取得"""
        soup = self.get_soup(url)

        # dl 要素から key→value を収集（先着順で重複を除去し、メイン求人データのみ取得）
        dl_data: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                key = dt.get_text(strip=True)
                if key and key not in dl_data:
                    raw = dd.get_text(separator=" ", strip=True)
                    dl_data[key] = re.sub(r"\s+", " ", raw).strip()

        # 法人名（dl優先、無ければ h1[1]）
        h1_els = soup.find_all("h1")
        entity_name = h1_els[1].get_text(strip=True) if len(h1_els) > 1 else ""
        hojin_name = dl_data.get("法人名", "") or entity_name

        # 法人種別をプレフィックスから抽出
        m_type = _HOJIN_TYPE_PATTERN.search(hojin_name)
        hojin_type = m_type.group(1) if m_type else ""

        # 勤務地から都道府県と住所を分離
        location = dl_data.get("勤務地", "")
        m_pref = _PREF_PATTERN.match(location)
        pref = m_pref.group(1) if m_pref else ""
        addr = location[m_pref.end():].strip() if m_pref else location

        # 求人番号（dl優先、なければ URL から抽出）
        job_num = dl_data.get("お問い合わせ求人番号", "")
        if not job_num:
            m_num = re.search(r"/d/(\d+)", url)
            job_num = m_num.group(1) if m_num else ""

        return {
            Schema.NAME: hojin_name or "名称非公開",
            Schema.URL: url,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: dl_data.get("サービス", "") or list_service,
            "求人番号": job_num,
            "法人名": hojin_name,
            "法人種別": hojin_type,
            "雇用形態": dl_data.get("雇用形態", ""),
            "最寄り駅": dl_data.get("アクセス", ""),
            "本社所在地": dl_data.get("本社(本拠地)", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KaigoshokuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://kaigoshoku.mynavi.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

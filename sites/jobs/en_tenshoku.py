"""
エン転職 — 求人掲載企業情報スクレイパー

取得対象:
    - 東京都の求人掲載中の企業情報（会社概要・採用条件）

取得フロー:
    1. https://employment.en-japan.com/k_tokyo/{N}/ を巡回（最大40ページ、50件/ページ）
    2. 各ページの求人アイテム（div.jobSearchListUnit）から詳細URLを収集
    3. 詳細ページ（/desc_{ID}/）で会社情報テーブル（table.dataTable）を解析して企業データを取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/en_tenshoku.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id en_tenshoku
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://employment.en-japan.com"
LIST_URL = "https://employment.en-japan.com/k_tokyo/"
MAX_PAGES = 40  # 40ページ × 50件 = 2000件（41ページ以降はアクセス不可）

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class EnTenshokuScraper(StaticCrawler):
    """エン転職 求人企業情報スクレイパー（employment.en-japan.com）"""

    DELAY = 1.5
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務時間",
        "仕事内容",
        "応募資格",
        "募集背景",
        "休日休暇",
        "福利厚生",
        "配属部署等",
        "売上高",
        "事業所",
        "関連会社",
        "採用ホームページ",
        "入社までの流れ",
        "応募受付方法",
        "面接地",
        "連絡先",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全ページの求人一覧 → 詳細ページをスクレイプ"""
        seen: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            list_url = f"{LIST_URL}{page}/" if page > 1 else LIST_URL
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", list_url)
                break

            # 初回ページで総件数を設定（進捗表示用）
            if page == 1:
                count_el = soup.select_one("div.num em")
                if count_el:
                    cnt_text = count_el.get_text(strip=True).replace(",", "")
                    if cnt_text.isdigit():
                        self.total_items = min(int(cnt_text), MAX_PAGES * 50)
                else:
                    self.total_items = MAX_PAGES * 50

            # 求人アイテムから詳細URLを収集
            items = soup.select("div.jobSearchListUnit")
            if not items:
                self.logger.info("pg%d: アイテムなし、終了", page)
                break

            page_urls: list[str] = []
            for item in items:
                # jobNameArea内のa.job hrefを取得（arearoute付きを除外して基本URLのみ使う）
                a_tag = item.select_one("div.jobNameArea a.job[href]")
                if not a_tag:
                    continue
                href = a_tag.get("href", "")
                # /desc_{ID}/ の形式に正規化（クエリパラメータ除去）
                m = re.match(r"(/desc_[^/?]+)", href)
                if not m:
                    continue
                detail_path = m.group(1) + "/"
                detail_url = BASE_URL + detail_path
                if detail_url not in seen:
                    seen.add(detail_url)
                    page_urls.append(detail_url)

            self.logger.info("pg%d: %d件の詳細URLを収集", page, len(page_urls))

            for detail_url in page_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s / %s", detail_url, e)
                    continue

            # ページネーション確認 - 次ページがなければ終了
            next_btn = soup.select_one("li.nextBtn a.next")
            if not next_btn:
                self.logger.info("pg%d: 次ページなし、終了", page)
                break

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから企業・求人情報を取得"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 会社名
        company_el = soup.select_one("div#descCompanyName div.company span.text")
        if company_el:
            data[Schema.NAME] = _clean(company_el.get_text())

        # 求人タイトル（職種名）
        job_name_el = soup.select_one("div#descJobName h2.name")
        if job_name_el:
            data["求人タイトル"] = _clean(job_name_el.get_text())

        # 募集要項テーブル（table.dataTable 内の th.icon.item.*）
        for tbl in soup.select("table.dataTable"):
            for row in tbl.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))

                if key == "仕事内容":
                    data["仕事内容"] = val
                elif key == "応募資格":
                    data["応募資格"] = val
                elif key == "募集背景":
                    data["募集背景"] = val
                elif key == "雇用形態":
                    data["雇用形態"] = val
                elif key in ("勤務地・交通", "勤務地"):
                    data["勤務地"] = val
                    # 都道府県抽出
                    m_pref = _PREF_PATTERN.search(val)
                    if m_pref:
                        data.setdefault(Schema.PREF, m_pref.group(1))
                elif key == "勤務時間":
                    data["勤務時間"] = val
                elif key == "給与":
                    data["給与"] = val
                elif key == "休日休暇":
                    data["休日休暇"] = val
                elif key in ("福利厚生・待遇",):
                    data.setdefault("福利厚生", val)
                elif key in ("配属部署", "先輩社員の声"):
                    existing = data.get("配属部署等", "")
                    data["配属部署等"] = (existing + " / " + val).strip(" /") if existing else val

                # 会社概要テーブル
                elif key == "設立":
                    data[Schema.OPEN_DATE] = val
                elif key == "代表者":
                    data[Schema.REP_NM] = val
                elif key == "資本金":
                    data[Schema.CAP] = val
                elif key == "従業員数":
                    data[Schema.EMP_NUM] = val
                elif key == "売上高":
                    data["売上高"] = val
                elif key == "事業内容":
                    data[Schema.LOB] = val
                elif key == "事業所":
                    data["事業所"] = val
                    # 最初の住所から郵便番号・住所を抽出（未設定の場合のみ）
                    m_post = re.search(r"〒\s*(\d{3})-?(\d{4})", val)
                    if m_post and Schema.POST_CODE not in data:
                        data[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"
                        addr_part = val[val.index(m_post.group(0)) + len(m_post.group(0)):].strip()
                        m_pref = _PREF_PATTERN.search(addr_part)
                        if m_pref:
                            data.setdefault(Schema.PREF, m_pref.group(1))
                        data.setdefault(Schema.ADDR, addr_part.split("■")[0].strip())
                elif key == "関連会社":
                    data["関連会社"] = val
                elif key == "企業ホームページ":
                    a_tag = td.select_one("a[href]")
                    if a_tag:
                        data[Schema.HP] = a_tag.get("href", val)
                    else:
                        data[Schema.HP] = val
                elif key == "採用ホームページ":
                    a_tag = td.select_one("a[href]")
                    if a_tag:
                        data["採用ホームページ"] = a_tag.get("href", val)
                    else:
                        data["採用ホームページ"] = val
                elif key == "入社までの流れ":
                    data["入社までの流れ"] = val
                elif key == "応募受付方法":
                    data["応募受付方法"] = val
                elif key == "面接地":
                    data["面接地"] = val
                elif key == "連絡先":
                    # 郵便番号・住所 を連絡先tdから抽出（事業所より優先度低め）
                    m_post = re.search(r"〒\s*(\d{3})-?(\d{4})", val)
                    if m_post and Schema.POST_CODE not in data:
                        data[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"
                        addr_candidate = val[val.index(m_post.group(0)) + len(m_post.group(0)):].strip()
                        # 担当・TEL行を除去して住所部分のみ取得
                        addr_candidate = re.split(r"\s*(担当|TEL)\s*", addr_candidate)[0].strip()
                        m_pref = _PREF_PATTERN.search(addr_candidate)
                        if m_pref:
                            data.setdefault(Schema.PREF, m_pref.group(1))
                        data.setdefault(Schema.ADDR, addr_candidate)
                    # 連絡先本文全体も保存
                    data["連絡先"] = val

        # 連絡先TELを td 内の構造から取得（addressUnit span）
        contact_th = soup.find("th", string=re.compile(r"^連絡先$"))
        if contact_th:
            contact_td = contact_th.find_next_sibling("td")
            if contact_td:
                for unit in contact_td.select("div.addressUnit"):
                    subj = unit.select_one("span.subject")
                    text = unit.select_one("span.text")
                    if subj and text and "TEL" in subj.get_text():
                        digits = re.sub(r"\D", "", text.get_text(strip=True))
                        if digits and Schema.TEL not in data:
                            data[Schema.TEL] = digits

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EnTenshokuScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

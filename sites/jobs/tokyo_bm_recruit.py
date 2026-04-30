"""
東京ビルメンお仕事さがし — 公益社団法人 東京ビルメンテナンス協会 採用ホームページ

取得対象:
    - tokyo-bm-recruit.jp の求人一覧 (ビルメン/清掃/警備/設備管理/事務営業)
    - 約 244 件 / 49 ページ (5件/ページ)
    - 各求人の詳細ページから、会社名・連絡先・仕事内容等を取得

取得フロー:
    1. 一覧ページ (KQA_{page}_5_DUD%2CID/MLlist.htm) を巡回
    2. 各ページの div.list_Block[data-id] から詳細URLを収集
    3. 詳細ページの dl/dt/dd ブロックを解析

実行方法:
    python scripts/sites/jobs/tokyo_bm_recruit.py
    python bin/run_flow.py --site-id tokyo_bm_recruit
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

BASE_URL = "https://www.tokyo-bm-recruit.jp"
LISTING_URL = "https://www.tokyo-bm-recruit.jp/tokyobmrecruit2/all/KQA_1_5_DUD%2CID/MLlist.htm"
LISTING_TPL = "https://www.tokyo-bm-recruit.jp/tokyobmrecruit2/all/KQA_{page}_5_DUD%2CID/MLlist.htm"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class TokyoBmRecruitScraper(StaticCrawler):
    """東京ビルメンお仕事さがし スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "雇用形態",
        "職種",
        "仕事内容",
        "資格・経験",
        "給与",
        "勤務曜日・時間",
        "勤務地",
        "交通アクセス",
        "休日・休暇",
        "待遇",
        "備考",
        "応募方法",
        "採用担当",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        entries = self._collect_listing_entries()
        self.total_items = len(entries)
        self.logger.info("詳細URL収集完了: %d 件", len(entries))

        for entry in entries:
            try:
                item = self._scrape_detail(entry)
                if item and item.get(Schema.NAME):
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s (%s)", entry.get("url"), e)
                continue

    def _collect_listing_entries(self) -> list[dict]:
        entries: list[dict] = []
        seen: set[str] = set()
        page = 1

        while True:
            page_url = LISTING_TPL.format(page=page)
            soup = self.get_soup(page_url)
            if soup is None:
                break

            blocks = soup.select("div.list_Block")
            if not blocks:
                break

            new_on_page = 0
            for block in blocks:
                job_id = (block.get("data-id") or "").strip()
                detail_link = block.select_one('a[href*="/MDdetail.htm"]')
                if not job_id or not detail_link:
                    continue
                if job_id in seen:
                    continue
                seen.add(job_id)

                href = detail_link.get("href", "").strip()
                detail_url = urljoin(BASE_URL, href)

                company = block.select_one(".company_name")
                title = block.select_one(".kyuujin_title")
                emp_items = block.select(".koyoukeitai_box li")

                entries.append({
                    "url": detail_url,
                    "job_id": job_id,
                    "company": _clean(company.get_text()) if company else "",
                    "title": _clean(title.get_text()) if title else "",
                    "employment": " / ".join(_clean(li.get_text()) for li in emp_items if _clean(li.get_text())),
                })
                new_on_page += 1

            self.logger.info("page %d: %d 件収集 (累計 %d)", page, new_on_page, len(entries))

            # ページネーション: 次ページへのリンクがあるかをチェック
            has_next = False
            for a in soup.select("a"):
                href = a.get("href", "")
                if f"KQA_{page + 1}_5_" in href:
                    has_next = True
                    break

            if not has_next or new_on_page == 0:
                break
            page += 1

        return entries

    def _scrape_detail(self, entry: dict) -> dict | None:
        url = entry["url"]
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            "求人ID": entry.get("job_id", ""),
            "求人タイトル": entry.get("title", ""),
            "雇用形態": entry.get("employment", ""),
        }

        # 詳細ページの h3 から会社名・タイトルを上書き(より新鮮な値を取得)
        h3 = soup.select_one("h3")
        if h3:
            company = h3.select_one(".company_name")
            title = h3.select_one(".kyuujin_title")
            if company:
                data[Schema.NAME] = _clean(company.get_text())
            if title and not data.get("求人タイトル"):
                data["求人タイトル"] = _clean(title.get_text())

        # 詳細ページの雇用形態(より正確)
        emp_items = soup.select(".koyoukeitai_box li")
        if emp_items:
            emp_text = " / ".join(_clean(li.get_text()) for li in emp_items if _clean(li.get_text()))
            if emp_text:
                data["雇用形態"] = emp_text

        # 一覧から取得した会社名でフォールバック
        if not data.get(Schema.NAME) and entry.get("company"):
            data[Schema.NAME] = entry["company"]

        # dl/dt/dd 形式の詳細フィールド群
        contact_addr = ""
        for dl in soup.select("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = _clean(dt.get_text())
            val = _clean(dd.get_text(" "))

            if key == "職種":
                data["職種"] = val
            elif key == "仕事内容":
                data["仕事内容"] = val
            elif key == "資格・経験":
                data["資格・経験"] = val
            elif key == "給与":
                data["給与"] = val
            elif key == "勤務曜日・時間":
                data["勤務曜日・時間"] = val
            elif key == "勤務地":
                data["勤務地"] = val
            elif key == "交通アクセス":
                data["交通アクセス"] = val
            elif key == "休日・休暇":
                data["休日・休暇"] = val
            elif key == "待遇":
                data["待遇"] = val
            elif key == "備考":
                data["備考"] = val
            elif key == "応募方法":
                data["応募方法"] = val
            elif key == "連絡先住所":
                contact_addr = val
            elif key == "応募先TEL":
                data[Schema.TEL] = val
            elif key == "採用担当":
                data["採用担当"] = val

        # 連絡先住所を 都道府県 + 住所 にパース(会社本社の住所として使用)
        if contact_addr:
            m = _PREF_PATTERN.match(contact_addr)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = contact_addr[m.end():].strip()
            else:
                data[Schema.ADDR] = contact_addr

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TokyoBmRecruitScraper()
    scraper.execute(LISTING_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

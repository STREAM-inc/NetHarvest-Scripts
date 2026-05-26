"""
女の転職type — 求人情報スクレイパー

取得対象:
    - woman-type.jp の求人掲載中の企業・求人情報
    - 詳細ページ（/job-offer/{ID}/）の会社情報・募集要項

取得フロー:
    1. /job-area/district{N}/p{P}/ を district1〜9 まで巡回（40件/ページ）
    2. 各ページの <section> 内 <a href="/job-offer/{ID}/"> から詳細URLを収集（job_id でユニーク化）
    3. 詳細ページの div.details (h3/h4 + .detail-right) を解析して Schema / EXTRA_COLUMNS に格納

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/type_2.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id type_2
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

BASE_URL = "https://woman-type.jp"
DISTRICTS = list(range(1, 10))  # district1 (北海道・東北) ～ district9 (沖縄)
MAX_PAGES_PER_DISTRICT = 80     # 安全側の上限。実際は district3 が最大で約52ページ

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_JOB_OFFER_PATTERN = re.compile(r"/job-offer/(\d+)/")
_POST_CODE_PATTERN = re.compile(r"〒\s*(\d{3})-?(\d{4})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _multiline(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t]+", " ", str(s)).strip()


class Type2JobScraper(StaticCrawler):
    """女の転職type スクレイパー（woman-type.jp）"""

    DELAY = 1.5
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "求人タイトル",
        "キャッチコピー",
        "雇用形態",
        "給与",
        "勤務地",
        "応募資格",
        "勤務時間",
        "仕事内容",
        "一日の仕事の流れ",
        "仕事の魅力",
        "アピールポイント本文",
        "アピールポイントタグ",
        "育児と両立しやすい",
        "休日休暇",
        "待遇・福利厚生・その他",
        "売上高",
        "選考プロセス",
        "採用担当",
        "関連リンク",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        collected_urls: list[str] = []

        # 全 district のリストページから job-offer URL を収集
        for d in DISTRICTS:
            for page in range(1, MAX_PAGES_PER_DISTRICT + 1):
                if page == 1:
                    list_url = f"{BASE_URL}/job-area/district{d}/"
                else:
                    list_url = f"{BASE_URL}/job-area/district{d}/p{page}/"

                soup = self.get_soup(list_url)
                if soup is None:
                    self.logger.warning("リスト取得失敗: %s", list_url)
                    break

                page_ids: list[str] = []
                for a in soup.find_all("a", href=_JOB_OFFER_PATTERN):
                    m = _JOB_OFFER_PATTERN.search(a.get("href", ""))
                    if not m:
                        continue
                    jid = m.group(1)
                    if jid in seen_ids:
                        continue
                    seen_ids.add(jid)
                    page_ids.append(jid)
                    collected_urls.append(f"{BASE_URL}/job-offer/{jid}/")

                self.logger.info(
                    "district%d pg%d: 新規 %d件 (累計 %d件)",
                    d, page, len(page_ids), len(collected_urls),
                )

                # 0件の場合はそのdistrictの終端
                if not page_ids and page > 1:
                    break
                # 次ページが pager に無ければ終わり
                next_link = soup.find("a", href=re.compile(rf"/job-area/district{d}/p{page + 1}/"))
                if not next_link:
                    self.logger.info("district%d pg%d: 次ページなし、district終了", d, page)
                    break

        self.total_items = len(collected_urls)
        self.logger.info("詳細URL収集完了: %d件", self.total_items)

        for detail_url in collected_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s / %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 会社名 (上部ヘッダ)
        name_el = soup.select_one(".company-name")
        if name_el:
            data[Schema.NAME] = _clean(name_el.get_text())

        # 求人タイトル / キャッチコピー
        h1 = soup.find("h1")
        if h1:
            data["求人タイトル"] = _clean(h1.get_text())
        catch = soup.find("h2")
        if catch and catch.get_text(strip=True):
            data["キャッチコピー"] = _clean(catch.get_text())

        # アピールポイントタグ (.appeal-point li.on)
        appeal_tags = [li.get_text(strip=True) for li in soup.select(".appeal-point li.on")]
        if appeal_tags:
            data["アピールポイントタグ"] = " / ".join(appeal_tags)

        # 各 .details ブロック (h3 or h4 ラベル + .detail-right)
        sections: dict[str, str] = {}
        for d in soup.select(".details"):
            h = d.select_one("h3, h4")
            v = d.select_one(".detail-right")
            if not h or not v:
                continue
            key = _clean(h.get_text())
            # h3 (大見出し: 仕事内容/募集要項/会社概要/応募・選考) は h4 と重複するためスキップ
            if h.name == "h3" and key in ("仕事内容", "募集要項", "会社概要", "応募・選考"):
                inner_h4 = v.select_one("h4")
                if inner_h4:
                    continue
            val = _multiline(v.get_text("\n", strip=True))
            if not val:
                continue
            if key not in sections:
                sections[key] = val

        # 募集要項
        if "仕事内容" in sections:
            data["仕事内容"] = sections["仕事内容"]
        if "一日の仕事の流れ" in sections:
            data["一日の仕事の流れ"] = sections["一日の仕事の流れ"]
        if "仕事の魅力" in sections:
            data["仕事の魅力"] = sections["仕事の魅力"]
        if "アピールポイント" in sections:
            data["アピールポイント本文"] = sections["アピールポイント"]
        if "雇用形態" in sections:
            data["雇用形態"] = sections["雇用形態"]
        if "給与" in sections:
            data["給与"] = sections["給与"]
        if "勤務地" in sections:
            data["勤務地"] = sections["勤務地"]
        if "応募資格" in sections:
            data["応募資格"] = sections["応募資格"]
        if "勤務時間" in sections:
            data["勤務時間"] = sections["勤務時間"]
        if "育児と両立しやすい" in sections:
            data["育児と両立しやすい"] = sections["育児と両立しやすい"]
        if "休日休暇" in sections:
            data["休日休暇"] = sections["休日休暇"]
        if "待遇・福利厚生・その他" in sections:
            data["待遇・福利厚生・その他"] = sections["待遇・福利厚生・その他"]

        # 会社概要
        if "会社名" in sections and not data.get(Schema.NAME):
            data[Schema.NAME] = _clean(sections["会社名"])
        if "事業内容" in sections:
            data[Schema.LOB] = sections["事業内容"]
        if "設立" in sections:
            data[Schema.OPEN_DATE] = _clean(sections["設立"])
        if "資本金" in sections:
            data[Schema.CAP] = _clean(sections["資本金"])
        if "売上高" in sections:
            data["売上高"] = _clean(sections["売上高"])
        if "従業員数" in sections:
            data[Schema.EMP_NUM] = _clean(sections["従業員数"])
        if "代表者" in sections:
            data[Schema.REP_NM] = _clean(sections["代表者"])

        # 応募・選考
        if "選考プロセス" in sections:
            data["選考プロセス"] = sections["選考プロセス"]
        if "関連リンク" in sections:
            data["関連リンク"] = sections["関連リンク"]

        # 問い合わせブロック (HP / 〒 / 住所 / 担当 / TEL)
        inq_h4 = next(
            (h for h in soup.find_all("h4") if h.get_text(strip=True) == "問い合わせ"),
            None,
        )
        if inq_h4:
            container = inq_h4.find_parent("div", class_="details")
            rh = container.select_one(".detail-right") if container else None
            if rh is not None:
                # HP リンク (外部 http(s))
                hp_a = rh.select_one('a[href^="http"]')
                if hp_a:
                    data[Schema.HP] = hp_a.get("href", "").strip()

                text = rh.get_text("\n", strip=True)

                # 郵便番号
                m_post = _POST_CODE_PATTERN.search(text)
                if m_post:
                    data[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"
                    # 〒以降から住所抽出
                    after = text[m_post.end():]
                    addr_line = ""
                    for line in after.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("担当") or line.lower().startswith("tel"):
                            break
                        addr_line += line
                    addr_line = addr_line.strip()
                    if addr_line:
                        m_pref = _PREF_PATTERN.search(addr_line)
                        if m_pref:
                            data.setdefault(Schema.PREF, m_pref.group(1))
                        data[Schema.ADDR] = addr_line

                # 担当者
                m_tan = re.search(r"担当者\s*[／/]\s*([^\n]+)", text)
                if m_tan:
                    data["採用担当"] = _clean(m_tan.group(1))

                # TEL — 数字列を抽出
                m_tel = re.search(r"tel\s*[／/]\s*([^\n]+)", text, re.IGNORECASE)
                if m_tel:
                    digits = re.sub(r"[^0-9-]", " ", m_tel.group(1))
                    nums = [d for d in digits.split() if re.fullmatch(r"[0-9-]{8,}", d)]
                    if nums:
                        data[Schema.TEL] = nums[0]

        # 求人タイトルが無く Schema.NAME も無い場合はスキップ
        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Type2JobScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

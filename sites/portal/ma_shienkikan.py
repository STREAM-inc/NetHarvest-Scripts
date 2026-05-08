# scripts/sites/portal/ma_shienkikan.py
"""
M&A支援機関登録制度データベース (ma-shienkikan.go.jp) — 支援機関スクレイパー

取得対象:
    - 一覧テーブル: 支援機関名、本店所在地、M&A専従者数、情報共有加盟有無、手数料体系
    - 詳細モーダル(Livewire API): 法人番号、代表者、住所、HP、資本金、従業員数、業種 等

取得フロー:
    1. 一覧ページ (?sort=corporate_name_kana&page=N) をページ送り
    2. 各行のLivewire IDを取得し、Livewire APIで詳細モーダルHTMLを取得
    3. 一覧 + 詳細をマージして yield

実行方法:
    # ローカルテスト
    python scripts/sites/portal/ma_shienkikan.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id ma_shienkikan
"""

import json
import re
import sys
import html as html_module
from pathlib import Path

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://ma-shienkikan.go.jp"
LIST_URL = BASE_URL + "/search?sort=corporate_name_kana&page={page}"
LIVEWIRE_URL = BASE_URL + "/livewire/message/frontend.published-card"


class MaShienkikanScraper(StaticCrawler):
    """M&A支援機関登録制度 支援機関スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "M&A支援機関の種類", "支援業務提供地域", "M&A支援業務開始時期",
        "M&A専従者数", "成約実績数", "情報共有加盟有無",
        # 一覧テーブル: 手数料体系
        "FA譲渡_成功報酬", "FA譲渡_算定方式", "FA譲渡_最低手数料", "FA譲渡_その他手数料",
        "FA譲受_算定方式",
        "仲介譲渡_成功報酬", "仲介譲渡_算定方式", "仲介譲渡_最低手数料", "仲介譲渡_その他手数料",
        "仲介譲受_算定方式",
    ]

    def _setup(self):
        super()._setup()
        self.session.verify = False

    def parse(self, url: str):
        # 1ページ目を取得して総件数・コンポーネント状態を確認
        resp = self.session.get(LIST_URL.format(page=1), timeout=self.TIMEOUT)
        resp.raise_for_status()
        page_html = resp.text
        soup = bs4.BeautifulSoup(page_html, "html.parser")

        # CSRF トークン取得
        csrf_meta = soup.select_one('meta[name="csrf-token"]')
        self._csrf_token = csrf_meta["content"] if csrf_meta else ""

        # 総件数
        total_text = soup.find(string=re.compile(r"全\s*[\d,]+\s*件"))
        total_items = 0
        if total_text:
            m = re.search(r"全\s*([\d,]+)\s*件", total_text)
            if m:
                total_items = int(m.group(1).replace(",", ""))
        self.logger.info("総件数: %d", total_items)
        self.total_items = total_items

        # 1ページ目を処理
        yield from self._process_page(page_html, soup)

        # 2ページ目以降
        total_pages = (total_items + 49) // 50 if total_items > 0 else 1
        for page in range(2, total_pages + 1):
            try:
                resp = self.session.get(LIST_URL.format(page=page), timeout=self.TIMEOUT)
                resp.raise_for_status()
                page_html = resp.text
                soup = bs4.BeautifulSoup(page_html, "html.parser")
                # CSRF更新
                csrf_meta = soup.select_one('meta[name="csrf-token"]')
                if csrf_meta:
                    self._csrf_token = csrf_meta["content"]
                yield from self._process_page(page_html, soup)
            except Exception as e:
                self.logger.warning("ページ %d の処理に失敗: %s", page, e)
                continue

    def _process_page(self, page_html: str, soup):
        """1ページ分の一覧テーブル解析 + 各行の詳細取得"""

        # published-card コンポーネントの初期状態を取得
        pub_card_state = self._extract_component_state(page_html, "frontend.published-card")

        # テーブル行を解析
        tbody = soup.select_one("table tbody")
        if not tbody:
            return

        for tr in tbody.select("tr"):
            tds = tr.select("td")
            if len(tds) < 15:
                continue

            # --- 一覧テーブルから取得 ---
            # wire:click="togglePublishedCardModal(ID)" からIDを抽出
            name_tag = tds[1].select_one("a[wire\\:click]")
            item_id = None
            if name_tag:
                m = re.search(r"togglePublishedCardModal\((\d+)\)", name_tag.get("wire:click", ""))
                if m:
                    item_id = int(m.group(1))

            name = name_tag.get_text(strip=True) if name_tag else tds[1].get_text(strip=True)
            pref = tds[2].get_text(strip=True)
            emp_ma = tds[3].get_text(strip=True)
            info_share = tds[4].get_text(strip=True)

            # td[5]-td[14]: 手数料体系
            fa_joto_seiko    = tds[5].get_text(strip=True)
            fa_joto_santei   = tds[6].get_text(strip=True)
            fa_joto_saitei   = tds[7].get_text(strip=True)
            fa_joto_sonota   = tds[8].get_text(strip=True)
            fa_joju_santei   = tds[9].get_text(strip=True)
            chu_joto_seiko   = tds[10].get_text(strip=True)
            chu_joto_santei  = tds[11].get_text(strip=True)
            chu_joto_saitei  = tds[12].get_text(strip=True)
            chu_joto_sonota  = tds[13].get_text(strip=True)
            chu_joju_santei  = tds[14].get_text(strip=True)

            item = {
                Schema.NAME: name,
                Schema.PREF: pref,
                "M&A専従者数": emp_ma,
                "情報共有加盟有無": info_share,
                "FA譲渡_成功報酬": fa_joto_seiko,
                "FA譲渡_算定方式": fa_joto_santei,
                "FA譲渡_最低手数料": fa_joto_saitei,
                "FA譲渡_その他手数料": fa_joto_sonota,
                "FA譲受_算定方式": fa_joju_santei,
                "仲介譲渡_成功報酬": chu_joto_seiko,
                "仲介譲渡_算定方式": chu_joto_santei,
                "仲介譲渡_最低手数料": chu_joto_saitei,
                "仲介譲渡_その他手数料": chu_joto_sonota,
                "仲介譲受_算定方式": chu_joju_santei,
            }

            # --- Livewire API で詳細データ取得 ---
            if item_id and pub_card_state:
                detail = self._fetch_detail(item_id, pub_card_state)
                if detail:
                    item.update(detail)

            yield item

    def _extract_component_state(self, page_html: str, component_name: str) -> dict | None:
        """ページHTMLから指定コンポーネントのLivewire初期状態を抽出"""
        for match in re.finditer(r'wire:initial-data="([^"]+)"', page_html):
            data = json.loads(html_module.unescape(match.group(1)))
            if data.get("fingerprint", {}).get("name") == component_name:
                return data
        return None

    def _fetch_detail(self, item_id: int, pub_card_state: dict) -> dict | None:
        """Livewire API で詳細モーダルHTMLを取得し、データを抽出"""
        payload = {
            "fingerprint": pub_card_state["fingerprint"],
            "serverMemo": pub_card_state["serverMemo"],
            "updates": [{
                "type": "fireEvent",
                "payload": {
                    "id": f"detail-{item_id}",
                    "event": "refreshApplicationModel",
                    "params": [item_id],
                },
            }],
        }
        headers = {
            "Content-Type": "application/json",
            "X-Livewire": "true",
            "X-CSRF-TOKEN": self._csrf_token,
            "Referer": LIST_URL.format(page=1),
        }

        try:
            resp = self.session.post(LIVEWIRE_URL, json=payload, headers=headers, timeout=self.TIMEOUT)
            resp.raise_for_status()
            result = resp.json()
        except Exception as e:
            self.logger.warning("詳細取得失敗 (ID=%d): %s", item_id, e)
            return None

        detail_html = result.get("effects", {}).get("html", "")
        if not detail_html:
            return None

        return self._parse_detail_html(detail_html)

    def _parse_detail_html(self, detail_html: str) -> dict:
        """詳細モーダルHTMLからデータ項目を抽出"""
        soup = bs4.BeautifulSoup(detail_html, "html.parser")
        data = {}

        # グリッド行 (ラベル: sm:grid sm:grid-cols-3) からキー・バリューを抽出
        for grid in soup.select("div.sm\\:grid"):
            label_el = grid.select_one("p")
            value_el = grid.select("span.sm\\:col-span-2, a.sm\\:col-span-2")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True).replace("\n", "")
            # <br> を除去した後のテキスト
            value = value_el[-1].get_text(strip=True) if value_el else ""

            if label == "法人番号":
                data[Schema.CO_NUM] = value
            elif label == "代表者氏名":
                data[Schema.REP_NM] = value
            elif label == "本店所在地":
                # 〒XXX-XXXX 都道府県... のパターン
                post_m = re.search(r"〒(\d{3}-?\d{4})", value)
                if post_m:
                    data[Schema.POST_CODE] = post_m.group(1)
                # 郵便番号部分を除去して住所を取得
                addr = re.sub(r"〒\d{3}-?\d{4}\s*", "", value).strip()
                data[Schema.ADDR] = addr
            elif label == "会社HP":
                href_tag = grid.select_one("a[href]")
                data[Schema.HP] = href_tag["href"] if href_tag else value
            elif label == "資本金":
                data[Schema.CAP] = value
            elif label == "従業員数":
                data[Schema.EMP_NUM] = value
            elif label == "支援業務提供都道府県":
                data["支援業務提供地域"] = value
            elif label == "M&A支援業務開始時期":
                data["M&A支援業務開始時期"] = value
            elif label in ("M&A支援業務専従者の従業員数",):
                pass  # 一覧テーブルから取得済み
            elif "成約実績" in label:
                data["成約実績数"] = value
            elif "情報共有" in label:
                pass  # 一覧テーブルから取得済み

        # 業種（グリッド外）
        for div in soup.select("div.inline-block.text-sm"):
            span_label = div.select_one("span.text-gray-800")
            if not span_label:
                continue
            label_text = span_label.get_text(strip=True)
            value_span = div.select("span")[-1] if div.select("span") else None
            if not value_span:
                continue
            value_text = value_span.get_text(strip=True)
            if label_text == "業種":
                data[Schema.CAT_SITE] = value_text
            elif label_text == "M&A支援機関の種類":
                data["M&A支援機関の種類"] = value_text

        # 事業主名（個人事業主の場合）
        for grid in soup.select("div.sm\\:grid"):
            label_el = grid.select_one("p")
            if label_el and "事業主名" in label_el.get_text():
                value_el = grid.select_one("span.sm\\:col-span-2")
                if value_el:
                    data[Schema.REP_NM] = value_el.get_text(strip=True)

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    scraper = MaShienkikanScraper()
    scraper.execute("https://ma-shienkikan.go.jp/search?sort=corporate_name_kana")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

import re
import ssl
import sys
from pathlib import Path
from typing import Generator

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class _LegacySSLAdapter(HTTPAdapter):
    """古いSSL再ネゴシエーション(RFC5746以前)を使うサーバー用アダプター"""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= 0x4  # SSL_OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)

BASE_URL = "https://hikariweb.ntt-east.co.jp/general/search"
STATUS = "東日本特約店・委託販売店"

PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


class HikaridwebSearchScraper(StaticCrawler):
    """NTT東日本特約店・販売委託店検索スクレイパー

    調査フェーズ: あ行〜わ行(keyword=1-10)の全一覧ページから詳細URLを収集。
    実行フェーズ: 各詳細ページをスクレイピング。
    多拠点企業(show_hide)は非表示テーブルから全拠点リンクを展開して収集。
    """

    DELAY = 1.5
    EXTRA_COLUMNS = ["部課・支店", "委託元", "販売区分", "備考"]

    def _setup(self):
        super()._setup()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = _LegacySSLAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def parse(self, url: str) -> Generator[dict, None, None]:
        # Phase 1: 調査フェーズ
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("調査フェーズ完了: %d 件の詳細URLを収集", len(detail_urls))

        # Phase 2: 実行フェーズ
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("スキップ: %s — %s", detail_url, e)

    def _collect_detail_urls(self) -> list[str]:
        """Phase 1: 全一覧ページから詳細URLを重複なく収集"""
        urls: list[str] = []
        seen: set[str] = set()

        for keyword in range(1, 11):
            list_url = f"{BASE_URL}/list.php?keyword={keyword}"
            soup = self.get_soup(list_url)
            if soup is None:
                continue

            shoplist = soup.find("table", class_="shoplist")
            if not shoplist:
                continue

            for cont_td in shoplist.find_all("td", class_="cont"):
                inner_tr = cont_td.find("tr")
                if not inner_tr:
                    continue
                center_td = inner_tr.find("td", class_="center")
                if not center_td:
                    continue
                link = center_td.find("a")
                if not link:
                    continue

                href = link.get("href", "")

                if "detail.php" in href:
                    full_url = f"{BASE_URL}/{href.lstrip('/')}"
                    if full_url not in seen:
                        seen.add(full_url)
                        urls.append(full_url)

                elif "show_hide" in href:
                    # 多拠点: 非表示テーブル(id="tb{N}")から全拠点リンクを展開
                    m = re.search(r"show_hide\((\d+)\)", href)
                    if not m:
                        continue
                    tb_id = f"tb{m.group(1)}"
                    hidden_table = soup.find(id=tb_id)
                    if not hidden_table:
                        continue
                    for sub_link in hidden_table.find_all("a", href=re.compile(r"detail\.php")):
                        sub_href = sub_link.get("href", "")
                        full_url = f"{BASE_URL}/{sub_href.lstrip('/')}"
                        if full_url not in seen:
                            seen.add(full_url)
                            urls.append(full_url)

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        """Phase 2: 詳細ページから全フィールドを取得"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        table = soup.find("table", class_="shopdetail")
        if not table:
            return None

        data: dict = {Schema.URL: url, Schema.STS_NM: STATUS}

        for row in table.find_all("tr"):
            left = row.find("td", class_="left")
            right = row.find("td", class_="right")
            if not left or not right:
                continue

            key = left.get_text(strip=True)
            val = right.get_text(" ", strip=True).strip()

            if "特約店名" in key:
                data[Schema.NAME] = val

            elif "部課・支店" in key:
                data["部課・支店"] = val

            elif "住所" in key:
                self._parse_address(right, data)

            elif "電話番号" in key:
                data[Schema.TEL] = val

            elif "URL" in key:
                a = right.find("a")
                hp = (a.get("href") or "").strip() if a else ""
                data[Schema.HP] = hp if hp and hp != "#" else ""

            elif "備考" in key:
                data["備考"] = val
                if "委託元特約店" in val:
                    data["販売区分"] = "販売委託店"
                    m = re.search(r"委託元特約店\s*(.+)", val)
                    if m:
                        data["委託元"] = m.group(1).strip()
                elif val:
                    data["販売区分"] = "特約店"

        if not data.get(Schema.NAME):
            return None
        return data

    def _parse_address(self, td, data: dict) -> None:
        text = td.get_text(" ", strip=True)
        m = re.search(r"(〒?\d{3}-\d{4})", text)
        if m:
            digits = re.sub(r"\D", "", m.group(1))
            if len(digits) == 7:
                data[Schema.POST_CODE] = f"〒{digits[:3]}-{digits[3:]}"
            addr_text = text[m.end():].strip()
        else:
            addr_text = text

        pref = next((p for p in PREFS if p in addr_text), "")
        if pref:
            data[Schema.PREF] = pref
            idx = addr_text.index(pref)
            data[Schema.ADDR] = addr_text[idx + len(pref):].strip()
        else:
            data[Schema.ADDR] = addr_text


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HikaridwebSearchScraper().execute(f"{BASE_URL}/list.php?keyword=1")

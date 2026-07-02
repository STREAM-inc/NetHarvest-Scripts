"""
CIIC 経営事項審査結果の公表 — 一般財団法人 建設業情報管理センター

取得対象:
    建設業者の経営事項審査結果の公表データ (商号名称検索の検索結果一覧)。
    許可番号・商号名称・代表者・所在地・新旧区分・審査基準日・大臣知事区分を取得する。

取得フロー:
    1. ルート url (https://www.ciic.or.jp/) から公表システム (www7.ciic.or.jp) への
       リンクを辿ってシステム URL を取得する (別ルート URL はハードコードしない)。
    2. システム内の「商号名称検索」フォームページを辿り、検索 index の URL・入力欄名・
       大臣知事区分の選択肢 (都道府県コード) をページから取得する。
    3. 大臣知事区分 (00 国土交通大臣 + 01〜47 各都道府県) × 商号名称カナ先頭一文字 で
       GET 検索し、検索結果一覧の各行を取得次第 yield する。ページ送りは「次頁」リンクを辿る。

備考 (取得方針):
    - 各社の「詳細」ボタンは PDF (application/pdf) を POST で返すため、財務諸表等の
      詳細データは本クローラーの対象外 (BeautifulSoup で解析できないため除外)。
    - 保存する値は許可番号・日付・区分コード・氏名・住所等の構造化情報のみ。
      自由記述の文章カラムは持たない。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ciic_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ciic_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 商号名称カナ先頭一文字 (清音・濁音・半濁音・長音記号相当を網羅)。
# 経審 DB の商号カナ読みはカタカナに正規化されているため、この集合で先頭一致検索を回すと
# ほぼ全件を列挙できる。
_KANA_HEADS = [
    "ア",
    "イ",
    "ウ",
    "エ",
    "オ",
    "カ",
    "キ",
    "ク",
    "ケ",
    "コ",
    "サ",
    "シ",
    "ス",
    "セ",
    "ソ",
    "タ",
    "チ",
    "ツ",
    "テ",
    "ト",
    "ナ",
    "ニ",
    "ヌ",
    "ネ",
    "ノ",
    "ハ",
    "ヒ",
    "フ",
    "ヘ",
    "ホ",
    "マ",
    "ミ",
    "ム",
    "メ",
    "モ",
    "ヤ",
    "ユ",
    "ヨ",
    "ラ",
    "リ",
    "ル",
    "レ",
    "ロ",
    "ワ",
    "ヲ",
    "ン",
    "ガ",
    "ギ",
    "グ",
    "ゲ",
    "ゴ",
    "ザ",
    "ジ",
    "ズ",
    "ゼ",
    "ゾ",
    "ダ",
    "ヂ",
    "ヅ",
    "デ",
    "ド",
    "バ",
    "ビ",
    "ブ",
    "ベ",
    "ボ",
    "パ",
    "ピ",
    "プ",
    "ペ",
    "ポ",
    "ヴ",
]

# 所在地から都道府県を切り出すためのリスト
_PREFECTURES = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]

# 詳細ボタンの onclick から審査基準日・許可番号を取り出す正規表現
_RE_KYOKA = re.compile(r"target_kyoka_bangou'\)\.val\(\"([^\"]*)\"")
_RE_KIJUNBI = re.compile(r"target_sinsa_kijunbi'\)\.val\(\"([^\"]*)\"")

# フォーム構造が変わった場合のフォールバック (通常は実ページから取得する)
_FALLBACK_INDEX_PATH = "cHVibGljX3NlYXJjaGVzL2luZGV4"
_FALLBACK_KANA_FIELD = (
    "logical_public_searches[shougou_meishou_attributes][shougou_meishou_kana]"
)
_FALLBACK_KUBUN_FIELD = (
    "logical_public_searches[shougou_meishou_attributes][daijin_tiji_kubun_kana]"
)

# 1 検索あたりのページ送り安全上限 (無限ループ防止)
_MAX_PAGES_PER_QUERY = 2000


def _clean(text) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("　", " ")).strip()


def _split_pref(address: str) -> tuple[str, str]:
    """所在地から (都道府県, それ以降) を返す。"""
    for pref in _PREFECTURES:
        if address.startswith(pref):
            return pref, address[len(pref) :].strip()
    return "", address


class CiicKeishinScraper(StaticCrawler):
    """CIIC 経営事項審査結果の公表 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "許可番号",  # 例: 13-011554
        "大臣知事区分",  # 例: 13東京都知事許可 / 00国土交通大臣許可
        "新旧区分",  # 例: 1 / 2 / 3 (申請時期を表す区分コード)
        "審査基準日",  # 例: 2025-06-30
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # --- 1. ルート url から公表システム (www7) の URL を辿る ---
        top = self.get_soup(url)
        if top is None:
            self.logger.error("ルートページを取得できませんでした: %s", url)
            return

        system_url = None
        for a in top.find_all("a", href=True):
            if "www7.ciic.or.jp" in a["href"]:
                system_url = a["href"]
                break
        if not system_url:
            self.logger.error("公表システムへのリンクが見つかりません (url=%s)", url)
            return
        self.logger.info("公表システム URL: %s", system_url)

        # --- 2. 検索フォームの index URL・入力欄名・大臣知事区分の選択肢を取得 ---
        index_url, kana_field, kubun_field, kubun_options = self._discover_form(
            system_url
        )
        if not kubun_options:
            self.logger.error("大臣知事区分の選択肢を取得できませんでした")
            return
        self.logger.info(
            "検索 index=%s / 区分 %d 種 / カナ %d 種",
            index_url,
            len(kubun_options),
            len(_KANA_HEADS),
        )

        # --- 3. 大臣知事区分 × カナ先頭一文字 で GET 検索し、取得次第 yield ---
        for code, label in kubun_options:
            for kana in _KANA_HEADS:
                query = urlencode(
                    {
                        kana_field: kana,
                        kubun_field: code,
                        "shougou_meishou_search": "検索開始",
                    }
                )
                page_url = f"{index_url}?{query}"
                yield from self._crawl_result_pages(page_url, system_url, label)

    # ------------------------------------------------------------------
    # フォーム情報の取得 (別ルート URL はハードコードしない)
    # ------------------------------------------------------------------

    def _discover_form(self, system_url: str):
        """システムトップ → 商号名称検索フォームを辿り、検索に必要な情報を返す。"""
        index_url = urljoin(system_url, _FALLBACK_INDEX_PATH)
        kana_field = _FALLBACK_KANA_FIELD
        kubun_field = _FALLBACK_KUBUN_FIELD
        kubun_options: list[tuple[str, str]] = []

        home = self.get_soup(system_url)
        form_page_url = None
        if home is not None:
            for a in home.find_all("a", href=True):
                img = a.find("img")
                alt = (img.get("alt") if img else "") or a.get_text(strip=True)
                if "商号名称検索" in alt and "入力方法" not in alt:
                    form_page_url = urljoin(system_url, a["href"])
                    break

        form_soup = self.get_soup(form_page_url) if form_page_url else home
        if form_soup is not None:
            form = None
            for f in form_soup.find_all("form"):
                if f.find("input", {"name": re.compile("shougou_meishou_kana")}):
                    form = f
                    break
            if form is not None:
                if form.get("action"):
                    index_url = urljoin(system_url, form["action"])
                kana_inp = form.find(
                    "input", {"name": re.compile("shougou_meishou_kana")}
                )
                kubun_sel = form.find(
                    "select", {"name": re.compile("daijin_tiji_kubun_kana")}
                )
                if kana_inp and kana_inp.get("name"):
                    kana_field = kana_inp["name"]
                if kubun_sel and kubun_sel.get("name"):
                    kubun_field = kubun_sel["name"]
                if kubun_sel:
                    for opt in kubun_sel.find_all("option"):
                        val = (opt.get("value") or "").strip()
                        # 2桁の都道府県コード (00〜47) のみ対象。空欄・99(全て) は除外
                        if re.fullmatch(r"\d{2}", val) and val != "99":
                            kubun_options.append((val, _clean(opt.get_text())))

        return index_url, kana_field, kubun_field, kubun_options

    # ------------------------------------------------------------------
    # 検索結果ページ (ページ送り含む) の巡回
    # ------------------------------------------------------------------

    def _crawl_result_pages(self, page_url: str, system_url: str, kubun_label: str):
        seen_pages: set[str] = set()
        pages = 0
        while page_url and page_url not in seen_pages and pages < _MAX_PAGES_PER_QUERY:
            seen_pages.add(page_url)
            pages += 1
            soup = self.get_soup(page_url)
            if soup is None:
                return
            yield from self._parse_results(soup, page_url, kubun_label)

            # 「次頁」リンク (GET) を辿る
            next_href = None
            for a in soup.find_all("a", href=True):
                if "次頁" in a.get_text():
                    next_href = a["href"]
                    break
            page_url = urljoin(system_url, next_href) if next_href else None

    def _parse_results(self, soup, page_url: str, kubun_label: str):
        table = soup.find("table", class_="table-search-index")
        if table is None:
            return
        rows = table.find_all("tr")
        for i, tr in enumerate(rows):
            link = tr.find("a", id=re.compile(r"^submit_link_"))
            if not link:
                continue
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue
            try:
                permit = _clean(tds[1].get_text())
                name = _clean(tds[2].get_text())
                shinkyu = _clean(tds[4].get_text())
                if not name:
                    continue

                onclick = link.get("onclick", "")
                m_kijunbi = _RE_KIJUNBI.search(onclick)
                kijunbi = m_kijunbi.group(1) if m_kijunbi else ""

                # 直後2行が代表者・所在地
                rep = ""
                address = ""
                if i + 1 < len(rows):
                    c = rows[i + 1].find_all("td")
                    if c:
                        rep = _clean(c[0].get_text())
                if i + 2 < len(rows):
                    c = rows[i + 2].find_all("td")
                    if c:
                        address = _clean(c[0].get_text())

                pref, addr_rest = _split_pref(address)

                yield {
                    Schema.NAME: name,
                    Schema.URL: page_url,
                    Schema.PREF: pref,
                    Schema.ADDR: addr_rest,
                    Schema.REP_NM: rep,
                    "許可番号": permit,
                    "大臣知事区分": kubun_label,
                    "新旧区分": shinkyu,
                    "審査基準日": kijunbi,
                }
            except Exception as e:
                self.logger.warning("レコード解析失敗: %s", e)
                continue


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CiicKeishinScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    公表システム (www7) の URL はこのトップページ内のリンクから parse() が導出する。
    scraper.execute("https://www.ciic.or.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

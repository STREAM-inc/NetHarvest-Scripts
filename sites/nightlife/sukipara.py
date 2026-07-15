"""
すきパラネット — すすきの（札幌）のナイト系・グルメ・美容・その他店舗情報総合サイト

取得対象:
    - 全ジャンルの掲載店舗（ニュークラブ/パブスナック/キャバクラ/ガールズバー/
      萌えカフェ/ホストクラブ/飲食店/美容サロン/その他）約340件の店舗情報

取得フロー:
    1. ルート URL から各ジャンル一覧 (shop_list.php?gid=N / shop_list_{beauty,gourmet,other}.php)
       とエリア一覧 (shop_list.php?aid=N) を urljoin で導出
    2. 各一覧を ?page=N で巡回し、店舗詳細リンク (shop/shop.php?id=N) を収集（ID で重複排除）
    3. 各詳細ページを取得し、取得のたびに即 yield（Pattern B / 早期 yield）

件数と一覧構成について（過去に 90 件しか取れなかった主因の記録）:
    この PHP サイトは「現在の表示ページ番号」をサーバ側セッションに保持している。しかも
    そのセッション状態は Cookie(PHPSESSID) だけでなく keep-alive の TCP コネクションにも
    紐づいており、一覧の末尾（範囲外ページ）を 1 度でも取得するとセッションが汚染され、
    以後 別一覧が空(0 件)を返し続ける。session.cookies.clear() だけでは復旧せず、
    コネクションプールごと作り直した新しい requests.Session でのみ復旧する。
    → 一覧(list path)ごとに self.session を作り直して汚染を遮断する(_reset_session)。
    加えて gid ジャンル一覧はナイト系の一部（約90件・掲載店のみ）しか出さないため、
    飲食/美容/その他ジャンル一覧と、全業種を地理で網羅する aid エリア一覧を併用して
    重複排除しながら約300件を取り切る（旧実装は汚染により約90件で頭打ちだった）。

    加えて、この WAF は再利用された keep-alive コネクション上のリクエストを高確率で
    403 にする（詳細ページの約半数が 403 になっていた）。_reset_session で
    `Connection: close` を付与し keep-alive を無効化して回避している。

利用規約について:
    rules.php（本サイトのご利用について）を確認済み。スクレイピング/クローリング/自動アクセスを
    明示的に禁止する条項は存在しない。ただし著作権条項でコンテンツの無断複製・転載・二次利用は
    禁止されているため、取得データの再配布・転載時は要注意（運営: 株式会社シーズ北海道）。

文字コード:
    全ページ EUC-JP。ただしレスポンスヘッダが Content-Type: text/html; charset=none を返すため
    StaticCrawler 既定の get_soup では文字化けする。get_soup を override し euc-jp を強制する。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/sukipara.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id sukipara
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# ジャンル一覧ページ（ルート URL からの相対パス）。gid=1..6 + 名前付き 3 ページを網羅する。
# gid=5(萌えカフェ) はトップのナビには出ないため明示的に列挙して取りこぼしを防ぐ。
_LIST_PATHS = [
    "shop_list.php?gid=1",  # ニュークラブ
    "shop_list.php?gid=2",  # パブスナック
    "shop_list.php?gid=3",  # キャバクラ
    "shop_list.php?gid=4",  # ガールズバー
    "shop_list.php?gid=5",  # 萌えカフェ
    "shop_list.php?gid=6",  # ホストクラブ
    "shop_list_gourmet.php",  # 飲食店
    "shop_list_beauty.php",   # 美容サロン
    "shop_list_other.php",    # その他業種
    # エリア一覧（全業種を地理で網羅）。gid ジャンル一覧に出ない店舗を補完する。
    "shop_list.php?aid=1",  # すすきの中心部
    "shop_list.php?aid=2",  # 周辺エリア
    "shop_list.php?aid=3",  # その他エリア
]

_ID_RE = re.compile(r"[?&]id=(\d+)")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)
# Google Maps InfoWindow の content 属性内に店舗公式サイトの href が入る（安定取得源）
_INFOWINDOW_RE = re.compile(r"InfoWindow\(\{\s*content:\s*\"(.*?)\"\s*\}\)", re.S)
_HREF_IN_JS_RE = re.compile(r"""href=['"]([^'"]+)['"]""")

_SNS_PATTERNS = {
    Schema.INSTA: re.compile(r"https?://[^\s'\"]*instagram\.com/[^\s'\"]+", re.I),
    Schema.X: re.compile(r"https?://[^\s'\"]*(?:twitter\.com|x\.com)/[^\s'\"]+", re.I),
    Schema.FB: re.compile(r"https?://[^\s'\"]*facebook\.com/[^\s'\"]+", re.I),
    Schema.TIKTOK: re.compile(r"https?://[^\s'\"]*tiktok\.com/[^\s'\"]+", re.I),
    Schema.LINE: re.compile(r"https?://[^\s'\"]*(?:line\.me|lin\.ee)/[^\s'\"]+", re.I),
}
# ネットワーク共通フッターリンク（各店舗の公式サイトではないので HP から除外）
_HP_EXCLUDE = ("sukipara.net", "manzoku.or.jp", "cluman.co.jp", "yukai-life.jp", "google.")


class SukiparaCrawler(StaticCrawler):
    """すきパラネット スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def _reset_session(self) -> None:
        """一覧ごとに requests.Session を作り直し、サーバ側ページ状態の汚染を遮断する。

        このサイトは表示ページ番号を keep-alive コネクションに紐づけて保持するため、
        cookies.clear() では復旧しない。コネクションプールごと新規セッションを張り直す。
        _setup() と同じリトライ/User-Agent 設定を適用する。

        さらに `Connection: close` を付与して keep-alive を無効化する。この WAF は
        再利用された keep-alive コネクション上のリクエストを高確率で 403 にするため
        (詳細ページの約半数が 403 になっていた)、毎回コネクションを閉じて回避する。
        """
        if self.session is not None:
            self.session.close()
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": self.USER_AGENT, "Connection": "close"})

    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        # Content-Type: charset=none のため既定判定では化ける。EUC-JP を強制する。
        try:
            response = self.session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()
            response.encoding = "euc-jp"
            return bs4.BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
                return None
            self.logger.error("通信エラー: %s", e)
            raise

    def parse(self, url: str):
        seen_ids: set[str] = set()  # 全一覧横断の重複排除（詳細を二度取得しない）

        for path in _LIST_PATHS:
            # 一覧ごとにセッションを張り直す。前一覧末尾の範囲外ページで汚染された
            # サーバ側ページ状態を捨て、この一覧を 1 ページ目から正しく取得するため。
            self._reset_session()

            list_base = urljoin(url, path)
            page = 1
            list_ids: set[str] = set()  # この一覧内で出現済みの ID（巡回終了判定に使う）
            while True:
                page_url = f"{list_base}&page={page}" if "?" in list_base else f"{list_base}?page={page}"
                list_soup = self.get_soup(page_url)
                if list_soup is None:
                    break

                # このページに載っている全店舗 ID（一覧内の新規判定はページ表示単位で行う）
                page_all_ids = []
                for a in list_soup.select("a[href*='shop/shop.php?id=']"):
                    m = _ID_RE.search(a.get("href", ""))
                    if m:
                        page_all_ids.append(m.group(1))

                # この一覧でまだ見ていない ID が 1 つも無い → 末尾(空ページ) or 短い一覧の
                # ページ番号クランプ（同じ内容の再表示）。当一覧の巡回を終了する。
                if not any(sid not in list_ids for sid in page_all_ids):
                    break
                list_ids.update(page_all_ids)

                # 全一覧横断でまだ詳細を取っていない店舗だけを yield 対象にする
                for sid in page_all_ids:
                    if sid in seen_ids:
                        continue
                    seen_ids.add(sid)
                    detail_url = urljoin(url, f"shop/shop.php?id={sid}")
                    try:
                        item = self._scrape_detail(detail_url)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.error("Failed %s: %s", detail_url, e)
                        continue

                page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one("#shopdata_name")
        if name_el is None:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.PREF: "北海道",  # 札幌・すすきの専門サイト。住所に都道府県が無い場合の既定値
        }

        # 名称カナ: 名称 h3 直前の <p>（ふりがな）
        kana_el = name_el.find_previous_sibling("p")
        if kana_el is not None:
            item[Schema.NAME_KANA] = kana_el.get_text(strip=True)

        # 情報テーブル: 業種 / 住所 / T E L / 営業時間 / 定休日（th→td）
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is None or td is None:
                continue
            header = re.sub(r"\s+", "", th.get_text())
            value = td.get_text(" ", strip=True)
            if not value:
                continue
            if header == "業種":
                item[Schema.CAT_SITE] = value
            elif header == "住所":
                m = _PREF_RE.match(value)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = value[m.end():].strip()
                else:
                    item[Schema.ADDR] = value
            elif header == "TEL":
                item[Schema.TEL] = value
            elif header == "営業時間":
                item[Schema.TIME] = value
            elif header == "定休日":
                item[Schema.HOLIDAY] = value

        # ページ全体の文字列（<script> 含む）から HP / SNS を抽出
        page_text = str(soup)

        # HP: Google Maps InfoWindow content 内の href（店舗公式サイト）
        mw = _INFOWINDOW_RE.search(page_text)
        if mw:
            for href in _HREF_IN_JS_RE.findall(mw.group(1)):
                if href.startswith("http") and not any(x in href for x in _HP_EXCLUDE):
                    item[Schema.HP] = href
                    break

        # SNS: 店舗紹介文中に埋め込まれた各 SNS URL（本文プロースは取得しない）
        for col, pat in _SNS_PATTERNS.items():
            m = pat.search(page_text)
            if m:
                item[col] = m.group(0).rstrip("\"'")

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SukiparaCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.sukipara.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

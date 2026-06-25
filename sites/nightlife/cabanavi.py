"""
キャバナビ (cabanavi.info) — 全国のキャバクラ・ガールズバー店舗スクレイパー

取得対象:
    - 店名 / 都道府県 / 住所 / TEL / 業種 / 営業時間 / 定休日 / HP
    - エリア / 最寄り駅 / 客席数 / 在籍数 / 平均年齢 / 衣装 / カラオケ
    - 最低料金 / TAXサービス料

取得フロー:
    1. ルート url (/shops/tohoku/) の親 /shops/ を基点に、全国 8 地方ブロック
       (北海道/東北/関東/北信越/東海/関西/中国・四国/九州・沖縄) の一覧 url を
       urljoin で派生させて順に巡回する
    2. 各地方の一覧ページ (/shops/{region}/?page=N) をページ送り (10件/ページ)
    3. 各 div.shopInfo からエリア・最寄り駅・詳細URL を取得
    4. 詳細ページ /shop/{id}/ の dl.detailRow__dl と table.detailRow__table から店舗情報を抽出
    5. 1件取得するごとに即 yield (Pattern B / 早期 yield)。詳細URLで重複排除

レート制限対策 (適応ペーシング / AIMD):
    cabanavi は nginx の limit_req が極端に厳しく、バーストを使い切ると HTTP 429 を返す
    (バースト容量〜10・補充レートは極遅)。重要なのは「補充レートに合わせて一定間隔で
    リクエストし続ければ 429 をほぼ踏まない」点。

    旧実装は _fetch_soup() の度に backoff を初期値(1.5秒)へ毎回リセットしていたため、
    新しい URL ごとに高速で投げて → バースト枯渇 → 429 → 3,6,12,24,48,60秒の長大バックオフ、
    を「1件ごとに」繰り返していた。学習したレートが次の URL に引き継がれず、1件あたり
    数分かかっていた。

    本実装ではペース秒数 self._delay をインスタンスに保持し、リクエスト間で共有する:
      - 通常時: 毎リクエスト前に self._delay だけ待機 (補充レートへの追従)
      - 429/503: self._delay を乗算で引き上げ (Retry-After があれば尊重) てから再試行
      - 成功時: self._delay を徐々に減衰 (floor=_BASE_DELAY) し、安全な範囲で再加速を試す
    これで全体が「持続可能なレート」へ収束し、429 の連発と長大待機を回避して全国 8 地方を
    効率良く巡回する。limit_req は同時接続に弱いため、並列化はせず逐次のまま据え置く。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/cabanavi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id cabanavi
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests
from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"\d{2,4}-\d{2,4}-\d{4}")
_TOTAL_PATTERN = re.compile(r"(\d+)件")


# 適応ペーシング (AIMD) パラメータ。self._delay をリクエスト間で共有し、
# nginx limit_req の補充レートへ収束させる。
_BASE_DELAY = 2.0      # 通常時の最小ペース秒数 (= self._delay の floor / 初期値)
_MAX_DELAY = 30.0      # ペース秒数の上限 (これ以上は引き上げない)
_DELAY_GROWTH = 1.6    # 429/503 のたびにペースを乗算で引き上げる係数 (additive-ish increase)
_DELAY_DECAY = 0.85    # 成功のたびにペースを乗算で戻す係数 (multiplicative decrease)
_MAX_RETRIES = 6       # 同一 URL に対する 429/503 再試行回数の上限
_RETRY_AFTER_CAP = 60.0  # Retry-After ヘッダを尊重する際の一回あたり待機上限

# 全国を網羅する 8 地方ブロックの slug。ルート url (/shops/tohoku/) の親 /shops/ を
# 基点に urljoin で各地方の一覧 url を派生させる (ルート url 自体もこの中に含まれる)。
_REGION_SLUGS = [
    "hokkaido",         # 北海道
    "tohoku",           # 東北
    "kanto",            # 関東
    "hokushinetsu",     # 北信越
    "tokai",            # 東海
    "kansai",           # 関西
    "chugoku-shikoku",  # 中国・四国
    "kyushu-okinawa",   # 九州・沖縄
]


class CabanaviScraper(StaticCrawler):
    """キャバナビ スクレイパー"""

    DELAY = 0  # フレームワーク yield 後 sleep は使わず parse() 内で手動管理
    EXTRA_COLUMNS = [
        "エリア", "最寄り駅",
        "客席数", "在籍数", "平均年齢", "衣装", "カラオケ",
        "最低料金", "TAXサービス料",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一の起点とし、その親 /shops/ から全国 8 地方の一覧 url を派生。
        self.total_items = 0
        self._seen_urls: set[str] = set()
        # リクエスト間で共有する適応ペース秒数。429 のたびに上げ、成功のたびに下げる。
        self._delay = _BASE_DELAY
        for region_url in self._region_urls(url):
            yield from self._parse_listing(region_url)

    def _region_urls(self, url: str) -> list[str]:
        """ルート url (.../shops/{region}/) の親 /shops/ を基点に全地方の一覧 url を生成。"""
        base = urljoin(url, "../")  # 例: https://cabanavi.info/shops/
        return [urljoin(base, f"{slug}/") for slug in _REGION_SLUGS]

    def _fetch_soup(self, url: str) -> BeautifulSoup | None:
        """全 HTTP 取得の単一窓口。適応ペース (self._delay) で待機し、429/503 を再試行。

        self._delay はインスタンス属性でリクエスト間に共有される。429/503 のたびに乗算で
        引き上げ、成功のたびに乗算で引き下げる (AIMD) ことで、nginx limit_req の持続可能な
        レートへ収束させる。旧実装のように毎リクエストで初期値へリセットしないため、一度
        レートを学習すれば次以降の URL では 429 をほぼ踏まず、長大バックオフを繰り返さない。
        """
        # parse() を経由しない直接呼び出し (テスト等) でも安全に動くよう遅延初期化。
        if not hasattr(self, "_delay"):
            self._delay = _BASE_DELAY

        for attempt in range(_MAX_RETRIES + 1):
            time.sleep(self._delay)  # 全リクエスト前に現在のペースで待機 (limit_req 追従)
            try:
                resp = self.session.get(url, timeout=self.TIMEOUT)
            except requests.exceptions.RequestException as e:
                self._delay = min(self._delay * _DELAY_GROWTH, _MAX_DELAY)
                self.logger.warning(
                    "通信エラー (再試行 ペース%.1fs): url=%s (%s)", self._delay, url, e,
                )
                continue

            if resp.status_code in (429, 503):
                # レート超過: 次回以降のペースを引き上げ、今回は追加で待ってから再試行。
                self._delay = min(self._delay * _DELAY_GROWTH, _MAX_DELAY)
                wait = self._retry_after(resp, default=self._delay)
                self.logger.info(
                    "レート制限 HTTP %s — ペースを%.1fsへ引き上げ、%.1f秒待機して再試行: %s",
                    resp.status_code, self._delay, wait, url,
                )
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                self.logger.warning("HTTP %s (スキップ): %s", resp.status_code, url)
                return None

            # 成功: ペースを少し戻し (floor=_BASE_DELAY)、安全な範囲で再加速を試す。
            self._delay = max(self._delay * _DELAY_DECAY, _BASE_DELAY)

            content_type = resp.headers.get("Content-Type", "")
            if "charset=" not in content_type.lower():
                resp.encoding = resp.apparent_encoding
            return BeautifulSoup(resp.text, "html.parser")

        self.logger.warning("リトライ上限に到達、スキップ: %s", url)
        return None

    def _retry_after(self, resp: requests.Response, default: float) -> float:
        """Retry-After ヘッダ (秒数指定) があれば尊重。無ければ default を返す。"""
        raw = resp.headers.get("Retry-After", "")
        if raw:
            try:
                return min(float(raw), _RETRY_AFTER_CAP)
            except ValueError:
                pass  # HTTP-date 形式は無視し default を使う
        return default

    def _parse_listing(self, region_url: str) -> Generator[dict, None, None]:
        first_soup = self._fetch_soup(region_url)
        if first_soup is None:
            return

        total_el = first_soup.select_one("p.mb-10.pc")
        if total_el:
            m = _TOTAL_PATTERN.search(total_el.get_text())
            if m:
                self.total_items = (self.total_items or 0) + int(m.group(1))

        page = 1
        while True:
            if page == 1:
                soup = first_soup
            else:
                soup = self._fetch_soup(f"{region_url}?page={page}")
            if soup is None:
                break

            items = soup.select("div.shopInfo")
            if not items:
                break

            for item in items:
                name_a = item.select_one(".shopInfo__name a")
                if not name_a or not name_a.get("href"):
                    continue

                # 相対 href も region_url から絶対化 (詳細 url も url 起点で派生)。
                detail_url = urljoin(region_url, name_a["href"])
                if detail_url in self._seen_urls:
                    continue
                self._seen_urls.add(detail_url)

                ps = item.select(".shopInfo__name p.typography-body-1")
                area_genre = self._text(ps[0]) if ps else ""
                area, _ = self._split_area_genre(area_genre)
                station = self._text(ps[1]) if len(ps) >= 2 else ""

                try:
                    record = self._scrape_detail(detail_url, area=area, station=station)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: url=%s (%s)", detail_url, e)
                    continue

                if record:
                    yield record

            next_link = soup.select_one(f'a[href*="?page={page + 1}"]')
            if not next_link:
                break
            page += 1

    def _scrape_detail(self, detail_url: str, area: str = "", station: str = "") -> dict | None:
        soup = self._fetch_soup(detail_url)
        if soup is None:
            return None

        dl_data = self._extract_dl(soup)
        tbl_data = self._extract_table(soup)

        name = self._clean(dl_data.get("店名", ""))
        if not name:
            self.logger.warning("店名が空: %s", detail_url)
            return None

        address = self._clean(dl_data.get("住所", ""))
        pref, addr_body = self._split_pref(address)

        tel_raw = self._clean(dl_data.get("電話番号", ""))
        tel = self._extract_tel(tel_raw)

        hp = self._extract_hp(soup, dl_data)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._clean(dl_data.get("業種", "")),
            Schema.TIME: self._clean(dl_data.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(dl_data.get("定休日", "")),
            Schema.HP: hp,
            "エリア": area,
            "最寄り駅": station,
            "客席数": tbl_data.get("客席数", ""),
            "在籍数": tbl_data.get("在籍数", ""),
            "平均年齢": tbl_data.get("平均年齢", ""),
            "衣装": tbl_data.get("衣装", ""),
            "カラオケ": tbl_data.get("カラオケ", ""),
            "最低料金": tbl_data.get("最低料金", ""),
            "TAXサービス料": tbl_data.get("TAX・サービス料", ""),
        }

    def _extract_dl(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        for dl in soup.select("dl.detailRow__dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if dt and dd:
                label = dt.get_text(strip=True)
                if label and label not in data:
                    data[label] = dd.get_text(" ", strip=True)
        return data

    def _extract_table(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        for table in soup.select("table.detailRow__table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    label = self._norm_label(th.get_text(strip=True))
                    if label and label not in data:
                        data[label] = td.get_text(" ", strip=True)
        return data

    def _norm_label(self, text: str) -> str:
        """◆・▼ などの装飾プレフィックスを除去し、スペースを詰める。"""
        return re.sub(r"^[◆▼■●◇▽□○★☆※\s]+", "", text or "").strip()

    def _extract_hp(self, soup: BeautifulSoup, dl_data: dict[str, str]) -> str:
        """ホームページ欄の <a href> を優先し、なければテキスト値を返す。"""
        for dl in soup.select("dl.detailRow__dl"):
            dt = dl.find("dt")
            if dt and "ホームページ" in dt.get_text(strip=True):
                dd = dl.find("dd")
                if dd:
                    a = dd.find("a", href=True)
                    if a:
                        return a["href"].strip()
                    return self._clean(dd.get_text(" ", strip=True))
        return ""

    def _extract_tel(self, raw: str) -> str:
        m = _TEL_PATTERN.search(raw or "")
        return m.group(0) if m else self._clean(raw)

    def _split_area_genre(self, text: str) -> tuple[str, str]:
        """'仙台・国分町/ガールズバー' → ('仙台・国分町', 'ガールズバー')"""
        text = self._clean(text)
        if "/" in text:
            parts = text.split("/", 1)
            return parts[0].strip(), parts[1].strip()
        return text, ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _text(self, node) -> str:
        if node is None:
            return ""
        return self._clean(node.get_text(" ", strip=True))

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CabanaviScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えると挙動がズレる。
    scraper.execute("https://cabanavi.info/shops/tohoku/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

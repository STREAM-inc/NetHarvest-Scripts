"""
引越し侍 (hikkoshizamurai) — 全国の引越し業者ポータル 会社概要スクレイパー

取得対象:
    - 引越し侍に掲載されている全国の引越し業者 (会社概要ページ /company/d-{code}/)
    - 各社の会社概要 (会社名 / 本社所在地 / 電話番号 / 設立・創業 / 資本金 /
      代表者 / 従業員数 / 事業内容 / ホームページ / 対応エリア)

取得フロー:
    1. ルート URL (/company) を取得し、ページ内の都道府県エリアリンク
       (/ranking/area/{region}/{pref}/) を動的に抽出する。
       ※ 全業者の一括リストは公開されていないため、都道府県ごとに探索する。
    2. 各都道府県ページを取得し、業者詳細リンク (/company/d-{code}/) を抽出。
       全都道府県を横断して seen 集合でグローバルに重複排除する。
    3. 新規の業者を 1 件見つけるたびに会社概要ページを取得して即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別ドメイン/別 URL はハードコードしない。
    - 会社紹介・特徴などの自由記述 PR 文は著作権リスクのため取得しない。
      会社概要テーブルの構造化項目 (住所・電話・設立等) のみを取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/hikkoshizamurai.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id hikkoshizamurai
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


# 住所先頭から都道府県を切り出すためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 都道府県エリアページ (/ranking/area/{region}/{pref}/)。region のみのリンクは除外。
_AREA_PATTERN = re.compile(r"/ranking/area/\d+/[a-z]+/?$")

# 業者詳細ページ識別子 (/company/d-{code}/)。voice/interview/gallery 等の配下は基底へ正規化。
_DETAIL_PATTERN = re.compile(r"/company/(d-[A-Za-z0-9_]+)/")

# 会社概要テーブルのラベル → Schema 定数 の対応 (部分一致)。
# 値が短い構造化項目のみを対象とし、自由記述 PR 文 (紹介文/特徴) は取得しない。
_SCHEMA_LABELS = [
    (("会社名", "社名", "正式名称", "企業名", "商号"), Schema.NAME),
    (("本社所在地", "所在地", "本社住所", "住所", "本社"), Schema.ADDR),
    (("電話番号", "電話", "TEL", "フリーダイヤル"), Schema.TEL),
    (("設立", "創業", "会社設立"), Schema.OPEN_DATE),
    (("資本金",), Schema.CAP),
    (("代表者", "代表取締役", "代表"), Schema.REP_NM),
    (("従業員数", "社員数", "従業員"), Schema.EMP_NUM),
    (("事業内容", "業務内容"), Schema.LOB),
    (("ホームページ", "公式サイト", "ウェブサイト", "URL", "ＵＲＬ", "HP"), Schema.HP),
]

# Schema に無いサイト固有の構造化項目 (短い構造化情報のみ。自由記述は取得しない)
_COL_LICENSE = "免許番号"
_COL_AREA = "対応エリア"


class Hikkoshizamurai(StaticCrawler):
    """引越し侍 全国引越し業者 会社概要スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_LICENSE, _COL_AREA]

    def parse(self, url: str):
        root = self.get_soup(url)
        if root is None:
            return

        # 1. ルートページから都道府県エリアページを動的抽出 (全業者一括リストは無い)
        area_urls = []
        seen_area = set()
        for a in root.select("a[href]"):
            href = a.get("href", "")
            if _AREA_PATTERN.search(href):
                au = urljoin(url, href)
                if au not in seen_area:
                    seen_area.add(au)
                    area_urls.append(au)

        # 2. 各都道府県ページを巡回し、業者詳細を 1 件見つけるたびに即 yield
        seen_codes = set()
        for area_url in area_urls:
            area_soup = self.get_soup(area_url)
            if area_soup is None:
                continue

            for a in area_soup.select("a[href]"):
                m = _DETAIL_PATTERN.search(a.get("href", ""))
                if not m:
                    continue
                code = m.group(1)
                if code in seen_codes:
                    continue
                seen_codes.add(code)

                detail_url = urljoin(url, f"/company/{code}/")
                fallback_name = a.get_text(strip=True)
                try:
                    item = self._scrape_detail(detail_url, fallback_name)
                    if item:
                        yield item
                except Exception as e:  # 個別業者の失敗は握りつぶして継続
                    self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                    continue

    def _scrape_detail(self, detail_url: str, fallback_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 会社概要テーブル/定義リストから {ラベル: 値} を収集
        labels: dict[str, str] = {}
        hp_value = ""
        for th, td in self._iter_label_value(soup):
            label = th.get_text(strip=True)
            if not label:
                continue
            text = td.get_text(" ", strip=True)
            if label not in labels and text:
                labels[label] = text
            # HP はリンク href を優先
            if not hp_value:
                link = td.select_one("a[href]")
                if link and re.search(r"ホームページ|公式|URL|ＵＲＬ|HP", label):
                    hp_value = link.get("href", "")

        item = {Schema.URL: detail_url}

        for keywords, const in _SCHEMA_LABELS:
            value = self._match_label(labels, keywords)
            if value:
                item[const] = value

        # HP はリンク href があればそちらを採用
        if hp_value:
            item[Schema.HP] = hp_value

        # NAME フォールバック: 会社概要に無ければ H1 (「○○の会社概要」)、それも無ければ一覧リンク文言
        if not item.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                name = re.sub(r"の会社概要$", "", h1.get_text(strip=True)).strip()
                if name:
                    item[Schema.NAME] = name
        if not item.get(Schema.NAME) and fallback_name:
            item[Schema.NAME] = fallback_name

        # 住所から都道府県を切り出し
        addr = item.get(Schema.ADDR, "")
        if addr:
            m = _PREF_PATTERN.search(addr)
            if m:
                item[Schema.PREF] = m.group(1)

        # 免許番号 (国土交通事業者番号など。短い構造化コードのみ)
        license_no = self._match_label(labels, ("免許番号", "事業者番号", "許可番号"))
        if license_no:
            item[_COL_LICENSE] = license_no

        # 対応エリア (構造化された短い項目のみ)
        area = self._match_label(labels, ("対応エリア", "営業エリア", "サービスエリア"))
        if area:
            item[_COL_AREA] = area

        return item

    @staticmethod
    def _iter_label_value(soup):
        """「会社概要」テーブル (th を含む) の th/td ペアのみを返す。

        口コミ評価・料金相場・地域別件数 (dl) など会社概要以外の表は対象外とし、
        会社名 th を持つ profile テーブルに限定することで誤マッピングを防ぐ。
        """
        profile = None
        for table in soup.select("table"):
            ths = [th.get_text(strip=True) for th in table.find_all("th")]
            if any("会社名" in t or "社名" in t for t in ths):
                profile = table
                break
        # フォールバック: 「会社概要」見出し直後の最初のテーブル
        if profile is None:
            for heading in soup.find_all(["h2", "h3"]):
                if "会社概要" in heading.get_text(strip=True):
                    profile = heading.find_next("table")
                    break
        if profile is None:
            return
        for tr in profile.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                yield th, td

    @staticmethod
    def _match_label(labels: dict[str, str], keywords) -> str:
        for label, value in labels.items():
            if any(kw in label for kw in keywords):
                return value
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Hikkoshizamurai()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    #    正規ドメインは .jp (.com は NXDOMAIN で到達不可)。
    scraper.execute("https://hikkoshizamurai.com/company")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

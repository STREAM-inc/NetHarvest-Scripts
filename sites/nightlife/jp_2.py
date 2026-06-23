"""
スタンバイ (jp.stanby.com) — ナイトワーク求人 スクレイパー

取得対象:
    LINEヤフーグループの求人検索エンジン「スタンバイ」のナイトワーク系
    カテゴリ求人 (キャバクラ / クラブ / パブスナック / ホストクラブ /
    フロアレディ / カウンターレディ / スナックレディ / ナイトマネージャー)。

取得フロー (一覧 + 詳細 / Pattern B):
    各ナイトワークカテゴリの検索結果ページ (Nuxt SSR で求人カードが
    HTML に直接描画される) を 1 ページずつ取得し、求人カード
    (div.job-list-item) を解析する。一覧カードには電話番号が無いため、
    各求人の詳細ページ (/jobs/<hash>) も取得し、「仕事内容」本文
    (div.card-text に SSR 描画される自由記述プロース) の下のほうに
    記載された TEL を拾ったうえで 1 件ずつ即 yield する。詳細ページは
    SSR 描画のため静的取得が可能。

ページネーション:
    パスベース。1 ページ目 = /r_<hash> , 2 ページ目以降 = /r_<hash>/{n}。
    求人カードが 0 件になったページで打ち切る。

ルート URL (SSOT = sites.yml):
    parse() は引数 url (= https://jp.stanby.com/) を唯一の起点とし、
    各カテゴリ URL は urljoin(url, "<hash>") で派生させる。8 つのカテゴリ
    ハッシュは「ナイトワーク」配下のカテゴリ識別子 (パスセグメント) であり、
    ホスト部は常に引数 url から導出される。

フィルタ方針 (備考: 「ナイトワークのスクレイピング」):
    対象 8 URL 自体がスタンバイの「ナイトワーク / 夜職」カテゴリページで
    あり、これがユーザー指定のナイトワーク範囲。カテゴリページには稀に
    キーワード一致の他業種プロモーション枠 (例: スポーツクラブ / リゾート
    クラブ) が混入するが、混入広告と正規のナイト求人 (例: CLUB RITZ 等) を
    構造的に分離できる目印が無く、誤って正規求人を除外するリスクが高い。
    そのため強制フィルタは実装せず、カテゴリ名を CAT_SITE に格納して
    下流での絞り込みを可能にする。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/jp_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jp_2
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# ── 「ナイトワーク / 夜職」配下のカテゴリ識別子 (パスセグメント) ──────────
# 引数 url (= https://jp.stanby.com/) に urljoin して各カテゴリ検索ページを
# 構築する。ホスト部はハードコードせず常に引数 url から導出する。
NIGHTWORK_CATEGORY_PATHS = [
    "r_ca266f4af0dd6759dfd77f0d94972e77",  # キャバクラ
    "r_acce19b43ed8bd96eeaf3dfd4d86173e",  # クラブ
    "r_50a64d18115765bd573569b469792b51",  # パブスナック
    "r_191c2aa8920599387683ae15d004abf7",  # ホストクラブ
    "r_5c2539cdd8866937f56dc1746d50ac92",  # フロアレディ
    "r_79cc4616f3811e6a3a26b27223d28e29",  # カウンターレディ
    "r_987c99303acf66ed91d5cbd60342b63f",  # スナックレディ
    "r_810c8ea36616d56ab7443b029bcc9f95",  # ナイトマネージャー
]

# 無限ループ防止の安全弁 (通常は求人カード 0 件で自然に打ち切る)。
MAX_PAGES_PER_CATEGORY = 500

# 求人カードの属性アイコン → 意味のマッピング。
# 並び順ではなくアイコンクラスで判定する (順序・有無が変動するため)。
_ATTR_ICON = {
    "icn-distance": "loc",       # 勤務地 + 最寄り駅
    "icn-money": "pay",          # 給与
    "icn-bag": "emp",            # 雇用形態
    "icn-calendar-clock": "freq",  # 勤務条件 (シフト / 休日 等)
}

# 都道府県の先頭マッチ (勤務地から PREF を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|"
    r"岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 電話番号の抽出パターン。詳細ページ「仕事内容」の自由記述本文から拾う
# (固定電話 / 携帯 / フリーダイヤル / 市外局番カッコ表記に対応)。
# 掲載が無い求人も多く、その場合は空文字となる。
_TEL_PATTERN = re.compile(
    r"(?<![\d-])"
    r"(?:"
    r"0\d{1,4}-\d{1,4}-\d{3,4}"          # 03-1234-5678 / 090-1234-5678 / 0120-12-3456
    r"|\(0\d{1,4}\)\s?\d{1,4}-?\d{3,4}"  # (03)1234-5678
    r")"
    r"(?![\d-])"
)

# 「仕事内容」本文中の TEL ラベル。【 TEL 】 / 【TEL】 / TEL: / 電話番号 / 電話 等に対応。
# ラベル直後に現れる電話番号を最優先で採用する。
_TEL_LABEL = re.compile(r"(?:【\s*TEL\s*】|TEL|電話番号|電話)\s*[:：]?\s*", re.I)


def _pick_tel(text: str) -> str:
    """テキストから電話番号を抽出する。無ければ空文字。

    「仕事内容」では本文の下のほうに【 TEL 】等のラベル付きで電話番号が
    記載されるため、まずラベル直後の番号を優先して拾う。ラベルが無い／
    ラベル直後に番号が見つからない場合は、本文中で最初に現れる電話番号を
    採用する。
    """
    if not text:
        return ""
    # 1) TEL / 電話 ラベル直後の番号を最優先 (本文下部の連絡先を狙う)
    for m in _TEL_LABEL.finditer(text):
        chunk = text[m.end():m.end() + 80]
        pm = _TEL_PATTERN.search(chunk)
        if pm:
            return _clean(pm.group(0))
    # 2) フォールバック: 本文中で最初に現れる電話番号
    m = _TEL_PATTERN.search(text)
    return _clean(m.group(0)) if m else ""


def _clean(s) -> str:
    """空白 (全角 U+3000 含む) を 1 つにまとめてトリム。"""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _job_key(link: str, company: str, title: str) -> str:
    """求人の安定した重複判定キーを作る。

    /jobs/<hash> はハッシュ、広告リダイレクト (c.stanby.com/v2/adpage) は
    id= パラメータを使う。どちらも取れなければ会社名+タイトルで代替する。
    広告 URL の tm= (タイムスタンプ) はリクエスト毎に変わるため鍵に使わない。
    """
    m = re.search(r"/jobs/([0-9a-f]{16,})", link)
    if m:
        return "j:" + m.group(1)
    try:
        qs = parse_qs(urlparse(link).query)
        if qs.get("id"):
            return "a:" + qs["id"][0]
    except Exception:
        pass
    return "k:" + _clean(company) + "|" + _clean(title)


class StanbyNightWorkScraper(StaticCrawler):
    """スタンバイ (jp.stanby.com) ナイトワーク求人 スクレイパー"""

    DELAY = 1.5
    # 参考スクレイピングカラムとの対応 (求人検索エンジンの一覧カードに載る項目のみ取得):
    #   名称→Schema.NAME / 都道府県→Schema.PREF / 住所→Schema.ADDR /
    #   業種→Schema.CAT_SITE は標準カラムへマップする。
    #   TEL→Schema.TEL は詳細ページ (/jobs/<hash>) の「仕事内容」本文の
    #   下のほうに記載された電話番号を拾う (掲載が無い求人は出力されない)。
    #   郵便番号・法人番号・代表者・代表者役職・資本金・売上・従業員数・
    #   設立日・事業内容・FAX・メール・HP・Instagram・Facebook・X・LINE公式 は
    #   一覧カードに掲載が無く取得できないため出力されない (Pipeline 側で
    #   観測されなかったカラムは CSV に書き出されない)。
    #   エリアは Schema 定数が無いため EXTRA_COLUMNS で保持する。
    EXTRA_COLUMNS = [
        "エリア",         # 勤務地の市区町村 (都道府県より下位のエリア)
        "求人タイトル",   # 募集職種の見出し (短いヘッドライン)
        "給与",           # 時給/日給/月給/年収 + 諸手当 (構造化された短文)
        "雇用形態",       # アルバイト・パート / 正社員 等
        "勤務条件",       # シフト制 / 単発 / 完全週休2日制 等の条件ラベル
        "最寄り駅",       # 勤務地に併記される最寄り駅・アクセス
        "特徴タグ",       # 未経験OK / 寮完備 / 即日払いOK 等の特徴ラベル
        "求人リンク",     # 求人個別リンク (応募/詳細への URL)
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for cat_path in NIGHTWORK_CATEGORY_PATHS:
            yield from self._parse_category(url, cat_path, seen)

    def _parse_category(self, url: str, cat_path: str, seen: set) -> Generator[dict, None, None]:
        category = ""
        page = 1
        while page <= MAX_PAGES_PER_CATEGORY:
            list_url = urljoin(url, cat_path) if page == 1 else urljoin(url, f"{cat_path}/{page}")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                category = self._extract_category(soup)
                # 進捗表示用に最初のカテゴリの総件数を控えめにセット
                if not getattr(self, "total_items", None):
                    cnt = soup.select_one("em.count-text")
                    if cnt:
                        digits = re.sub(r"[^\d]", "", cnt.get_text())
                        if digits:
                            self.total_items = int(digits)

            cards = soup.select("div.job-list-item")
            if not cards:
                break  # これ以上ページが無い

            for card in cards:
                try:
                    item = self._parse_card(card, url, list_url, category)
                except Exception as e:
                    self.logger.warning("カード解析失敗 (%s): %s", list_url, e)
                    continue
                if not item:
                    continue
                key = item.pop("_key", "")
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                yield item

            page += 1

    def _extract_category(self, soup) -> str:
        """パンくず (JSON-LD BreadcrumbList) の末尾要素をカテゴリ名とする。"""
        for b in soup.select('script[type="application/ld+json"]'):
            try:
                d = json.loads(b.string or "")
            except Exception:
                continue
            if d.get("@type") == "BreadcrumbList":
                items = d.get("itemListElement") or []
                if items:
                    name = items[-1].get("name")
                    if name:
                        return _clean(name)
        # フォールバック: H1 "○○ の求人・仕事・採用" から切り出す
        h1 = soup.select_one("h1")
        if h1:
            return _clean(h1.get_text(" ").split("の求人")[0])
        return ""

    def _parse_card(self, card, root_url: str, list_url: str, category: str) -> dict | None:
        title_a = card.select_one("h2.title a.title-link")
        title = _clean(title_a.get_text()) if title_a else _clean(
            card.select_one("h2.title").get_text() if card.select_one("h2.title") else ""
        )
        company_el = card.select_one("p.company")
        company = _clean(company_el.get_text()) if company_el else ""

        # NAME = 店舗/企業名。無ければ求人タイトルで代替。
        name = company or title
        if not name:
            return None

        # 属性アイコンを意味ごとに収集
        attrs: dict[str, str] = {}
        for li in card.select("ul.attribution-items li"):
            icon = li.select_one("span.base-icon")
            text = li.select_one("span.text")
            if not icon or not text:
                continue
            key = next((_ATTR_ICON[c] for c in icon.get("class", []) if c in _ATTR_ICON), None)
            if key:
                attrs[key] = _clean(text.get_text())

        # 特徴タグ
        labels = [
            _clean(l.get_text())
            for l in card.select("ul.feature-label-list li span.feature-label")
            if _clean(l.get_text())
        ]

        # 求人個別リンク (絶対 URL 化)
        link = ""
        if title_a and title_a.get("href"):
            link = urljoin(root_url, title_a.get("href"))

        # TEL: 一覧カードには電話番号が無いため、詳細ページ (/jobs/<hash>) の
        # 「仕事内容」本文を取得し、その下のほうに記載された TEL を拾う。
        # (掲載が無い求人も多く、その場合は空文字となる)。
        tel = self._fetch_tel(link)

        data = {
            Schema.URL: list_url,           # データ取得元 (一覧ページ)
            Schema.NAME: name,
            Schema.CAT_SITE: category,      # サイト定義のカテゴリ (キャバクラ 等)
            Schema.TEL: tel,
            "求人タイトル": title,
            "給与": attrs.get("pay", ""),
            "雇用形態": attrs.get("emp", ""),
            "勤務条件": attrs.get("freq", ""),
            "特徴タグ": " / ".join(labels),
            "求人リンク": link,
        }

        # 勤務地 "都道府県 市区 地名 / 最寄り駅 徒歩N分" を分解
        loc = attrs.get("loc", "")
        addr_part, station = self._split_location(loc)
        m = _PREF_PATTERN.match(addr_part)
        if m:
            data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = addr_part[m.end():].strip()
        else:
            data[Schema.ADDR] = addr_part
        data["最寄り駅"] = station
        # エリア = 住所先頭の市区町村 (都道府県より下位のエリア区分)
        data["エリア"] = data.get(Schema.ADDR, "").split(" ")[0] if data.get(Schema.ADDR) else ""

        data["_key"] = _job_key(link, company, title)
        return data

    def _fetch_tel(self, detail_url: str) -> str:
        """詳細ページ (/jobs/<hash>) の「仕事内容」本文から TEL を拾う。

        本文 (自由記述プロース) は詳細ページの div.card-text に SSR 描画され、
        静的取得が可能。本文の下のほうに【 TEL 】等のラベル付きで電話番号が
        記載されるため、その番号を抽出する。

        スタンバイの /jobs/<hash> ページ以外 (広告リダイレクト等) や取得失敗時、
        本文に番号が無い求人は空文字を返す。
        """
        if not detail_url or not re.search(r"/jobs/[0-9a-f]{16,}", detail_url):
            return ""
        try:
            soup = self.get_soup(detail_url)
        except Exception as e:
            self.logger.warning("詳細ページ取得失敗 (%s): %s", detail_url, e)
            return ""
        if soup is None:
            return ""
        # 「仕事内容」本文は div.card-text に分割描画される。全本文を連結して
        # TEL ラベル → 番号の順で探す。本文が取れなければページ全体を対象にする。
        blocks = soup.select("div.card-text")
        text = "\n".join(b.get_text("\n") for b in blocks) if blocks else soup.get_text("\n")
        return _pick_tel(text)

    @staticmethod
    def _split_location(loc: str) -> tuple[str, str]:
        """勤務地文字列を (住所部, 最寄り駅部) に分解する。

        例: "東京都 新宿区 歌舞伎町 / 西武新宿駅 徒歩3分"
              -> ("東京都 新宿区 歌舞伎町", "西武新宿駅 徒歩3分")
            "富山県 富山市 総曲輪"  -> ("富山県 富山市 総曲輪", "")
            "東京都"                -> ("東京都", "")
        """
        if not loc:
            return "", ""
        parts = [p.strip() for p in loc.split("/") if p.strip()]
        if not parts:
            return "", ""
        addr = parts[0]
        station = " / ".join(parts[1:]) if len(parts) > 1 else ""
        return addr, station


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = StanbyNightWorkScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://jp.stanby.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

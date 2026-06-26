"""
WorkinFree (workin.jp) — 求人企業情報スクレイパー

取得対象:
    求人サイト「Workin.jp」の全求人から、掲載企業の企業情報
    (企業名・住所・電話番号・代表者・設立・資本金・従業員数・事業内容・HP) と、
    求人の構造化メタ情報 (職種・雇用形態・給与種別/金額・勤務地・こだわり条件 等) を収集する。

取得フロー:
    Workin.jp はトップ/エリアページが Next.js のクライアントサイドフェッチで描画され、
    実データは公開 JSON API (https://api.workin.jp/api/v1/jobs?pref={slug}&page=N&perpage=50)
    から供給される。全件一括の一覧は存在せず、都道府県エリアごとに分かれているため
    (備考の指示に準拠)、47 都道府県スラッグを順に、各エリアを page=1 から
    data が空になるまでページングし、1 件取得するごとに即 yield する (Pattern B)。

    API ホストは引数 url のホストから派生させる (workin.jp -> api.workin.jp)。
    各求人の取得URL (Schema.URL) も引数 url を起点に /{pref}/jobs/{kid}/1 を urljoin で生成する。

著作権配慮:
    キャッチコピー・PR・仕事内容本文・応募資格・待遇・応募方法・勤務時間本文・給与本文・
    メッセージ等の「自由記述プロース」は EXTRA に含めない。給与は構造化された
    種別 (時給/月給) と下限/上限金額のみを取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/workinfree.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id workinfree
"""

import html
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# Workin.jp のエリアは英語スラッグの都道府県 (/tokyo, /osaka …) に分かれている。
# 全件一括の一覧が無いため、47 都道府県スラッグを順に巡回する (備考の指示に準拠)。
_PREF_SLUGS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 1 リクエストあたりの取得件数。read timeout を避けるため小さめに固定 (<=50)。
_PERPAGE = 50

# 住所先頭の都道府県を切り出す
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|"
    r"富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|"
    r"島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)


def _clean(value) -> str:
    """HTML エンティティ・<br>・nbsp・連続空白を正規化した素のテキストを返す。"""
    if value is None:
        return ""
    s = html.unescape(str(value))
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.IGNORECASE)
    s = s.replace(" ", " ").replace("　", " ")
    return re.sub(r"\s+", " ", s).strip()


class WorkinfreeScraper(StaticCrawler):
    """WorkinFree (workin.jp) 求人企業情報スクレイパー

    都道府県エリアごとに公開 JSON API をページングし、各求人を企業情報として yield する。
    """

    DELAY = 0.5  # API は 1 リクエストで最大 50 件返すため、件あたりの待機は控えめでよい
    EXTRA_COLUMNS = [
        "屋号",
        "職種",
        "職種カテゴリ",
        "雇用形態",
        "給与種別",
        "給与下限",
        "給与上限",
        "勤務地",
        "勤務地エリア",
        "交通アクセス",
        "こだわり条件",
        "採用担当",
        "選考方法",
        "掲載開始日",
        "掲載終了日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとし、API ホストを派生させる (workin.jp -> api.workin.jp)。
        parsed = urlparse(url)
        api_base = f"{parsed.scheme}://api.{parsed.netloc}/api/v1/jobs"

        for slug in _PREF_SLUGS:
            page = 1
            while True:
                api_url = f"{api_base}?pref={slug}&perpage={_PERPAGE}&page={page}"
                try:
                    resp = self.session.get(api_url, timeout=self.TIMEOUT)
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("API取得エラー (スキップ): %s — %s", api_url, e)
                    break

                records = payload.get("data") or []
                if not records:
                    break

                self.logger.info("%s page=%d: %d件", slug, page, len(records))
                for rec in records:
                    try:
                        item = self._build_item(rec, slug, url)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning(
                            "レコード変換エラー (スキップ): kid=%s — %s",
                            rec.get("kid"), e,
                        )
                        continue
                    if item:
                        yield item

                if len(records) < _PERPAGE:
                    break
                page += 1

    def _build_item(self, rec: dict, slug: str, root_url: str) -> dict | None:
        name = _clean(rec.get("kokyaku_name"))
        if not name:
            return None

        kid = rec.get("kid")
        detail_url = urljoin(root_url, f"{slug}/jobs/{kid}/1") if kid else root_url

        data: dict = {
            Schema.URL: detail_url,
            Schema.NAME: name,
        }

        # 住所 (企業所在地) + 都道府県切り出し
        addr = _clean(f"{rec.get('addr_name2', '')}{rec.get('addr_name3', '')}")
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = addr[m.end():].strip()
            else:
                data[Schema.ADDR] = addr

        self._set(data, Schema.POST_CODE, rec.get("yubin_no"))
        self._set(data, Schema.TEL, rec.get("daihyo_tel"))
        self._set(data, Schema.REP_NM, rec.get("daihyo_name"))
        self._set(data, Schema.OPEN_DATE, rec.get("setsuritsu_date"))
        self._set(data, Schema.CAP, rec.get("shihonkin"))
        self._set(data, Schema.EMP_NUM, rec.get("jugyoinsu"))
        self._set(data, Schema.LOB, rec.get("gyoshu_naiyo"))
        self._set(data, Schema.HP, rec.get("url"))

        # --- EXTRA (構造化された短いラベル/数値のみ) ---
        self._set(data, "屋号", rec.get("yago_name"))
        self._set(data, "職種", rec.get("shokushu_name"))

        sd = rec.get("shokushu_detail") or {}
        cat_parts: list[str] = []
        for k in ("parent_shokushu_name", "shokushu_gr_name", "shokushu_name"):
            part = _clean(sd.get(k))
            if part and part not in cat_parts:
                cat_parts.append(part)
        self._set(data, "職種カテゴリ", " > ".join(cat_parts))

        self._set(data, "雇用形態", rec.get("koyokeitai_gr_name"))
        self._set(data, "給与種別", rec.get("kyuyo_name"))

        # 金額は 0 を未設定扱いにする
        for col, key in (("給与下限", "kingaku_dn"), ("給与上限", "kingaku_up")):
            amount = rec.get(key)
            if amount not in (None, "", 0, "0"):
                data[col] = str(amount)

        self._set(data, "勤務地", rec.get("map_address1"))

        areas = rec.get("areas") or []
        if areas:
            a = areas[0]
            area_txt = " ".join(
                _clean(a.get(k)) for k in ("todofuken_name", "shiku_name") if _clean(a.get(k))
            )
            self._set(data, "勤務地エリア", area_txt)

        self._set(data, "交通アクセス", rec.get("tsukin"))

        tags = rec.get("kodawari_tags") or []
        kodawari = " / ".join(_clean(t.get("name")) for t in tags if _clean(t.get("name")))
        self._set(data, "こだわり条件", kodawari)

        self._set(data, "採用担当", rec.get("kokyaku_tanto_name"))
        self._set(data, "選考方法", rec.get("senko_houhou"))

        for col, key in (("掲載開始日", "pub_start_time"), ("掲載終了日", "pub_end_time")):
            v = _clean(rec.get(key))
            if v:
                data[col] = v[:10]

        return data

    @staticmethod
    def _set(data: dict, key, value) -> None:
        cleaned = _clean(value)
        if cleaned:
            data[key] = cleaned


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WorkinfreeScraper()
    # 🔒 sites.yml に登録する url と完全一致させる (SSOT = sites.yml)。
    scraper.execute("https://workin.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

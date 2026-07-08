"""
全国警備業協会（AJSSA）会員名簿(滋賀県) — 一般社団法人滋賀県警備業協会 会員企業一覧

取得対象:
    - 滋賀県警備業協会の正会員・賛助会員（合計約113社）
    - 会員区分・会社名・電話番号・FAX番号・郵便番号・所在地・業務（区分）・HP

取得フロー:
    /enterprise/ の単一ページに 2 つのテーブル (table.tbl-member) がある。
      - 1 つ目 = 正会員一覧 (50音順)
      - 2 つ目 = 賛助会員一覧 (50音順)
    ページネーション・詳細ページは無く、各行に全項目が揃っている。
    列順: [番号, 会員名, 電話番号, FAX番号, 郵便番号, 所在地, 業務]
    会員名セルにリンクがある場合はそれが会社 HP。
    業務欄は正会員が数字コード (例 "2/1")、賛助会員が語句 (例 "コピー機販売")。
      数字コードの凡例: 1…施設 2…交通誘導 3…貴重品運搬 4…身辺 5…機械
    都道府県は所在地から抽出。市区町村のみの表記は主要政令市を補完し、
    それ以外は滋賀県協会の会員のため「滋賀県」で補完する。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/ajssa_23.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_23
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


_POST_RE = re.compile(r"(\d{3}[-‐−]\d{4})")

# 業務欄の数字コード凡例 (ページ内 【業務欄の記号について】 より)
_GYOMU_CODE = {
    "1": "施設",
    "2": "交通誘導",
    "3": "貴重品運搬",
    "4": "身辺",
    "5": "機械",
}

# 所在地が「都道府県」を冠する場合はそのまま抽出
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|"
    r"富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|"
    r"島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)

# 都道府県を冠さない政令市等の先頭表記 → 都道府県 (滋賀以外の会員を補正)
_CITY_PREF = {
    "京都市": "京都府",
    "大阪市": "大阪府",
    "堺市": "大阪府",
    "名古屋市": "愛知県",
    "神戸市": "兵庫県",
    "横浜市": "神奈川県",
    "川崎市": "神奈川県",
}

_DEFAULT_PREF = "滋賀県"


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _split_pref(addr: str):
    """所在地文字列を (都道府県, 残り住所) に分割する。"""
    if not addr:
        return "", ""
    m = _PREF_RE.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    for city, pref in _CITY_PREF.items():
        if addr.startswith(city):
            return pref, addr
    return _DEFAULT_PREF, addr


def _decode_gyomu(raw: str) -> str:
    """業務欄が数字コードなら日本語ラベルに変換 (例 '2/1' -> '交通誘導/施設')。"""
    raw = _clean(raw)
    if not raw:
        return ""
    tokens = re.split(r"[\/／,、・\s]+", raw)
    if all(tok in _GYOMU_CODE for tok in tokens if tok):
        return "/".join(_GYOMU_CODE[tok] for tok in tokens if tok)
    return raw


class Ajssa23(StaticCrawler):
    """全国警備業協会（AJSSA）会員名簿(滋賀県) スクレイパー"""

    DELAY = 1.5
    # 番号=掲載番号, FAX/会員区分/業務内容 は Schema に該当が無いためサイト固有列として保持
    EXTRA_COLUMNS = ["掲載番号", "会員区分", "FAX", "業務内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)

        tables = soup.select("table.tbl-member")
        if not tables:
            return

        # 先に全データ行を集めて total_items を確定 (取得は 1 件ずつ即 yield)
        jobs = []  # (会員区分, tr)
        for table in tables:
            heading_el = table.find_previous(["h1", "h2", "h3", "h4", "caption"])
            heading = _clean(heading_el.get_text()) if heading_el else ""
            kubun = "賛助会員" if "賛助" in heading else "正会員"
            for tr in table.select("tr"):
                if tr.find("td") is not None:
                    jobs.append((kubun, tr))

        self.total_items = len(jobs)

        for kubun, tr in jobs:
            try:
                tds = tr.find_all("td")
                if len(tds) < 6:
                    continue

                no = _clean(tds[0].get_text(strip=True))

                name_cell = tds[1]
                name = _clean(name_cell.get_text(strip=True))
                if not name:
                    continue

                link = name_cell.find("a", href=True)
                hp = link["href"].strip() if link else ""

                tel = _clean(tds[2].get_text(strip=True))
                fax = _clean(tds[3].get_text(strip=True))

                post_raw = _clean(tds[4].get_text(strip=True))
                m = _POST_RE.search(post_raw)
                post_code = m.group(1) if m else post_raw

                addr_raw = _clean(tds[5].get_text(" ", strip=True))
                pref, addr = _split_pref(addr_raw)

                gyomu_raw = _clean(tds[6].get_text(strip=True)) if len(tds) > 6 else ""
                gyomu_label = _decode_gyomu(gyomu_raw)

                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.PREF: pref,
                    Schema.POST_CODE: post_code,
                    Schema.ADDR: addr,
                    Schema.TEL: tel,
                    Schema.HP: hp,
                    Schema.CAT_SITE: gyomu_raw,
                    "掲載番号": no,
                    "会員区分": kubun,
                    "FAX": fax,
                    "業務内容": gyomu_label,
                }
            except Exception as e:  # noqa: BLE001
                self.logger.warning("行の解析に失敗: %s", e)
                continue


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa23()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.shikeikyou.jp/enterprise/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

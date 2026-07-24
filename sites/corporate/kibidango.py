"""
Kibidango(きびだんご) — クラウドファンディングのプロジェクト一覧クローラー

取得対象:
    - https://kibidango.com/projects の全プロジェクト (?page=N で全ページ, 約65ページ / 約1,560件)
    - 各プロジェクト詳細ページ (/projects/{id}) の
      「特定商取引法に基づく表記」dl から事業者情報を収集:
        事業者 / 運営責任者 / 住所 / 連絡先(電話番号) / メールアドレス /
        登録番号(法人番号) / ホームページ
      および一覧カードのプロジェクト名・プロジェクトオーナー(/users/{id})

取得フロー:
    1. 一覧ページ (url) を ?page=N で巡回。各ページの
       [data-test-selector="project-card"] からプロジェクト名・詳細 URL・
       オーナー名・オーナー URL を取得
    2. カードごとに詳細ページを取得し、特商法 dl を dt→dd 辞書化して
       Schema/EXTRA カラムへマッピング。1 件取得ごとに即 yield
    3. カードが 0 件になったページで打ち切り

備考:
    - 特商法 dl の長文項目 (特典価格 / 申込期限 / 支払方法 / 引渡時期 /
      特典変更 / キャンセル / プロフィール本文) は自由記述プロースのため
      著作権リスクを避けて取得しない
    - 事業者がプラットフォーム運営 (きびだんご株式会社) のプロジェクトは
      その情報がそのまま記載されるため値をそのまま採用する

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/kibidango.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id kibidango
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF_RE = re.compile(
    r"^\s*("
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
    r")"
)

_CO_NUM_RE = re.compile(r"T?(\d{13})")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class Kibidango(StaticCrawler):
    """Kibidango(きびだんご) クラウドファンディング スクレイパー"""

    DELAY = 1.5
    # 既定の Chrome/94 UA だとサーバが 406 を返すため新しめの UA に上書き
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "事業者",
        "プロジェクトオーナー",
        "オーナーURL",
    ]

    def prepare(self):
        # 既定のヘッダーだと 406 Not Acceptable を返すため Accept 等を補う
        if self.session is not None:
            self.session.headers.update(
                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                }
            )

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while True:
            list_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした: %s", list_url)
                break

            cards = soup.select('[data-test-selector="project-card"]')
            if not cards:
                logger.info("ページ %d でカードが 0 件のため終了", page)
                break

            logger.info("ページ %d: カード %d 件", page, len(cards))

            for card in cards:
                title_a = card.select_one('h5 a[href*="/projects/"]') or card.select_one(
                    'a[href*="/projects/"]'
                )
                if not title_a or not title_a.get("href"):
                    continue
                detail_url = urljoin(url, title_a.get("href"))
                name = _clean(title_a.get_text())

                owner_a = card.select_one('a[href*="/users/"]')
                owner_name = _clean(owner_a.get_text()) if owner_a else ""
                owner_url = urljoin(url, owner_a.get("href")) if owner_a and owner_a.get("href") else ""

                try:
                    item = self._scrape_detail(detail_url, name, owner_name, owner_url)
                    if item:
                        yield item
                except Exception as e:
                    logger.warning("詳細ページの解析に失敗: %s — %s", detail_url, e)
                    # 最低限の情報 (一覧で取れた分) は落とさず yield する
                    yield {
                        Schema.URL: detail_url,
                        Schema.NAME: name,
                        "プロジェクトオーナー": owner_name,
                        "オーナーURL": owner_url,
                    }

            page += 1

    def _scrape_detail(
        self, url: str, name: str, owner_name: str, owner_url: str
    ) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        if not name:
            h1 = soup.find("h1")
            name = _clean(h1.get_text() if h1 else "")

        info = self._parse_shotorihiki(soup)

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.REP_NM: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.CO_NUM: "",
            Schema.HP: "",
            Schema.EMAIL: info.get("メール", ""),
            "事業者": info.get("事業者", ""),
            "プロジェクトオーナー": owner_name,
            "オーナーURL": owner_url,
        }

        item[Schema.REP_NM] = info.get("運営責任者", "")
        item[Schema.TEL] = info.get("電話", "")
        item[Schema.HP] = info.get("ホームページ", "")

        # 法人番号 (登録番号) — 13 桁だけを採用
        reg = info.get("登録番号", "")
        m = _CO_NUM_RE.search(reg.replace("-", "").replace(" ", ""))
        if m:
            item[Schema.CO_NUM] = m.group(1)

        # 住所 → 都道府県 + 以降を分割
        addr = info.get("住所", "")
        if addr:
            pm = _PREF_RE.match(addr)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = addr[pm.end():].strip()
            else:
                item[Schema.ADDR] = addr

        return item

    @staticmethod
    def _parse_shotorihiki(soup) -> dict:
        """特定商取引法に基づく表記の dl を dt キーワードで辞書化する。

        長文の自由記述項目 (価格 / 期限 / 支払 / 引渡 / 変更 / キャンセル) は
        著作権リスクを避けて取り込まない。
        """
        result: dict = {}
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            if not dts:
                continue
            # 事業者情報を含む dl かどうかを判定
            keys = [_clean(dt.get_text()) for dt in dts]
            if not any(("事業者" in k or "運営責任者" in k) for k in keys):
                continue
            for i, dt in enumerate(dts):
                key = _clean(dt.get_text())
                if i >= len(dds):
                    continue
                val = _clean(dds[i].get_text(" ", strip=True))
                if not val:
                    continue
                if "事業者" in key:
                    result["事業者"] = val
                elif "責任者" in key:
                    result["運営責任者"] = val
                elif "電話" in key:
                    result["電話"] = val
                elif "住所" in key or "所在地" in key:
                    result["住所"] = val
                elif "メール" in key:
                    result["メール"] = val
                elif "登録番号" in key or "法人番号" in key:
                    result["登録番号"] = val
                elif "ホームページ" in key or key == "URL":
                    a = dds[i].find("a")
                    result["ホームページ"] = (
                        a.get("href") if a and a.get("href") else val
                    )
                # それ以外 (価格/期限/支払/引渡/変更/キャンセル) は取り込まない
        return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Kibidango()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://kibidango.com/projects")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

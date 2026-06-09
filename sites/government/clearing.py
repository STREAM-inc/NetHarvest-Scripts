"""
登録貸金業者情報検索 (金融庁) — 全国の登録貸金業者の公表情報

運営: 金融庁 (clearing.fsa.go.jp/kashikin)
一覧URL: https://clearing.fsa.go.jp/kashikin/index.php

取得対象:
    - 全国 (47都道府県) の登録貸金業者の公表レコード
    - 商号・名称 / 代表者名 / 所在地 / 電話番号 / 登録番号 / 登録(更新)日 等の構造化情報

取得フロー (検索フォームの POST を都道府県ごとに再現する):
    1. eria1 (所在地・第一候補) に都道府県名をセットして index.php へ POST
       → その都道府県に本店を置く業者の検索結果一覧 (全件・ページ送り無し) を取得
    2. 結果テーブルは 1業者 = 2行 (<tr>) 構成:
         行1: 登録番号(機関/更新回数/番号) + 登録更新日/人格/行政処分/商号/代表者/
              フリガナ/郵便番号/所在地/電話番号/データ更新日/広告用電話番号リンク (計14セル)
         行2: 日本貸金業協会会員番号 (colspan=3 の1セル)
    3. 1業者を取得するごとに即 yield する (リストに全情報が揃うため詳細ページ巡回は不要)

設計メモ:
    - 検索フォームは method=post (GET は入力フォームを返すだけ)。session.post で再現する。
    - ページネーションは存在しない。1都道府県=1POSTで全件がテーブルに展開される
      (東京都=634件・沖縄県=45件を1ページで返すことを確認済み)。
    - 業者の登録は本店所在地が属する1都道府県にのみ現れるため都道府県横断の重複は基本無いが、
      念のため登録番号(機関+番号)で重複除去する。
    - 「広告用電話番号」は koukokutel.php への難読化リンク (1件ごとに追加リクエストが必要) のため
      取得しない (早期 yield / 通信負荷の観点で除外)。
    - 詳細ページは存在せず、長文の自由記述カラムも無い (著作権リスク無し)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/clearing.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id clearing
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

INDEX_URL = "https://clearing.fsa.go.jp/kashikin/index.php"

# 所在地プルダウン (eria1) の都道府県。フォームの option 順をそのまま使用する。
PREFECTURES = [
    "東京都", "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "神奈川県", "新潟県", "山梨県",
    "長野県", "富山県", "石川県", "福井県", "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県",
    "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "熊本県",
    "大分県", "宮崎県", "鹿児島県", "福岡県", "佐賀県", "長崎県", "沖縄県",
]

# 登録更新日 (YYYY/MM/DD) を持つ <tr> を業者レコードの開始行とみなす
_DATE_RE = re.compile(r"^\d{4}/\d{2}/\d{2}$")


def _clean(s) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class ClearingScraper(StaticCrawler):
    """登録貸金業者情報検索 (金融庁) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "登録番号",                  # 例: 沖縄県知事（２）第04255号 (機関+更新回数+番号の連結)
        "登録機関",                  # 例: 東京都知事 / 関東財務局長 / 沖縄総合事務局長
        "登録更新回数",              # 例: 2 (（２）から数字のみ抽出)
        "登録更新日",                # 例: 2024/02/24
        "人格",                      # 例: 個人 / 法人
        "行政処分",                  # 例: (通常は空。短いラベルのみ)
        "代表者名フリガナ",          # 例: ナカチ ツバサ
        "データ更新日",              # 例: 2024/02/26 現在
        "日本貸金業協会会員番号",    # 例: 第006167号
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        self.total_items = len(PREFECTURES)  # 進捗は都道府県数ベース (件数は事前に不明)

        for pref in PREFECTURES:
            soup = self._search_pref(pref)
            if soup is None:
                continue
            count = 0
            for item in self._parse_results(soup, pref):
                key = item.get("_dedup_key") or ""
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                item.pop("_dedup_key", None)
                count += 1
                yield item
            self.logger.info("%s: %d 件", pref, count)
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 都道府県ごとの検索 POST
    # ------------------------------------------------------------------

    def _search_pref(self, pref: str):
        data = {
            "toroku_no_1": "",
            "toroku_no_2": "",
            "eria1": pref,
            "eria2": "未選択",
            "eria3": "未選択",
            "syougou": "",
            "daihyousya": "",
            "telno": "",
            "submit_b": "検索開始",
        }
        try:
            resp = self.session.post(INDEX_URL, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.warning("検索POST失敗 pref=%s: %s", pref, e)
            return None

    # ------------------------------------------------------------------
    # 検索結果テーブルの解析 (1業者 = 2行)
    # ------------------------------------------------------------------

    def _parse_results(self, soup, pref: str) -> Generator[dict, None, None]:
        rows = soup.find_all("tr")
        for idx, tr in enumerate(rows):
            tds = tr.find_all("td")
            if len(tds) < 14:
                continue
            cells = [_clean(td.get_text()) for td in tds]
            # 開始行判定: 4番目のセル(登録更新日)が YYYY/MM/DD
            if not _DATE_RE.match(cells[3]):
                continue
            # 次の行の最終セル = 日本貸金業協会会員番号
            kyokai = ""
            if idx + 1 < len(rows):
                nxt = rows[idx + 1].find_all("td")
                if nxt:
                    kyokai = _clean(nxt[-1].get_text())
            try:
                item = self._build_item(cells, kyokai, pref)
            except Exception as e:
                self.logger.warning("レコード解析失敗: %s", e)
                continue
            if item:
                yield item

    def _build_item(self, cells: list[str], kyokai: str, pref: str) -> dict | None:
        registrar = cells[0]              # 例: 沖縄県知事 / 関東財務局長
        update_cnt = cells[1]             # 例: （２）
        reg_code = cells[2]               # 例: 第04255号
        reg_date = cells[3]               # 登録(更新)日
        jinkaku = cells[4]                # 人格
        syobun = cells[5]                 # 行政処分
        name = cells[6]                   # 商号・名称
        rep = cells[7]                    # 代表者名
        rep_kana = cells[8]               # 代表者名（フリガナ）
        post = cells[9]                   # 郵便番号
        address = cells[10]               # 本店（主たる営業所）
        tel = cells[11]                   # 電話番号
        data_date = cells[12]             # データ更新日

        if not name:
            return None

        # 登録番号フル (機関 + 更新回数 + 番号)
        full_reg_no = f"{registrar}{update_cnt}{reg_code}"
        # 更新回数の数字のみ
        m = re.search(r"\d+", update_cnt)
        update_num = m.group(0) if m else ""

        # 所在地から都道府県を分離 (先頭が検索都道府県と一致すれば取り除く)
        addr_rest = address
        if addr_rest.startswith(pref):
            addr_rest = addr_rest[len(pref):].strip()

        return {
            Schema.NAME: name,
            Schema.URL: INDEX_URL,
            Schema.PREF: pref,
            Schema.ADDR: addr_rest,
            Schema.POST_CODE: post,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            "登録番号": full_reg_no,
            "登録機関": registrar,
            "登録更新回数": update_num,
            "登録更新日": reg_date,
            "人格": jinkaku,
            "行政処分": syobun,
            "代表者名フリガナ": rep_kana,
            "データ更新日": data_date,
            "日本貸金業協会会員番号": kyokai,
            # 重複除去: 登録番号フルで一意
            "_dedup_key": full_reg_no,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ClearingScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

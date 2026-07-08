"""
全国警備業協会（AJSSA）会員名簿(奈良県) — 一般社団法人奈良県警備業協会 加盟会員一覧

取得対象:
    - 加盟会員(56社)および賛助会員の一覧
    - 会社名 / 住所(郵便番号・都道府県分離) / 電話番号 / 業務種別 / HP / 会員区分 / 警備員募集

取得フロー:
    - 一覧ページ (https://www.nakeikyo.or.jp/member) は単一の静的ページ。
    - .member-block1 (加盟会員) と .member-block2 (賛助会員) 内の <dl> を1社=1レコードとして走査。
      各データ <dl> は <dd> の位置で 会社名/住所/電話/業務種別(/募集) を保持する
      (先頭に <dt> のみのヘッダ行があるため dd 数で除外)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/ajssa_27.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_27
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県プレフィックス
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 都道府県を省略しがちな主要市 → 都道府県
_CITY_TO_PREF = {
    "大阪市": "大阪府",
    "堺市": "大阪府",
    "京都市": "京都府",
    "神戸市": "兵庫県",
    "名古屋市": "愛知県",
    "横浜市": "神奈川県",
    "川崎市": "神奈川県",
    "札幌市": "北海道",
    "福岡市": "福岡県",
    "広島市": "広島県",
}

# 業務種別の略号 → 正式名称
_TYPE_MAP = {
    "施": "施設警備",
    "交": "交通誘導警備",
    "雑": "雑踏警備",
    "貴": "貴重品運搬警備",
    "身": "身辺警備",
    "機": "機械警備",
}

_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class AjssaNara(StaticCrawler):
    """全国警備業協会（AJSSA）会員名簿(奈良県) スクレイパー"""

    DELAY = 1.5
    # 奈良県協会のホーム都道府県。都道府県表記を省いた住所の既定値に使う。
    _DEFAULT_PREF = "奈良県"
    EXTRA_COLUMNS = ["会員区分", "警備員募集"]

    def parse(self, url: str):
        soup = self.get_soup(url)

        blocks = [
            ("加盟会員", soup.select_one(".member-block1")),
            ("賛助会員", soup.select_one(".member-block2")),
        ]

        # 総件数(進捗表示用)を先に確定
        total = 0
        for _, block in blocks:
            if block is None:
                continue
            for dl in block.select("dl"):
                if len(dl.select("dd")) >= 3:
                    total += 1
        self.total_items = total

        for membership, block in blocks:
            if block is None:
                continue
            for dl in block.select("dl"):
                dds = dl.select("dd")
                if len(dds) < 3:
                    # <dt> のみのヘッダ行
                    continue
                try:
                    item = self._parse_dl(dds, membership, url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(f"パース失敗 ({membership}): {e}")
                    continue

    def _parse_dl(self, dds, membership: str, url: str) -> dict | None:
        # 会社名 + HP
        name_dd = dds[0]
        name = name_dd.get_text(strip=True)
        if not name:
            return None
        hp = ""
        a = name_dd.select_one("a[href]")
        if a and a.get("href", "").startswith("http"):
            hp = a["href"].strip()

        # 住所 (〒 + 都道府県 + 住所)
        addr_raw = dds[1].get_text(" ", strip=True) if len(dds) > 1 else ""
        addr_raw = re.sub(r"\s+", " ", addr_raw).strip()
        post_code = ""
        m = _POST_PATTERN.search(addr_raw)
        if m:
            post_code = m.group(1)
            addr_raw = _POST_PATTERN.sub("", addr_raw).strip()
        pref, addr = self._split_pref(addr_raw)

        # 電話番号
        tel = dds[2].get_text(strip=True) if len(dds) > 2 else ""

        # 業務種別
        gyomu = ""
        if len(dds) > 3:
            tokens = [p.get_text(strip=True) for p in dds[3].select("p")]
            tokens = [t for t in tokens if t]
            if tokens:
                gyomu = "/".join(_TYPE_MAP.get(t, t) for t in tokens)
            else:
                gyomu = dds[3].get_text(" ", strip=True)

        # 警備員募集 (加盟会員のみ; 〇 or 空)
        boshu = ""
        if len(dds) > 4:
            boshu = dds[4].get_text(strip=True)

        return {
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: gyomu,
            Schema.HP: hp,
            Schema.URL: url,
            "会員区分": membership,
            "警備員募集": boshu,
        }

    def _split_pref(self, address: str):
        """住所文字列から都道府県を分離する。"""
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        for city, pref in _CITY_TO_PREF.items():
            if address.startswith(city):
                return pref, address
        # 都道府県省略の住所は奈良県協会の地元とみなす
        return self._DEFAULT_PREF, address


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AjssaNara()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.nakeikyo.or.jp/member")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

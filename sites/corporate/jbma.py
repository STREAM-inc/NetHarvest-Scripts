"""
公益社団法人 全国ビルメンテナンス協会（JBMA）会員一覧名簿 — 会員企業ディレクトリ

取得対象:
    - JBMA 会員一覧名簿ページ (corp.php) に掲載された全会員企業 (約 3,435 件)
    - 各会員の企業名・所属協会・住所・郵便番号・電話番号・FAX・業務内容・HP

取得フロー:
    - corp.php は 1 ページに全会員を `div.box` として静的出力する (ページネーション無し)。
    - 1 ページを取得し、各 `.box` を 1 件ずつ即 yield する。

備考 / 除外方針:
    - 「自社PR」は運営者/企業が書いた自由記述の宣伝文 (プロース) のため、著作権リスク回避で除外。
    - 「業務内容」はカンマ区切りの業務カテゴリ (構造化された短語) のため Schema.LOB として取得。
    - robots.txt に `Disallow: /meibo/` が存在する点は運用側で要確認 (利用規約自体の禁止明記ではない)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jbma.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jbma
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 郵便番号 (〒 に続く 7 桁)
_POST_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 都道府県 (住所先頭からの抽出)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)

_TEL_RE = re.compile(r"TEL[:：]\s*([0-9０-９\-ー－()（）]+)")
_FAX_RE = re.compile(r"FAX[:：]\s*([0-9０-９\-ー－()（）]+)")


class Jbma(StaticCrawler):
    """全国ビルメンテナンス協会（JBMA）会員一覧名簿 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["所属協会", "FAX"]

    def parse(self, url: str):
        soup = self.get_soup(url)
        boxes = soup.select("div.box")
        self.total_items = len(boxes)

        for box in boxes:
            try:
                item = self._parse_box(box, url)
                if item:
                    yield item
            except Exception as e:  # noqa: BLE001 — 個別会員のパース失敗はスキップして継続
                self.logger.warning("会員のパースに失敗 (スキップ): %s", e)
                continue

    def _parse_box(self, box, url: str) -> dict | None:
        name_el = box.select_one(".name")
        if not name_el:
            return None

        # HP: 企業名リンクの href (掲載がある会員のみ)
        hp = ""
        a = name_el.select_one("a[href]")
        if a and a.get("href", "").startswith("http"):
            hp = a["href"].strip()

        # 所属協会: .associ のテキスト (「所属協会 :」以降)
        associ = ""
        associ_el = name_el.select_one(".associ")
        if associ_el:
            associ = associ_el.get_text(strip=True)
            if ":" in associ or "：" in associ:
                associ = re.split(r"[:：]", associ, 1)[-1].strip()

        # 企業名: .associ を除いた .name の残りテキスト
        name_copy = BeautifulSoup(str(name_el), "html.parser")
        for d in name_copy.select(".associ"):
            d.decompose()
        name = name_copy.get_text(" ", strip=True)
        if not name:
            return None

        tds = box.select("table td")

        # td[0]: 住所 (郵便番号 + 住所 + MAP リンク)
        post_code = addr = pref = ""
        if len(tds) >= 1:
            td0 = BeautifulSoup(str(tds[0]), "html.parser")
            for tag in td0.select("span, a"):  # ラベル「住所」と MAP リンクを除去
                tag.decompose()
            addr_text = td0.get_text("\n", strip=True)
            m = _POST_RE.search(addr_text)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"
                addr_text = _POST_RE.sub("", addr_text)
            addr = " ".join(addr_text.split())
            pm = _PREF_RE.search(addr)
            if pm:
                pref = pm.group(0)

        # td[1]: 電話番号・FAX
        tel = fax = ""
        if len(tds) >= 2:
            tel_text = tds[1].get_text("\n", strip=True)
            tm = _TEL_RE.search(tel_text)
            if tm:
                tel = tm.group(1).strip()
            fm = _FAX_RE.search(tel_text)
            if fm:
                fax = fm.group(1).strip()

        # td[2]: 業務内容 (構造化リスト) / 自社PR (プロース → 除外)
        lob = ""
        if len(tds) >= 3:
            td2 = tds[2]
            label_el = td2.select_one("span")
            label = label_el.get_text(strip=True) if label_el else ""
            if label == "業務内容":
                td2_copy = BeautifulSoup(str(td2), "html.parser")
                for tag in td2_copy.select("span"):
                    tag.decompose()
                lob = " ".join(td2_copy.get_text(" ", strip=True).split())

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.LOB: lob,
            Schema.URL: url,
            "所属協会": associ,
            "FAX": fax,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jbma()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.j-bma.or.jp/meibo/corp.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

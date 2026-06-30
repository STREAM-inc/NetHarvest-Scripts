"""
弁護士ドットコム — 弁護士検索 プロフィール情報スクレイパー

取得対象 (一覧 → 詳細で完結):
    - 弁護士名 / 名称カナ / 都道府県 / 住所 / TEL / 詳細URL
    - 所属事務所 / 所属弁護士会 / 最寄駅 / 注力分野 / 解決事例数 (EXTRA, いずれも短い構造化値)

取得フロー:
    一覧 (検索結果) ページ /search/result/?page=N を巡回する (末尾ページまで約 594 ページ,
    総 8,200 名超)。各ページの弁護士カセット (div.p-lawyer-cassette) から、一覧にしか
    無い構造化フィールド (最寄駅 / 注力分野 / 解決事例数) と詳細 URL を取り出し、
    詳細ページ /{pref}/a_{area}/l_{id}/ を 1 件取得するたびに即 yield する
    (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。

    料金表・解決事例本文・自己紹介 (PR文) などの自由記述プロースは著作権リスク回避のため
    取得しない (Schema.LOB / DESCRIPTION も同様に除外)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/bengo4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bengo4
"""

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


# 詳細ページ URL の判定 (/{pref}/a_{area}/l_{lawyer_id}/)。
_DETAIL_RE = re.compile(r"/[a-z]+/a_\d+/l_\d+/?$")

# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREF})\s*(.*)$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


def _strip_suffix(name: str) -> str:
    """カセット/詳細の見出しから "弁護士"・"プロフィール" の接尾辞を除去する。"""
    name = re.sub(r"(弁護士|プロフィール)\s*", "", name)
    return _clean(name)


class Bengo4Scraper(StaticCrawler):
    """弁護士ドットコム 弁護士検索 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["所属事務所", "所属弁護士会", "最寄駅", "注力分野", "解決事例数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cassettes = soup.select("div.p-lawyer-cassette")
            if not cassettes:
                break

            # 1 ページ目で総件数 (≈8,200 名) を ETA 用に設定
            if page == 1 and self.total_items is None:
                total_el = soup.select_one('[class*="total"]')
                if total_el:
                    m = re.search(r"([\d,]+)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for cas in cassettes:
                # 詳細 URL (カセット内の弁護士名リンク)
                detail_url = None
                for a in cas.select("a[href]"):
                    href = a.get("href", "")
                    if _DETAIL_RE.search(href):
                        detail_url = urljoin(url, href)
                        break
                if not detail_url:
                    continue

                # 一覧カセットにしか無い構造化フィールドを先に取り出す
                list_fields = self._extract_list_fields(cas)

                try:
                    item = self._scrape_detail(detail_url, list_fields)
                except Exception as e:  # noqa: BLE001 — 個別アイテムのエラーは握りつぶして続行
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue

                if item:
                    yield item

            page += 1

    def _extract_list_fields(self, cas) -> dict:
        """一覧カセットから詳細ページに無い短い構造化値を取り出す。"""
        fields = {}

        # 最寄駅 + 徒歩分 (例: "東池袋（東池袋四丁目）駅 徒歩6分")
        trans = cas.select_one('[class*="cassette-transportation"]')
        if trans:
            fields["最寄駅"] = _clean(trans.get_text(" ", strip=True))

        # 注力分野タグ (例: 不動産・建築 / 遺産相続 …) — 短い構造化ラベルのみ
        tags = [
            _clean(t.get_text(strip=True))
            for t in cas.select('[class*="scroll-snap-tab--field"]')
        ]
        tags = [t for t in tags if t]
        if tags:
            # 重複除去しつつ順序維持
            fields["注力分野"] = "/".join(dict.fromkeys(tags))

        # 解決事例数 (例: "解決事例 6" → 6)
        rec = cas.select_one('[class*="track-record"]')
        if rec:
            m = re.search(r"(\d+)", rec.get_text())
            if m:
                fields["解決事例数"] = m.group(1)

        return fields

    def _scrape_detail(self, url: str, list_fields: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # 弁護士名 (例: "小師 健志 弁護士 プロフィール" → "小師 健志")
        name_el = soup.select_one(".p-lawyer-profile-header__name")
        if name_el:
            item[Schema.NAME] = _strip_suffix(name_el.get_text(" ", strip=True))

        # 名称カナ (例: "こもろ たけし")
        kana_el = soup.select_one(".p-lawyer-profile-header__kana")
        if kana_el:
            item[Schema.NAME_KANA] = _clean(kana_el.get_text(" ", strip=True))

        # 所在地 (例: "東京都 豊島区東池袋4-25-12 サンシャイン・サイド9階")
        addr_el = soup.select_one(".p-lawyer-profile-law-firm__address") or soup.select_one(
            ".p-lawyer-profile-header__address"
        )
        if addr_el:
            addr = _clean(addr_el.get_text(" ", strip=True))
            addr = re.sub(r"^所在地[:：]\s*", "", addr)
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = _clean(m.group(2))
            else:
                item[Schema.ADDR] = addr

        # TEL (tel: リンク)。全角→半角は Pipeline が正規化する
        tel_a = soup.select_one('a[href^="tel:"]')
        if tel_a:
            item[Schema.TEL] = _clean(tel_a.get("href", "").replace("tel:", ""))

        # 所属事務所 (例: "所属事務所 弁護士法人若井綜合法律事務所")
        firm_el = soup.select_one(".p-lawyer-profile-header__law-firm")
        if firm_el:
            firm = re.sub(r"^所属事務所[:：]?\s*", "", _clean(firm_el.get_text(" ", strip=True)))
            if firm:
                item["所属事務所"] = firm

        # 所属弁護士会 等の data-list (キー/値ペア)
        for di in soup.select(".p-lawyer-profile-data-list__item"):
            key = di.select_one(".p-lawyer-profile-data-list__key")
            val = di.select_one(".p-lawyer-profile-data-list__value")
            if key and val and "弁護士会" in key.get_text():
                item["所属弁護士会"] = _clean(val.get_text(" ", strip=True))

        # 一覧由来の短い構造化値をマージ
        for k, v in list_fields.items():
            if v:
                item[k] = v

        # 必須フィールド (NAME) が取れない詳細はスキップ
        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Bengo4Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.bengo4.com/search/result/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

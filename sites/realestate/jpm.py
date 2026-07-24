# -*- coding: utf-8 -*-
"""
公益財団法人日本賃貸住宅管理協会（日管協） — 会員検索（支部別 正会員一覧）

取得対象:
    - 全国 47 都道府県支部の正会員（賃貸住宅管理会社）一覧
    - 預り金保証制度加入会員は「会社情報」詳細ページの構造化情報も取得

取得フロー（Pattern B / 取得即 yield）:
    ルート支部ページ(all.php?cid=N)
      → ページ内の支部ナビから全 cid(=都道府県)URL を抽出
        → 各支部ページの会員 dl(div.map_link を含む)を列挙
          → 会員ごとに一覧情報を組み立て、預り金保証会員は
             ../deposit/info/{code}.html を取得してマージし即 yield

備考:
    - 「一覧がない」ため、引数 url(東京=cid=14)を起点に全都道府県を巡回する。
      巡回先 URL はすべて引数 url から urljoin で派生させる(ルートのハードコード禁止)。
    - 詳細ページの自由記述カラム(事業内容/会社沿革/当社アピール/賃貸管理実績 等)は
      著作権リスクのため取得しない。構造化された事実情報のみを EXTRA に格納する。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/jpm.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jpm
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_CID_PATTERN = re.compile(r"cid=(\d+)")

# 詳細ページのラベル → Schema 定数（構造化・事実情報のみ）
_DETAIL_SCHEMA = {
    "会社設立年月日": Schema.OPEN_DATE,
    "資本金": Schema.CAP,
    "代表者氏名": Schema.REP_NM,
    "代表電話番号": Schema.TEL,
    "会社の営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
    "従業員": Schema.EMP_NUM,
}

# 詳細ページのラベル → EXTRA_COLUMNS 名（構造化・事実情報のみ）
_DETAIL_EXTRA = {
    "主な役員の氏名": "役員氏名",
    "店舗数": "店舗数",
    "資格保有者数": "資格保有者数",
    "加盟団体": "加盟団体",
    "免許・許可": "免許・許可",
    "主要取引金融機関": "主要取引金融機関",
    "管理戸数": "管理戸数",
    "居住用賃貸住宅の家主数": "家主数",
    "居住用賃貸住宅の敷金の保全方法": "敷金保全方法",
}


def _norm(text: str) -> str:
    """全角スペースを半角に寄せて余分な空白を畳む。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


class JpmScraper(StaticCrawler):
    """日管協 会員検索スクレイパー（支部別一覧→会社情報詳細 / 取得即 yield）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "支部",
        "預り金保証制度",
        "会社情報URL",
        "役員氏名",
        "店舗数",
        "資格保有者数",
        "加盟団体",
        "免許・許可",
        "主要取引金融機関",
        "管理戸数",
        "家主数",
        "敷金保全方法",
    ]

    def parse(self, url: str):
        root = self.get_soup(url)
        if root is None:
            logger.warning("ルートページ取得失敗: %s", url)
            return

        # 支部ナビから全 cid(=都道府県)ページ URL を抽出（引数 url から派生）
        branch_urls: list[str] = []
        seen_cid: set[str] = set()
        for a in root.find_all("a", href=True):
            href = a["href"]
            if "all.php" not in href:
                continue
            m = _CID_PATTERN.search(href)
            if not m or m.group(1) in seen_cid:
                continue
            seen_cid.add(m.group(1))
            branch_urls.append(urljoin(url, href))

        if not branch_urls:
            branch_urls = [url]

        # 引数 url の支部(cid)を先頭に並べ替え
        given = _CID_PATTERN.search(url)
        if given:
            given_cid = given.group(1)
            branch_urls.sort(
                key=lambda u: (
                    (_CID_PATTERN.search(u) or [None, None])[1] != given_cid
                )
            )

        for burl in branch_urls:
            bsoup = root if _same_cid(burl, url) else self.get_soup(burl)
            if bsoup is None:
                logger.warning("支部ページ取得失敗: %s", burl)
                continue
            yield from self._parse_branch(bsoup, burl)

    def _parse_branch(self, soup, page_url: str):
        # 支部名（都道府県）を <title> から抽出（例: 「... 東京都支部正会員一覧」）
        branch = ""
        if soup.title:
            tm = re.search(r"([^\s>]+?)支部", soup.title.get_text())
            if tm:
                branch = tm.group(1)

        dls = [d for d in soup.find_all("dl") if d.find("div", class_="map_link")]
        for dl in dls:
            try:
                item = self._parse_company(dl, page_url, branch)
            except Exception:  # noqa: BLE001 - 個別会員のエラーはスキップ
                logger.exception("会員のパースに失敗: %s", page_url)
                continue
            if item:
                yield item

    def _parse_company(self, dl, page_url: str, branch: str) -> dict | None:
        dt = dl.find("dt")
        if dt is None:
            return None

        # 会社名 + HP（HP リンクが無い会員は dt の直下テキストが会社名）
        name, hp = "", ""
        link = dt.find("a", href=True)
        if link and "maps.google" not in link["href"]:
            name = _norm(link.get_text())
            hp = link["href"].strip()
        else:
            name = _norm("".join(dt.find_all(string=True, recursive=False)))
        if not name:
            return None

        # 郵便番号 + 住所
        post_code, addr, pref = "", "", ""
        add_block = dl.find("div", class_="add_block")
        if add_block:
            raw = add_block.get_text("\n")
            pm = _POST_PATTERN.search(raw)
            if pm:
                post_code = pm.group(1)
            addr = _norm(_POST_PATTERN.sub("", raw))
            prefm = _PREF_PATTERN.match(addr)
            if prefm:
                pref = prefm.group(1)

        # 預り金保証制度・会社情報詳細リンク
        deposit_flag, detail_url = "", ""
        join_block = dl.find("div", class_="join_block")
        if join_block:
            deposit_flag = "加入"
            da = join_block.find(
                "a", href=lambda h: bool(h) and "deposit/info" in h
            )
            if da:
                detail_url = urljoin(page_url, da["href"])

        item = {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.HP: hp,
            Schema.TEL: "",
            Schema.REP_NM: "",
            Schema.CAP: "",
            Schema.OPEN_DATE: "",
            Schema.EMP_NUM: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            "支部": branch,
            "預り金保証制度": deposit_flag,
            "会社情報URL": detail_url,
            "役員氏名": "",
            "店舗数": "",
            "資格保有者数": "",
            "加盟団体": "",
            "免許・許可": "",
            "主要取引金融機関": "",
            "管理戸数": "",
            "家主数": "",
            "敷金保全方法": "",
        }

        # 会社情報詳細ページ（存在する会員のみ）をマージ
        if detail_url:
            self._merge_detail(item, detail_url)

        return item

    def _merge_detail(self, item: dict, detail_url: str) -> None:
        dsoup = self.get_soup(detail_url)
        if dsoup is None:
            logger.warning("詳細ページ取得失敗: %s", detail_url)
            return

        data: dict[str, str] = {}
        for tr in dsoup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                data[_norm(th.get_text())] = _norm(td.get_text(" "))

        for label, value in data.items():
            if not value:
                continue
            if label in _DETAIL_SCHEMA:
                item[_DETAIL_SCHEMA[label]] = value
            elif label in _DETAIL_EXTRA:
                item[_DETAIL_EXTRA[label]] = value

        # 住所が一覧側で取れていない場合のみ詳細の本店所在で補完
        if not item[Schema.ADDR]:
            head = data.get("本店所在", "")
            if head:
                item[Schema.ADDR] = head
                prefm = _PREF_PATTERN.match(head)
                if prefm and not item[Schema.PREF]:
                    item[Schema.PREF] = prefm.group(1)


def _same_cid(url_a: str, url_b: str) -> bool:
    ma = _CID_PATTERN.search(url_a)
    mb = _CID_PATTERN.search(url_b)
    return bool(ma and mb and ma.group(1) == mb.group(1))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JpmScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jpm.jp/branch/all.php?cid=14#list")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

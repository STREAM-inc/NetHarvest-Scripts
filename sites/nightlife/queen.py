"""
体入Queen｜関西版 (kansai.queenwork.jp) — ナイトワーク系体験入店求人サイト掲載店舗スクレイパー

関東版 (queenwork.jp / scripts/sites/nightlife/queen_work.py) と同一システム構成の姉妹サイト。
大阪・京都・兵庫・滋賀・奈良・和歌山 等 関西エリアの掲載店舗を取得する。

取得対象:
    - キャバクラ / ガールズバー / コンカフェ / スナック / クラブ 等の掲載全店舗

取得フロー:
    1. joblist.html (求人一覧) を起点に、#joblist 内の詳細リンク (/job{id}/) を収集
       - ページ送りは <link rel="next" href="joblist_p{N}.html"> を辿る
       - ※関西版の sitemap.xml は関東版(queenwork.jp)のURLを返す誤設定のため使用しない
    2. 各詳細ページから <b>ラベル</b><span>値</span> パターンで店舗情報を抽出

備考 (2026-08 時点):
    関西版は現在掲載店舗が 0 件 (全 joblist / area フィルタが「現在求人情報は…」)。
    セレクタ・URL 組み立てのバグではなく出典データが空。関西に実店舗が載れば
    joblist.html に出て自動で取得される。ロジックは在庫のある関東版 (queenwork.jp) の
    joblist.html に同一 parse を流すことで検証可能 (3件以上取得できる)。

実行方法:
    python scripts/sites/nightlife/queen.py
    python bin/run_flow.py --site-id queen
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


# 詳細ページ URL (.../job{id}/) 判定。ドメインは問わずパス末尾で照合する。
_JOB_PATH_RE = re.compile(r"/job\d+/?$")

# 一覧の巡回上限 (無限ループ防止)。1ページ20件想定。
_MAX_PAGES = 200

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


def _multiline(text: str) -> str:
    if not text:
        return ""
    lines = [_clean(line) for line in text.replace("\r", "\n").split("\n")]
    result: list[str] = []
    prev_empty = False
    for line in lines:
        if line == "":
            if not prev_empty and result:
                result.append("")
            prev_empty = True
        else:
            result.append(line)
            prev_empty = False
    return "\n".join(result).strip()


def _get_field(soup, label: str) -> str:
    """<b>label</b> と同じ親要素内の <span> テキストを返す。"""
    for b in soup.find_all("b"):
        if _clean(b.get_text()) == label:
            parent = b.parent
            if parent:
                span = parent.find("span")
                if span:
                    return _multiline(span.get_text("\n", strip=False))
    return ""


class QueenWorkKansaiScraper(StaticCrawler):
    """体入Queen｜関西版 (kansai.queenwork.jp) 掲載店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "最寄り駅",
        "給与",
        "職種",
        "待遇",
        "雇用形態",
        "応募方法",
        "受付時間",
        "担当者名",
    ]

    def parse(self, url: str):
        # url = sites.yml 登録の正規ルート (例: https://kansai.queenwork.jp/)
        list_url = urljoin(url, "joblist.html")
        seen: set[str] = set()

        for _ in range(_MAX_PAGES):
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # #joblist 内の求人詳細リンク (/job{id}/) を収集
            container = soup.select_one("#joblist") or soup
            page_details: list[str] = []
            for a in container.find_all("a", href=True):
                href = a["href"].strip()
                if not _JOB_PATH_RE.search(href):
                    continue
                detail_url = urljoin(url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                page_details.append(detail_url)

            for detail_url in page_details:
                try:
                    item = self._scrape_detail(detail_url, url)
                    if item:
                        yield item
                except Exception:
                    self.logger.exception("詳細取得失敗: %s", detail_url)
                    continue

            # 次ページ (<link rel="next" href="joblist_p{N}.html">)
            next_link = soup.find("link", rel="next")
            next_href = next_link.get("href") if next_link else None
            if not next_href:
                break
            next_url = urljoin(url, next_href)
            if next_url == list_url:
                break
            list_url = next_url

    def _scrape_detail(self, detail_url: str, root_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        name = _clean(_get_field(soup, "店舗名"))
        if not name:
            h1 = soup.select_one("h1")
            if h1:
                raw = _clean(h1.get_text(" ", strip=True))
                name = re.sub(r"（[^）]+）の公式求人情報$", "", raw).strip()
        if not name:
            # 掲載終了/存在しない求人 (プレースホルダページ) は空タイトル → スキップ
            return None

        # 住所: <b>住所</b> の <span> から郵便番号・都道府県・住所を分離
        postal, pref, addr = "", "", ""
        for b in soup.find_all("b"):
            if _clean(b.get_text()) == "住所":
                addr_span = b.parent.find("span") if b.parent else None
                if addr_span:
                    for a_tag in addr_span.find_all("a"):
                        a_tag.decompose()
                    raw_addr = _clean(addr_span.get_text(" ", strip=True))
                    m_post = re.search(r"(\d{3}-\d{4})", raw_addr)
                    if m_post:
                        postal = m_post.group(1)
                        raw_addr = raw_addr.replace(postal, "").strip()
                    m_pref = _PREF_PATTERN.match(raw_addr)
                    if m_pref:
                        pref = m_pref.group(1)
                        addr = raw_addr[m_pref.end():].strip()
                    else:
                        addr = raw_addr
                break

        return {
            Schema.URL:       detail_url,
            Schema.NAME:      name,
            Schema.POST_CODE: postal,
            Schema.PREF:      pref,
            Schema.ADDR:      addr,
            Schema.TEL:       _clean(_get_field(soup, "電話番号")),
            Schema.CAT_SITE:  _clean(_get_field(soup, "業種")),
            Schema.TIME:      _get_field(soup, "勤務時間"),
            Schema.HOLIDAY:   _clean(_get_field(soup, "休日")),
            "エリア":         _clean(_get_field(soup, "エリア")),
            "最寄り駅":       _get_field(soup, "最寄駅"),
            "給与":           _get_field(soup, "給与"),
            "職種":           _clean(_get_field(soup, "職種")),
            "待遇":           _get_field(soup, "待遇"),
            "雇用形態":       _clean(_get_field(soup, "雇用形態")),
            "応募方法":       _get_field(soup, "応募方法"),
            "受付時間":       _clean(_get_field(soup, "受付時間")),
            "担当者名":       _clean(_get_field(soup, "担当")),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = QueenWorkKansaiScraper()
    scraper.execute("https://kansai.queenwork.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

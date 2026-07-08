"""
一般社団法人日本能率協会 JMA会員企業一覧 (member-list.jma.or.jp) — 会員企業リスト

取得対象:
    - JMA 会員企業の 会員番号 / 法人名 / 入会年月 / HP URL (一覧ページから)
    - 各社 HP を訪問し、ベストエフォートで TEL・郵便番号・住所・都道府県を補完

取得フロー:
    1. /companies/index/{0..9} の 10 ページ (五十音: あ〜わ) を巡回
    2. 各ページの table 行から 会員番号・法人名・入会年月・HP を取得
    3. 各社の HP トップを訪問し、TEL/住所を抽出。無ければ「会社概要/会社案内/
       企業情報/アクセス/company/about」等のリンクを 1 段辿って再抽出
    4. 1 社取得ごとに即 yield (Pattern B)

備考対応:
    "index で複数ページを選択 → 法人名と HP を取得 → HP に入って TEL・住所等を補完"
    という指示に基づく実装。TEL/住所は各社サイト構造に依存するためベストエフォート
    (取得できない場合は空文字)。法人名・HP は一覧から確実に取得できる。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/member_list.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id member_list
"""

import re
import sys
from collections import deque
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# --- 抽出用の正規表現 ---
# TEL: "TEL/電話/Phone" ラベル付きを優先し、無ければ裸の電話番号
_TEL_LABELED = re.compile(r"(?:TEL|Tel|電話|℡|Phone|PHONE)[^0-9]{0,6}(0\d{1,4}[-‐(（]\d{1,4}[-‐)）]\d{3,4})")
_TEL_BARE = re.compile(r"0\d{1,4}[-‐]\d{1,4}[-‐]\d{3,4}")
# 郵便番号 + それに続く住所らしき日本語文字列
_POST_ADDR = re.compile(r"〒?\s*(\d{3}[-−－]\d{4})[\s　]*([一-龥ぁ-んァ-ヶ][^\n]{3,45})")
# 住所の末尾に紛れ込むノイズ (TEL/FAX/受付 等) の手前で切る
_ADDR_STOP = re.compile(r"(TEL|Tel|電話|℡|Phone|FAX|Fax|受付|営業|MAP|Map|地図|アクセス|\s{2,})")
# 都道府県
_PREF = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)

# 会社概要ページ候補リンクのスコアリング (テキスト・href 双方を対象)
# TEL/住所は「会社概要」に無く「事業所・アクセス・お問い合わせ」側にあることも多いため、
# BFS でこれら候補を広く辿れるようキーワードを厚めに用意する。
_LINK_SCORES = [
    ("会社概要", 10), ("企業概要", 10), ("会社案内", 9), ("企業情報", 8),
    ("会社情報", 8), ("会社データ", 8), ("所在地", 8), ("事業所", 7),
    ("拠点", 6), ("オフィス", 6), ("アクセス", 6), ("地図", 5),
    ("お問い合わせ", 5), ("問い合わせ", 5), ("問合せ", 5),
    ("overview", 7), ("outline", 7), ("profile", 6), ("gaiyo", 7),
    ("kaisha", 6), ("company", 5), ("corporate", 5), ("about", 4),
    ("offices", 6), ("office", 5), ("location", 6), ("access", 6),
    ("map", 5), ("contact", 5), ("inquiry", 4),
]


class MemberList(StaticCrawler):
    """一般社団法人日本能率協会 JMA会員企業一覧 スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 15
    # 一覧固有の構造化カラム (文章プロースは含めない)
    EXTRA_COLUMNS = ["会員番号", "入会年月"]
    # 各社 HP 内を BFS で辿る際の最大取得ページ数 (トップ含む)。
    # 必要な情報 (TEL/住所) が揃った時点で早期終了する。
    HP_MAX_PAGES = 8

    def parse(self, url: str):
        # 引数 url を唯一のルートとし、末尾のページ番号を 0〜9 に差し替えて派生
        rows_on_first = 0
        for page in range(10):
            page_url = re.sub(r"/\d+$", f"/{page}", url)
            soup = self.get_soup(page_url)
            if soup is None:
                continue

            table = soup.select_one("table")
            if table is None:
                continue

            trs = table.select("tr")
            data_rows = [tr for tr in trs if tr.select("td")]
            if page == 0:
                rows_on_first = len(data_rows)
                # ざっくりした総件数見積り (ETA 表示用)
                self.total_items = rows_on_first * 10 if rows_on_first else None

            for tr in data_rows:
                try:
                    item = self._build_item(tr, url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("行の解析に失敗 (スキップ): %s", e)
                    continue

    def _build_item(self, tr, root_url: str) -> dict | None:
        tds = tr.select("td")
        if len(tds) < 2:
            return None

        member_no = tds[0].get_text(strip=True)
        # 法人名 (アンカーがあればそのテキスト、無ければセル全体)
        name_a = tr.select_one("a[href]")
        name = (name_a.get_text(strip=True) if name_a else tds[1].get_text(strip=True))
        if not name:
            return None

        hp = ""
        if name_a:
            href = name_a.get("href", "").strip()
            if href and href.startswith("http"):
                hp = href

        join_ym = tds[2].get_text(strip=True) if len(tds) >= 3 else ""

        item = {
            Schema.URL: root_url,
            Schema.NAME: name,
            Schema.HP: hp,
            Schema.TEL: "",
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            "会員番号": member_no,
            "入会年月": join_ym,
        }

        # 備考: HP に入って TEL・住所を補完 (ベストエフォート)
        if hp:
            tel, post, addr = self._scrape_hp(hp)
            item[Schema.TEL] = tel
            item[Schema.POST_CODE] = post
            if addr:
                m = _PREF.match(addr)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = addr[m.end():].strip()
                else:
                    item[Schema.ADDR] = addr

        return item

    def _scrape_hp(self, hp: str):
        """会社 HP を BFS で辿り、TEL/郵便番号/住所をベストエフォートで抽出。

        トップページに連絡先が無く「会社概要」ページにも無い (「事業所・アクセス」等の
        別ページにある) ケースがあるため、同一ホスト内を会社概要スコア順の幅優先探索で
        辿り、必要情報 (TEL・住所) が揃うか HP_MAX_PAGES に達するまで補完する。
        """
        start = urldefrag(hp)[0]
        host = urlparse(start).netloc
        if not host:
            return "", "", ""

        tel = post = addr = ""
        visited: set[str] = set()
        # (優先度, url) のフロンティア。優先度が高い候補から取得する。
        # トップページは最優先 (large sentinel) で必ず最初に処理する。
        frontier: deque = deque()
        frontier.append((10_000, start))
        seen = {start}

        while frontier and len(visited) < self.HP_MAX_PAGES:
            # 優先度が最も高い候補を取り出す (会社概要 > 事業所 > … の順で辿る)
            best_i = max(range(len(frontier)), key=lambda i: frontier[i][0])
            _, cur = frontier[best_i]
            del frontier[best_i]
            if cur in visited:
                continue
            visited.add(cur)

            soup = self.get_soup(cur)
            if soup is None:
                continue

            c_tel, c_post, c_addr = self._extract_contact(soup)
            tel = tel or c_tel
            post = post or c_post
            addr = addr or c_addr
            # 必要な情報が揃ったら早期終了
            if tel and addr:
                break

            # まだ余力があれば、このページから会社概要系リンクを収集して frontier に追加
            if len(visited) < self.HP_MAX_PAGES:
                for score, link in self._scored_links(soup, cur, host):
                    if link not in seen:
                        seen.add(link)
                        frontier.append((score, link))

        return tel, post, addr

    @staticmethod
    def _extract_contact(soup):
        # script/style を除去してテキスト化
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        txt = soup.get_text(" ", strip=True)

        tel = ""
        m = _TEL_LABELED.search(txt)
        if m:
            tel = m.group(1)
        else:
            m = _TEL_BARE.search(txt)
            if m:
                tel = m.group(0)

        post = addr = ""
        m = _POST_ADDR.search(txt)
        if m:
            post = m.group(1).replace("−", "-").replace("－", "-")
            addr = m.group(2).strip()
            sm = _ADDR_STOP.search(addr)
            if sm:
                addr = addr[: sm.start()].strip()
        return tel, post, addr

    @staticmethod
    def _scored_links(soup, base: str, host: str):
        """同一ホスト内の会社概要系リンクを (スコア, 絶対URL) で列挙する (score>0 のみ)。"""
        out: dict[str, int] = {}
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if (not href or href.startswith("#")
                    or href.startswith("javascript") or "mailto:" in href
                    or href.startswith("tel:")):
                continue
            blob = (a.get_text(" ", strip=True) + " " + href).lower()
            score = 0
            for kw, w in _LINK_SCORES:
                if kw.lower() in blob:
                    score = max(score, w)
            if score <= 0:
                continue
            abs_url = urldefrag(urljoin(base, href))[0]
            parsed = urlparse(abs_url)
            # 同一ホストの http(s) のみ。PDF/画像等は除外。
            if parsed.scheme not in ("http", "https") or parsed.netloc != host:
                continue
            if re.search(r"\.(pdf|jpg|jpeg|png|gif|zip|docx?|xlsx?|pptx?)$", parsed.path, re.I):
                continue
            out[abs_url] = max(out.get(abs_url, 0), score)
        return [(score, url) for url, score in out.items()]


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MemberList()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://member-list.jma.or.jp/companies/index/0")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

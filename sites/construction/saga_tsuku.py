# scripts/sites/construction/saga_tsuku.py
"""
サガツク 仕事の広場 投稿者 (saga-tsuku.jp)

対象サイト: https://saga-tsuku.jp/search_work_articles?page=1&page_count=100

取得対象 (備考指示):
    - 粒度 = 会員 (/member/{会員ID} 単位)。同一会員の 2 件目以降の記事はスキップ
    - フィルタ (2025年以降・エリア) は後工程で行うため全件取得
    - 低並列 (逐次) + リクエスト間隔 DELAY 秒で負荷配慮

取得フロー (Pattern B: 会員 1 件取得ごとに即 yield):
    記事一覧 ?page=N&page_count=100 (約41ページ, 投稿日の新しい順)
      → 記事詳細 /show_work_articles/{記事ID}
           (投稿日・更新日・募集業種職種・投稿者プロフィールリンク)
      → 投稿者プロフィール /member/{会員ID}
           (事業所名・登録者氏名・TEL・メール・住所・事業内容・HP)

ページネーション:
    引数 url の page クエリを差し替えて巡回。記事リンク 0 件で打ち切る。

カラム補足:
    - 名称: 一覧では省略 ("Amm設備(ア...") されるためプロフィールの「会社名／事業所名（屋号）」
    - 最新投稿日: 記事の投稿日と更新日の新しい方。一覧は投稿日降順なので、会員を
      最初に検出した記事 (= 最新投稿の記事) の値を採用する。古い記事の方が後から
      更新されているケースは考慮しない
    - 未入力項目はサイト上 "-" 表示 → 空文字
    - 自己紹介・PR / 記事本文 / 業種ごとの＜仕事内容＞は自由記述の長文のため取得しない
      (事業内容は備考で明示指定されたため取得)

実行方法:
    python scripts/sites/construction/saga_tsuku.py
    docker compose exec worker python /app/bin/run_flow.py --site-id saga_tsuku
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    "東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    "滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    "香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_ARTICLE_RE = re.compile(r"^/show_work_articles/(\d+)$")
_MEMBER_RE = re.compile(r"^/member/(\d+)")
_DATE_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})\s*(投稿|更新)")
_MAX_PAGES = 200  # 暴走防止 (実際は約41ページ)


class SagaTsukuScraper(StaticCrawler):
    """サガツク 仕事の広場 投稿者 スクレイパー"""

    DELAY = 3.0       # リクエスト間隔 (秒)。_fetch() で通信ごとに待機
    ITEM_DELAY = 0.0  # 通信ごとに待機済みのためアイテム間ウェイトは不要

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "登録者氏名",
        "会員ID",
        "会員種別",
        "事業所区分",
        "対応可能エリア",
        "最新投稿日",
        "記事の投稿日",
        "記事の更新日",
        "募集業種・職種",
        "記事詳細URL",
    ]

    _last_request_at: float = 0.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_members: set[str] = set()
        seen_articles: set[str] = set()

        for page in range(1, _MAX_PAGES + 1):
            soup = self._fetch(self._page_url(url, page))
            if soup is None:
                break

            article_ids: list[str] = []
            for a in soup.select('a[href^="/show_work_articles/"]'):
                m = _ARTICLE_RE.match(a.get("href", ""))
                if m and m.group(1) not in seen_articles and m.group(1) not in article_ids:
                    article_ids.append(m.group(1))
            if not article_ids:
                break

            for article_id in article_ids:
                seen_articles.add(article_id)
                article_url = urljoin(url, f"/show_work_articles/{article_id}")
                article = self._parse_article(article_url)
                if not article or not article["member_id"]:
                    continue
                member_id = article["member_id"]
                if member_id in seen_members:
                    continue
                seen_members.add(member_id)

                item = self._parse_member(urljoin(url, f"/member/{member_id}"), member_id)
                if item is None:
                    continue
                item.update({
                    "最新投稿日": max(article["posted"], article["updated"]),
                    "記事の投稿日": article["posted"],
                    "記事の更新日": article["updated"],
                    "募集業種・職種": article["industry"],
                    "記事詳細URL": article_url,
                })
                yield item

    # ------------------------------------------------------------------
    # 通信
    # ------------------------------------------------------------------
    def _fetch(self, url: str):
        remain = self.DELAY - (time.monotonic() - self._last_request_at)
        if remain > 0:
            time.sleep(remain)
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            self._last_request_at = time.monotonic()
        return soup

    @staticmethod
    def _page_url(url: str, page: int) -> str:
        parts = urlparse(url)
        query = parse_qs(parts.query)
        query["page"] = [str(page)]
        query.setdefault("page_count", ["100"])
        return urlunparse(parts._replace(query=urlencode(query, doseq=True)))

    # ------------------------------------------------------------------
    # 記事詳細
    # ------------------------------------------------------------------
    def _parse_article(self, article_url: str) -> dict | None:
        soup = self._fetch(article_url)
        if soup is None:
            return None

        member_id = ""
        for a in soup.select('a[href^="/member/"]'):
            m = _MEMBER_RE.match(a.get("href", ""))
            if m:
                member_id = m.group(1)
                break

        posted = updated = ""
        # p.date は「募集期間：…まで」の場合もあるため date_area 内を優先
        date_el = soup.select_one("div.date_area p.date") or soup.select_one("p.date")
        if date_el:
            for y, mo, d, kind in _DATE_RE.findall(date_el.get_text(" ", strip=True)):
                value = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                if kind == "投稿":
                    posted = value
                else:
                    updated = value

        # 募集記事は「募集業種・職種」、仕事を探す記事は「業種・職種」
        industry = ""
        for dt in soup.select("dt"):
            if dt.get_text(strip=True) in ("募集業種・職種", "業種・職種"):
                dd = dt.find_next_sibling("dd")
                if dd:
                    industry = self._clean(dd.get_text(" ", strip=True))
                break

        return {"member_id": member_id, "posted": posted, "updated": updated, "industry": industry}

    # ------------------------------------------------------------------
    # 投稿者プロフィール
    # ------------------------------------------------------------------
    def _parse_member(self, member_url: str, member_id: str) -> dict | None:
        soup = self._fetch(member_url)
        if soup is None:
            return None

        info: dict[str, object] = {}
        for tr in soup.select("div.basic table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                info[th.get_text("", strip=True)] = td

        def text(label: str, sep: str = " ") -> str:
            td = info.get(label)
            return self._clean(td.get_text(sep, strip=True)) if td else ""

        name = text("会社名／事業所名（屋号）")
        if not name:
            return None

        # 業種・職種: 各 div.business_type_item の先頭行 (業種名) のみ。＜仕事内容＞は自由記述のため除外
        categories = []
        td = info.get("業種・職種")
        if td:
            for div in td.select("div.business_type_item"):
                first = next((s.strip() for s in div.find_all(string=True, recursive=False) if s.strip()), "")
                label = re.split(r"[　\s]{2,}", first)[0].strip()
                if label and label not in categories:
                    categories.append(label)

        hp = ""
        td = info.get("ホームページ")
        if td:
            a = td.find("a", href=True)
            hp = (a["href"].strip() if a else "") or text("ホームページ")
            if not hp.startswith("http"):
                hp = ""

        post_code, pref, addr = self._split_address(text("住所"))

        member_type = ""
        h1 = soup.find("h1")
        if h1:
            for span in h1.find_all("span"):
                t = span.get_text(strip=True)
                if t.endswith("会員"):
                    member_type = t
                    break

        return {
            Schema.URL: member_url,
            Schema.NAME: name,
            Schema.TEL: text("電話番号"),
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.EMAIL: text("メールアドレス"),
            Schema.LOB: text("事業内容", "\n"),
            Schema.HP: hp,
            Schema.CAT_SITE: " / ".join(categories),
            "登録者氏名": text("登録者氏名"),
            "会員ID": member_id,
            "会員種別": member_type,
            "事業所区分": text("事業所区分"),
            "対応可能エリア": text("対応可能エリア"),
        }

    # ------------------------------------------------------------------
    # ユーティリティ
    # ------------------------------------------------------------------
    @staticmethod
    def _clean(value: str) -> str:
        value = (value or "").strip()
        return "" if value in ("-", "－", "なし") else value

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        raw = (raw or "").strip()
        post_code = ""
        m = re.match(r"〒?\s*(\d{3}[-－ー]?\d{4})\s*", raw)
        if m:
            digits = re.sub(r"\D", "", m.group(1))
            post_code = f"{digits[:3]}-{digits[3:]}"
            raw = raw[m.end():].strip()
        pref = ""
        m = re.match(rf"({_PREFS})\s*", raw)
        if m:
            pref = m.group(1)
            raw = raw[m.end():].strip()
        return post_code, pref, raw


if __name__ == "__main__":
    scraper = SagaTsukuScraper()
    scraper.execute("https://saga-tsuku.jp/search_work_articles?page=1&page_count=100")

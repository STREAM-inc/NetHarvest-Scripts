"""
日経メッセ (NIKKEI MESSE) 出展者一覧 — 日本経済新聞社主催展示会の出展者を全展示会分取得する

取得対象:
    - JAPAN SHOP / リテールテックJAPAN / SECURITY SHOW / フランチャイズ・ショー /
      人手不足対策展 / インバウンドビジネス展 / 建築・建材展 / ライティング＆家電フェア /
      ビルメンCONNECT / エコプロ各展 / 大阪開催分 など、トップページから辿れる全展示会
    - 2026-11 時点で 21 展示会・約 1,580 出展者 (各展示会とも掲載中の回のみ公開。
      過去回の一覧は公開されておらず、掲載が残っている回 = 現行一覧を取得する)

取得フロー:
    1. 起点 url (トップページ) から展示会トップ /{code}/ のリンクを列挙
    2. 各展示会トップから出展者一覧 /exhibitor/index/{CODE}/ja へのリンクを取得
       (一覧リンクの無い展示会はスキップ)
    3. 出展者一覧は1ページに全件 (ページ送りなし)。展示会名と開催年 "(2026年)" を取得
    4. 一覧カード (p.ttl.exhibitors) 内の出展者ごとに詳細 /exhibitor/info/{CODE}/ja/{ID}/ を
       取得して即 yield。共同出展者で詳細ページが無いものは一覧の名称 (+外部リンク) のみで yield

備考:
    - 1社1展示会1行。同一展示会内の重複 (同一ID / 同一名称) は除外
    - 事業内容は備考で取得指定があるため Schema.LOB に格納 (展示内容/PR文は取得しない)
    - 利用規約 (rule.html) にスクレイピング禁止条項なし (2026-11-27 確認)

実行方法:
    python scripts/sites/corporate/messe.py
    python bin/run_flow.py --site-id messe
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

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
_PREF_RE = re.compile(rf"^({_PREFS})")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class MesseCrawler(StaticCrawler):
    """日経メッセ 出展者一覧 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "展示会名",
        "開催年",
        "展示会コード",
        "小間番号",
        "出展エリア",
        "出展内容の分類",
        "担当部署",
        "英文社名",
        "出展者詳細URL",
    ]

    _SHOW_LINK_RE = re.compile(r"^/([a-z]{2})/$")
    _INDEX_RE = re.compile(r"/exhibitor/index/([A-Za-z0-9]+)/ja/?$")
    _INFO_RE = re.compile(r"/exhibitor/info/([A-Za-z0-9]+)/ja/(\d+)/?")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _txt(node) -> str:
        if node is None:
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

    def _safe_soup(self, url: str):
        try:
            return self.get_soup(url)
        except requests.exceptions.RequestException as e:
            self.logger.warning("取得失敗 (スキップ): %s — %s", url, e)
            return None

    def _show_codes(self, url: str, soup) -> list:
        """トップページから展示会トップ /{code}/ を出現順に列挙する"""
        host = urlparse(url).netloc
        codes = []
        for a in soup.find_all("a", href=True):
            full = urljoin(url, a["href"])
            p = urlparse(full)
            if p.netloc != host:
                continue
            m = self._SHOW_LINK_RE.match(p.path)
            if m and m.group(1) not in codes and m.group(1) not in ("en", "ex"):
                codes.append(m.group(1))
        return codes

    def _parse_detail(self, soup) -> dict:
        d = {}
        h2 = soup.select_one("h2")
        d["name"] = self._txt(h2)

        for th in soup.find_all("th"):
            label = self._txt(th)
            td = th.find_next_sibling("td")
            if td is None:
                continue
            if label == "住所":
                raw = self._txt(td)
                zm = _ZIP_RE.search(raw)
                d["zip"] = zm.group(1) if zm else ""
                addr = _ZIP_RE.sub("", raw, count=1).strip() if zm else raw
                # 都道府県の後ろに入る余分な空白を詰める ("東京都 千代田区" → "東京都千代田区")
                addr = re.sub(rf"^({_PREFS})\s+", r"\1", addr)
                d["addr"] = addr
                pm = _PREF_RE.match(addr)
                d["pref"] = pm.group(1) if pm else ""
            elif label == "電話番号":
                d["tel"] = self._txt(td)
            elif label == "URL":
                a = td.find("a", href=True)
                d["hp"] = a["href"].strip() if a else self._txt(td)
            elif label == "担当部署":
                d["dept"] = self._txt(td)
            elif label == "英文社名":
                d["en"] = self._txt(td)

        biz = soup.select_one("div.businessDescriptionContainer div.content")
        if biz is not None:
            d["lob"] = biz.get_text("\n", strip=True)

        # 出展エリア / 出展内容の分類
        for sub in soup.select("div.subTitle"):
            label = self._txt(sub)
            box = sub.parent
            vals = [self._txt(x) for x in box.select("li")] or [
                self._txt(x) for x in box.find_all(["p", "a"]) if self._txt(x)
            ]
            vals = [v for v in vals if v and v != label]
            if label == "出展エリア":
                d["area"] = " / ".join(dict.fromkeys(vals))
            elif label == "出展内容の分類":
                d["cats"] = " / ".join(dict.fromkeys(vals))

        # ソーシャルメディア
        for card in soup.select("div.socialContainer div.card"):
            title = self._txt(card.select_one("span.title")).lower()
            body = card.select_one("div.body")
            link = ""
            if body is not None:
                a = body.find("a", href=True)
                if a:
                    link = a["href"].strip()
                else:
                    fb = body.select_one("[data-href]")
                    if fb:
                        link = fb["data-href"].strip()
            if not link:
                continue
            if "twitter" in title or title == "x":
                d["x"] = link
            elif "facebook" in title:
                d["fb"] = link
            elif "instagram" in title:
                d["insta"] = link
            elif "tiktok" in title:
                d["tiktok"] = link
            elif "line" in title:
                d["line"] = link
        return d

    # -------------------------------------------------------------------- parse
    def parse(self, url: str):
        top = self.get_soup(url)
        if top is None:
            self.logger.error("トップページを取得できませんでした: %s", url)
            return

        codes = self._show_codes(url, top)
        self.logger.info("展示会トップ候補: %d 件 %s", len(codes), codes)

        seen_index = set()
        for code in codes:
            show_soup = self._safe_soup(urljoin(url, f"/{code}/"))
            if show_soup is None:
                continue
            index_url = ""
            for a in show_soup.find_all("a", href=True):
                full = urljoin(url, a["href"])
                if self._INDEX_RE.search(urlparse(full).path):
                    index_url = full
                    break
            if not index_url:
                self.logger.info("出展者一覧なし: /%s/", code)
                continue
            show_code = self._INDEX_RE.search(urlparse(index_url).path).group(1)
            if show_code in seen_index:
                continue
            seen_index.add(show_code)

            idx = self._safe_soup(index_url)
            if idx is None:
                continue
            title = self._txt(idx.title)
            show_name = title.split("|", 1)[1].strip() if "|" in title else title
            ym = re.search(r"\((\d{4})年\)", idx.get_text(" ", strip=True))
            year = ym.group(1) if ym else ""

            seen = set()
            for li in idx.select("div.listbox li"):
                names_p = li.select_one("p.ttl.exhibitors")
                if names_p is None:
                    continue
                booth = self._txt(li.select_one("p.booth-number"))
                booth = re.sub(r"^小間番号\s*[:：]\s*", "", booth)

                # 共同出展者は "/" 区切りで a タグ (詳細 or 外部HP) / 素テキストが混在
                for part in names_p.decode_contents().split(" / "):
                    frag = BeautifulSoup(part, "html.parser")
                    name = self._txt(frag)
                    if not name:
                        continue
                    a = frag.find("a", href=True)
                    detail_url, hp, ex_id = "", "", ""
                    if a is not None:
                        full = urljoin(url, a["href"])
                        m = self._INFO_RE.search(full)
                        if m:
                            detail_url, ex_id = full, m.group(2)
                        elif full.startswith("http"):
                            hp = full
                    key = ex_id or name
                    if key in seen:
                        continue
                    seen.add(key)

                    d = {}
                    if detail_url:
                        ds = self._safe_soup(detail_url)
                        if ds is not None:
                            d = self._parse_detail(ds)

                    yield {
                        Schema.URL: detail_url or index_url,
                        Schema.NAME: d.get("name") or name,
                        Schema.PREF: d.get("pref", ""),
                        Schema.POST_CODE: d.get("zip", ""),
                        Schema.ADDR: d.get("addr", ""),
                        Schema.TEL: d.get("tel", ""),
                        Schema.HP: d.get("hp") or hp,
                        Schema.LOB: d.get("lob", ""),
                        Schema.X: d.get("x", ""),
                        Schema.FB: d.get("fb", ""),
                        Schema.INSTA: d.get("insta", ""),
                        Schema.TIKTOK: d.get("tiktok", ""),
                        Schema.LINE: d.get("line", ""),
                        "展示会名": show_name,
                        "開催年": year,
                        "展示会コード": show_code,
                        "小間番号": booth,
                        "出展エリア": d.get("area", ""),
                        "出展内容の分類": d.get("cats", ""),
                        "担当部署": d.get("dept", ""),
                        "英文社名": d.get("en", ""),
                        "出展者詳細URL": detail_url,
                    }


if __name__ == "__main__":
    scraper = MesseCrawler()
    scraper.execute("https://messe.nikkei.co.jp/")

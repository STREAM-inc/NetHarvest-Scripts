"""
インタビューマガジン B.S.TIMES — 掲載企業 / 設置協力先 スクレイパー

対象サイト: https://bs-times.com/html/magazine.html

取得対象:
    (1) 記事掲載企業 (掲載区分=記事掲載)
        誌面紹介 (html/magazine.html) に並ぶ全 66 号の各号ページ
        (../vol{N}/news.cgi) を巡回する。各号ページの table.table01 1 個が
        1 記事で、td01a=記事カテゴリ / span.ttl01=掲載名称 /
        span.ttl02=記事見出し / a[href=N.html]=記事詳細。
        詳細ページ (#magazine) 末尾の「■〓■〓…」区切りブロックに
        名称 / 代表者 / 所在地 / TEL / HP / SNS が改行区切りで載っている。
        区切りが無い初期号は本文末尾の短い行から所在地/TEL/HP を拾う。
        ※ Vol.5 はフリップブック (vol5book) のみで記事ページが無いため 0 件。
    (2) 設置協力先 (掲載区分=設置協力先)
        誌面紹介ページからリンクされる設置場所紹介 (../secchi/news.cgi) の
        「○北海道」欄の協力先名称 (都道府県=北海道)。

    出力は全国分。後工程で北海道のみ抽出できるよう 都道府県 / 住所 を必ず出す。
    記事本文・リード文 (td04) は長文の自由記述のため著作権リスク回避で取得しない。
    記事見出しは備考で明示的に取得指示があるため EXTRA に含める。

文字コード:
    サーバーは Content-Type に charset を付けない Shift_JIS (実体 CP932)。
    get_soup の apparent_encoding 推測に任せると ㈱・髙 等で化けるため、
    セッションのレスポンスフックで charset=cp932 を補う。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/b_s_times.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id b_s_times
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 各号ページ (例: ../vol66/news.cgi)
_VOL_RE = re.compile(r"vol(\d+)/news\.cgi$")
# 記事詳細 (例: 12.html)
_DETAIL_RE = re.compile(r"^\d+\.html$")

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREF})")
# 都道府県を省略した政令市等 → 都道府県 の補完
_CITY_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県", "千葉市": "千葉県",
    "横浜市": "神奈川県", "川崎市": "神奈川県", "相模原市": "神奈川県",
    "新潟市": "新潟県", "静岡市": "静岡県", "浜松市": "静岡県", "名古屋市": "愛知県",
    "京都市": "京都府", "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県",
    "岡山市": "岡山県", "広島市": "広島県", "北九州市": "福岡県", "福岡市": "福岡県",
    "熊本市": "熊本県",
}
_POST_RE = re.compile(r"〒\s*(\d{3}-?\d{4})")
# 住所らしい行: 都道府県始まり or 市区町村+番地数字
_ADDR_RE = re.compile(rf"^(〒\s*\d{{3}}-?\d{{4}}\s*)?(({_PREF})|\S{{1,6}}[市区郡]).*[0-9０-９一二三四五六七八九十]")
_TEL_RE = re.compile(r"(?:TEL|ＴＥＬ|電話)[^0-9０-９]{0,6}([0-9０-９][0-9０-９\-－‐()（） ]{8,}[0-9０-９])", re.I)
_FAX_RE = re.compile(r"FAX[^0-9]{0,4}([0-9][0-9\-() ]{8,}[0-9])", re.I)
_URL_RE = re.compile(r"https?://\S+")
_REP_RE = re.compile(r"^(代表取締役社長|代表取締役会長|代表取締役|代表社員|代表理事|代表|理事長|院長|社長|会長|店主|オーナー|園長|学長|校長|所長|取締役\S*|専務取締役|常務取締役)[\s　]+(.+)$")
_SNS_DOMAINS = {
    "instagram.com": Schema.INSTA, "facebook.com": Schema.FB,
    "twitter.com": Schema.X, "x.com": Schema.X, "tiktok.com": Schema.TIKTOK,
    "lin.ee": Schema.LINE, "line.me": Schema.LINE,
}


def _clean(s: str) -> str:
    return re.sub(r"[\s　]+", " ", s or "").strip()


def _split_addr(addr: str) -> tuple[str, str, str]:
    """所在地 → (郵便番号, 都道府県, 住所)。都道府県省略時は政令市名から補完。"""
    post = ""
    m = _POST_RE.search(addr)
    if m:
        post = m.group(1)
        addr = _clean(addr[m.end():])
    m = _PREF_RE.match(addr)
    if m:
        return post, m.group(1), _clean(addr[m.end():])
    for city, pref in _CITY_PREF.items():
        if addr.startswith(city):
            return post, pref, addr
    return post, "", addr


class BSTimesScraper(StaticCrawler):
    """インタビューマガジン B.S.TIMES スクレイパー"""

    DELAY = 3.0
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = ["掲載区分", "掲載号", "発行日", "記事見出し", "FAX"]

    def _force_cp932(self) -> None:
        """charset 無しのレスポンスに charset=cp932 を補う (一度だけ登録)。"""
        if getattr(self, "_cp932_hooked", False):
            return

        def _hook(resp, *args, **kwargs):
            ct = resp.headers.get("Content-Type", "")
            if "text/html" in ct.lower() and "charset=" not in ct.lower():
                resp.headers["Content-Type"] = ct + "; charset=cp932"
                # encoding はフック実行前にヘッダから決定済み (ISO-8859-1) のため上書きする
                resp.encoding = "cp932"
            return resp

        self.session.hooks.setdefault("response", []).append(_hook)
        self._cp932_hooked = True

    def parse(self, url: str) -> Generator[dict, None, None]:
        self._force_cp932()
        soup = self.get_soup(url)
        if soup is None:
            return

        # 号リンク + 発行日見出し (h3: "Vol.66　2026.10/15発行" / "創刊号　2015.11/15発行")
        vols: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for a in soup.select("#shimenshokai a[href]") or soup.select("a[href]"):
            href = a.get("href", "")
            m = _VOL_RE.search(href)
            if not m:
                continue
            vol_url = urljoin(url, href)
            if vol_url in seen:
                continue
            seen.add(vol_url)
            h3 = a.find_previous("h3")
            h3_text = _clean(h3.get_text()) if h3 else ""
            date_m = re.search(r"(\d{4}\.\d{1,2}/\d{1,2})", h3_text)
            vols.append((vol_url, f"Vol.{m.group(1)}", date_m.group(1) if date_m else ""))

        secchi = soup.select_one('a[href*="secchi/news.cgi"]')

        # (1) 記事掲載企業: 号ごとに一覧 → 詳細を 1 件ずつ取得即 yield
        for vol_url, vol_label, pub_date in vols:
            vsoup = self.get_soup(vol_url)
            if vsoup is None:
                continue
            for table in vsoup.select("table.table01"):
                item = self._parse_list_entry(table, vol_url)
                if item is None:
                    continue
                item["掲載号"] = vol_label
                item["発行日"] = pub_date
                detail_url = item.pop("_detail")
                if detail_url:
                    dsoup = self.get_soup(detail_url)
                    if dsoup is not None:
                        info = self._parse_detail(dsoup, detail_url)
                        detail_name = info.pop("_name", "")
                        item.update(info)
                        if not item[Schema.NAME]:
                            item[Schema.NAME] = detail_name
                if not item[Schema.NAME]:
                    continue
                yield item

        # (2) 設置協力先: 「○北海道」欄
        if secchi is not None:
            secchi_url = urljoin(url, secchi["href"])
            ssoup = self.get_soup(secchi_url)
            if ssoup is not None:
                yield from self._parse_secchi(ssoup, secchi_url, "北海道")

    # ------------------------------------------------------------------
    def _parse_list_entry(self, table: bs4.Tag, vol_url: str) -> dict | None:
        td02 = table.select_one("td.td02")
        if td02 is None:
            return None
        ttl01 = td02.select_one("span.ttl01")
        if ttl01 is not None:
            # Vol.3〜: span.ttl01=名称 / span.ttl02=見出し
            name = " / ".join(_clean(s).strip("【】") for s in ttl01.stripped_strings)
            ttl02 = td02.select_one("span.ttl02")
            headline = " ".join(_clean(s) for s in ttl02.stripped_strings) if ttl02 else ""
        else:
            # 初期号: "【名称】<br>見出し" または見出しのみ (名称は詳細から補完)
            text = " ".join(_clean(s) for s in td02.stripped_strings)
            m = re.match(r"^【([^】]+)】\s*(.*)$", text)
            name, headline = (m.group(1).strip(), m.group(2).strip()) if m else ("", text)
        cat = table.select_one("td.td01a")
        if cat is not None:
            cat_text = _clean(cat.get_text())
        else:
            img = table.select_one("td.td01 img[alt]")
            cat_text = _clean(img["alt"]) if img else ""
        detail = ""
        for a in table.select("a[href]"):
            if _DETAIL_RE.match(a["href"].strip()):
                detail = urljoin(vol_url, a["href"].strip())
                break
        return {
            Schema.URL: detail or vol_url,
            Schema.NAME: name,
            Schema.CAT_SITE: cat_text,
            "記事見出し": headline,
            "掲載区分": "記事掲載",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.REP_NM: "",
            Schema.POS_NM: "",
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.FB: "",
            Schema.X: "",
            Schema.LINE: "",
            Schema.TIKTOK: "",
            "FAX": "",
            "_detail": detail,
        }

    def _info_lines(self, soup: bs4.BeautifulSoup) -> list[str]:
        """詳細末尾の会社情報行を返す。■区切りブロック優先、無ければ本文末尾。"""
        main = soup.select_one("#magazine") or soup.select_one("#main_center")
        if main is None:
            return []
        lines = [_clean(s) for s in main.get_text("\n").split("\n")]
        lines = [s for s in lines if s]
        marks = [i for i, s in enumerate(lines) if s.startswith("■") and len(s) >= 6 and set(s) <= set("■〓□◆◇=＝-")]
        if len(marks) >= 2:
            return lines[marks[-2] + 1:marks[-1]]
        # フォールバック: 最後の長文 (本文/コラム) 以降の短い行
        tail: list[str] = []
        for s in reversed(lines):
            if len(s) > 60:
                break
            tail.append(s)
        tail.reverse()
        return [s for s in tail if not s.startswith(("［", "["))][-8:]

    def _parse_detail(self, soup: bs4.BeautifulSoup, detail_url: str) -> dict:
        out: dict = {}
        lines = self._info_lines(soup)
        name = ""
        for i, s in enumerate(lines):
            if (not name and not _TEL_RE.search(s) and not _FAX_RE.search(s) and not _URL_RE.search(s)
                    and not _ADDR_RE.match(s) and not _REP_RE.match(s) and not s.startswith(("【", "〒"))
                    and len(s) <= 40):
                name = s
            if not out.get(Schema.TEL):
                m = _TEL_RE.search(s)
                if m:
                    out[Schema.TEL] = _clean(m.group(1))
            if not out.get("FAX"):
                m = _FAX_RE.search(s)
                if m:
                    out["FAX"] = _clean(m.group(1))
            if not out.get(Schema.ADDR) and _ADDR_RE.match(s) and not _TEL_RE.search(s):
                post, pref, addr = _split_addr(s)
                out[Schema.POST_CODE], out[Schema.PREF], out[Schema.ADDR] = post, pref, addr
            if not out.get(Schema.REP_NM):
                m = _REP_RE.match(s)
                if m and len(s) <= 30:
                    out[Schema.POS_NM], out[Schema.REP_NM] = m.group(1), _clean(m.group(2))
            if s in ("【Instagram】", "Instagram") and i + 1 < len(lines) and not out.get(Schema.INSTA):
                out[Schema.INSTA] = lines[i + 1]
            for u in _URL_RE.findall(s):
                key = next((k for d, k in _SNS_DOMAINS.items() if d in u), Schema.HP)
                if not out.get(key):
                    out[key] = u
        # リンク要素 (テキストが省略表記のこともある)
        main = soup.select_one("#magazine")
        if main is not None and not out.get(Schema.HP):
            for a in main.select('a[href^="http"]'):
                href = a["href"]
                if "bs-times.com" in href:
                    continue
                key = next((k for d, k in _SNS_DOMAINS.items() if d in href), Schema.HP)
                if not out.get(key) and _clean(a.get_text()) in lines:
                    out[key] = href
        out["_name"] = name
        return out

    def _parse_secchi(self, soup: bs4.BeautifulSoup, page_url: str, region: str) -> Generator[dict, None, None]:
        """設置場所紹介ページの「○{region}」欄の協力先を yield。"""
        main = soup.select_one("#main_center") or soup
        for td in main.select("td"):
            lines = [_clean(s) for s in td.get_text("\n").split("\n")]
            lines = [s for s in lines if s]
            in_region = False
            for s in lines:
                if s.startswith("○"):
                    in_region = s.lstrip("○").strip() == region
                    continue
                if not in_region or s.startswith(("【", "・")) or s == "ほか":
                    continue
                name = re.sub(r"[\s]*他$", "", s).strip()
                pref = region if region in ("北海道",) else ""
                m = re.search(r"[（(]([^()（）]+)[)）]$", name)
                if m and _PREF_RE.match(m.group(1)):
                    pref = _PREF_RE.match(m.group(1)).group(1)
                    name = name[:m.start()].strip()
                if not name:
                    continue
                yield {
                    Schema.URL: page_url,
                    Schema.NAME: name,
                    Schema.PREF: pref,
                    "掲載区分": "設置協力先",
                }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BSTimesScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://bs-times.com/html/magazine.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
エステラブワーク (job.eslove.jp) — メンズエステ求人情報スクレイパー

取得対象:
    - メンズエステ / リラクゼーション系店舗の求人掲載情報（全国）

取得フロー:
    1. トップページから都道府県別一覧URL (/{region}/{pref}) を収集
    2. 各都道府県ページを ?page=N でページネーション (全ページ)
    3. a[href^="/detail/"] リンクを収集
    4. 各詳細ページから dt/dd・th/td テキストマッチングでフィールド抽出

実行方法:
    python scripts/sites/nightlife/estelove_work.py
    python bin/run_flow.py --site-id estelove_work

注意:
    本サイトは Google Cloud Armor により、データセンター / VPN 帯域の IP を
    ブロックする。住宅回線または専用サーバーから実行すること。
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


BASE_URL = "https://job.eslove.jp"

# /{region}/{prefecture} 形式の URL だけを拾う（数字・記号を含む path は除外）
_PREF_URL_RE = re.compile(r"^/[a-z]+/[a-z]+$")

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"(0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4})")

# ---- フィールドラベル候補（テキストマッチング用） ----
_SALARY_LBL    = {"給与", "お給料", "日給", "時給", "給料", "報酬", "給与・待遇"}
_ADDR_LBL      = {"勤務地", "住所", "所在地", "エリア・勤務地"}
_EMPLOY_LBL    = {"雇用形態", "雇用形態・勤務形態", "契約形態"}
_JOBDESC_LBL   = {"仕事内容", "お仕事内容", "業務内容", "仕事の内容"}
_QUALIFY_LBL   = {"応募資格", "応募条件", "対象となる方", "応募について", "資格・経験"}
_BENEFIT_LBL   = {"待遇", "福利厚生", "待遇・福利厚生", "待遇/福利厚生"}
_HOURS_LBL     = {"勤務時間", "営業時間", "出勤時間", "シフト"}
_TRANSPORT_LBL = {"交通費", "交通費支給"}
_AREA_LBL      = {"エリア", "勤務エリア", "地域", "最寄り駅", "アクセス", "エリア/最寄駅"}
_TEL_LBL       = {"TEL", "電話番号", "連絡先", "お電話"}

MAX_PAGES = 300


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _multiline(text: str) -> str:
    if not text:
        return ""
    lines = [_clean(ln) for ln in text.replace("\r", "\n").split("\n")]
    out: list[str] = []
    prev_blank = False
    for ln in lines:
        if ln == "":
            if not prev_blank and out:
                out.append("")
            prev_blank = True
        else:
            out.append(ln)
            prev_blank = False
    return "\n".join(out).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """住所文字列から (郵便番号, 都道府県, 住所以降) を返す。"""
    if not raw:
        return "", "", ""
    post = ""
    m = _POST_RE.search(raw)
    if m:
        post = m.group(1)
        if "-" not in post:
            post = f"{post[:3]}-{post[3:]}"
    text = _POST_RE.sub("", raw).strip()
    pref, addr = "", text
    pm = _PREF_RE.search(text)
    if pm:
        pref = pm.group(1)
        addr = text[pm.end():].strip()
    return post, pref, addr


def _build_info_dict(soup: BeautifulSoup) -> dict[str, str]:
    """dl/dt/dd と table/th/td からラベル→値辞書を構築する。"""
    info: dict[str, str] = {}
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            k = _clean(dt.get_text(" ", strip=True))
            v = _multiline(dd.get_text("\n", strip=True))
            if k and k not in info:
                info[k] = v
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                k = _clean(th.get_text(" ", strip=True))
                v = _multiline(td.get_text("\n", strip=True))
                if k and k not in info:
                    info[k] = v
    return info


def _match(info: dict[str, str], labels: set[str]) -> str:
    """ラベルセット内のキーを info から探して最初にヒットした値を返す。"""
    for k, v in info.items():
        if k in labels or any(lbl in k for lbl in labels):
            return v
    return ""


def _extract_social(soup: BeautifulSoup) -> dict[str, str]:
    social = {"hp": "", "instagram": "", "tiktok": "", "x": "", "line": "", "facebook": ""}
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("tel:") or href.startswith("mailto:"):
            continue
        low = href.lower()
        if "instagram.com" in low and not social["instagram"]:
            social["instagram"] = href
        elif "tiktok.com" in low and not social["tiktok"]:
            social["tiktok"] = href
        elif ("twitter.com" in low or "x.com" in low) and not social["x"] and "intent" not in low:
            social["x"] = href
        elif "line.me" in low and not social["line"]:
            social["line"] = href
        elif "facebook.com" in low and not social["facebook"]:
            social["facebook"] = href
    return social


class EsteloveWorkScraper(DynamicCrawler):
    """エステラブワーク (job.eslove.jp) メンズエステ求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "給与",
        "雇用形態",
        "仕事内容",
        "応募資格",
        "待遇・福利厚生",
        "交通費",
        "エリア",
    ]

    def parse(self, url: str):
        # Step 1: トップページから都道府県別一覧 URL を収集
        pref_urls = self._collect_pref_urls(url)
        self.logger.info("都道府県ページ収集: %d 件", len(pref_urls))

        # Step 2: 各都道府県ページから /detail/{ID} URL を全ページ収集
        detail_urls: list[str] = []
        seen_detail: set[str] = set()
        for pref_url in pref_urls:
            for u in self._collect_detail_urls(pref_url):
                if u not in seen_detail:
                    seen_detail.add(u)
                    detail_urls.append(u)

        self.total_items = len(detail_urls)
        self.logger.info("詳細ページ合計: %d 件", self.total_items)

        # Step 3: 各詳細ページを取得
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception:
                self.logger.exception("詳細取得失敗: %s", detail_url)

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------

    def _collect_pref_urls(self, base_url: str) -> list[str]:
        """トップページのナビゲーションから /{region}/{pref} URL を収集する。"""
        soup = self.get_soup(base_url)
        if soup is None:
            return []
        result: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            path = (a.get("href") or "").strip()
            if _PREF_URL_RE.match(path):
                full = urljoin(BASE_URL, path)
                if full not in seen:
                    seen.add(full)
                    result.append(full)
        return result

    def _collect_detail_urls(self, pref_url: str) -> list[str]:
        """都道府県ページを ?page=N で巡回し、/detail/{ID} リンクを収集する。"""
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, MAX_PAGES + 1):
            page_url = f"{pref_url}?page={page}" if page > 1 else pref_url
            soup = self.get_soup(page_url)
            if soup is None:
                break
            found_on_page = False
            for a in soup.find_all("a", href=True):
                href = (a.get("href") or "").strip()
                if href.startswith("/detail/"):
                    full = urljoin(BASE_URL, href)
                    found_on_page = True
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
            if not found_on_page:
                break
        self.logger.info("  %s: %d 件", pref_url, len(urls))
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 店舗名: h1 からタイトルサフィックスを除去
        name = ""
        h1 = soup.find("h1")
        if h1:
            raw = _clean(h1.get_text(" ", strip=True))
            raw = re.sub(r"\s*のメンズエステ求人情報.*$", "", raw).strip()
            raw = re.sub(r"^【公式】\s*", "", raw).strip()
            name = raw
        if not name:
            return None

        # 情報テーブル (dt/dd, th/td)
        info = _build_info_dict(soup)

        # 住所・都道府県
        raw_addr = _match(info, _ADDR_LBL)
        post, pref, addr = _split_address(raw_addr)

        # 都道府県がページ上で取れない場合はタイトルから補完
        if not pref:
            title_tag = soup.find("title")
            if title_tag:
                m = _PREF_RE.search(title_tag.get_text())
                if m:
                    pref = m.group(1)

        # TEL: ラベルマッチ → tel: リンク → テキスト正規表現
        tel = _match(info, _TEL_LBL)
        if not tel:
            tel_a = soup.find("a", href=re.compile(r"^tel:"))
            if tel_a:
                tel = tel_a.get("href", "").replace("tel:", "").strip()
        if not tel:
            m = _TEL_RE.search(soup.get_text(" ", strip=True))
            if m:
                tel = m.group(1)

        # 公式 HP: "公式HP" ラベルの隣の a タグ
        hp = ""
        for k, v in info.items():
            if "公式" in k and ("hp" in k.lower() or "hp" in v.lower() or v.startswith("http")):
                hp = v
                break
        if not hp:
            for a in soup.find_all("a", string=re.compile(r"公式(HP|サイト|ホームページ)", re.I)):
                hp = a.get("href", "").strip()
                if hp:
                    break

        social = _extract_social(soup)

        return {
            Schema.URL:       url,
            Schema.NAME:      name,
            Schema.PREF:      pref,
            Schema.POST_CODE: post,
            Schema.ADDR:      addr,
            Schema.TEL:       tel,
            Schema.HP:        hp,
            Schema.LINE:      social["line"],
            Schema.INSTA:     social["instagram"],
            Schema.TIKTOK:    social["tiktok"],
            Schema.X:         social["x"],
            Schema.FB:        social["facebook"],
            Schema.TIME:      _match(info, _HOURS_LBL),
            # EXTRA_COLUMNS
            "給与":           _match(info, _SALARY_LBL),
            "雇用形態":       _match(info, _EMPLOY_LBL),
            "仕事内容":       _match(info, _JOBDESC_LBL),
            "応募資格":       _match(info, _QUALIFY_LBL),
            "待遇・福利厚生": _match(info, _BENEFIT_LBL),
            "交通費":         _match(info, _TRANSPORT_LBL),
            "エリア":         _match(info, _AREA_LBL),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EsteloveWorkScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

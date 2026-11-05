"""
マイベストプロ北海道 — 北海道の登録専門家(プロ)一覧

対象サイト: https://mbp-japan.com/hokkaido/

取得フロー:
    1. 起点 url (/hokkaido/) のジャンルリンク (/hokkaido/search/area:1/genre:...) から
       都道府県全域のエリアコード (area:1) を導出し、/hokkaido/search/area:1/ を一覧とする
       (全 133 件・1 ページ 15 人)。札幌市限定の area:1:01001 は使わない。
    2. 一覧を ?page=N で巡回し、プロ個別ページ /hokkaido/{プロID}/ を取得して即 yield
    3. 個別ページの div.pro-profile-blk 内 dl (dt ラベル → dd 値) から抽出

備考:
    - ホームページの href は /hokkaido/cc/... のリダイレクタ (robots.txt で Disallow) のため
      辿らず、リンクテキスト (実 URL) を採用する。
    - 一覧の並び順は訪問者(Cookie セッション)ごとにランダム。同一 Session 内では固定のため
      self.session を使い回す get_soup() で全ページ巡回すれば 133 件を網羅できる
      (並び替えパラメータは存在しない)。
    - User-Agent はブラウザを偽装しないクローラ UA (NetHarvestBot/1.0)。
    - 既存 mbp_japan.py と同一サイト・同一範囲 (重複 site_id)。
    - TEL はサイトに掲載されない。専門分野は未記入のプロが多い (出典データ欠落)。
    - 事業内容・特徴・コラム等の自由記述は出力しない。専門分野は依頼者指定のため出力する。

実行方法:
    python scripts/sites/service/mbp_japan_2.py
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

root_path = Path(__file__).resolve().parents[3]
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    "新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    "奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    "熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREFS})")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_AREA_RE = re.compile(r"/search/(area:\d+)/")
_PRO_RE = re.compile(r"^/[a-z]+/([A-Za-z0-9_-]+)/$")
_MAX_PAGES = 100


class MbpJapan2Scraper(StaticCrawler):
    """マイベストプロ北海道 スクレイパー"""

    DELAY = 4.0  # 低頻度アクセス
    USER_AGENT = "NetHarvestBot/1.0"  # ブラウザを偽装しない正直な UA
    EXTRA_COLUMNS = ["専門分野"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        top = self.get_soup(url)
        area = "area:1"
        if top is not None:
            for a in top.select("a[href*='/search/area:']"):
                m = _AREA_RE.search(a["href"])
                if m:
                    area = m.group(1)
                    break
        list_url = urljoin(url, f"search/{area}/")
        site_prefix = urljoin(url, "/").rstrip("/")
        section = urljoin(url, ".").replace(site_prefix, "")  # 例: /hokkaido/

        seen: set[str] = set()
        for page in range(1, _MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            pro_urls = []
            for a in soup.select("a[href]"):
                href = a["href"].split("?")[0].split("#")[0]
                if href.startswith(site_prefix):
                    href = href[len(site_prefix):]
                if not href.startswith(section) or "/search/" in href or "/cc/" in href:
                    continue
                if not _PRO_RE.match(href):
                    continue
                full = urljoin(url, href)
                if full not in seen:
                    seen.add(full)
                    pro_urls.append(full)
            if not pro_urls:
                break
            for pro_url in pro_urls:
                item = self._parse_detail(pro_url)
                if item:
                    yield item

    def _parse_detail(self, pro_url: str) -> dict | None:
        soup = self.get_soup(pro_url)
        if soup is None:
            return None
        dl = None
        for cand in soup.select("div.pro-profile-blk dl, dl"):
            if cand.find("dt", string=re.compile("氏名")):
                dl = cand
                break
        if dl is None:
            logger.warning("プロフィール無し: %s", pro_url)
            return None

        fields: dict[str, bs4.Tag] = {}
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                fields.setdefault(dt.get_text(strip=True), dd)

        def text(label: str, sep: str = " ") -> str:
            dd = fields.get(label)
            value = dd.get_text(sep, strip=True) if dd is not None else ""
            return "" if value in ("-", "－", "ー") else value

        # 氏名「佐々木毅（ささきつよし）」→ 氏名のみ
        rep = re.sub(r"[（(].*?[）)]\s*$", "", text("氏名")).strip()
        if not rep:
            h1 = soup.select_one("h1")
            rep = h1.get_text(" ", strip=True) if h1 else ""

        # 職種: 先頭の直下テキストが主たる肩書き、div.another が全職種
        pos = ""
        cat_site = ""
        job_dd = fields.get("職種")
        if job_dd is not None:
            another = job_dd.select_one("div.another")
            pos = "".join(
                s.strip() for s in job_dd.find_all(string=True, recursive=False)
                if not isinstance(s, bs4.Comment)
            ).strip()
            if another is not None:
                cat_site = " / ".join(another.stripped_strings)
            if not pos:
                pos = cat_site.split(" / ")[0] if cat_site else ""

        company = ""
        for label in ("会社名", "事務所名", "屋号", "店舗名", "教室名", "団体名", "施設名", "法人名"):
            company = text(label)
            if company:
                break
        if not company:
            for label, dd in fields.items():
                if label.endswith("名") and label != "氏名":
                    company = dd.get_text(" ", strip=True)
                    if company:
                        break

        post_code = pref = addr = ""
        addr_dd = fields.get("住所") or fields.get("所在地")
        if addr_dd is not None:
            parts = [
                s.strip() for s in addr_dd.find_all(string=True, recursive=False)
                if not isinstance(s, bs4.Comment) and s.strip()
            ]
            for part in parts:
                m = _ZIP_RE.fullmatch(part.replace(" ", ""))
                if m:
                    post_code = m.group(1)
                else:
                    addr = (addr + part).strip()
            m = _PREF_RE.match(addr)
            if m:
                pref = m.group(1)
                addr = addr[m.end():].strip()

        hp = ""
        hp_dd = fields.get("ホームページ")
        if hp_dd is not None:
            for a in hp_dd.select("a"):
                t = a.get_text(strip=True)
                if t.startswith("http"):
                    hp = t
                    break

        return {
            Schema.URL: pro_url,
            Schema.NAME: company or rep,
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.CAT_SITE: cat_site,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.HP: hp,
            Schema.TIME: text("営業時間") or text("受付時間"),
            Schema.HOLIDAY: text("定休日"),
            "専門分野": text("専門分野"),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MbpJapan2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://mbp-japan.com/hokkaido/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

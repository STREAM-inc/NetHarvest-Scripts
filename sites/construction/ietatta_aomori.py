# scripts/sites/construction/ietatta_aomori.py
"""
イエタッタ青森 (ietatta-aomori.com) — 青森の住宅会社ポータル 登録会社一覧

取得対象:
    - 登録会社一覧 (registrationCorp.php) に掲載された全社 (所在地が県外の会社も含む)
    - 名称・TEL(ヘッダ電話欄)・都道府県・住所・代表者・役職・設立・資本金・従業員数・
      URL(HP)・営業時間・定休日・メール・SNS
      + 施工対応エリア / 取扱工法 / 参考坪単価 / 建設許可番号 / 宅建業免許番号 / FAX

取得フロー:
    url (registrationCorp.php) を取得 → ul.static-doc__list-mark 内の /company/{id} リンクを抽出
    (ページ送り無し・1ページに全社)
      → 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    詳細ページの「会社情報」ブロック (.event_info .e_data: <p>ラベル</p><p>値</p>) をラベル駆動で読む。
    TEL は備考指示どおりヘッダの電話欄 (.tipG) を優先し、無ければ会社情報の「電話番号」。

注意:
    - 「会社のこだわり・コンセプト」「保証体制」「アフターメンテ」は自由記述の文章のため、
      著作権リスクを避けて取得しない。
    - 利用規約 (sitePolicy.php) にスクレイピングを明示禁止する条項は無い (第5条は会員の私的利用範囲)。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/ietatta_aomori.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ietatta_aomori
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


_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|.{2,3}県)"
)
_POS_RE = re.compile(
    r"^((?:代表取締役|取締役|代表社員|代表理事|理事長|社長|会長|代表|所長|店長|"
    r"専務|常務|CEO)[^\s　]*)[\s　]+(.+)$"
)
_DETAIL_PATH_RE = re.compile(r"/company/\d+/?$")


class IetattaAomoriScraper(StaticCrawler):
    """イエタッタ青森 登録会社 スクレイパー"""

    # 備考: アクセス間隔1秒以上 (基盤ルール 3.0〜5.0 秒)
    DELAY = 3.0

    EXTRA_COLUMNS = [
        "FAX",
        "施工対応エリア",
        "取扱工法",
        "参考坪単価",
        "建設許可番号",
        "宅建業免許番号",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("一覧ページを取得できませんでした: %s", url)
            return

        anchors = soup.select("ul.static-doc__list-mark a[href]") or soup.select(
            "a[href*='/company/']"
        )
        seen = set()
        detail_urls = []
        for a in anchors:
            href = urljoin(url, a["href"])
            if not _DETAIL_PATH_RE.search(href) or href in seen:
                continue
            seen.add(href)
            detail_urls.append(href)
        self.logger.info("登録会社リンク: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            item = self._parse_detail(detail_url)
            if item:
                yield item

    def _parse_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("詳細ページを取得できませんでした: %s", url)
            return None

        info = {}
        links = {}
        for row in soup.select(".event_info .e_data"):
            ps = row.find_all("p", recursive=False)
            if len(ps) < 2:
                continue
            label = ps[0].get_text(strip=True)
            if not label or label in info:
                continue
            info[label] = " ".join(ps[1].get_text(" ", strip=True).split())
            links[label] = [a.get("href", "") for a in ps[1].select("a[href]")]

        name = info.get("社名", "")
        if not name:
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            self.logger.warning("社名が取れないためスキップ: %s", url)
            return None

        # TEL: ヘッダ電話欄 (.tipG) 優先
        tel = ""
        for tip in soup.select(".tipG"):
            ps = tip.find_all("p", recursive=False)
            if len(ps) >= 2 and "電話" in ps[0].get_text(strip=True):
                tel = ps[1].get_text(" ", strip=True)
                break
        if not tel:
            tel = info.get("電話番号", "")

        pref, addr = "", info.get("会社所在地", "")
        m = _PREF_RE.match(addr)
        if m:
            pref, addr = m.group(1), addr[m.end():].strip()

        rep_raw = info.get("代表者", "")
        pos, rep = "", rep_raw
        m = _POS_RE.match(rep_raw)
        if m:
            pos, rep = m.group(1), m.group(2).strip()

        hp = next((h for h in links.get("URL", []) if h.startswith("http")), "") or info.get("URL", "")

        sns = {Schema.INSTA: "", Schema.FB: "", Schema.X: "", Schema.LINE: "", Schema.TIKTOK: ""}
        for href in links.get("SNS", []):
            if "instagram.com" in href and not sns[Schema.INSTA]:
                sns[Schema.INSTA] = href
            elif "facebook.com" in href and not sns[Schema.FB]:
                sns[Schema.FB] = href
            elif ("twitter.com" in href or "//x.com" in href) and not sns[Schema.X]:
                sns[Schema.X] = href
            elif ("line.me" in href or "lin.ee" in href or "/line" in href) and not sns[Schema.LINE]:
                sns[Schema.LINE] = href
            elif "tiktok.com" in href and not sns[Schema.TIKTOK]:
                sns[Schema.TIKTOK] = href

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.OPEN_DATE: info.get("会社設立", ""),
            Schema.CAP: info.get("資本金", ""),
            Schema.EMP_NUM: info.get("従業員数", ""),
            Schema.HP: hp,
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.EMAIL: info.get("E-mail", ""),
            **sns,
            "FAX": info.get("Fax", ""),
            "施工対応エリア": info.get("施工対応エリア", ""),
            "取扱工法": info.get("取扱工法", ""),
            "参考坪単価": info.get("参考坪単価", ""),
            "建設許可番号": info.get("建設許可番号", ""),
            "宅建業免許番号": info.get("宅建業免許番号", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IetattaAomoriScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://ietatta-aomori.com/registrationCorp.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

"""
イエタッタ石川 (www.xn----566as40brkc895c.com / 石川-住宅.com) — 登録会社一覧
対象サイト: https://www.xn----566as40brkc895c.com/registrationCorp.php

取得対象:
    - 登録会社一覧 (registrationCorp.php) に掲載された全社 (2026-11 時点 157 社)。
      所在地が石川県外の会社も含めて全社取得する (備考指示)。

取得フロー:
    ルート(url = registrationCorp.php) の ul.static-doc__list-mark 内リンク /company/{id}
      → 一覧は 1 ページに全社掲載 (ページ送り無し)
      ※ 会社 ID は連番ではなく 8748〜12259 等・欠番あり (/company/1 は 404)
      → 詳細ページを 1 件取得するごとに即 yield
    詳細ページ:
      - ヘッダ電話欄: div.signCom > div.tipG の 2 つ目の <p> (TEL)
      - 会社情報: div.event_info div.e_data の <p>ラベル</p><p>値</p> の組
        (社名/会社所在地/施工対応エリア/電話番号/Fax/URL/E-mail/営業時間/定休日/代表者/
         取扱工法/会社設立/資本金/従業員数/建設許可番号/宅建業免許番号/参考坪単価/SNS 等。
         掲載項目は会社ごとに可変)

注意:
    - 「会社のこだわり・コンセプト」「アフターメンテ」「保証体制」「会社紹介文」は
      自由記述の長文のため、著作権リスクを避けて取得しない。
    - 利用規約 (sitePolicy.php) にスクレイピング/クローリングの明示的な禁止条項は無い。
    - 備考指示によりアクセス間隔は 1 秒以上 (DELAY = 1.5)。

実行方法:
    python scripts/sites/construction/xn_566as40brkc895c.py
    docker compose exec worker python /app/bin/run_flow.py --site-id xn_566as40brkc895c
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
_ERA_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _to_date(raw: str) -> str:
    """「昭和56年4月7日」「2005年3月」等を YYYY-MM-DD / YYYY-MM / YYYY に整形。解釈不能なら原文。"""
    s = raw.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    m = re.search(r"(明治|大正|昭和|平成|令和)\s*(元|\d{1,2})\s*年(?:\s*(\d{1,2})\s*月)?(?:\s*(\d{1,2})\s*日)?", s)
    if m:
        year = _ERA_BASE[m.group(1)] + (1 if m.group(2) == "元" else int(m.group(2)))
        month, day = m.group(3), m.group(4)
    else:
        m = re.search(r"(\d{4})\s*[年/.\-](?:\s*(\d{1,2})\s*[月/.\-]?)?(?:\s*(\d{1,2})\s*日?)?", s)
        if not m:
            return raw
        year, month, day = int(m.group(1)), m.group(2), m.group(3)
    out = f"{year:04d}"
    if month:
        out += f"-{int(month):02d}"
        if day:
            out += f"-{int(day):02d}"
    return out


class Xn566as40brkc895cScraper(StaticCrawler):
    """イエタッタ石川 登録会社 スクレイパー"""

    DELAY = 1.5  # 備考: アクセス間隔 1 秒以上

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "施工対応エリア",
        "取扱工法",
        "参考坪単価",
        "許可番号",
        "宅建業免許番号",
        "FAX",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"一覧ページの取得に失敗しました: {url}")

        anchors = soup.select("ul.static-doc__list-mark a[href*='/company/']")
        if not anchors:
            anchors = soup.select("a[href*='/company/']")

        seen: set[str] = set()
        for a in anchors:
            detail_url = urljoin(url, a.get("href", ""))
            m = re.search(r"/company/(\d+)/?$", detail_url)
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))

            item = self._parse_detail(detail_url, _clean(a.get_text()))
            if item:
                yield item

    def _parse_detail(self, detail_url: str, list_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return None

        info: dict[str, str] = {}
        sns: list[str] = []
        for row in soup.select("div.event_info div.e_data"):
            ps = row.find_all("p", recursive=False)
            if len(ps) < 2:
                continue
            label = _clean(ps[0].get_text())
            if label == "SNS":
                sns = [x.get("href", "") for x in ps[1].select("a[href]")]
                continue
            if label == "URL":
                link = ps[1].find("a", href=True)
                value = link["href"].strip() if link else _clean(ps[1].get_text())
            else:
                value = _clean(ps[1].get_text(" "))
            if label and label not in info:
                info[label] = value

        # ヘッダ電話欄 (備考指定)。無ければ会社情報の電話番号
        tel = ""
        tip = soup.select_one("div.signCom div.tipG")
        if tip:
            tps = tip.find_all("p")
            if len(tps) >= 2:
                tel = _clean(tps[1].get_text())
        tel = tel or info.get("電話番号", "")

        h1 = soup.select_one("div.signCom h1")
        name = info.get("社名") or (_clean(h1.get_text()) if h1 else "") or list_name
        if not name:
            self.logger.warning("社名が取得できません: %s", detail_url)
            return None

        full_addr = info.get("会社所在地", "")
        post_code = ""
        # 郵便番号は先頭のほか「〇〇県〒xxx-xxxx …」のように県名直後に入る会社もある
        pm = re.search(r"〒\s*(\d{3})[-－ー]?(\d{4})", full_addr) or re.match(
            r"\s*(\d{3})[-－ー](\d{4})", full_addr
        )
        if pm:
            post_code = f"{pm.group(1)}-{pm.group(2)}"
            full_addr = _clean(full_addr.replace(pm.group(0), "", 1))
        pref = ""
        am = _PREF_RE.match(full_addr)
        if am:
            pref = am.group(1)
            addr = _clean(full_addr[am.end():])
        else:
            addr = full_addr

        founded = info.get("会社設立") or info.get("設立", "")

        def _sns(domain_pat: str) -> str:
            return next((h for h in sns if re.search(domain_pat, h)), "")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: info.get("代表者", ""),
            Schema.OPEN_DATE: _to_date(founded) if founded else "",
            Schema.CAP: info.get("資本金", ""),
            Schema.EMP_NUM: info.get("従業員数", ""),
            Schema.HP: info.get("URL", ""),
            Schema.EMAIL: info.get("E-mail", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.INSTA: _sns(r"instagram\.com"),
            Schema.FB: _sns(r"facebook\.com"),
            Schema.X: _sns(r"(twitter|x)\.com"),
            Schema.LINE: _sns(r"line\.me"),
            Schema.TIKTOK: _sns(r"tiktok\.com"),
            "施工対応エリア": info.get("施工対応エリア", ""),
            "取扱工法": info.get("取扱工法", ""),
            "参考坪単価": info.get("参考坪単価", ""),
            "許可番号": info.get("建設許可番号", ""),
            "宅建業免許番号": info.get("宅建業免許番号", ""),
            "FAX": info.get("Fax", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Xn566as40brkc895cScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.xn----566as40brkc895c.com/registrationCorp.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

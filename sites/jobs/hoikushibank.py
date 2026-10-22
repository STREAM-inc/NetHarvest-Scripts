"""
保育士バンク！ 施設一覧 (https://www.hoikushibank.com/facility) — 保育園・幼稚園等の施設情報クローラー

取得対象:
    - 全国 (全エリア) の保育関連施設 約19,500件 (認可保育園/幼稚園/学童保育/児童発達支援 等)
    - 施設名 / 施設形態 / 所在地 / アクセス / 開所時間 / 定員 / 運営法人 / 公式サイト / SNS ほか

取得フロー:
    1. 一覧 (sites.yml の url) を ?page=N で巡回 (1ページ35件、カードが0件になったら終了)
    2. 一覧カード h2.card__title の /branch/{id} を詳細 URL として取得
    3. 施設詳細 /branch/{id} を 1 件取得するごとに即 yield
       - div.info-list > div.info-item の .info-item__label (ラベル) と残りテキスト (値)
       - section.organization-info-pc の 法人名 / 法人ページ / 公式サイト / SNS リンク

備考:
    - 電話番号は施設ごとには掲載されていない (ヘッダの 0120 は保育士バンク！の窓口なので取得しない)
    - 施設紹介文・キャッチコピー等の自由記述は著作権リスクのため取得しない

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/hoikushibank.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id hoikushibank
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_CARD_SELECTOR = "h2.card__title a[href^='/branch/']"
_BRANCH_PATTERN = re.compile(r"/branch/(\d+)")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"^〒?\s*(\d{3})-?(\d{4})\s*")

# SNS ドメイン → 出力カラム
_SNS_COLUMNS = (
    ("instagram.com", Schema.INSTA),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("facebook.com", Schema.FB),
    ("tiktok.com", Schema.TIKTOK),
    ("youtube.com", "YouTube"),
)


class HoikushibankScraper(StaticCrawler):
    """保育士バンク！ 施設一覧 スクレイパー"""

    DELAY = 3.0  # 待機時間（秒）
    EXTRA_COLUMNS = [
        "法人名",
        "法人ページURL",
        "アクセス",
        "開所日延長保育",
        "園児対象年齢・定員",
        "特別保育",
        "受動喫煙防止措置の状況",
        "YouTube",
        "施設ID",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        while True:
            soup = self.get_soup(self._page_url(url, page))
            if soup is None:
                break
            links = soup.select(_CARD_SELECTOR)
            if not links:
                logger.info("カード0件のため終了: page=%d", page)
                break

            for a in links:
                m = _BRANCH_PATTERN.search(a.get("href", ""))
                if not m or m.group(1) in seen:
                    continue
                seen.add(m.group(1))
                detail_url = urljoin(url, f"/branch/{m.group(1)}")
                item = self._parse_detail(detail_url, m.group(1))
                if item:
                    yield item
            page += 1

    @staticmethod
    def _page_url(url: str, page: int) -> str:
        """引数 url のクエリに page=N を設定した一覧 URL を返す。"""
        parts = urlsplit(url)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "page"]
        if page > 1:
            query.append(("page", str(page)))
        return urlunsplit(parts._replace(query=urlencode(query)))

    def _parse_detail(self, detail_url: str, branch_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        info = self._info_items(soup)
        org = soup.select_one("section.organization-info-pc") or soup.select_one(
            "section.organization-info"
        )
        corp_el = soup.select_one("p.organization-name")
        corp_name = corp_el.get_text(" ", strip=True) if corp_el else ""

        name = info.get("施設名", "")
        if not name:
            h1 = soup.select_one("h1.heading__name")
            if h1:
                suffix = h1.select_one(".heading__name-suffix")
                if suffix:
                    suffix.extract()
                name = h1.get_text(strip=True)

        addr = info.get("所在地", "")
        post_code = ""
        m = _POST_CODE_PATTERN.match(addr)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            addr = addr[m.end():]
        pref_m = _PREF_PATTERN.match(addr)
        pref = pref_m.group(1) if pref_m else ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr[len(pref):].strip() if pref else addr,
            Schema.CAT_SITE: info.get("施設形態", ""),
            Schema.OPEN_DATE: info.get("設立年月", ""),
            Schema.TIME: info.get("開所時間", ""),
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.LINE: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            "法人名": corp_name,
            "法人ページURL": "",
            "アクセス": info.get("アクセス", ""),
            "開所日延長保育": info.get("開所日延長保育", ""),
            "園児対象年齢・定員": info.get("園児対象年齢", ""),
            "特別保育": info.get("特別保育", ""),
            "受動喫煙防止措置の状況": info.get("受動喫煙防止措置の状況", ""),
            "YouTube": "",
            "施設ID": branch_id,
        }

        if org:
            for a in org.select("a[href]"):
                href = a["href"].strip()
                text = a.get_text(strip=True)
                if href.startswith("/company/"):
                    item["法人ページURL"] = urljoin(detail_url, href)
                elif "公式サイト" in text:
                    item[Schema.HP] = href
                else:
                    for domain, column in _SNS_COLUMNS:
                        if domain in href and not item[column]:
                            item[column] = href
                            break
        return item

    @staticmethod
    def _info_items(soup: bs4.BeautifulSoup) -> dict[str, str]:
        """施設情報 (div.info-list) の ラベル→値 を返す。"""
        result: dict[str, str] = {}
        for it in soup.select("div.info-list > div.info-item"):
            label_el = it.select_one(".info-item__label")
            if not label_el:
                continue
            label = label_el.get_text(strip=True)
            head = it.select_one(".info-item__head")
            if head:
                head.extract()
            tokens = [t for t in it.stripped_strings]
            if label == "園児対象年齢":
                value = HoikushibankScraper._format_ages(tokens)
            elif label == "アクセス":
                # 交通手段の行のみ採用 (■/※ で始まる補足行は除外)
                lines = [
                    ln.strip()
                    for ln in "\n".join(tokens).splitlines()
                    if ln.strip() and not ln.strip().startswith(("■", "※"))
                ]
                value = " / ".join(lines)
            else:
                value = " ".join(tokens)
            result[label] = re.sub(r"\s+", " ", value).replace("： ", "：").strip()
        return result

    @staticmethod
    def _format_ages(tokens: list[str]) -> str:
        """['0歳', ..., '5歳', '9人', ...] → '0歳:9人 1歳:9人 ...'"""
        ages = [t for t in tokens if t.endswith("歳")]
        counts = [t for t in tokens if not t.endswith("歳")]
        if ages and len(ages) == len(counts):
            return " ".join(f"{a}:{c}" for a, c in zip(ages, counts))
        return " ".join(tokens)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HoikushibankScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.hoikushibank.com/facility")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

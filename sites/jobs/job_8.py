"""
JOB静岡 (job.atimes.co.jp/shizuoka) — アルバイトタイムス運営の静岡県 正社員求人サイト

取得フロー:
    1. 引数 url (一覧 /shizuoka/list?page=1) の page パラメータだけを差し替えて ?page=N を巡回する
       (1 ページ 25 求人、約 156 ページ / 3,898 求人)。
       ※ robots.txt で /shizuoka/list*sta* 等が Disallow のため絞り込みパラメータは使わない。
       ※ 範囲外の page は求人カード 0 件の 200 が返るため、カード 0 件 or 新規 ID ゼロで打ち切る。
       ※ ?page=1 は /shizuoka/list へ 301 されるが内容は同一。
    2. 一覧の求人カード (div.c-jobDetailCard__btns 内の「詳細を見る」リンク) から詳細 URL を取得
       (サイドバーのおすすめ求人は除外)。1 社カードに複数求人がぶら下がるため、求人単位で処理する。
    3. 詳細 /shizuoka/ad/{tys|tya|tyb|tyc..}/{10桁ID}[?consultant=consultant] を 1 件ずつ取得し即 yield。
       - 企業情報タブ: div.c-singleTabCard の h2 見出し (社名/事業内容/資本金/所在地/URL) → 本文
       - TEL: 電話応募モーダル div.p-single__telModal--content の a[href^=tel:] と (担) 部署
       - 掲載期間: p.c-singleCard__period
       - 職種: 「この職種の仕事を探す」の職種カテゴリ (dl.p-single__related--item)

掲載区分:
    URL に ?consultant=consultant がある求人は「エージェント経由」(アチーヴキャリアの職業紹介)。
    その TEL (0120-887-708)・担当 (アチーヴキャリア受付担当)・URL (achieve.atimes.co.jp) は
    掲載企業のものではないため TEL / TEL担当部署 / HP を空欄にする。

所在地:
    「本社：〒420-0817　静岡県…」「本社／〒…　静岡市葵区…(都道府県省略)」「（本社）静岡県…」が混在する。
    先頭ラベルを除去し郵便番号・都道府県・住所に分割する。都道府県省略かつ郵便番号が
    静岡県の範囲 (410〜439) の場合のみ「静岡県」を補完する。原文は EXTRA「所在地」に残す。

利用規約 (https://job.atimes.co.jp/info/agreement):
    スクレイピング・クローリングを明示的に禁止する条項は無い
    (第5条は著作物の複製・転載等の禁止 — 取得データの利用範囲には注意)。

取得しないフィールド (著作権リスク: 自由記述プロース):
    キャッチコピー / 募集の背景 / 仕事内容 / 職場情報 / 待遇・福利厚生 / その他 / 応募方法 等の本文

実行方法:
    python scripts/sites/jobs/job_8.py
    docker compose exec worker python /app/bin/run_flow.py --site-id job_8
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 詳細ページ URL (例: /shizuoka/ad/tys/0028753891, /content_access/show/...)
_DETAIL_PATTERN = re.compile(r"/(?:[a-z]+/ad/[a-z]+/\d{10}|content_access/show/)")
# 求人 ID (10 桁)
_JOB_ID_PATTERN = re.compile(r"/(\d{10})(?:[/?]|$)")
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})\s*[-－ー‐]\s*(\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 所在地の先頭ラベル (本社：/ 本社／ / （本社） / 【本社】 など)
_ADDR_LABEL_PATTERN = re.compile(r"^(?:[（(【\[]\s*[^）)】\]]{1,10}[）)】\]]|[^\s：:／/〒]{1,10}\s*[：:／/])\s*")
_PERIOD_PATTERN = re.compile(r"掲載期間\s*[：:]\s*(.+)")
_CONSULTANT_TEL = "0120-887-708"

# 範囲外 page を指定しても 0 件で返るため通常は到達しない保険
_MAX_PAGES = 400


class Job8Scraper(StaticCrawler):
    """JOB静岡 スクレイパー"""

    DELAY = 1.5  # 備考: アクセス間隔は 1 秒以上
    EXTRA_COLUMNS = [
        "求人URL",
        "掲載区分",
        "所在地",
        "TEL担当部署",
        "掲載期間",
        "職種",
        "求人タイトル",
    ]

    def parse(self, url: str):
        seen: set[str] = set()
        start_page = self._page_of(url)
        for page in range(start_page, start_page + _MAX_PAGES):
            list_url = self._with_page(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            detail_urls = []
            for a in soup.select("div.c-jobDetailCard__btns a[href]"):
                href = urljoin(list_url, a["href"])
                if not _DETAIL_PATTERN.search(href):
                    continue
                key = self._job_key(href)
                if key in seen:
                    continue
                seen.add(key)
                detail_urls.append(href)

            if not detail_urls:
                logger.info("page=%d で新規求人なし → 終了", page)
                break
            logger.info("page=%d: %d 件", page, len(detail_urls))

            for detail_url in detail_urls:
                item = self._parse_detail(detail_url)
                if item:
                    yield item

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    @staticmethod
    def _page_of(url: str) -> int:
        for k, v in parse_qsl(urlsplit(url).query):
            if k == "page" and v.isdigit():
                return max(int(v), 1)
        return 1

    @staticmethod
    def _with_page(url: str, page: int) -> str:
        parts = urlsplit(url)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "page"]
        query.append(("page", str(page)))
        return urlunsplit(parts._replace(query=urlencode(query)))

    @staticmethod
    def _job_key(href: str) -> str:
        m = _JOB_ID_PATTERN.search(href)
        return m.group(1) if m else href

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページ取得失敗: %s", detail_url)
            return None

        is_agent = "consultant=consultant" in detail_url
        cards = self._company_cards(soup)

        name = cards.get("社名", "")
        if not name:
            name = self._ld_org_name(soup)
        if not name:
            logger.warning("社名が取得できない: %s", detail_url)
            return None

        raw_addr = cards.get("所在地", "")
        post_code, pref, addr = self._split_address(raw_addr)

        hp = ""
        if not is_agent:
            hp = self._company_url(soup)

        tel, tel_charge = ("", "") if is_agent else self._tel(soup)

        period = ""
        period_el = soup.select_one("p.c-singleCard__period, p.c-singleTab__btm--period")
        if period_el:
            m = _PERIOD_PATTERN.search(period_el.get_text(" ", strip=True))
            if m:
                period = re.sub(r"\s+", " ", m.group(1)).strip()

        h1 = soup.select_one("h1")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.LOB: cards.get("事業内容", ""),
            Schema.CAP: cards.get("資本金", ""),
            Schema.HP: hp,
            "求人URL": detail_url,
            "掲載区分": "エージェント経由" if is_agent else "直接掲載",
            "所在地": raw_addr,
            "TEL担当部署": tel_charge,
            "掲載期間": period,
            "職種": self._job_types(soup),
            "求人タイトル": h1.get_text(" ", strip=True) if h1 else "",
        }

    @staticmethod
    def _company_cards(soup) -> dict[str, str]:
        """企業情報タブ (社名/事業内容/資本金/所在地) の見出し→本文。先勝ち。"""
        result: dict[str, str] = {}
        for card in soup.select("div.c-singleTabCard"):
            head = card.select_one(".c-singleTabCard__head")
            body = card.select_one(".c-singleTabCard__body")
            if not head or not body:
                continue
            label = head.get_text(strip=True)
            if label in result:
                continue
            lines = [ln.strip() for ln in body.get_text("\n").splitlines() if ln.strip()]
            result[label] = " ".join(lines)
        return result

    @staticmethod
    def _company_url(soup) -> str:
        for card in soup.select("div.c-singleTabCard"):
            head = card.select_one(".c-singleTabCard__head")
            if head and head.get_text(strip=True) == "URL":
                a = card.select_one("a[href^=http]")
                return a["href"].strip() if a else ""
        return ""

    @staticmethod
    def _tel(soup) -> tuple[str, str]:
        for content in soup.select("div.p-single__telModal--content"):
            a = content.select_one("a[href^='tel:']")
            if not a:
                continue
            tel = a.get_text(strip=True) or a["href"][4:]
            if tel == _CONSULTANT_TEL:
                return "", ""
            charge_el = content.select_one(".p-single__telModal--charge")
            charge = charge_el.get_text(" ", strip=True) if charge_el else ""
            charge = re.sub(r"^\s*[（(]担[）)]\s*", "", charge)
            return tel, charge
        return "", ""

    @staticmethod
    def _job_types(soup) -> str:
        for dl in soup.select("dl.p-single__related--item"):
            dt = dl.find("dt")
            if dt and "職種" in dt.get_text():
                dd = dl.find("dd")
                if not dd:
                    return ""
                names = [a.get_text(strip=True) for a in dd.find_all("a")] or [dd.get_text(strip=True)]
                names = [re.sub(r"の求人$", "", n).strip() for n in names if n]
                return " / ".join(dict.fromkeys(n for n in names if n))
        return ""

    @staticmethod
    def _ld_org_name(soup) -> str:
        el = soup.select_one('script[type="application/ld+json"]')
        if not el or not el.string:
            return ""
        try:
            data = json.loads(el.string)
        except json.JSONDecodeError:
            return ""
        org = data.get("hiringOrganization") if isinstance(data, dict) else None
        return (org or {}).get("name", "").strip() if isinstance(org, dict) else ""

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """所在地原文 → (郵便番号, 都道府県, 住所)。複数拠点の場合は先頭のみ。"""
        if not raw:
            return "", "", ""
        text = raw.replace("　", " ").strip()
        text = _ADDR_LABEL_PATTERN.sub("", text, count=1)

        post_code = ""
        m = _POST_PATTERN.search(text)
        if m and m.start() <= 2:
            post_code = f"{m.group(1)}-{m.group(2)}"
            text = text[m.end():].strip()
        text = text.lstrip("〒 ").strip()
        # 2 拠点目以降 (「支店：〒…」等) は落とす
        text = re.split(r"\s+(?=[^\s：:／/]{1,10}[：:／/]\s*〒)", text, maxsplit=1)[0].strip()

        pref = ""
        m = _PREF_PATTERN.match(text)
        if m:
            pref = m.group(1)
            text = text[m.end():].strip()
        elif post_code and 410 <= int(post_code[:3]) <= 439:
            pref = "静岡県"
        return post_code, pref, text


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Job8Scraper()
    scraper.execute("https://job.atimes.co.jp/shizuoka/list?page=1")

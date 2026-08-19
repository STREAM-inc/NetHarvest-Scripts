"""
エスタマ (esjob.jp) — メンズエステ求人ポータル「エスタマ求人」の掲載店舗スクレイパー (esjob_3)

取得対象:
    - トップページ (https://esjob.jp/) のヘッダ地域ナビから全国6地域
      (関東 / 関西 / 中部 / 北海道・東北 / 九州・沖縄 / 中国・四国) の求人一覧を辿り、
      掲載されている全店舗 (レコード粒度 = 1店舗1行) を取得する。
    - 店舗名 / 都道府県 / エリア / TEL / 営業時間 / 公式サイト / LINE などの基本情報に加え、
      求人条件 (日給・歩合率・応募年齢・勤務時間・こだわり条件タグ) を EXTRA として取得する。

取得フロー:
    1. 起点 URL (= sites.yml の url = サイトトップ) を取得し、ヘッダ地域ナビ
       (header .h-nav の "/kanto/" 等 1階層パス) から 6地域の一覧 URL
       ({region}joblist/) を導出する。ナビが取れない場合は既知スラッグで補完。
       ※ "/" (全国) の一覧は関東と同一内容 (/joblist/ → /kanto/joblist/ に 302) のため除外。
    2. 各地域の一覧を {region}joblist/ → {region}joblist/pN/ で全ページ巡回
       (1ページ100件, li.item-job。カードが 0 件になったら打ち切り)。
    3. 一覧カードから詳細 /shop/{id}/ を辿り、1件取得するごとに即 yield する。
       shop_id で全地域横断の dedup を行う (同一店舗が複数求人で重複掲載されるため)。

備考 (仕様上の注意):
    - 番地入りの住所はサイト上に存在しない。住所カラムにはエリア表記
      (例「池袋北口・西口」) を入れ、最寄駅ベースの勤務地は EXTRA「勤務地」に入れる。
    - 都道府県・業種はパンくず (ld+json BreadcrumbList) から導出する。
    - 掲載日 (.new_message__postdate) は無い店舗があるため、その場合は空欄。
    - フッターの Facebook (estamajob) はサイト運営者のものなので店舗 SNS からは除外する。
      店舗固有の SNS は実質 LINE のみ (Instagram/X/TikTok/メールは調査 8 件で 0 件) だが、
      掲載があれば拾えるよう実装してある。
    - キャッチコピー・求人本文・体験入店の説明・採用担当メッセージ・Q&A・お給料獲得例は
      長文の自由記述のため取得しない (著作権リスク回避)。
    - 利用規約 (https://esjob.jp/terms/) にスクレイピング/自動取得の禁止条項は無く、
      robots.txt は存在しない (404) ことを確認済み (2026-08-19)。

実行方法:
    python scripts/sites/nightlife/esjob_3.py
    python bin/run_flow.py --site-id esjob_3
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 当サイトはメンズエステ専門の求人ポータルのため、パンくずから取れない場合の既定値
DEFAULT_CATEGORY = "メンズエステ"

# 1地域あたりのページ数上限 (関東で31ページ。異常時の無限ループ防止)
MAX_PAGES_PER_REGION = 300

# ヘッダ地域ナビが取れなかった場合のフォールバック用スラッグ
FALLBACK_REGION_SLUGS = ["kanto", "kansai", "chubu", "hokkaido", "kyushu", "etc"]

# 詳細ページの「こだわり条件」ブロック (dt ラベル → EXTRA カラム名)
CONDITION_BLOCKS = {
    "人気の条件": "人気の条件",
    "大切なお金のこと": "お金の条件",
    "気になるお店の環境": "お店の環境",
    "待遇・働きやすさ": "待遇・働きやすさ",
}

_SHOP_ID_RE = re.compile(r"/shop/(\d+)/")
_TOTAL_RE = re.compile(r"([\d,]+)\s*件\s*がヒット")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_STAFF_PREFIX_RE = re.compile(r"^\s*担当\s*[:：]\s*")
_RATE_RE = re.compile(r"歩合率\s*([\d.]+%(?:\s*[～〜~]\s*[\d.]+%)?)")
_GUEST_NUM_RE = re.compile(r"1日あたりの接客数\s*(\S+)")
_GUEST_TIME_RE = re.compile(r"1回の接客時間\s*(\S+)")
# 自サイト・グループサイトのドメイン (公式サイト判定から除外する)
_OWN_HOST_RE = re.compile(r"(^|\.)(esjob|estama)\.jp$", re.IGNORECASE)

# パンくずの都道府県表記 (例「東京」「北海道」) → 正式名称
_PREF_FULL = {
    "北海道": "北海道",
    "青森": "青森県", "岩手": "岩手県", "宮城": "宮城県", "秋田": "秋田県",
    "山形": "山形県", "福島": "福島県",
    "茨城": "茨城県", "栃木": "栃木県", "群馬": "群馬県", "埼玉": "埼玉県",
    "千葉": "千葉県", "東京": "東京都", "神奈川": "神奈川県",
    "新潟": "新潟県", "富山": "富山県", "石川": "石川県", "福井": "福井県",
    "山梨": "山梨県", "長野": "長野県", "岐阜": "岐阜県", "静岡": "静岡県",
    "愛知": "愛知県", "三重": "三重県",
    "滋賀": "滋賀県", "京都": "京都府", "大阪": "大阪府", "兵庫": "兵庫県",
    "奈良": "奈良県", "和歌山": "和歌山県",
    "鳥取": "鳥取県", "島根": "島根県", "岡山": "岡山県", "広島": "広島県",
    "山口": "山口県",
    "徳島": "徳島県", "香川": "香川県", "愛媛": "愛媛県", "高知": "高知県",
    "福岡": "福岡県", "佐賀": "佐賀県", "長崎": "長崎県", "熊本": "熊本県",
    "大分": "大分県", "宮崎": "宮崎県", "鹿児島": "鹿児島県", "沖縄": "沖縄県",
}


def _clean(text: str) -> str:
    """空白・改行を正規化した文字列を返す。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


class EsJob3Scraper(StaticCrawler):
    """エスタマ (esjob.jp) スクレイパー — 掲載店舗単位"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "地域",
        "エリア",
        "店舗形態",
        "勤務地",
        "掲載日",
        "担当者名",
        "日給下限",
        "日給上限",
        "歩合率",
        "応募年齢",
        "募集勤務時間",
        "求人タグ",
        "人気の条件",
        "お金の条件",
        "お店の環境",
        "待遇・働きやすさ",
        "1日あたりの接客数",
        "1回の接客時間",
        "求人用公式サイト",
        "エステ魂掲載ページ",
    ]

    def parse(self, url: str):
        # 起点 URL (= sites.yml の url = サイトトップ) を唯一のルートとして地域一覧を導出する
        regions = self._resolve_regions(url)
        seen_shop_ids: set[str] = set()

        for region_name, region_list_url in regions:
            for page in range(1, MAX_PAGES_PER_REGION + 1):
                list_url = region_list_url if page == 1 else f"{region_list_url}p{page}/"
                soup = self.get_soup(list_url)
                if soup is None:
                    logger.warning("一覧の取得に失敗しました: %s", list_url)
                    break

                if page == 1:
                    self._accumulate_total(soup)

                cards = self._collect_cards(soup, list_url)
                if not cards:
                    # 掲載が尽きた (最終ページの次) → この地域は終了
                    break

                for shop_id, detail_url in cards.items():
                    if shop_id in seen_shop_ids:
                        continue
                    seen_shop_ids.add(shop_id)
                    try:
                        item = self._scrape_detail(detail_url, region_name)
                    except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                        logger.warning("詳細の処理に失敗しました (%s): %s", detail_url, e)
                        continue
                    if item:
                        yield item

    # ------------------------------------------------------------------
    # 一覧まわり
    # ------------------------------------------------------------------
    def _resolve_regions(self, url: str) -> list[tuple[str, str]]:
        """起点 URL (トップ) のヘッダ地域ナビから (地域名, 求人一覧URL) のリストを導出する。"""
        regions: list[tuple[str, str]] = []
        soup = self.get_soup(url)

        if soup is not None:
            for a in soup.select("header .h-nav a[href]"):
                href = a.get("href", "").strip()
                # 地域ルートは "/kanto/" のような1階層パス。
                # "/" (全国) は関東と同一内容 (/joblist/ が /kanto/joblist/ に転送) のため除外
                if not re.fullmatch(r"/[a-z_-]+/", href) or href == "/":
                    continue
                list_url = urljoin(url, href + "joblist/")
                name = _clean(a.get_text(" ", strip=True))
                if list_url not in [u for _, u in regions]:
                    regions.append((name, list_url))
        else:
            logger.warning("起点ページを取得できませんでした: %s", url)

        # ナビから地域が取れない/足りない場合は既知スラッグで補完する (URL は起点から派生)
        for slug in FALLBACK_REGION_SLUGS:
            list_url = urljoin(url, f"/{slug}/joblist/")
            if list_url not in [u for _, u in regions]:
                logger.warning("地域ナビに %s が無いため補完します", slug)
                regions.append(("", list_url))

        logger.info("巡回対象の地域: %s", [f"{n or '?'}={u}" for n, u in regions])
        return regions

    @staticmethod
    def _collect_cards(soup, list_url: str) -> dict[str, str]:
        """一覧ページのカードを {shop_id: 詳細URL} にまとめる (ページ内の重複を排除)。"""
        cards: dict[str, str] = {}
        for card in soup.select("li.item-job"):
            link = card.select_one("a.item-title[href]") or card.select_one("a.shop_name[href]")
            if link is None:
                continue
            detail_url = urljoin(list_url, link["href"])
            m = _SHOP_ID_RE.search(urlparse(detail_url).path)
            if not m:
                continue
            cards.setdefault(m.group(1), detail_url)
        return cards

    def _accumulate_total(self, soup) -> None:
        """一覧ページの「N 件がヒット」を総件数 (進捗 ETA 用) に加算する。"""
        pager = soup.select_one(".pagination")
        if pager is None:
            return
        m = _TOTAL_RE.search(pager.get_text(" ", strip=True))
        if not m:
            return
        self.total_items = (self.total_items or 0) + int(m.group(1).replace(",", ""))

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, region_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("詳細の取得に失敗しました: %s", url)
            return None

        # 店舗情報 dl (店名 / 営業時間 / 電話番号 / 勤務地 / 公式サイト)
        shop_info: dict[str, object] = {}
        shop_dl = soup.select_one("dl.contents_box--shopinfo")
        if shop_dl:
            for dt in shop_dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is not None:
                    shop_info[_clean(dt.get_text(" ", strip=True))] = dd

        name = self._text(soup, "article.main_shopinfo h1") or self._text(soup, "h1")
        if not name:
            name = self._dd_text(shop_info.get("店名"))
        if not name:
            logger.warning("店名を取得できませんでした: %s", url)
            return None

        # パンくず (ld+json BreadcrumbList): [TOP, 地域, 都道府県, エリア, 店名]
        crumbs = self._breadcrumbs(soup)
        pref = self._pref_from_crumbs(crumbs)
        category = self._category_from_crumbs(crumbs)

        # 「エリア / 店舗形態」(例: 池袋北口・西口 / 派遣・出張専門)
        area, shop_type = "", ""
        sub = self._text(soup, "p.main_shopinfo__sub")
        if sub:
            parts = [p.strip() for p in sub.split("/")]
            area = parts[0] if parts else ""
            shop_type = " / ".join(parts[1:]) if len(parts) > 1 else ""
        if not area and len(crumbs) >= 4:
            # パンくず4番目「池袋 メンズエステ求人」からエリア名だけ拾う
            area = re.sub(r"\s*メンズエステ求人\s*$", "", crumbs[3]).strip()

        # 電話番号: 店舗情報 dl → tel: リンクの順にフォールバック
        tel = self._pick_tel(self._dd_text(shop_info.get("電話番号")))
        if not tel:
            for a in soup.select('a[href^="tel:"]'):
                tel = self._pick_tel(a.get("href", "").replace("tel:", "")) or self._pick_tel(
                    _clean(a.get_text(" ", strip=True))
                )
                if tel:
                    break

        homepage, recruit_site, estama_page = self._official_sites(shop_info.get("公式サイト"))
        sns = self._social_links(soup)
        salary_min, salary_max, rate = self._salary(soup)
        conditions = self._conditions(soup)
        guest_num, guest_time = self._service_stats(soup)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: area,
            Schema.TEL: tel,
            Schema.CAT_SITE: category,
            Schema.TIME: self._dd_text(shop_info.get("営業時間")),
            Schema.HP: homepage,
            Schema.LINE: sns["line"],
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
            Schema.EMAIL: sns["email"],
            Schema.URL: url,
            "地域": region_name,
            "エリア": area,
            "店舗形態": shop_type,
            "勤務地": self._dd_text(shop_info.get("勤務地")) or self._jobinfo(soup, "勤務地"),
            "掲載日": self._text(soup, ".new_message__postdate"),
            "担当者名": _STAFF_PREFIX_RE.sub("", self._text(soup, "p.recruit_staff__name")),
            "日給下限": salary_min,
            "日給上限": salary_max,
            "歩合率": rate,
            "応募年齢": self._age(soup),
            "募集勤務時間": self._jobinfo(soup, "勤務時間"),
            "求人タグ": ";".join(
                _clean(p.get_text(" ", strip=True)) for p in soup.select(".job__tag p")
            ),
            "人気の条件": conditions.get("人気の条件", ""),
            "お金の条件": conditions.get("お金の条件", ""),
            "お店の環境": conditions.get("お店の環境", ""),
            "待遇・働きやすさ": conditions.get("待遇・働きやすさ", ""),
            "1日あたりの接客数": guest_num,
            "1回の接客時間": guest_time,
            "求人用公式サイト": recruit_site,
            "エステ魂掲載ページ": estama_page,
        }

    # ------------------------------------------------------------------
    # 詳細ページの部分抽出
    # ------------------------------------------------------------------
    def _official_sites(self, dd) -> tuple[str, str, str]:
        """公式サイト dd から (公式サイト, 求人用公式サイト, エステ魂掲載ページ) を返す。

        dd は「●公式サイト <URL> ●求人用 公式サイト <URL> ●エステ魂掲載ページ <URL>」の形式。
        リンク直前のラベル文言で振り分け、ラベルが無い外部リンクは公式サイト扱いにする。
        """
        homepage = recruit = estama = ""
        if dd is None:
            return homepage, recruit, estama

        for a in dd.select("a[href]"):  # type: ignore[union-attr]
            href = a.get("href", "").strip()
            if not href.startswith("http"):
                continue
            host = urlparse(href).netloc
            # 直前のテキスト (ラベル) を見て種別を判定する
            label = ""
            prev = a.find_previous(string=True)
            while prev is not None and not _clean(str(prev)):
                prev = prev.find_previous(string=True)
            if prev is not None:
                label = _clean(str(prev))

            if _OWN_HOST_RE.search(host):
                if not estama:
                    estama = href
            elif "求人" in label and not recruit:
                recruit = href
            elif not homepage:
                homepage = href
            elif not recruit:
                recruit = href
        return homepage, recruit, estama

    @staticmethod
    def _social_links(soup) -> dict[str, str]:
        """店舗固有の SNS リンク / メールアドレスを拾う (サイト運営者のアカウントは除外)。"""
        found = {"line": "", "insta": "", "x": "", "fb": "", "tiktok": "", "email": ""}
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if href.startswith("mailto:"):
                if not found["email"]:
                    found["email"] = href[len("mailto:"):].split("?")[0]
                continue
            host = urlparse(href).netloc.lower()
            if "line.me" in host or "lin.ee" in host:
                key = "line"
            elif "instagram.com" in host:
                key = "insta"
            elif "twitter.com" in host or host.endswith("x.com"):
                key = "x"
            elif "facebook.com" in host:
                # フッターの estamajob はサイト運営者のアカウントなので除外する
                if "estamajob" in href:
                    continue
                key = "fb"
            elif "tiktok.com" in host:
                key = "tiktok"
            else:
                continue
            if not found[key]:
                found[key] = href
        return found

    @staticmethod
    def _salary(soup) -> tuple[str, str, str]:
        """お給料 dl から (日給下限, 日給上限, 歩合率) を返す。"""
        dl = soup.select_one("dl.jobdetails__jobinfo_item-salary")
        if dl is None:
            return "", "", ""
        amounts = [_clean(e.get_text(" ", strip=True)) for e in dl.select("span.txt-salary")]
        low = amounts[0] if amounts else ""
        high = amounts[1] if len(amounts) > 1 else ""
        m = _RATE_RE.search(_clean(dl.get_text(" ", strip=True)).replace(" ", ""))
        return low, high, m.group(1) if m else ""

    @staticmethod
    def _age(soup) -> str:
        """応募資格 dl の年齢スパンから「18歳〜40歳」を組み立てる (本文プロースは含めない)。"""
        ages = [
            _clean(e.get_text(" ", strip=True))
            for e in soup.select("dl.jobdetails__jobinfo_item span.txt-age")
        ]
        if not ages:
            return ""
        return "〜".join(ages[:2])

    def _jobinfo(self, soup, label: str) -> str:
        """求人情報 dl (お給料/応募資格/勤務時間/勤務地) から指定ラベルの値を返す。"""
        for dl in soup.select("dl.jobdetails__jobinfo_item"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if dt is None or dd is None:
                continue
            if _clean(dt.get_text(" ", strip=True)) == label:
                return _clean(dd.get_text(" ", strip=True))
        return ""

    @staticmethod
    def _conditions(soup) -> dict[str, str]:
        """こだわり条件ブロックの有効タグを {EXTRAカラム名: "A;B;C"} で返す。"""
        result: dict[str, str] = {}
        for dl in soup.select("dl.contents_block"):
            dt = dl.find("dt")
            if dt is None:
                continue
            column = CONDITION_BLOCKS.get(_clean(dt.get_text(" ", strip=True)))
            if column is None:
                continue
            tags = [
                _clean(p.get_text(" ", strip=True))
                for p in dl.select("p.job_condition__tag.available")
            ]
            result[column] = ";".join(t for t in tags if t)
        return result

    @staticmethod
    def _service_stats(soup) -> tuple[str, str]:
        """「接客について」ブロックから (1日あたりの接客数, 1回の接客時間) を返す。"""
        for dl in soup.select("dl.contents_block"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if dt is None or dd is None:
                continue
            if _clean(dt.get_text(" ", strip=True)) != "接客について":
                continue
            text = _clean(dd.get_text(" ", strip=True))
            num = _GUEST_NUM_RE.search(text)
            tim = _GUEST_TIME_RE.search(text)
            return (num.group(1) if num else "", tim.group(1) if tim else "")
        return "", ""

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _breadcrumbs(soup) -> list[str]:
        """ld+json BreadcrumbList の要素名リストを返す。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text() or ""
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict) and node.get("@type") == "BreadcrumbList":
                    return [
                        _clean(str(e.get("name", "")))
                        for e in node.get("itemListElement", [])
                        if isinstance(e, dict)
                    ]
        return []

    @staticmethod
    def _pref_from_crumbs(crumbs: list[str]) -> str:
        """パンくず3番目「東京 メンズエステ求人」→「東京都」。"""
        if len(crumbs) < 3:
            return ""
        head = crumbs[2].split()[0] if crumbs[2].split() else ""
        return _PREF_FULL.get(head, "")

    @staticmethod
    def _category_from_crumbs(crumbs: list[str]) -> str:
        """パンくず2番目「関東 メンズエステ求人」→「メンズエステ」。"""
        if len(crumbs) < 2:
            return DEFAULT_CATEGORY
        parts = crumbs[1].split()
        # 先頭トークンは地域名 (関東 / 北海道・東北 等) なので除く
        body = " ".join(parts[1:]) if len(parts) > 1 else crumbs[1]
        body = re.sub(r"求人\s*$", "", body).strip()
        return body or DEFAULT_CATEGORY

    @staticmethod
    def _pick_tel(text: str) -> str:
        """文字列から 03-XXXX-XXXX / 0X0-XXXX-XXXX 形式の電話番号を取り出す。"""
        if not text:
            return ""
        m = _TEL_RE.search(text.replace("−", "-").replace("ー", "-"))
        return m.group(0) if m else ""

    @staticmethod
    def _dd_text(dd) -> str:
        return _clean(dd.get_text(" ", strip=True)) if dd is not None else ""

    @staticmethod
    def _text(soup, selector: str) -> str:
        el = soup.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el is not None else ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EsJob3Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://esjob.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

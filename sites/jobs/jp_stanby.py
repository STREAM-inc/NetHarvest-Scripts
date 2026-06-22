"""
jp スタンバイ — 国内最大級の求人情報一括検索サイト

取得対象:
    - 全国47都道府県の求人情報（検索結果カードから取得）

取得フロー:
    1. 47都道府県を順に巡回
    2. 各都道府県で {url}search?l={都道府県}&p={page} をページング
    3. 検索結果カードから求人情報を取得
    4. TEL はカードになければ詳細ページの仕事内容テキストから抽出
    5. カードが0件になった時点でその都道府県の巡回を終了

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/jp_stanby.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jp
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlencode, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 全国47都道府県（検索パラメータ l= に使用）
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 都道府県パターン（PREF/ADDR 分割用）
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 1都道府県あたりの最大ページ数（暴走防止のセーフティ）
_MAX_PAGES_PER_PREF = 200

# 電話番号パターン（固定・携帯・フリーダイヤル）
_TEL_RE = re.compile(
    r"(?<!\d)"
    r"(0(?:\d{1,4}[-‐‑‒–－\s]\d{1,4}[-‐‑‒–－\s]\d{3,4}"
    r"|[789]0[-‐‑‒–－\s]\d{4}[-‐‑‒–－\s]\d{4}"
    r"|120[-‐‑‒–－\s]\d{3}[-‐‑‒–－\s]\d{3,4}"
    r"|800[-‐‑‒–－\s]\d{3}[-‐‑‒–－\s]\d{4}))"
    r"(?!\d)"
)


def _pick_attr(card, icon_cls: str) -> str:
    """attribution-item から特定アイコンクラスに対応するテキストを返す"""
    for attr in card.select("p.attribution-item"):
        if attr.select_one(f"span.{icon_cls}"):
            text_el = attr.select_one("span.caption-medium.text")
            return text_el.get_text(strip=True) if text_el else ""
    return ""


def _split_pref_addr(location: str):
    """「東京都 練馬区 / 光が丘駅」→ (「東京都」, 「練馬区 / 光が丘駅」)"""
    m = _PREF_RE.match(location)
    if m:
        pref = m.group(1)
        addr = location[m.end():].strip()
        return pref, addr
    return "", location


def _pick_tel(text: str) -> str:
    """テキストから最初の電話番号を返す（ハイフン正規化済み）"""
    m = _TEL_RE.search(text)
    if not m:
        return ""
    return re.sub(r"[-‐‑‒–－\s]+", "-", m.group(1))


def _stanby_job_url(card, base: str) -> str:
    """カードから /jobs/ の直リンクを取得。広告枠（有料掲載）は空文字を返す"""
    link = card.select_one("a[href*='/jobs/']")
    if not link:
        return ""
    href = link["href"]
    # tid トラッキングパラメータを除去してプレビューのみ残す
    if "?" in href:
        path, qs = href.split("?", 1)
        params = [p for p in qs.split("&") if not p.startswith("tid=")]
        href = path + ("?" + "&".join(params) if params else "")
    return urljoin(base, href)


class JpStanbyScraper(StaticCrawler):
    """jp スタンバイ 求人情報スクレイパー (jp.stanby.com)"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["企業名", "給与", "雇用形態", "勤務形態", "特徴"]
    # 詳細ページ取得遅延（TEL 抽出のためカードに TEL が無い場合のみ使用）
    DETAIL_DELAY = 1.0

    def parse(self, url: str):
        # url = "https://jp.stanby.com/" — 都道府県別検索URLのルート
        base = url.rstrip("/") + "/"
        search_base = f"{base}search"

        for pref in _PREFS:
            page = 1
            while page <= _MAX_PAGES_PER_PREF:
                page_url = f"{search_base}?{urlencode({'l': pref, 'p': page})}"
                try:
                    soup = self.get_soup(page_url)
                except Exception as e:
                    self.logger.warning(f"ページ取得失敗 {page_url}: {e}")
                    break

                if soup is None:
                    break
                cards = soup.select("div.job-card")
                if not cards:
                    break  # この都道府県の結果が尽きた

                for card in cards:
                    try:
                        title_el = card.select_one("h2.title a.title-link")
                        if not title_el:
                            continue
                        title = title_el.get_text(strip=True)

                        company_el = card.select_one("p.company")
                        company = company_el.get_text(strip=True) if company_el else ""

                        location = _pick_attr(card, "icn-distance")
                        pref_val, addr = _split_pref_addr(location)
                        # 場所情報が都道府県名で始まらない場合（市区町村のみ）は検索都道府県で補完
                        if not pref_val:
                            pref_val = pref

                        salary = _pick_attr(card, "icn-money")
                        employment = _pick_attr(card, "icn-bag")
                        schedule = _pick_attr(card, "icn-calendar-clock")

                        features = ", ".join(
                            f.get_text(strip=True)
                            for f in card.select("span.feature-label")
                        )

                        job_url = _stanby_job_url(card, base)

                        # カード全文から TEL を先探し、なければ詳細ページの仕事内容を参照
                        tel = _pick_tel(card.get_text())
                        if not tel and job_url:
                            try:
                                detail_soup = self.get_soup(job_url)
                                if detail_soup:
                                    # 仕事内容セクションを優先、なければページ全文
                                    desc_el = (
                                        detail_soup.select_one(".job-description")
                                        or detail_soup.select_one("[data-testid='description']")
                                        or detail_soup.select_one(".detail-content")
                                    )
                                    search_text = desc_el.get_text() if desc_el else detail_soup.get_text()
                                    tel = _pick_tel(search_text)
                            except Exception as e:
                                self.logger.debug(f"詳細ページTEL取得失敗 {job_url}: {e}")

                        yield {
                            Schema.NAME: title,
                            Schema.URL: job_url,
                            Schema.PREF: pref_val,
                            Schema.ADDR: addr,
                            Schema.TEL: tel,
                            "企業名": company,
                            "給与": salary,
                            "雇用形態": employment,
                            "勤務形態": schedule,
                            "特徴": features,
                        }
                    except Exception as e:
                        self.logger.warning(f"カードのパースに失敗: {e}")
                        continue

                page += 1


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JpStanbyScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://jp.stanby.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

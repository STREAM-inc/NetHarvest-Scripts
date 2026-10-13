"""
メカジョブ_整備士 (自動車整備士求人ナビ / seibishi.me) — 整備士・自動車検査員の求人

取得対象:
    職種「整備士」(ID=1) と「自動車検査員」(ID=2) の求人票 (計 4,862 件 / 2026-10-13 時点)。
    内訳は 整備士 4,745 件 + 検査員 117 件 で、サイト側の複合フィルタ URL
    /jobs/index/1_2/ の総件数と一致する (= 取りこぼしなく両職種を網羅できる)。
    人材紹介・派遣型の媒体のため、企業名非公開・派遣会社が雇用主の求人も除外せず取得し、
    「雇用区分」カラムで正社員紹介／派遣の別を判別できるようにする。

取得フロー:
    一覧 /jobs/index/1_2/ (20件/ページ) を ?page=N でページ送りし、各 .jobs-list-item から
    詳細 /jobs/detail/{job_id}/{shop_id}/ へ遷移する。詳細を 1 件取得するごとに即 yield する
    (Pattern B — 全件バッファしない)。範囲外ページは 404、最終ページは rel="next" が消える。

    詳細ページは dl > dt(h3) / dd のラベル駆動。確認できたラベルは
        募集職種 / 雇用形態 / 仕事内容 / 応募資格 / 求める人材 / 給与 / 勤務時間 /
        待遇・福利厚生 / 休日・休暇 / 社名 / 店舗名 / 勤務地 / 連絡先  (以上 60/60 件で出現)
        就業場所 (22/60) / 職場の雰囲気 (10/60)
    勤務地の都道府県・住所と掲載日は JSON-LD (JobPosting) から取得する。画面上に掲載日・
    更新日の表記は無く、datePosted が唯一の日付ソース。

    年収 (収入例) は詳細ページでは HTML コメント化されているため一覧の「収入例」を採用し、
    無い場合のみ詳細の HTML コメントからフォールバック取得する。

    TEL は「連絡先」が問い合わせフォーム誘導の定型文で、サンプル 60 件すべてで電話番号の
    掲載が無かった。番号表記がある求人のみ取得する (大半は空になる想定)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/seibishi_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id seibishi_2
"""

import re
import sys
import json
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import Comment

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 職種フィルタ: 1=整備士, 2=検査員 (備考の「整備士」「自動車検査員」に対応)
_LIST_PATH = "jobs/index/1_2/"
_ITEMS_PER_PAGE = 20
_MAX_PAGES = 600  # 無限ループ防止の保険 (実際は約 244 ページ)

_MEDIA_NAME = "メカジョブ"
# 募集職種がこのいずれかを含む求人のみ採用する (一覧は絞り込み済みだが念のため)
_TARGET_CATEGORIES = ("整備士", "検査員")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_NUM_RE = re.compile(r"\d[\d,]*")
_TEL_RE = re.compile(r"0\d{1,4}[-ー－(（]\d{1,4}[-ー－)）]?\d{3,4}")
_DATE_RE = re.compile(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})")
_INCOME_COMMENT_RE = re.compile(r"年収例.*?<dd>(.*?)</dd>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")

# 求人本文 (Schema.DESCRIPTION) に結合する詳細ラベル。
# ※自由記述を含むが、備考で「求人本文全文 (仕事内容・応募資格・企業情報)」と
#   明示的に取得を指示されているため対象に含める。
#   逆に「待遇・福利厚生」「職場の雰囲気」は指示対象外の長文のため取得しない。
_BODY_LABELS = ("仕事内容", "応募資格", "求める人材")


def _text(el) -> str:
    """<br> を改行に置き換えてテキスト化する (この媒体の dd は br 区切り)。"""
    if el is None:
        return ""
    for br in el.find_all("br"):
        br.replace_with("\n")
    # 区切りを "" にする: <br> 由来の改行と get_text の区切りが二重にならないようにする
    return el.get_text("")


def _clean(text: str) -> str:
    """改行コードを LF に統一し、前後の空白・余分な空行を整理する。"""
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t　]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _flat(text: str) -> str:
    """改行も含めて 1 行に正規化する (短いラベル値用)。"""
    return re.sub(r"\s+", " ", text or "").strip()


class Seibishi2Crawler(StaticCrawler):
    """メカジョブ_整備士 (自動車整備士求人ナビ) スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "媒体名",
        "求人タイトル",
        "店舗名",
        "雇用形態",
        "雇用区分",
        "年収",
        "給与",
        "掲載日",
    ]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとし、職種フィルタ付き一覧を導出する
        list_root = urllib.parse.urljoin(url, _LIST_PATH)

        page = 1
        max_page = None
        while page <= _MAX_PAGES:
            page_url = list_root if page == 1 else f"{list_root}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            items = soup.select(".jobs-list-item")
            if not items:
                break

            if page == 1:
                max_page = self._read_total(soup)

            for item_el in items:
                try:
                    base = self._parse_list_item(item_el, list_root)
                    if not base:
                        continue
                    detail = self._scrape_detail(base[Schema.URL])
                    if detail is None:
                        continue
                    # 職種フィルタ: 整備士・自動車検査員以外は除外する
                    category = detail.get(Schema.CAT_SITE, "")
                    if category and not any(c in category for c in _TARGET_CATEGORIES):
                        continue
                    base.update({k: v for k, v in detail.items() if v})
                    yield base
                except Exception as e:  # noqa: BLE001 — 1 件の失敗で全体を止めない
                    self.logger.warning("求人の解析に失敗: %s", e)
                    continue

            # ページ送りは rel="next" を優先し、無ければ総件数から算出した最終ページで止める
            if soup.select_one('.paging a[rel="next"]') is None:
                break
            if max_page is not None and page >= max_page:
                break
            page += 1

    # ------------------------------------------------------------------ 一覧

    def _read_total(self, soup) -> int | None:
        """一覧の総件数を読み取り、self.total_items と最終ページ番号を決める。"""
        el = soup.select_one(".paging p") or soup.select_one("h1")
        if el is None:
            return None
        m = _NUM_RE.search(el.get_text())
        if not m:
            return None
        total = int(m.group(0).replace(",", ""))
        self.total_items = total
        return (total + _ITEMS_PER_PAGE - 1) // _ITEMS_PER_PAGE

    def _parse_list_item(self, item_el, list_root: str) -> dict | None:
        """一覧カードから求人 URL・企業名・求人タイトル・年収 (収入例) を取得する。"""
        a = item_el.select_one('a[href*="/jobs/detail/"]')
        if not a:
            return None

        company = item_el.select_one(".jobs-companyname")
        catch = item_el.select_one(".jobs-list-catchcopy")
        item = {
            Schema.URL: urllib.parse.urljoin(list_root, a["href"]),
            Schema.NAME: _flat(company.get_text()) if company else "",
            "求人タイトル": _flat(catch.get_text()) if catch else "",
            "媒体名": _MEDIA_NAME,
        }

        # 年収は詳細では HTML コメント化されているため、一覧の「収入例」を主ソースにする
        for dl in item_el.select(".jobs-list-info dl"):
            dt, dd = dl.find("dt"), dl.find("dd")
            if dt and dd and _flat(dt.get_text()) == "収入例":
                item["年収"] = _flat(dd.get_text(" "))
        return item

    # ------------------------------------------------------------------ 詳細

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        main = soup.select_one(".job-detail-area")
        if main is None:
            return None

        # 「あなたへのおすすめ求人」は別求人のカードなので解析対象から外す
        for rec in main.select(".jobs-recommend-list"):
            rec.decompose()

        labels: dict[str, str] = {}
        for dl in main.select("dl"):
            dt, dd = dl.find("dt"), dl.find("dd")
            if not dt or not dd:
                continue
            label = _flat(dt.get_text())
            if label and label not in labels:
                labels[label] = _clean(_text(dd))

        data: dict = {}

        # 求人タイトルはキャッチコピー (h1 は SEO 用のページタイトルなのでフォールバック)
        catch = main.select_one(".jobs-detail-catchcopy") or soup.find("h1")
        if catch:
            data["求人タイトル"] = _flat(catch.get_text())

        if labels.get("社名"):
            data[Schema.NAME] = _flat(labels["社名"])
        if labels.get("店舗名"):
            data["店舗名"] = _flat(labels["店舗名"])
        if labels.get("募集職種"):
            data[Schema.CAT_SITE] = _flat(labels["募集職種"])

        employment = _flat(labels.get("雇用形態", ""))
        if employment:
            data["雇用形態"] = employment
            data["雇用区分"] = self._classify_employment(employment)

        if labels.get("給与"):
            data["給与"] = labels["給与"]
        if labels.get("勤務時間"):
            data[Schema.TIME] = _flat(labels["勤務時間"])
        if labels.get("休日・休暇"):
            data[Schema.HOLIDAY] = _flat(labels["休日・休暇"])

        # 勤務地 (JSON-LD 優先、無ければ「勤務地」ラベルの 1 行目)
        ld = self._parse_jsonld(soup)
        self._set_address(data, ld, labels.get("勤務地", ""))

        # 掲載日 (JSON-LD datePosted。画面上には掲載日・更新日の表記が無い)
        m = _DATE_RE.search(_flat(str(ld.get("datePosted", ""))))
        if m:
            data["掲載日"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 企業名のフォールバック (「社名」ラベルが無く JSON-LD にのみある場合)
        if not data.get(Schema.NAME):
            org = ld.get("hiringOrganization") or {}
            if isinstance(org, dict) and org.get("name"):
                data[Schema.NAME] = _flat(str(org["name"]))

        # TEL: 「連絡先」はフォーム誘導の定型文が大半。番号表記がある場合のみ採用
        tel = self._pick_tel(labels)
        if tel:
            data[Schema.TEL] = tel

        # 年収: 詳細では HTML コメント化されているためコメントから拾う (一覧で取れなかった場合用)
        income = self._income_from_comment(main)
        if income:
            data["年収"] = income

        # 求人本文 (仕事内容・応募資格・求める人材・企業情報) — 備考で明示指示された取得項目
        data[Schema.DESCRIPTION] = self._build_body(main, labels)
        return data

    @staticmethod
    def _classify_employment(employment: str) -> str:
        """雇用形態の表記から「正社員紹介／派遣」の別を判定する。"""
        if "派遣" in employment:
            return "派遣"
        if "正社員" in employment:
            return "正社員紹介"
        return employment

    @staticmethod
    def _parse_jsonld(soup) -> dict:
        """JobPosting の JSON-LD を取得する (制御文字を含むため strict=False)。"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw, strict=False)
            except ValueError:
                continue
            if isinstance(data, list):
                data = next((d for d in data if isinstance(d, dict)), {})
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}

    @staticmethod
    def _set_address(data: dict, ld: dict, fallback: str) -> None:
        """勤務地の都道府県・住所 (市区町村以降) を設定する。"""
        job_location = ld.get("jobLocation")
        if isinstance(job_location, list):
            job_location = next((x for x in job_location if isinstance(x, dict)), {})
        address = (job_location or {}).get("address") or {}
        pref = _flat(str(address.get("addressRegion", "")))
        rest = _flat(
            f"{address.get('addressLocality', '')}{address.get('streetAddress', '')}"
        )
        if pref and rest:
            data[Schema.PREF] = pref
            data[Schema.ADDR] = rest
            return

        # フォールバック: 「勤務地」ラベルの 1 行目 (2 行目以降は最寄駅などのアクセス情報)
        line = _flat(fallback.split("\n")[0]) if fallback else ""
        if not line:
            if pref:
                data[Schema.PREF] = pref
            return
        m = _PREF_PATTERN.match(line)
        if m:
            data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = line[m.end():].strip()
        else:
            if pref:
                data[Schema.PREF] = pref
            data[Schema.ADDR] = line

    @staticmethod
    def _pick_tel(labels: dict) -> str:
        for key in ("連絡先", "応募方法", "問い合わせ先"):
            m = _TEL_RE.search(labels.get(key, ""))
            if m:
                return m.group(0)
        return ""

    @staticmethod
    def _income_from_comment(main) -> str:
        """コメントアウトされた「年収例」の dd を取り出す。"""
        for comment in main.find_all(string=lambda t: isinstance(t, Comment)):
            if "年収例" not in comment:
                continue
            m = _INCOME_COMMENT_RE.search(str(comment))
            if m:
                return _flat(_TAG_RE.sub(" ", m.group(1)))
        return ""

    @staticmethod
    def _build_body(main, labels: dict) -> str:
        """仕事内容・応募資格・求める人材・企業情報を結合した求人本文を組み立てる。"""
        blocks = []
        for label in _BODY_LABELS:
            value = labels.get(label)
            if value:
                blocks.append(f"【{label}】\n{value}")

        # 企業情報 (ページ冒頭の企業紹介ブロック)
        copy_el = main.select_one(".job-detail-main-copy")
        if copy_el:
            parts = []
            for el in copy_el.select("h4, p.description"):
                text = _clean(_text(el))
                if text:
                    parts.append(text)
            if parts:
                blocks.append("【企業情報】\n" + "\n".join(parts))
        return "\n\n".join(blocks)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Seibishi2Crawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://seibishi.me/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

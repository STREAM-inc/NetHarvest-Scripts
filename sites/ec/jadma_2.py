"""
日本通信販売協会（JADMA）会員 — 会員企業検索

取得対象:
    - JADMA (公益社団法人 日本通信販売協会) の会員企業一覧
      (正会員 + 賛助会員、合計約 596 社)
    - 会員社名 / カナ / 所在地 / URL / 取扱商材 (カテゴリ) を中心に、
      代表者名・役職・電話・消費者窓口・会員種別・入会日も取得する。

取得フロー:
    - 一覧ページ (https://jadma.or.jp/membercompany/fmember) は Next.js App Router 製で、
      会員データは "Server Action" (POST) で 1 ページ 50 件ずつ取得される。
    - parse() は同じ URL に Next-Action ヘッダ付き POST を投げ、返ってきた RSC
      レスポンス中の JSON ({"companies":[...],"totalCount":N}) をパースして
      1 社ずつ即 yield する (取得即 yield / 全件バッファしない)。
    - pageParam を 0,1,2,... と進め、companies が空になったら終了。
    - 取扱商材 (categories) はコード ("28,15" 等) で返るため、会員種別
      (正会員=商品カテゴリ / 賛助会員=サービスカテゴリ) に応じたマップで名称へ復号する。

実行方法:
    # ローカルテスト
    python scripts/sites/ec/jadma_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jadma_2
"""

import json
import logging
import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 会員企業検索の Server Action ID (Next.js ビルドに紐づく。ビルド更新で変わり得る)。
# 引数: [{pageParam, keyword, memberType, abcOrder, category}] を text/plain(JSON) で POST。
_ACTION_ID = "f0233e1a5d9788b5676723e17027c39bd5a6373e"

# 1 リクエストあたりの返却件数 (サーバ既定)。
_PAGE_SIZE = 50

# 取扱商材コード → 名称。正会員 (isRegular=true) は「取扱商品」カテゴリを用いる。
_PRODUCT_CATEGORIES = {
    "1": "紳士衣料品", "2": "婦人衣料品", "3": "下着", "4": "子供・ベビー衣料用品",
    "5": "その他の衣料品", "6": "服飾雑貨", "7": "靴・鞄",
    "8": "アクセサリー、宝石、貴金属、時計", "9": "その他の服飾雑貨・貴金属",
    "10": "家具・収納用品", "11": "インテリア、敷物、寝具",
    "12": "ＴＶ・ビデオ・ＤＶＤ・ブルーレイ機器、カメラ、オーディオ類",
    "13": "パソコン（周辺機器を含む）", "14": "家庭電気製品",
    "15": "食器・台所家庭用品・トイレタリー", "16": "その他の家具・家電家庭用品",
    "17": "日曜大工、花・ガーデニング、ペット関連",
    "18": "スポーツ・レジャー用品、乗り物関連", "19": "手芸、工作用品",
    "20": "ゲーム機、おもちゃ、パソコン/ゲームソフト（ダウンロードサービスを除く）",
    "21": "本・雑誌・コミック（ダウンロードサービスを除く）",
    "22": "ＣＤ及びＤＶＤソフト（ダウンロードサービスを除く）",
    "23": "ダウンロードサービス（電子書籍・音楽・パソコン・ゲーム・映像など）",
    "24": "美術工芸品、骨董品", "25": "文房具、事務用品", "26": "その他の趣味、娯楽品",
    "27": "美容、健康・医療器具", "28": "化粧品", "29": "医薬品・介護用品等その他",
    "30": "食料品（健康食品、地方特産品・産直品・飲料を除く）", "31": "地方特産品・産直品",
    "32": "健康食品", "33": "飲料（水・酒類、ソフトドリンク等）", "34": "その他の食料品",
    "35": "通信教育講座", "36": "旅行", "37": "保険・金融",
    "38": "コンサート、演劇のチケット", "39": "交通機関等のチケット", "40": "その他のサービス",
    "41": "その他", "42": "ギフト・お取り寄せ", "43": "防災用品",
}

# 取扱商材コード → 名称。賛助会員 (isRegular=false) は「提供サービス」カテゴリを用いる。
_SERVICE_CATEGORIES = {
    "1": "商品企画", "2": "市場/企業調査/メディア・情報収集",
    "3": "コンサルタント/マーケティング", "4": "システム関連/一元管理/セキュリティ",
    "5": "サイト制作/カート/ASP/アプリ",
    "6": "クラウドソーシング/効率化・自動化ツール/ツール連携",
    "7": "決済サービス/回収代行/提携クレジット", "8": "広告代理業・カタログ製作",
    "9": "集客/分析/SEO/データフィード/CRM/広告・SNS運用", "10": "UGC/動画コマース",
    "11": "DM/印刷関係", "12": "AI/チャットボット/WEB接客/問い合わせ一元管理",
    "13": "コールセンター", "14": "在庫管理/返品対応", "15": "物流倉庫/配送会社/置き配",
    "16": "出荷管理/フルフィルメント", "17": "印刷関連/梱包/パッケージ/用紙",
    "18": "その他", "19": "原料調達/OEM", "20": "人材派遣",
}

# RSC レスポンス中の companies JSON を取り出す起点。
_COMPANIES_MARKER = '{"companies":'
# joinDate は "$D2011-07-15T00:00:00.000Z" 形式で返る。
_JOINDATE_RE = re.compile(r"\$D?(\d{4}-\d{2}-\d{2})")


class JadmaMember(StaticCrawler):
    """日本通信販売協会（JADMA）会員 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["取扱商材コード", "消費者窓口", "会員種別", "入会日"]

    def _fetch_page(self, url: str, page_param: int) -> list[dict]:
        """Server Action を叩いて 1 ページ分の会員 dict リストを返す。"""
        payload = [{
            "pageParam": page_param,
            "keyword": "",
            "memberType": "",
            "abcOrder": "",
            "category": "",
        }]
        headers = {
            "Next-Action": _ACTION_ID,
            "Content-Type": "text/plain;charset=UTF-8",
            "Accept": "text/x-component",
            "Referer": url,
        }
        resp = self.session.post(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            timeout=self.TIMEOUT,
        )
        resp.raise_for_status()
        # text/x-component は charset 未指定で返るため requests が latin1 と誤判定する。
        # 実体は UTF-8 なので明示デコードして文字化けを防ぐ。
        text = resp.content.decode("utf-8", errors="replace")

        start = text.find(_COMPANIES_MARKER)
        if start < 0:
            logger.warning("companies が見つかりません (page=%s)", page_param)
            return []
        # {"companies":...,"totalCount":N} を JSON として厳密に切り出す。
        obj, _ = json.JSONDecoder().raw_decode(text[start:])
        total = obj.get("totalCount")
        if total is not None and self.total_items is None:
            self.total_items = total
        return obj.get("companies") or []

    @staticmethod
    def _decode_categories(codes: str, is_regular: bool) -> str:
        """"28,15" 等のコード列を会員種別に応じた名称へ復号し「、」で連結する。"""
        table = _PRODUCT_CATEGORIES if is_regular else _SERVICE_CATEGORIES
        names = []
        for c in (codes or "").split(","):
            c = c.strip()
            if not c:
                continue
            names.append(table.get(c, c))
        return "、".join(names)

    @staticmethod
    def _address(company: dict) -> str:
        """companyCity + companyAddress を結合して所在地 (都道府県以降) を作る。"""
        parts = [
            (company.get("companyCity") or "").strip(),
            (company.get("companyAddress") or "").strip(),
        ]
        return "".join(p for p in parts if p)

    def parse(self, url: str):
        page_param = 0
        while True:
            companies = self._fetch_page(url, page_param)
            if not companies:
                break

            for company in companies:
                try:
                    is_regular = bool(company.get("isRegular"))
                    codes = company.get("categories") or ""

                    join_date = ""
                    m = _JOINDATE_RE.search(company.get("joinDate") or "")
                    if m:
                        join_date = m.group(1)

                    yield {
                        Schema.NAME: (company.get("companyName") or "").strip(),
                        Schema.NAME_KANA: (company.get("companyNameFurigana") or "").strip(),
                        Schema.POST_CODE: (company.get("companyPostCode") or "").strip(),
                        Schema.PREF: (company.get("companyPrefecture") or "").strip(),
                        Schema.ADDR: self._address(company),
                        Schema.TEL: (company.get("companyPhone") or "").strip(),
                        Schema.REP_NM: (company.get("companyRepName") or "").strip(),
                        Schema.POS_NM: (company.get("companyRepPosition") or "").strip(),
                        Schema.HP: (company.get("companySiteUrl") or "").strip(),
                        Schema.CAT_SITE: self._decode_categories(codes, is_regular),
                        Schema.URL: url,
                        "取扱商材コード": codes,
                        "消費者窓口": (company.get("companyContact") or "").strip(),
                        "会員種別": "正会員" if is_regular else "賛助会員",
                        "入会日": join_date,
                    }
                except Exception as e:  # noqa: BLE001 — 個別レコードの失敗はスキップして継続
                    logger.warning("会員データの解析に失敗 (page=%s): %s", page_param, e)
                    continue

            # 最終ページ (返却が満たない) なら打ち切り。
            if len(companies) < _PAGE_SIZE:
                break

            page_param += 1
            time.sleep(self.DELAY)  # ページ送り間のポライトネス待機


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JadmaMember()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://jadma.or.jp/membercompany/fmember")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

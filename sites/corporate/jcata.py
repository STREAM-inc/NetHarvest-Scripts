"""
一般社団法人 在日華人旅行業協会 (jcata.net) — 会員一覧スクレイパー

取得対象:
    - 協会トップページに掲載されている「会員一覧」
    - 会員企業の名称・代表者名・役職・都道府県

取得フロー:
    トップページ (parse() の引数 url = sites.yml の正規 URL) を取得し、
    「会員一覧」見出しに続く Wix の折りたたみテキスト (collapsible-text) ブロックを
    抽出する。ブロック内は 1 行 1 会員で、
        氏名｜役職｜会社名（都道府県）
    の形式（全角縦棒 ｜ 区切り）になっている。各行を分解して 1 件ずつ即 yield する。

    ※ 会員一覧はトップページ内に静的テキストとして埋め込まれており、会員ごとの
       詳細ページは存在しない。したがって全件のソース URL は起点 (url) となる。
    ※ 旧実装は「開催予定のイベント」一覧 (Wix Events) を取得していたが、
       追加指示により会員一覧の取得へ切り替えた。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jcata.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jcata
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# sites.yml に登録された正規 URL（コンテナ実行・ローカル実行で一致させる）
ROOT_URL = "https://www.jcata.net/"

# 会員エントリのフィールド区切りに使われる全角縦棒
_DELIM = "｜"

# 47 都道府県の正式名称
_PREFS = [
    "北海道",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
# 略記（都/道/府/県 を省いた形, 例: "東京" "大阪" "三重"）→ 正式名称。
# 北海道は省略形が存在しないため正式名のみを対象とする。
_PREF_STEMS = [(re.sub(r"[都道府県]$", "", p), p) for p in _PREFS if p != "北海道"]
# stem 完全一致で判定するため衝突は起きないが、長い stem を優先しておく
_PREF_STEMS.sort(key=lambda x: len(x[0]), reverse=True)

# 所在地括弧内の区切り（"東京･埼玉･大阪" や "東京都/大阪府" 等）
_LOC_SPLIT_RE = re.compile(r"[／/･・、,]")
# 全角括弧（非ネスト）。会社形態の （株）（有）（合）と所在地 （大阪府）を含む
_PAREN_RE = re.compile(r"（([^（）]*)）")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _match_pref(token: str) -> str:
    """都道府県トークンを正式名称に正規化する（一致しなければ空文字）。"""
    token = token.strip()
    if not token:
        return ""
    if token in _PREFS:
        return token
    for stem, full in _PREF_STEMS:
        if token == stem:
            return full
    return ""


def _split_company_pref(field: str) -> tuple[str, str]:
    """会社名フィールドを「会社名」と「都道府県」に分離する。

    入力例:
        "（株）日中文化旅行センター（大阪府）"  → ("（株）日中文化旅行センター", "大阪府")
        "金冠国际旅行社（東京都）／ロイヤルジュエリー（株）（愛知県）"
            → ("金冠国际旅行社／ロイヤルジュエリー（株）", "東京都")
        "華青旅行（韓国）"                       → ("華青旅行（韓国）", "")

    所在地（都道府県）を含む括弧のみを会社名から除去し、最初に見つかった
    都道府県を返す。（株）等の会社形態を表す括弧は会社名に残す。
    """
    pref = ""
    company = field
    for m in _PAREN_RE.finditer(field):
        tokens = _LOC_SPLIT_RE.split(m.group(1))
        matched = [p for p in (_match_pref(t) for t in tokens) if p]
        if matched:
            if not pref:
                pref = matched[0]
            # 所在地括弧なので会社名から取り除く
            company = company.replace(m.group(0), "")
    return _clean(company), pref


def _find_member_block(soup) -> str:
    """会員一覧のテキストブロックを返す（見つからなければ空文字）。

    会員行は全角縦棒 ｜ 区切りなので、折りたたみテキスト要素のうち ｜ を
    最も多く含むものを会員一覧とみなす（Wix の自動生成 id に依存しない）。
    """
    if soup is None:
        return ""
    candidates = soup.select(
        'p.wixui-collapsible-text__text, [data-testid="ellipsis_text_viewer_text_wrapper"]'
    )
    best, best_count = "", 0
    for el in candidates:
        text = el.get_text("\n", strip=False)
        count = text.count(_DELIM)
        if count > best_count:
            best, best_count = text, count
    return best


class JcataCrawler(StaticCrawler):
    """一般社団法人 在日華人旅行業協会 会員一覧スクレイパー"""

    # 取得は起点ページ 1 枚のみ。会員ごとの追加リクエストは無いため待機不要。
    DELAY = 0.0

    def parse(self, url: str):
        # 引数 url（= 正規 URL）を唯一の起点とする
        soup = self.get_soup(url)
        if soup is None:
            return

        block = _find_member_block(soup)
        if not block:
            self.logger.warning("会員一覧ブロックが見つかりませんでした: %s", url)
            return

        # 1 行 1 会員。｜ 区切りを含む行のみを会員エントリとして扱う。
        lines = [ln for ln in re.split(r"\n+", block) if _DELIM in ln]
        self.total_items = len(lines)

        for line in lines:
            try:
                parts = [p.strip() for p in line.split(_DELIM)]
                if len(parts) < 3:
                    continue

                rep_name = _clean(parts[0])
                position = _clean(parts[1])
                # 会社名に ｜ は通常含まれないが、念のため残りを結合
                company_field = _DELIM.join(parts[2:])
                company, pref = _split_company_pref(company_field)

                if not company:
                    continue

                yield {
                    Schema.NAME: company,
                    Schema.URL: url,
                    Schema.REP_NM: rep_name,
                    Schema.POS_NM: position,
                    Schema.PREF: pref,
                }
            except Exception as e:
                self.logger.warning("会員エントリの解析に失敗: %s — %s", line, e)
                continue


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JcataCrawler()
    scraper.execute(ROOT_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

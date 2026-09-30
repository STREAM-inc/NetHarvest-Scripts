"""
【STREAMREQ-18917】インド観光省 NIDHI+ 認定旅行会社 Classified（インド）
 — streamreq_18917_nidhi_classified

取得対象:
    インド観光省 NIDHI+ (National Integrated Database of Hospitality Industry) の
    「Tourism Service Provider (categoryCode=02)」の **Classified** 一覧
    (= 観光省の認定を受けた事業者。2026-09-30 時点 1,578 件)。
    そのうち依頼指示により **認定区分に "Travel Agent" または "MICE" を含む社のみ**
    を出力する (インバウンド専業・国内ツアー専業・送迎専業・アドベンチャー専業は
    日本向け旅行を販売しないため取得時点で除外)。

    ※ 自己申告のみの Registered 一覧 (/home/directory、12,481 件) は観光省の認定を
      経ていないため対象外 (依頼指示)。

サイト構造 (Phase 1 調査結果 / 2026-09-30 実測):
    - 完全なサーバーレンダリング HTML。JS 無しで全データが入っている → StaticCrawler。
    - 詳細ページは存在しない (社名の <a> は href="javascript:void(0)")。
      1 社分の情報はすべて一覧の div.listing-block 内にある。
        h2.hotel-heading              … 会社名
        p.address                     … 住所。改行区切りで
                                        [番地〜, (市), 州, 郵便番号(6桁)]
                                        市が空の行 (Delhi 等) があり 3 行になる
        div.share-location > span     … (認定レベル, 認定区分) のペアが 1 組以上
                                        認定レベルは style="color: #f1476b;" 側
                                        例: レベル "Experienced Tourism Service Providers"
                                            区分   "Travel Agent (Experienced)"
        div.share-location > div      … share.png の後ろが事業種別
                                        (Tour Operator / Travel Agent /
                                         Tourist Transport Operator)
        div.contact-details           … mail.png=メール / call.png=TEL /
                                        website.png=HP (50 件中 39 件のみ)
    - ページネーション: ?pageno=N (1 ページ 50 件)。最終ページの次は listing-block が
      0 件になるだけでエラーにはならない (pageno=33 で 0 件を確認)。
      pager は「1 2 … Next」の窓表示で総ページ数は出ないため、
      「50 件未満 or 0 件で打ち切り」で判定する。

州別クロールにする理由 (依頼指示):
    stateCode を付けずにページ送りすると並び順が揺れて重複・取りこぼしが出る。
    そこで /api/KeyValues/states (36 州/UT) で州コードを取得し、州ごとに
    ?stateCode=XX を付けて全ページを巡回する。
    各州の「N classified found」の合計 = 1,578 = 州指定なしの総件数であることを
    2026-09-30 に確認済み (= 州別巡回で母集団を取りこぼさない)。
    それでも州内で同一社が 2 行掲載されるケースがある (Delhi: 602 行 / 574 社) ため、
    依頼指示どおり **社名 + TEL** で重複除去する。

データ上の注意点:
    - 認定区分は 1 社で複数持つことがある
      (例: "Domestic Tour Operators (Experienced)" + "Inbound Tour Operators
       (Experienced)")。全部を " / " で連結して保持し、フィルタ判定にも全部を使う。
      まったく同じ区分が 2 回出る行もあるため重複は畳む。
    - 住所の郵便番号はインドの PIN コード (6 桁)。Schema.POST_CODE は日本の 7 桁
      前提で 6 桁を空に落とすため、EXTRA「郵便番号(PIN)」に入れる。
      Schema.ADDR は依頼指示どおり市・州・郵便番号を含む全文を入れる。
    - Schema.PREF は日本の都道府県用カラムのため空にする (州は EXTRA「州」)。
    - TEL は +91 を含む国際表記に正規化する (10 桁の携帯は +91-XXXXX-XXXXX)。
      ただしフレームワークの正規化 (src/utils/normalizer.py) が "+" を落とすため、
      CSV 上は "91-XXXXX-XXXXX" になる。原文は EXTRA「電話番号(原文)」に残す。

取得しないフィールド (除外):
    - 無し。このページに長文の自由記述 (店舗紹介文等) は存在せず、
      すべて短い構造化ラベル・連絡先のみ。

名寄せ:
    海外所在の事業者のため STX 名寄せは対象外 (依頼指示)。

利用規約 / robots.txt (2026-09-30 確認):
    - https://nidhi.tourism.gov.in/robots.txt は 404 (robots.txt の指定なし)。
    - 利用規約にスクレイピング・自動取得を禁じる文言なし
      (著作権ポリシーは転載時の許可に関する記載のみ)。
    → 収集継続可能と判断。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18917_nidhi_classified.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py \
        --site-id streamreq_18917_nidhi_classified
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 1 ページあたりの掲載件数 (これ未満なら最終ページ)
_PAGE_SIZE = 50
# 1 州あたりのページ数の安全上限 (Delhi = 604 件 / 13 ページ が最大)
_MAX_PAGES_PER_STATE = 60

# 州一覧 API (ページ内 JS が使っているもの)
_STATES_API_PATH = "/api/KeyValues/states"

# 認定レベル (ピンク文字) の span を見分ける style
_TIER_STYLE_PATTERN = re.compile(r"#f1476b", re.IGNORECASE)

# 依頼指示の取得対象: 認定区分に Travel Agent / MICE を含む社のみ
_TARGET_ACCREDITATION_PATTERN = re.compile(r"travel\s*agent|\bmice\b", re.IGNORECASE)

# インドの PIN コード (6 桁)
_PIN_PATTERN = re.compile(r"^\d{6}$")

# メールアドレスらしき文字列
_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")

class StreamReq18917NidhiClassified(StaticCrawler):
    """インド観光省 NIDHI+ Classified (観光省認定の観光サービス事業者) スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "国",
        "都市",
        "州",
        "郵便番号(PIN)",
        "認定区分",
        "認定レベル",
        "電話番号(原文)",
        "備考",
    ]

    COUNTRY = "インド"

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """ルート URL (= sites.yml の url) を起点に、州ごとに全ページを巡回する。

        1 ページ取得するたびに、その場で該当社を yield する
        (全件をためてから出力しないこと)。
        """
        seen: set[tuple[str, str]] = set()
        scanned = 0
        matched = 0

        for state_code, state_name in self._fetch_states(url):
            for page in range(1, _MAX_PAGES_PER_STATE + 1):
                page_url = self._page_url(url, state_code, page)
                soup = self.get_soup(page_url)
                if soup is None:
                    self.logger.warning("取得失敗のためこの州を打ち切ります: %s", page_url)
                    break

                blocks = soup.select("div.listing-block")
                if not blocks:
                    break
                scanned += len(blocks)

                for block in blocks:
                    item = self._parse_block(block, page_url, state_name)
                    if item is None:
                        continue
                    key = self._dedupe_key(item)
                    if key in seen:
                        continue
                    seen.add(key)
                    matched += 1
                    yield item

                if len(blocks) < _PAGE_SIZE:
                    break
            else:
                self.logger.warning("ページ数の上限に達しました (state=%s)", state_code)

        self.logger.info(
            "走査 %d 行 / 該当 (Travel Agent・MICE) %d 社を出力しました", scanned, matched
        )

    # ------------------------------------------------------------------ #
    # 州一覧の取得
    # ------------------------------------------------------------------ #
    def _fetch_states(self, url: str) -> list[tuple[str, str]]:
        """ルート URL と同一オリジンの州一覧 API から (州コード, 州名) を取得する。

        API が取れなかった場合は州で絞らない 1 巡 (stateCode 空) にフォールバックする。
        依頼指示どおり通常は州別に巡回する。
        """
        parts = urlsplit(url)
        api_url = urlunsplit((parts.scheme, parts.netloc, _STATES_API_PATH, "", ""))
        try:
            response = self.session.get(api_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            data = json.loads(response.text)
        except Exception as e:  # noqa: BLE001 — API 不通時も州無しで続行させる
            self.logger.warning(
                "州一覧 API を取得できませんでした (%s)。州で絞らずに巡回します: %s", e, api_url
            )
            return [("", "")]

        states = [
            (str(row.get("key") or "").strip(), str(row.get("value") or "").strip())
            for row in data
            if str(row.get("key") or "").strip()
        ]
        if not states:
            self.logger.warning("州一覧 API が空でした。州で絞らずに巡回します: %s", api_url)
            return [("", "")]

        self.logger.info("州一覧 %d 件を取得しました: %s", len(states), api_url)
        return states

    # ------------------------------------------------------------------ #
    # URL 組み立て
    # ------------------------------------------------------------------ #
    @staticmethod
    def _page_url(url: str, state_code: str, page: int) -> str:
        """ルート URL のクエリ (categoryCode 等) を保ったまま stateCode / pageno を差し替える。"""
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["stateCode"] = state_code
        query["pageno"] = str(page)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))

    # ------------------------------------------------------------------ #
    # 1 社分のパース
    # ------------------------------------------------------------------ #
    def _parse_block(self, block: bs4.Tag, page_url: str, state_name: str) -> dict | None:
        """listing-block を辞書化する。対象外 (Travel Agent / MICE 以外) は None。"""
        name = self._text(block.select_one("h2.hotel-heading"))
        if not name:
            return None

        tiers, accreditations = self._accreditations(block)
        if not any(_TARGET_ACCREDITATION_PATTERN.search(a) for a in accreditations):
            # インバウンド専業・国内ツアー専業・送迎専業・アドベンチャー専業は対象外
            return None

        street, city, state, pin = self._split_address(block.select_one("p.address"))
        # 一覧を州で絞っている場合、住所側に州名が無くても州コード由来の州名で補う
        state = state or state_name

        email = self._contact(block, "mail.png")
        tel_raw = self._contact(block, "call.png")
        website = self._contact(block, "website.png")

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.PREF: "",  # 日本の都道府県用カラムのため海外事業者では空
            Schema.ADDR: self._join_address(street, city, state, pin),
            Schema.TEL: self._normalize_tel(tel_raw),
            Schema.EMAIL: self._extract_email(email),
            Schema.HP: self._normalize_url(website),
            Schema.CAT_SITE: self._business_type(block),
            "国": self.COUNTRY,
            "都市": city,
            "州": state,
            "郵便番号(PIN)": pin,
            "認定区分": " / ".join(accreditations),
            "認定レベル": " / ".join(tiers),
            "電話番号(原文)": tel_raw,
            "備考": self._build_note(accreditations),
        }

    # ------------------------------------------------------------------ #
    # 認定区分 / 事業種別
    # ------------------------------------------------------------------ #
    @classmethod
    def _accreditations(cls, block: bs4.Tag) -> tuple[list[str], list[str]]:
        """(認定レベル一覧, 認定区分一覧) を返す。どちらも重複は畳み、出現順を保つ。

        div.share-location の span は
            [認定レベル (style に #f1476b), 認定区分] のペアが 1 組以上並ぶ。
        同じ区分が 2 回出る行があるため重複除去する。
        """
        holder = block.select_one("div.share-location")
        if holder is None:
            return [], []

        tiers: list[str] = []
        accreditations: list[str] = []
        for span in holder.find_all("span"):
            text = cls._text(span)
            if not text:
                continue
            bucket = tiers if _TIER_STYLE_PATTERN.search(span.get("style") or "") else accreditations
            if text not in bucket:
                bucket.append(text)
        return tiers, accreditations

    @classmethod
    def _business_type(cls, block: bs4.Tag) -> str:
        """share.png アイコンの後ろに出る事業種別を返す。

        例: Tour Operator / Travel Agent / Tourist Transport Operator
        """
        img = block.select_one("div.share-location img[src*='share.png']")
        if img is None or img.parent is None:
            return ""
        return cls._text(img.parent)

    # ------------------------------------------------------------------ #
    # 住所
    # ------------------------------------------------------------------ #
    @classmethod
    def _split_address(cls, tag: bs4.Tag | None) -> tuple[str, str, str, str]:
        """p.address を (番地〜, 市, 州, 郵便番号) に分解する。

        改行区切りで末尾から [郵便番号(6桁), 州, 市] の順に並ぶ。
        市が空で 3 行しかない行 (Delhi 等) があるため、末尾から詰めて解釈する。
        """
        if tag is None:
            return "", "", "", ""

        lines = [cls._clean(line) for line in tag.get_text("\n").split("\n")]
        lines = [line for line in lines if line]
        if not lines:
            return "", "", "", ""

        pin = ""
        if _PIN_PATTERN.match(lines[-1]):
            pin = lines[-1]
            lines = lines[:-1]

        state = lines.pop() if len(lines) >= 2 else ""
        city = lines.pop() if len(lines) >= 2 else ""
        street = " ".join(lines)
        return street, city, state, pin

    @staticmethod
    def _join_address(street: str, city: str, state: str, pin: str) -> str:
        """依頼指示どおり市・州・郵便番号を含む住所全文を組み立てる。"""
        return " ".join(part for part in (street, city, state, pin) if part).strip()

    # ------------------------------------------------------------------ #
    # 連絡先
    # ------------------------------------------------------------------ #
    @classmethod
    def _contact(cls, block: bs4.Tag, icon: str) -> str:
        """div.contact-details のうち、指定アイコンの行のテキストを返す。"""
        img = block.select_one(f"div.contact-details img[src*='{icon}']")
        if img is None or img.parent is None:
            return ""
        return cls._text(img.parent)

    @staticmethod
    def _extract_email(value: str) -> str:
        """メール欄からアドレス部分のみを取り出す (取れなければ空)。"""
        m = _EMAIL_PATTERN.search((value or "").replace(" ", ""))
        return m.group(0) if m else ""

    # ------------------------------------------------------------------ #
    # 正規化ヘルパー
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """ノーブレークスペース等を通常空白にし、連続空白を 1 つに畳む。"""
        return re.sub(r"[ \t\r ]+", " ", (text or "").replace("\xa0", " ")).strip()

    @classmethod
    def _text(cls, tag: bs4.Tag | None) -> str:
        """タグのテキストを 1 行に畳んで返す。"""
        if tag is None:
            return ""
        return cls._clean(re.sub(r"\s+", " ", tag.get_text(" ", strip=True)))

    @classmethod
    def _dedupe_key(cls, item: dict) -> tuple[str, str]:
        """依頼指示どおり「社名 + TEL」で重複を判定するキー。"""
        name = re.sub(r"[\s.,&()\-]+", "", item.get(Schema.NAME, "")).lower()
        tel = re.sub(r"\D", "", item.get(Schema.TEL, ""))
        return name, tel

    @staticmethod
    def _normalize_tel(raw: str) -> str:
        """インドの電話番号を +91 を含む国際表記に正規化する。

        - 10 桁の携帯・固定は "+91-XXXXX-XXXXX" にする (依頼指示)。
        - 先頭の国内プレフィックス 0 / 国番号 91・0091 の重複付与は取り除く。
        - ※ CSV 出力時はフレームワークの正規化で "+" が落ち "91-…" になる。
          原文は EXTRA「電話番号(原文)」に保持している。
        """
        digits = re.sub(r"\D", "", raw or "")
        if not digits:
            return ""

        if digits.startswith("0091"):
            digits = digits[4:]
        elif digits.startswith("91") and len(digits) > 10:
            digits = digits[2:]
        digits = digits.lstrip("0")
        if not digits:
            return ""

        if len(digits) == 10:
            return f"+91-{digits[:5]}-{digits[5:]}"
        return f"+91-{digits}"

    @staticmethod
    def _normalize_url(raw: str) -> str:
        """スキームが欠けた URL ("www.example.com") に https を補う。URL でなければ空。"""
        url = (raw or "").strip()
        if not url or "." not in url:
            return ""
        if url.startswith(("http://", "https://")):
            return url
        return f"https://{url}"

    @staticmethod
    def _build_note(accreditations: list[str]) -> str:
        """備考を組み立てる (観光省認定である旨と認定区分を残す)。"""
        notes = ["インド観光省 NIDHI+ 認定事業者一覧 (Classified) 掲載"]
        if accreditations:
            notes.append("認定区分: " + " / ".join(accreditations))
        return " / ".join(notes)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = StreamReq18917NidhiClassified()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nidhi.tourism.gov.in/home/classified?categoryCode=02&pageno=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")

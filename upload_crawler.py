#!/usr/bin/env python3
"""
GitHub へのクローラー登録スクリプト

使用方法:
  python upload_crawler.py /path/to/crawler.py
"""

import sys
import shutil
import subprocess
from pathlib import Path
import yaml


def get_user_input():
    """対話的に必要な情報を取得"""
    from datetime import datetime

    print("\n" + "=" * 60)
    print("NetHarvest クローラー登録")
    print("=" * 60)

    site_id = input("\n【site_id】(英語、他と被らないように): ").strip()
    if not site_id:
        print("エラー: site_id は必須です")
        sys.exit(1)

    name = input("【name】(日本語表示名): ").strip()
    if not name:
        print("エラー: name は必須です")
        sys.exit(1)

    url = input("【url】(対象URL): ").strip()
    if not url:
        print("エラー: url は必須です")
        sys.exit(1)

    # スケジュール選択（時間は固定で午前3時）
    print("\n【実行期間】(午前3時に実行)")
    print("  1: 毎月同じ日（デフォルト: 本日から1ヶ月ごと）")
    print("  2: 毎週同じ曜日（例: 毎週月曜）")
    print("  3: 毎日")
    print("  4: 手動実行のみ (null)")

    schedule_choice = input("選択 (1): ").strip() or "1"

    today = datetime.now()

    if schedule_choice == "1":
        # デフォルト：毎月同じ日（本日と同じ日）
        day = today.day
        schedule = f"0 3 {day} * *"
        print(f"✓ 毎月 {day} 日午前3時に実行します")

    elif schedule_choice == "2":
        # 毎週同じ曜日
        weekday_names = ["月", "火", "水", "木", "金", "土", "日"]
        today_weekday = (today.weekday() + 1) % 7  # 0=月, ..., 6=日
        print(f"  デフォルト: 毎週{weekday_names[today_weekday]}曜日")
        print("  (曜日番号: 1=月, 2=火, 3=水, 4=木, 5=金, 6=土, 0=日)")

        weekday_input = input(f"曜日 ({today_weekday}): ").strip() or str(today_weekday)
        try:
            weekday = int(weekday_input)
            schedule = f"0 3 * * {weekday}"
            print(f"✓ 毎週{weekday_names[weekday % 7]}曜日午前3時に実行します")
        except ValueError:
            print("エラー: 有効な曜日番号を入力してください")
            sys.exit(1)

    elif schedule_choice == "3":
        # 毎日
        schedule = "0 3 * * *"
        print("✓ 毎日午前3時に実行します")

    elif schedule_choice == "4":
        # 手動実行のみ
        schedule = None
        print("✓ 手動実行のみ（自動スケジュール実行なし）")

    else:
        print("エラー: 有効な選択肢を入力してください (1-4)")
        sys.exit(1)

    return {
        "site_id": site_id,
        "name": name,
        "url": url,
        "schedule": schedule,
    }


def detect_category_and_filename(py_file: Path, site_id: str) -> tuple[str, str]:
    """ファイル名からカテゴリとモジュール名を自動検出"""
    # デフォルト：site_id をそのままカテゴリとして使用
    category = site_id
    filename = py_file.name

    return category, filename


def upload_crawler(py_file_path: str):
    """クローラーを NetHarvest に登録して GitHub に push"""
    py_file = Path(py_file_path).resolve()

    # ファイル存在確認
    if not py_file.exists():
        print(f"エラー: ファイルが見つかりません: {py_file}")
        sys.exit(1)

    if not py_file.suffix == ".py":
        print(f"エラー: Python ファイル (.py) をアップロードしてください")
        sys.exit(1)

    # ユーザー入力を取得
    info = get_user_input()
    site_id = info["site_id"]
    name = info["name"]
    url = info["url"]
    schedule = info["schedule"]

    # カテゴリとモジュール名を決定
    category, filename = detect_category_and_filename(py_file, site_id)

    # NetHarvest scripts ディレクトリ
    scripts_dir = Path(__file__).parent.resolve()
    sites_dir = scripts_dir / "sites" / category

    # カテゴリディレクトリを作成
    sites_dir.mkdir(parents=True, exist_ok=True)

    # Python ファイルをコピー
    dest_file = sites_dir / filename
    shutil.copy2(py_file, dest_file)
    print(f"✓ ファイルをコピー: {dest_file}")

    # module パス（.py 拡張子なし）
    module_name = filename.replace(".py", "")
    module_path = f"{category}.{module_name}"

    # sites.yml を読み込み
    sites_yml = scripts_dir / "sites.yml"
    with open(sites_yml, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # site_id の重複チェック
    existing_ids = [site["site_id"] for site in data.get("sites", [])]
    if site_id in existing_ids:
        print(f"警告: site_id '{site_id}' は既に登録されています")
        overwrite = input("上書きしますか？ (y/n): ").strip().lower()
        if overwrite != "y":
            sys.exit(1)
        # 既存エントリを削除
        data["sites"] = [s for s in data["sites"] if s["site_id"] != site_id]

    # 新しいエントリを作成
    new_entry = {
        "site_id": site_id,
        "name": name,
        "module": module_path,
        "url": url,
        "schedule": schedule,
        "enabled": True,
    }

    # sites.yml に追記
    data["sites"].append(new_entry)

    with open(sites_yml, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    print(f"✓ sites.yml を更新: site_id='{site_id}'")

    # Git 操作
    try:
        os.chdir(scripts_dir)

        # git add
        subprocess.run(["git", "add", f"sites/{category}/", "sites.yml"], check=True)
        print("✓ git add 完了")

        # git commit
        commit_msg = f"Add {name} scraper ({site_id})"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        print(f"✓ git commit 完了: '{commit_msg}'")

        # git push
        subprocess.run(["git", "push"], check=True)
        print("✓ git push 完了")

        print("\n" + "=" * 60)
        print("✅ 登録完了！")
        print("=" * 60)
        print(f"site_id: {site_id}")
        print(f"name: {name}")
        print(f"module: {module_path}")
        print(f"url: {url}")

        if schedule:
            print(f"schedule: {schedule}")
            print(f"実行時刻: 午前3時")
        else:
            print(f"schedule: null（手動実行のみ）")

        print(f"\nNetHarvest が定期実行を開始します。")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git 操作エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import os

    if len(sys.argv) < 2:
        print("使用方法:")
        print(f"  python {Path(__file__).name} <python_file>")
        print("\n例:")
        print(f"  python {Path(__file__).name} /path/to/crawler.py")
        sys.exit(1)

    upload_crawler(sys.argv[1])

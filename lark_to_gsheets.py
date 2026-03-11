"""
毎月初・前月営業データ自動転記スクリプト
==================================
Lark Baseの「SKU管理」から以下のデータを抽出し、
Googleスプレッドシートに転記する自動化スクリプトです。
タスクスケジューラ等で毎月1日に実行することを想定しています。

- 営業活動ログ: 前月分のみ抽出して追記
- 案件管理: 全件を毎回洗い替え（最新スナップショット）
"""

import os
import sys
import json
import tempfile
import datetime
from datetime import timezone, timedelta
import httpx
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. 設定情報
# ==========================================
# --- Lark API 設定 ---
# ローカル実行時はハードコード値を使用、GitHub Actions時は環境変数から取得
LARK_APP_ID = os.environ.get("LARK_APP_ID", "cli_a906e1b4f2b89e1a")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET", "NgVNvsbGBrQDhT5AU7y4rgTAeHAdCmkS")
LARK_BASE_URL = "https://open.larksuite.com/open-apis"
LARK_APP_TOKEN = "NQxnbGpgdaqWKIs7wPXjtIXYpqc"  # SKU管理Base
LARK_ACTIVITY_TABLE_ID = "tblUEvTiI9utmCbR"  # 営業活動ログテーブル
LARK_ANKEN_TABLE_ID = "tblLpMc6NXOjWpiK"      # 案件管理テーブル

# --- Google Sheets API 設定 ---
GOOGLE_JSON_KEY = os.environ.get("GOOGLE_JSON_KEY", "spreadsheet-auto-489713-ed7b05684ccc.json")
TARGET_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1aw3rSunxI2plyWMEawaolNYepsK9Z0FQCUpuiy3-pME"

# タイムゾーンと曜日設定
JST = timezone(timedelta(hours=9))
WEEKDAYS_JA = ["月", "火", "水", "木", "金", "土", "日"]

# 取得対象となるカラム（営業活動ログ）
ACTIVITY_COLUMNS = [
    "日報コード", "活動日", "曜日", "担当者", "活動区分", "行動区分",
    "訪問先", "面談者", "内容", "提案SKU", "売れ筋商品情報",
    "人事情報", "次回訪問日", "案件ステータス",
]

# 取得対象となるカラム（案件管理）
ANKEN_COLUMNS = [
    "案件名", "担当者", "ステータス", "得意先", "区分", "優先度",
    "予算（円）", "確度", "ToDo", "課題",
    "実績訪問回数", "目標訪問回数",
]

# ==========================================
# 2. 関数定義 (Lark側)
# ==========================================
def get_lark_token() -> str:
    """Lark APIの認証トークンを取得"""
    resp = httpx.post(
        f"{LARK_BASE_URL}/auth/v3/tenant_access_token/internal",
        json={"app_id": LARK_APP_ID, "app_secret": LARK_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Lark auth failed: {data.get('msg')}")
    return data["tenant_access_token"]

def fetch_all_lark_records(token: str, table_id: str) -> list:
    """ページネーションで指定テーブルの全レコードを取得"""
    headers = {"Authorization": f"Bearer {token}"}
    all_records = []
    page_token = None

    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        resp = httpx.get(
            f"{LARK_BASE_URL}/bitable/v1/apps/{LARK_APP_TOKEN}/tables/{table_id}/records",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"Lark API error: {data.get('msg')}")

        items = data.get("data", {}).get("items", [])
        all_records.extend(items)

        if not data.get("data", {}).get("has_more", False):
            break
        page_token = data["data"]["page_token"]

    return all_records

def ts_to_date_str(ts) -> str:
    """ミリ秒タイムスタンプ → YYYY-MM-DD 文字列"""
    if not ts: return ""
    try:
        dt = datetime.datetime.fromtimestamp(int(ts) / 1000, tz=JST)
        return dt.strftime("%Y-%m-%d")
    except: return ""

def ts_to_date_obj(ts):
    """ミリ秒タイムスタンプ → datetime.date オブジェクト (比較用)"""
    if not ts: return None
    try:
        return datetime.datetime.fromtimestamp(int(ts) / 1000, tz=JST).date()
    except: return None

def ts_to_weekday(ts) -> str:
    """ミリ秒タイムスタンプ → 曜日"""
    if not ts: return ""
    try:
        dt = datetime.datetime.fromtimestamp(int(ts) / 1000, tz=JST)
        return WEEKDAYS_JA[dt.weekday()]
    except: return ""

def extract_text(value) -> str:
    """Larkの各種フィールド値をテキストに変換"""
    if value is None: return ""
    if isinstance(value, str): return value.strip()
    if isinstance(value, (int, float)): return str(value)
    if isinstance(value, list):
        texts = []
        for item in value:
            if isinstance(item, str): texts.append(item)
            elif isinstance(item, dict) and item.get("text"): texts.append(item.get("text"))
        return ", ".join(texts)
    if isinstance(value, dict):
        return value.get("text", "") or str(value)
    return str(value)

def transform_activity_record(fields: dict) -> list:
    """営業活動ログのレコードをリストに変換"""
    activity_ts = fields.get("活動日時")
    row_dict = {
        "日報コード": extract_text(fields.get("日報コード")),
        "活動日": ts_to_date_str(activity_ts),
        "曜日": ts_to_weekday(activity_ts),
        "担当者": extract_text(fields.get("担当者")),
        "活動区分": extract_text(fields.get("活動区分")),
        "行動区分": extract_text(fields.get("行動区分")),
        "訪問先": extract_text(fields.get("訪問先")),
        "面談者": extract_text(fields.get("面談者")),
        "内容": extract_text(fields.get("内容")),
        "提案SKU": extract_text(fields.get("提案SKU")),
        "売れ筋商品情報": extract_text(fields.get("売れ筋商品情報")),
        "人事情報": extract_text(fields.get("人事情報")),
        "次回訪問日": ts_to_date_str(fields.get("次回訪問日")),
        "案件ステータス": extract_text(fields.get("案件ステータス")),
    }
    return [row_dict.get(col, "") for col in ACTIVITY_COLUMNS]

def transform_anken_record(fields: dict) -> list:
    """案件管理のレコードをリストに変換"""
    budget = fields.get("予算（円）")
    row_dict = {
        "案件名": extract_text(fields.get("案件名")),
        "担当者": extract_text(fields.get("担当者")),
        "ステータス": extract_text(fields.get("ステータス")),
        "得意先": extract_text(fields.get("得意先")),
        "区分": extract_text(fields.get("区分")),
        "優先度": extract_text(fields.get("優先度")),
        "予算（円）": str(int(budget)) if budget else "",
        "確度": extract_text(fields.get("確度")),
        "ToDo": extract_text(fields.get("ToDo")),
        "課題": extract_text(fields.get("課題")),
        "実績訪問回数": extract_text(fields.get("実績訪問回数")),
        "目標訪問回数": extract_text(fields.get("目標訪問回数")),
    }
    return [row_dict.get(col, "") for col in ANKEN_COLUMNS]

# ==========================================
# 3. 前月判定のロジック
# ==========================================
def get_previous_month_range():
    """実行時の「前月」の開始日と終了日を取得して返す"""
    today = datetime.date.today()
    # 当月の1日を取得
    first_day_of_this_month = today.replace(day=1)
    # 前月の末日を取得
    last_day_of_last_month = first_day_of_this_month - datetime.timedelta(days=1)
    # 前月の1日を取得
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    return first_day_of_last_month, last_day_of_last_month

# ==========================================
# 4. Google Sheets 連携
# ==========================================
def get_gsheets_client():
    """Google Sheets認証済みクライアントとスプレッドシートを返す"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    # GOOGLE_JSON_KEY がファイルパスならそのまま、JSON文字列なら一時ファイルに書き出す
    if os.path.isfile(GOOGLE_JSON_KEY):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON_KEY, scopes)
    else:
        key_data = json.loads(GOOGLE_JSON_KEY)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(key_data, scopes)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_url(TARGET_SPREADSHEET_URL)
    return sh

def append_activity_to_gsheets(sh, rows: list):
    """営業活動ログをスプレッドシートに追記する"""
    print("  [営業活動ログ] 書き込み中...")
    sheet_name = "営業活動ログ"
    try:
        worksheet = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"    シート '{sheet_name}' を新規作成し、ヘッダー行を追加します。")
        worksheet = sh.add_worksheet(title=sheet_name, rows="100", cols="20")
        worksheet.append_row(ACTIVITY_COLUMNS)

    if len(rows) > 0:
        worksheet.append_rows(rows)
        print(f"    [OK] {len(rows)}件を追記しました。")
    else:
        print("    [INFO] 追記するデータがありませんでした。")

def write_anken_to_gsheets(sh, rows: list):
    """案件管理をスプレッドシートに洗い替えで書き込む"""
    print("  [案件管理] 書き込み中...")
    sheet_name = "案件管理"
    try:
        worksheet = sh.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        print(f"    シート '{sheet_name}' を新規作成します。")
        worksheet = sh.add_worksheet(title=sheet_name, rows="200", cols="15")

    worksheet.append_row(ANKEN_COLUMNS)
    if len(rows) > 0:
        worksheet.append_rows(rows)
        print(f"    [OK] {len(rows)}件を書き込みました。")
    else:
        print("    [INFO] 書き込むデータがありませんでした。")

# ==========================================
# 5. メイン処理
# ==========================================
def main():
    print("--- 毎月初・前月営業データ自動転記 処理開始 ---")

    # 前月の期間を計算
    start_date, end_date = get_previous_month_range()
    print(f"対象期間: {start_date} 〜 {end_date}")

    # 1. Lark認証
    print("\n[1/4] Larkへの認証を実行中...")
    try:
        lark_token = get_lark_token()
    except Exception as e:
        print(f"[Error] Larkの認証に失敗: {e}")
        return

    # 2. 営業活動ログ取得＋前月フィルタ
    print("\n[2/4] 営業活動ログを取得し、前月分を抽出中...")
    activity_records = fetch_all_lark_records(lark_token, LARK_ACTIVITY_TABLE_ID)
    activity_rows = []
    for rec in activity_records:
        fields = rec.get("fields", {})
        activity_ts = fields.get("活動日時")
        record_date = ts_to_date_obj(activity_ts)
        if record_date and start_date <= record_date <= end_date:
            activity_rows.append(transform_activity_record(fields))
    activity_rows.sort(key=lambda row: row[1])
    print(f"  抽出完了: 前月分 {len(activity_rows)} 件")

    # 3. 案件管理取得（全件）
    print("\n[3/4] 案件管理を取得中...")
    anken_records = fetch_all_lark_records(lark_token, LARK_ANKEN_TABLE_ID)
    anken_rows = [transform_anken_record(rec.get("fields", {})) for rec in anken_records]
    print(f"  取得完了: {len(anken_rows)} 件")

    # 4. Google Sheetsへ書き込み
    print("\n[4/4] Google Sheetsへの書き込みを開始します...")
    try:
        sh = get_gsheets_client()
        append_activity_to_gsheets(sh, activity_rows)
        write_anken_to_gsheets(sh, anken_rows)
    except Exception as e:
        print(f"[Error] スプレッドシートの書き込みに失敗: {e}")
        raise e

    print("\n--- 処理がすべて完了しました ---")

if __name__ == "__main__":
    main()

"""
한국투자증권 코스피/코스닥 전일 지수를 조회하여 Google Spreadsheet에 기록
매일 오전 8시(KST) GitHub Actions에서 실행
컬럼: 일자 | 코스피-거래대금 | 코스닥-거래대금 | 코스피-종가 | 코스닥-종가
"""

import os
import json
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import gspread
from google.oauth2.service_account import Credentials

from kis_stock_price import get_access_token

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_daily_index_chart(
    app_key: str,
    app_secret: str,
    access_token: str,
    index_code: str,
    start_date: str,
    end_date: str,
    is_mock: bool = False,
) -> list:
    """
    업종 기간별 시세 조회 (일봉)
    tr_id: FHKUP03500100
    """
    if is_mock:
        base_url = "https://openapivts.koreainvestment.com:29443"
    else:
        base_url = "https://openapi.koreainvestment.com:9443"

    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKUP03500100",
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": index_code,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": "D",
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json().get("output2", [])


def get_gspread_client() -> gspread.Client:
    """Google Sheets 클라이언트 생성"""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise Exception("GOOGLE_CREDENTIALS_JSON 환경변수가 설정되지 않았습니다.")

    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def main():
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    is_mock = os.environ.get("KIS_IS_MOCK", "false").lower() == "true"
    spreadsheet_id = os.environ.get("GOOGLE_SPREADSHEET_ID")

    if not app_key or not app_secret:
        print("오류: KIS_APP_KEY, KIS_APP_SECRET 환경변수를 설정하세요.")
        return

    # 전일까지 최근 7일 범위로 조회 (주말/공휴일 대비, 오늘 제외)
    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.strftime("%Y%m%d")
    start_date = (yesterday - timedelta(days=6)).strftime("%Y%m%d")

    print("=== 코스피/코스닥 전일 지수 → Google Sheets 업데이트 ===")
    print(f"조회 범위: {start_date} ~ {end_date}")
    print()

    # 1. 토큰 발급
    print("토큰 발급 중...")
    access_token = get_access_token(app_key, app_secret, is_mock)
    print("토큰 발급 완료")

    # 2. 코스피 일별 데이터 조회
    print("코스피 조회 중...")
    kospi_data = get_daily_index_chart(
        app_key, app_secret, access_token, "0001", start_date, end_date, is_mock
    )

    time.sleep(0.5)

    # 3. 코스닥 일별 데이터 조회
    print("코스닥 조회 중...")
    kosdaq_data = get_daily_index_chart(
        app_key, app_secret, access_token, "1001", start_date, end_date, is_mock
    )

    # 4. 날짜별 데이터 정리
    kospi_by_date = {}
    for item in kospi_data:
        date = item.get("stck_bsop_date", "")
        if date:
            kospi_by_date[date] = {
                "거래대금": int(item.get("acml_tr_pbmn", 0)),
                "종가": float(item.get("bstp_nmix_prpr", 0)),
            }

    kosdaq_by_date = {}
    for item in kosdaq_data:
        date = item.get("stck_bsop_date", "")
        if date:
            kosdaq_by_date[date] = {
                "거래대금": int(item.get("acml_tr_pbmn", 0)),
                "종가": float(item.get("bstp_nmix_prpr", 0)),
            }

    all_dates = sorted(set(kospi_by_date.keys()) | set(kosdaq_by_date.keys()))

    if not all_dates:
        print("조회된 거래일 데이터가 없습니다. (공휴일/주말)")
        return

    # 5. 스프레드시트에서 기존 날짜 확인 후 신규분만 추가
    print("스프레드시트 확인 중...")
    client = get_gspread_client()
    sheet = client.open_by_key(spreadsheet_id).sheet1
    existing_dates = set(sheet.col_values(1))  # A열 기존 날짜들

    new_rows = []
    for date in all_dates:
        formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        if formatted_date in existing_dates:
            continue
        kospi = kospi_by_date.get(date, {"거래대금": 0, "종가": 0})
        kosdaq = kosdaq_by_date.get(date, {"거래대금": 0, "종가": 0})
        new_rows.append([
            formatted_date,
            kospi["거래대금"],
            kosdaq["거래대금"],
            kospi["종가"],
            kosdaq["종가"],
        ])

    if not new_rows:
        print("추가할 신규 데이터가 없습니다. (이미 최신)")
        return

    # 기존 데이터 끝 다음 행부터 추가
    next_row = len(sheet.col_values(1)) + 1
    end_row = next_row + len(new_rows) - 1
    sheet.update(values=new_rows, range_name=f"A{next_row}:E{end_row}")

    print(f"\n{len(new_rows)}일치 신규 데이터 추가 완료:")
    for row in new_rows:
        print(f"  {row[0]} | 코스피 {row[3]:,.2f} ({row[1]:,}백만) | 코스닥 {row[4]:,.2f} ({row[2]:,}백만)")


if __name__ == "__main__":
    main()

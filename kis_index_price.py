"""
한국투자증권 Open API - 코스피/코스닥 지수 조회
업종 현재지수, 거래량, 거래대금을 조회합니다.
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from kis_stock_price import get_access_token

load_dotenv()


# 주요 지수 코드
INDEX_CODES = {
    "0001": "코스피",
    "1001": "코스닥",
}


def get_index_price(
    app_key: str,
    app_secret: str,
    access_token: str,
    index_code: str,
    is_mock: bool = False,
) -> dict:
    """
    업종(지수) 현재가 조회
    https://apiportal.koreainvestment.com/apiservice/domestic-stock-quotations
    tr_id: FHPUP02100000 (업종현재지수)
    """
    if is_mock:
        base_url = "https://openapivts.koreainvestment.com:29443"
    else:
        base_url = "https://openapi.koreainvestment.com:9443"

    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-index-price"

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHPUP02100000",
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": "U",  # U: 업종(지수)
        "FID_INPUT_ISCD": index_code,     # 0001: 코스피, 1001: 코스닥
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()


def format_index_info(data: dict, index_code: str) -> str:
    """지수 조회 결과를 보기 좋게 포맷팅"""
    output = data.get("output", {})
    index_name = INDEX_CODES.get(index_code, index_code)

    if not output:
        return f"[{index_name}] 데이터 조회 실패"

    # 지수 값
    current_index = float(output.get("bstp_nmix_prpr", 0))
    # 전일대비
    change = float(output.get("bstp_nmix_prdy_vrss", 0))
    change_rate = output.get("bstp_nmix_prdy_ctrt", "N/A")
    # 거래량
    volume = int(output.get("acml_vol", 0))
    # 거래대금 (단위: 백만원)
    trading_value = int(output.get("acml_tr_pbmn", 0))
    # 시가/고가/저가
    open_index = float(output.get("bstp_nmix_oprc", 0))
    high_index = float(output.get("bstp_nmix_hgpr", 0))
    low_index = float(output.get("bstp_nmix_lwpr", 0))

    # 상승/하락 부호
    sign = "+" if change > 0 else ""

    result = f"""
{'='*50}
{index_name} 지수
조회시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}
현재지수:   {current_index:,.2f}
전일대비:   {sign}{change:,.2f} ({sign}{change_rate}%)
시가:       {open_index:,.2f}
고가:       {high_index:,.2f}
저가:       {low_index:,.2f}
거래량:     {volume:,}주
거래대금:   {trading_value:,}백만원
{'='*50}
"""
    return result


def main():
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    is_mock = os.environ.get("KIS_IS_MOCK", "false").lower() == "true"

    if not app_key or not app_secret:
        print("오류: KIS_APP_KEY, KIS_APP_SECRET 환경변수를 설정하세요.")
        return

    print("한국투자증권 코스피/코스닥 지수 조회")
    print(f"모드: {'모의투자' if is_mock else '실전투자'}")
    print()

    try:
        print("토큰 발급 중...")
        access_token = get_access_token(app_key, app_secret, is_mock)
        print("토큰 발급 완료")
        print()

        for index_code, index_name in INDEX_CODES.items():
            try:
                data = get_index_price(
                    app_key, app_secret, access_token, index_code, is_mock
                )
                print(format_index_info(data, index_code))
            except Exception as e:
                print(f"[{index_name}] 조회 실패: {e}")

    except Exception as e:
        print(f"오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()

"""
한국투자증권 Open API - 주식 현재가 조회
GitHub Actions에서 실행하도록 설계됨
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_access_token(app_key: str, app_secret: str, is_mock: bool = False) -> str:
    """
    접근 토큰 발급 (OAuth 2.0)
    https://apiportal.koreainvestment.com/apiservice/oauth2/tokenP
    """
    if is_mock:
        base_url = "https://openapivts.koreainvestment.com:29443"  # 모의투자
    else:
        base_url = "https://openapi.koreainvestment.com:9443"  # 실전투자

    url = f"{base_url}/oauth2/tokenP"

    headers = {
        "content-type": "application/json"
    }

    body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    data = response.json()

    if "access_token" not in data:
        raise Exception(f"토큰 발급 실패: {data}")

    return data["access_token"]


def get_stock_price(
    app_key: str,
    app_secret: str,
    access_token: str,
    stock_code: str,
    is_mock: bool = False
) -> dict:
    """
    주식 현재가 조회
    https://apiportal.koreainvestment.com/apiservice/domestic-stock-quotations
    """
    if is_mock:
        base_url = "https://openapivts.koreainvestment.com:29443"
    else:
        base_url = "https://openapi.koreainvestment.com:9443"

    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price"

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": "FHKST01010100"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",  # J: 주식, ETF, ETN
        "FID_INPUT_ISCD": stock_code     # 종목코드 (ex: 005930)
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()


def format_price_info(data: dict, stock_code: str) -> str:
    """조회 결과를 보기 좋게 포맷팅"""
    output = data.get("output", {})

    if not output:
        return f"[{stock_code}] 데이터 조회 실패"

    result = f"""
{'='*50}
종목코드: {stock_code}
조회시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}
현재가:    {int(output.get('stck_prpr', 0)):,}원
전일대비:  {output.get('prdy_vrss', 'N/A')}원 ({output.get('prdy_ctrt', 'N/A')}%)
시가:      {int(output.get('stck_oprc', 0)):,}원
고가:      {int(output.get('stck_hgpr', 0)):,}원
저가:      {int(output.get('stck_lwpr', 0)):,}원
거래량:    {int(output.get('acml_vol', 0)):,}주
{'='*50}
"""
    return result


def main():
    # 환경변수에서 설정 읽기
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    stock_codes = os.environ.get("KIS_STOCK_CODES", "005930")  # 기본값: 삼성전자
    is_mock = os.environ.get("KIS_IS_MOCK", "false").lower() == "true"

    if not app_key or not app_secret:
        print("오류: KIS_APP_KEY, KIS_APP_SECRET 환경변수를 설정하세요.")
        print("GitHub Actions: Repository Settings > Secrets에 등록")
        print("로컬: .env 파일에 설정")
        return

    # 여러 종목 조회 지원 (쉼표로 구분)
    codes = [code.strip() for code in stock_codes.split(",")]

    print(f"한국투자증권 주식 시세 조회")
    print(f"모드: {'모의투자' if is_mock else '실전투자'}")
    print(f"조회 종목: {codes}")
    print()

    try:
        # 1. 토큰 발급
        print("토큰 발급 중...")
        access_token = get_access_token(app_key, app_secret, is_mock)
        print("토큰 발급 완료")
        print()

        # 2. 각 종목 시세 조회
        for stock_code in codes:
            try:
                data = get_stock_price(
                    app_key, app_secret, access_token, stock_code, is_mock
                )
                print(format_price_info(data, stock_code))
            except Exception as e:
                print(f"[{stock_code}] 조회 실패: {e}")

    except Exception as e:
        print(f"오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()

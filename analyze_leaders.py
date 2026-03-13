"""
국내 대장주 분석 -> JSON 데이터 축적 -> GitHub Pages 대시보드.

매 평일 장 마감 후 GitHub Actions에서 실행.
수집 흐름:
1. 등락률 순위 API -> 상위 N개 선정
2. 종목별 상세 시세 조회 (시총, 거래대금, 거래량증가율, 상한가)
3. 상한가 종목만 분봉 API로 최초 도달시간 조회
4. Gemini API로 테마/섹터 분류
5. 대장주 점수 산출 -> data/ 폴더에 JSON 축적
"""

import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

load_dotenv()

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")

# ── API 상수 ──────────────────────────────────────────────────────────────────

FLUCTUATION_API = "/uapi/domestic-stock/v1/ranking/fluctuation"
FLUCTUATION_TR_ID = "FHPST01700000"

INQUIRE_PRICE_API = "/uapi/domestic-stock/v1/quotations/inquire-price"
INQUIRE_PRICE_TR_ID = "FHKST01010100"

MINUTE_CHART_API = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
MINUTE_CHART_TR_ID = "FHKST03010200"

DEF_FLUCT_PARAMS = {
    "fid_cond_mrkt_div_code": "J",
    "fid_cond_scr_div_code": "20170",
    "fid_input_iscd": "0000",
    "fid_rank_sort_cls_code": "0",
    "fid_input_cnt_1": "0",
    "fid_prc_cls_code": "0",
    "fid_input_price_1": "",
    "fid_input_price_2": "",
    "fid_vol_cnt": "",
    "fid_trgt_cls_code": "0",
    "fid_trgt_exls_cls_code": "0",
    "fid_div_cls_code": "0",
    "fid_rsfl_rate1": "",
    "fid_rsfl_rate2": "",
}

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"


# ── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def _base_url(is_mock: bool = False) -> str:
    if is_mock:
        return "https://openapivts.koreainvestment.com:29443"
    return "https://openapi.koreainvestment.com:9443"


def _get_access_token(app_key: str, app_secret: str, is_mock: bool = False) -> str:
    """OAuth 토큰 발급."""
    url = f"{_base_url(is_mock)}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}
    resp = requests.post(url, headers={"content-type": "application/json"}, json=body)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise Exception(f"토큰 발급 실패: {data}")
    return data["access_token"]


def get_or_refresh_token(app_key: str, app_secret: str, is_mock: bool = False) -> str:
    """token.json 캐시 확인 후 재사용 또는 새로 발급."""
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            expiry = datetime.fromisoformat(data["expiry"])
            if datetime.now() < expiry - timedelta(hours=1):
                print(f"캐시된 토큰 재사용 (만료: {data['expiry']})")
                return data["access_token"]
    except Exception:
        pass
    print("새 토큰 발급 중...")
    access_token = _get_access_token(app_key, app_secret, is_mock)
    expiry = datetime.now() + timedelta(hours=24)
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token, "expiry": expiry.isoformat()}, f)
    print(f"토큰 저장 완료 (만료: {expiry.isoformat()})")
    return access_token


def _kis_headers(
    access_token: str,
    app_key: str,
    app_secret: str,
    tr_id: str,
    tr_cont: str = "",
) -> dict:
    h = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
    }
    if tr_cont:
        h["tr_cont"] = tr_cont
    return h


# ── 1. 등락률 순위 ────────────────────────────────────────────────────────────

def fetch_fluctuation_top(
    access_token: str,
    app_key: str,
    app_secret: str,
    is_mock: bool = False,
    min_advance: float = 10.0,
    top_n: int = 100,
) -> list[dict]:
    """등락률 순위 조회. fid_input_cnt_1 offset으로 30개씩 페이징."""
    url = f"{_base_url(is_mock)}{FLUCTUATION_API}"
    all_rows: list[dict] = []
    seen_codes: set[str] = set()

    for offset in range(0, 300, 30):  # 최대 300위까지
        params = dict(DEF_FLUCT_PARAMS)
        params["fid_input_cnt_1"] = str(offset)

        headers = _kis_headers(access_token, app_key, app_secret, FLUCTUATION_TR_ID)
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        chunk = data.get("output", [])
        if not chunk:
            break

        # 마지막 종목의 등락률이 min_advance 미만이면 이 페이지까지만
        last_rate = 0.0
        new_count = 0
        for row in chunk:
            code = row.get("stck_shrn_iscd", "")
            if code in seen_codes:
                continue
            seen_codes.add(code)
            try:
                rate = float(row.get("prdy_ctrt", 0))
            except (ValueError, TypeError):
                rate = 0.0
            row["_rate"] = rate
            all_rows.append(row)
            last_rate = rate
            new_count += 1

        print(f"    offset={offset}: {new_count}개 (last {last_rate:+.2f}%)")

        if last_rate < min_advance:
            break
        time.sleep(0.15)

    result = [r for r in all_rows if r["_rate"] >= min_advance]
    result.sort(key=lambda r: r["_rate"], reverse=True)
    return result[:top_n]


# ── 2. 종목 상세 시세 ─────────────────────────────────────────────────────────

def fetch_stock_detail(
    access_token: str,
    app_key: str,
    app_secret: str,
    stock_code: str,
    is_mock: bool = False,
) -> dict:
    """개별 종목 현재가 시세."""
    url = f"{_base_url(is_mock)}{INQUIRE_PRICE_API}"
    headers = _kis_headers(access_token, app_key, app_secret, INQUIRE_PRICE_TR_ID)
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    out = resp.json().get("output", {})

    if not out:
        return {
            "시가총액_억": 0, "거래대금_백만": 0, "거래량증가율": 0.0,
            "상한가": "", "현재가": "",
        }

    acml_vol = int(out.get("acml_vol") or 0)
    prdy_vol = int(out.get("prdy_vol") or 0)
    vol_rate = round((acml_vol / prdy_vol - 1) * 100, 1) if prdy_vol > 0 else 0.0

    return {
        "시가총액_억": int(out.get("hts_avls") or 0),
        "거래대금_백만": int(out.get("acml_tr_pbmn") or 0) // 1_000_000,
        "거래량증가율": vol_rate,
        "상한가": out.get("stck_mxpr", ""),
        "현재가": out.get("stck_prpr", ""),
    }


# ── 3. 상한가 최초 도달시간 (분봉 스캔) ───────────────────────────────────────

def fetch_upper_limit_time(
    access_token: str,
    app_key: str,
    app_secret: str,
    stock_code: str,
    upper_limit_price: str,
    is_mock: bool = False,
) -> str:
    """분봉 API로 당일 고가 == 상한가인 최초 시간 탐색."""
    url = f"{_base_url(is_mock)}{MINUTE_CHART_API}"
    first_hit = None
    prev_hour = "160000"

    for _ in range(20):
        if prev_hour <= "090000":
            break

        headers = _kis_headers(access_token, app_key, app_secret, MINUTE_CHART_TR_ID)
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": prev_hour,
            "FID_PW_DATA_INCU_YN": "Y",
        }

        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        candles = resp.json().get("output2", [])

        if not candles:
            break

        new_last = candles[-1].get("stck_cntg_hour", "")
        for c in candles:
            if c.get("stck_hgpr", "") == upper_limit_price:
                first_hit = c.get("stck_cntg_hour", "")

        if new_last >= prev_hour:
            break
        prev_hour = new_last
        time.sleep(0.1)

    if first_hit and len(first_hit) >= 6:
        return f"{first_hit[:2]}:{first_hit[2:4]}:{first_hit[4:6]}"
    return "-"


# ── 4. Gemini 테마 분류 ───────────────────────────────────────────────────────

def classify_themes(stocks: list[dict], api_key: str) -> dict[str, list[str]]:
    """Gemini API로 종목들을 테마/섹터별로 분류. {테마명: [종목코드, ...]} 반환."""
    stock_list = "\n".join(
        f"- {s['종목코드']} {s['종목명']}" for s in stocks
    )

    prompt = f"""아래는 오늘 한국 주식시장에서 급등한 종목 리스트입니다.
이 종목들을 테마/섹터별로 분류해주세요.

규칙:
- 하나의 종목이 여러 테마에 속할 수 있습니다
- 테마명은 간결하게 (예: "AI", "2차전지", "반도체", "바이오", "방산", "로봇")
- 관련성이 명확한 것만 분류하고, 애매하면 "기타"로
- 반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트 없이 JSON만 출력하세요.

응답 형식:
{{"테마명": ["종목코드1", "종목코드2"], "테마명2": ["종목코드3"]}}

종목 리스트:
{stock_list}"""

    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1},
    }

    resp = requests.post(
        f"{GEMINI_API_URL}?key={api_key}",
        headers={"Content-Type": "application/json"},
        json=body,
    )
    resp.raise_for_status()

    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    # Gemini가 ```json ... ``` 으로 감쌀 수 있으므로 정리
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]  # 첫 줄 제거
    if text.endswith("```"):
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    return json.loads(text)


# ── 5. 점수 산출 ──────────────────────────────────────────────────────────────

def score_leader(stock: dict) -> float:
    """
    대장주 점수 (등락률 기반):
      - 등락률: 기본 점수 (등락률 값 그대로)
      - 거래대금 순위 역수: max 30점
      - 상한가 보너스: 도달 시 +20점, 09:30 이전 +10점 추가
    """
    score = 0.0

    rate = stock.get("등락률", 0.0)
    score += rate

    rank_t = stock.get("거래대금_순위", 0)
    if rank_t and rank_t > 0:
        score += 30.0 / rank_t

    up_time = stock.get("상한가시간", "-")
    if up_time != "-":
        score += 20.0
        try:
            hh, mm = int(up_time[:2]), int(up_time[3:5])
            if hh < 9 or (hh == 9 and mm <= 30):
                score += 10.0
        except (ValueError, IndexError):
            pass

    return round(score, 2)


# ── 6. 데이터 축적 ───────────────────────────────────────────────────────────

def _load_json(path: str, default=None):
    if default is None:
        default = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def accumulate_data(stocks: list[dict], theme_map: dict, date_str: str):
    """일별 데이터 저장 + stocks.json, themes.json 마스터 업데이트."""
    os.makedirs(DAILY_DIR, exist_ok=True)

    # 종목별 테마 역매핑
    stock_themes: dict[str, list[str]] = {}
    for theme, codes in theme_map.items():
        for code in codes:
            stock_themes.setdefault(code, [])
            if theme not in stock_themes[code]:
                stock_themes[code].append(theme)

    # 1) 일별 데이터 저장
    daily_data = []
    for s in stocks:
        daily_data.append({
            "종목코드": s["종목코드"],
            "종목명": s["종목명"],
            "등락률": s["등락률"],
            "거래대금_백만": s.get("거래대금_백만", 0),
            "시가총액_억": s.get("시가총액_억", 0),
            "거래량증가율": s.get("거래량증가율", 0.0),
            "상한가시간": s.get("상한가시간", "-"),
            "대장주_점수": s.get("대장주_점수", 0),
            "테마": stock_themes.get(s["종목코드"], []),
        })

    daily_path = os.path.join(DAILY_DIR, f"{date_str}.json")
    _save_json(daily_path, daily_data)
    print(f"  일별 데이터: {daily_path}")

    # 2) stocks.json 마스터 업데이트
    stocks_path = os.path.join(DATA_DIR, "stocks.json")
    stocks_master = _load_json(stocks_path)

    for s in stocks:
        code = s["종목코드"]
        if code not in stocks_master:
            stocks_master[code] = {
                "종목명": s["종목명"],
                "테마": [],
                "상승일": [],
            }
        entry = stocks_master[code]
        entry["종목명"] = s["종목명"]
        # 테마 병합
        for t in stock_themes.get(code, []):
            if t not in entry["테마"]:
                entry["테마"].append(t)
        # 상승일 추가
        if date_str not in entry["상승일"]:
            entry["상승일"].append(date_str)
            entry["상승일"] = entry["상승일"][-90:]  # 최근 90일만 유지

    _save_json(stocks_path, stocks_master)
    print(f"  종목 마스터: {stocks_path} ({len(stocks_master)}개)")

    # 3) themes.json 마스터 업데이트
    themes_path = os.path.join(DATA_DIR, "themes.json")
    themes_master = _load_json(themes_path)

    for theme, codes in theme_map.items():
        if theme not in themes_master:
            themes_master[theme] = {
                "종목": [],
                "활성일": [],
            }
        entry = themes_master[theme]
        # 종목 병합
        for code in codes:
            if code not in entry["종목"]:
                entry["종목"].append(code)
        # 활성일 추가
        if date_str not in entry["활성일"]:
            entry["활성일"].append(date_str)
            entry["활성일"] = entry["활성일"][-90:]

    _save_json(themes_path, themes_master)
    print(f"  테마 마스터: {themes_path} ({len(themes_master)}개 테마)")

    return daily_path


# ── 7. Google Drive 업로드 ────────────────────────────────────────────────────

def upload_to_drive(file_path: str, folder_id: str, creds_json: str) -> str:
    """파일을 Google Drive 폴더에 업로드."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    scopes = ["https://www.googleapis.com/auth/drive.file"]
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    filename = os.path.basename(file_path)
    file_metadata = {"name": filename, "parents": [folder_id]}

    if file_path.endswith(".json"):
        mime = "application/json"
    else:
        mime = "application/octet-stream"

    media = MediaFileUpload(file_path, mimetype=mime, resumable=True)
    result = service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    file_id = result.get("id", "")
    print(f"  Drive 업로드 완료: {filename} (ID: {file_id})")
    return file_id


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    is_mock = os.environ.get("KIS_IS_MOCK", "false").lower() == "true"
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

    if not app_key or not app_secret:
        print("오류: KIS_APP_KEY, KIS_APP_SECRET 환경변수를 설정하세요.")
        return

    date_str = datetime.now().strftime("%Y-%m-%d")

    print(f"=== 대장주 분석 ({date_str}) ===\n")

    # 1. 토큰
    print("[1/6] 토큰 발급...")
    access_token = get_or_refresh_token(app_key, app_secret, is_mock)

    # 2. 등락률 순위
    print("[2/6] 등락률 순위 조회 (상위 50)...")
    fluct = fetch_fluctuation_top(access_token, app_key, app_secret, is_mock)

    if not fluct:
        print("조건을 만족하는 종목이 없습니다. (공휴일/장 미개장)")
        return

    codes_info = []
    for row in fluct:
        code = str(row.get("stck_shrn_iscd", "")).strip()
        if code and len(code) >= 5:
            codes_info.append({
                "종목코드": code,
                "종목명": str(row.get("hts_kor_isnm", "")).strip(),
                "등락률": row["_rate"],
            })
    print(f"    -> {len(codes_info)}개 종목 선정")

    # 3. 종목별 상세 시세
    print(f"[3/6] 종목별 시세 조회...")
    for item in codes_info:
        detail = fetch_stock_detail(access_token, app_key, app_secret, item["종목코드"], is_mock)
        item.update(detail)
        time.sleep(0.1)

    # 4. 상한가 종목 분봉 스캔
    upper_candidates = [
        s for s in codes_info
        if s.get("현재가") and s.get("상한가") and s["현재가"] == s["상한가"]
    ]
    print(f"[4/6] 상한가 종목 도달시간 조회 ({len(upper_candidates)}개)...")
    for s in upper_candidates:
        hit_time = fetch_upper_limit_time(
            access_token, app_key, app_secret,
            s["종목코드"], s["상한가"], is_mock,
        )
        s["상한가시간"] = hit_time
        print(f"    {s['종목코드']} {s['종목명']} -> {hit_time}")
        time.sleep(0.1)

    for s in codes_info:
        if "상한가시간" not in s:
            s["상한가시간"] = "-"

    # 5. Gemini 테마 분류
    theme_map = {}
    if gemini_key:
        print("[5/6] Gemini 테마 분류...")
        try:
            theme_map = classify_themes(codes_info, gemini_key)
            for theme, codes in theme_map.items():
                print(f"    {theme}: {len(codes)}개")
        except Exception as e:
            print(f"    테마 분류 실패 (계속 진행): {e}")
    else:
        print("[5/6] GEMINI_API_KEY 미설정 - 테마 분류 생략")

    # 6. 점수 산출 & 정렬
    print("[6/6] 점수 산출...")

    sorted_by_vol = sorted(codes_info, key=lambda x: x.get("거래대금_백만", 0), reverse=True)
    for i, s in enumerate(sorted_by_vol, 1):
        s["거래대금_순위"] = i

    for s in codes_info:
        s["대장주_점수"] = score_leader(s)

    codes_info.sort(key=lambda x: x["대장주_점수"], reverse=True)

    # 결과 출력
    print(f"\n>> 대장주 {len(codes_info)}건 분석 완료")
    for s in codes_info[:10]:
        upper = " [상한가]" if s["상한가시간"] != "-" else ""
        print(f"  {s['종목코드']} {s['종목명']:<12s} {s['등락률']:>+7.2f}%{upper}  점수:{s['대장주_점수']:.1f}")

    # 데이터 축적
    print("\n>> 데이터 축적 중...")
    daily_path = accumulate_data(codes_info, theme_map, date_str)

    # Google Drive 업로드
    if creds_json and folder_id:
        print("\n>> Google Drive 업로드 중...")
        upload_to_drive(daily_path, folder_id, creds_json)
    else:
        print("\n>> Google Drive 환경변수 미설정 - 업로드 생략")


if __name__ == "__main__":
    main()

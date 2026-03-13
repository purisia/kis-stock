"""
국내 대장주 분석 -> JSON 데이터 축적 -> GitHub Pages 대시보드.

매 평일 장 마감 후 GitHub Actions에서 실행.
수집 흐름:
1. FinanceDataReader로 당일 전체 종목 시세 수집 (필수 - 실패 시 스크립트 중단)
2. 상한가 종목만 KIS 분봉 API로 도달시간 조회 (선택 - 실패해도 계속)
3. Gemini API로 테마/섹터 분류 (선택 - 실패해도 계속)
4. 대장주 점수 산출 -> docs/data/ 폴더에 JSON 축적
"""

import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

load_dotenv()

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.json")
DATA_DIR = os.path.join(os.path.dirname(__file__), "docs", "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")

# ── API 상수 ──────────────────────────────────────────────────────────────────

MINUTE_CHART_API = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
MINUTE_CHART_TR_ID = "FHKST03010200"

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite-preview:generateContent"


# ── 1. FinanceDataReader 주가 수집 (필수) ────────────────────────────────────

def fetch_rising_stocks(min_rate: float = 10.0) -> list[dict]:
    """FinanceDataReader로 당일 등락률 10%+ 종목 수집."""
    import FinanceDataReader as fdr
    import pandas as pd

    kospi = fdr.StockListing("KOSPI")
    kosdaq = fdr.StockListing("KOSDAQ")
    all_stocks = pd.concat([kospi, kosdaq], ignore_index=True)

    rising = all_stocks[all_stocks["ChagesRatio"] >= min_rate].copy()
    rising = rising.sort_values("ChagesRatio", ascending=False).reset_index(drop=True)

    results = []
    for _, row in rising.iterrows():
        # 상한가 판별: 등락률 29.5%+ & 고가 == 종가
        is_upper = row["ChagesRatio"] >= 29.5 and row["High"] == row["Close"]
        results.append({
            "종목코드": row["Code"],
            "종목명": row["Name"],
            "시가": int(row["Open"]),
            "고가": int(row["High"]),
            "종가": int(row["Close"]),
            "등락률": round(row["ChagesRatio"], 2),
            "거래대금_백만": int(row["Amount"] / 1e6),
            "시가총액_억": int(row["Marcap"] / 1e8),
            "거래량증가율": 0.0,
            "상한가시간": "-",
            "_is_upper": is_upper,
            "_close": int(row["Close"]),
        })

    return results


# ── 2. KIS 상한가 도달시간 (선택) ────────────────────────────────────────────

def _base_url(is_mock: bool = False) -> str:
    if is_mock:
        return "https://openapivts.koreainvestment.com:29443"
    return "https://openapi.koreainvestment.com:9443"


def _get_access_token(app_key: str, app_secret: str, is_mock: bool = False) -> str:
    url = f"{_base_url(is_mock)}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret}
    resp = requests.post(url, headers={"content-type": "application/json"}, json=body)
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise Exception(f"토큰 발급 실패: {data}")
    return data["access_token"]


def get_or_refresh_token(app_key: str, app_secret: str, is_mock: bool = False) -> str:
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            expiry = datetime.fromisoformat(data["expiry"])
            if datetime.now() < expiry - timedelta(hours=1):
                print(f"    캐시된 토큰 재사용 (만료: {data['expiry']})")
                return data["access_token"]
    except Exception:
        pass
    print("    새 토큰 발급 중...")
    access_token = _get_access_token(app_key, app_secret, is_mock)
    expiry = datetime.now() + timedelta(hours=24)
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": access_token, "expiry": expiry.isoformat()}, f)
    return access_token


def _kis_headers(access_token: str, app_key: str, app_secret: str, tr_id: str) -> dict:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
    }


def fetch_upper_limit_time(
    access_token: str, app_key: str, app_secret: str,
    stock_code: str, upper_limit_price: str, is_mock: bool = False,
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


def enrich_upper_limit_times(stocks: list[dict], app_key: str, app_secret: str, is_mock: bool):
    """상한가 종목들의 도달시간을 KIS API로 채움."""
    upper_candidates = [s for s in stocks if s.get("_is_upper")]
    if not upper_candidates:
        return

    access_token = get_or_refresh_token(app_key, app_secret, is_mock)

    for s in upper_candidates:
        hit_time = fetch_upper_limit_time(
            access_token, app_key, app_secret,
            s["종목코드"], str(s["_close"]), is_mock,
        )
        s["상한가시간"] = hit_time
        print(f"    {s['종목코드']} {s['종목명']} -> {hit_time}")
        time.sleep(0.15)


# ── 3. Gemini 테마 분류 (선택) ───────────────────────────────────────────────

def _gemini_parse_json(resp_json: dict) -> dict:
    parts = resp_json["candidates"][0]["content"]["parts"]
    text = ""
    for part in parts:
        if "text" in part:
            text += part["text"]
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
    if text.endswith("```"):
        text = text.rsplit("```", 1)[0]
    return json.loads(text.strip())


def classify_themes(stocks: list[dict], api_key: str, existing_themes: list[str] = None) -> tuple[dict[str, list[str]], dict[str, str]]:
    """Gemini API로 종목 테마 분류.
    2단계: 1) 웹검색으로 급등사유 파악 2) 테마 통합 분류.
    existing_themes: 기존 themes.json의 테마명 목록 (일관성 유지용)
    반환: (테마맵, 종목별 분류사유 dict)
    """
    url = f"{GEMINI_API_URL}?key={api_key}"
    headers = {"Content-Type": "application/json"}

    # 1단계: 웹 검색으로 각 종목 급등 사유 파악 (배치)
    batch_size = 15
    stock_reasons: dict[str, str] = {}

    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i + batch_size]
        stock_list = "\n".join(f"- {s['종목코드']} {s['종목명']}" for s in batch)

        prompt = f"""아래 한국 주식 종목들이 오늘 급등했습니다.
각 종목을 웹 검색해서 다음을 조사해주세요:

1. 실제 사업내용 (주력 제품/서비스)
2. 최근 3개월 뉴스/기사 기반 급등 사유
3. 밸류체인 연결: 이 회사의 제품이 어떤 산업에 쓰이는지 (예: 절삭공구 → 반도체/AI 장비 가공, 전선 → 전력기기)

종목명만으로 추측하지 말고 반드시 최근 기사를 검색하세요.
한국경제, 매일경제, 이데일리 등 한국 경제 언론사 기사와 DART 공시를 중심으로 조사하세요.
특히 "왜 오늘 올랐는지"보다 "어떤 테마/산업과 연결되는지"가 중요합니다.

형식 (JSON): {{"종목코드": "주력사업 | 밸류체인연결 | 최근3개월 핵심 재료/뉴스"}}

종목:
{stock_list}"""

        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],
            "generationConfig": {"temperature": 0.1},
        }

        for attempt in range(3):
            resp = requests.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                try:
                    reasons = _gemini_parse_json(resp.json())
                    stock_reasons.update(reasons)
                    break
                except (json.JSONDecodeError, KeyError, IndexError) as e:
                    print(f"    검색 배치 {i//batch_size+1} 파싱 실패: {e}")
            else:
                wait = 15 * (attempt + 1)
                print(f"    검색 배치 {i//batch_size+1} HTTP {resp.status_code}, {wait}초 대기...")
                time.sleep(wait)

        if i + batch_size < len(stocks):
            time.sleep(15)

    if not stock_reasons:
        return {}, {}

    print(f"    검색 완료: {len(stock_reasons)}개 종목 사유 파악")

    # 2단계: 검색 결과 기반 테마 통합 분류
    reasons_text = "\n".join(f"- {code} {reason}" for code, reason in stock_reasons.items())
    existing_list = ""
    if existing_themes:
        existing_list = "\n기존 테마 목록 (가능하면 이 이름을 우선 사용):\n" + ", ".join(existing_themes)

    classify_prompt = f"""아래는 오늘 급등한 한국 주식 종목들의 사업내용, 밸류체인, 최근 뉴스입니다.
이 정보를 바탕으로 테마/섹터별로 분류해주세요.

규칙:
1. 같은 재료/뉴스로 동반 상승한 종목들을 하나의 테마로 묶기
2. 밸류체인 연결 고려: 직접 해당 산업이 아니더라도 제품이 해당 산업에 공급되면 포함
   예: 절삭공구 회사 → 반도체 장비 가공에 사용 → "AI 반도체/반도체 장비" 테마 가능
   예: 전선/케이블 → 전력망 → "전력기기" 테마 가능
3. 테마명은 시장에서 실제 쓰는 구체적 이름 (예: "AI 반도체", "비만치료제", "전력기기", "HBM")
4. 기존 테마에 해당하는 종목은 반드시 기존 테마명을 그대로 사용. 새 테마는 기존에 없는 경우에만 생성
5. 테마 수는 5~15개 사이로 유지. 너무 세분화 금지
6. 한 종목이 여러 테마에 속할 수 있음
7. 어울리는 테마 없으면 "기타"로
{existing_list}

JSON만 출력:
{{"테마명": ["종목코드1", "종목코드2"]}}

종목 정보:
{reasons_text}"""

    body = {
        "contents": [{"parts": [{"text": classify_prompt}]}],
        "generationConfig": {"temperature": 0.1},
    }

    time.sleep(5)
    for attempt in range(3):
        resp = requests.post(url, headers=headers, json=body)
        if resp.status_code == 200:
            try:
                theme_map = _gemini_parse_json(resp.json())
                return theme_map, stock_reasons
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"    분류 파싱 실패: {e}")
        else:
            wait = 15 * (attempt + 1)
            print(f"    분류 HTTP {resp.status_code}, {wait}초 대기...")
            time.sleep(wait)

    return {}, stock_reasons


# ── 4. 점수 산출 ──────────────────────────────────────────────────────────────

def score_leader(stock: dict) -> float:
    score = stock.get("등락률", 0.0)

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


# ── 5. 데이터 축적 ───────────────────────────────────────────────────────────

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


def accumulate_data(stocks: list[dict], theme_map: dict, date_str: str, stock_reasons: dict = None):
    """일별 데이터 저장 + stocks.json, themes.json 마스터 업데이트."""
    if stock_reasons is None:
        stock_reasons = {}
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
            "시가": s.get("시가", 0),
            "고가": s.get("고가", 0),
            "종가": s.get("종가", 0),
            "등락률": s["등락률"],
            "거래대금_백만": s.get("거래대금_백만", 0),
            "시가총액_억": s.get("시가총액_억", 0),
            "거래량증가율": s.get("거래량증가율", 0.0),
            "상한가시간": s.get("상한가시간", "-"),
            "대장주_점수": s.get("대장주_점수", 0),
            "테마": stock_themes.get(s["종목코드"], []),
            "분류사유": stock_reasons.get(s["종목코드"], ""),
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
                "상승일": {},
            }
        entry = stocks_master[code]
        entry["종목명"] = s["종목명"]
        if isinstance(entry.get("상승일"), list):
            entry["상승일"] = {d: 0.0 for d in entry["상승일"]}
        for t in stock_themes.get(code, []):
            if t not in entry["테마"]:
                entry["테마"].append(t)
        entry["상승일"][date_str] = s.get("등락률", 0.0)

    _save_json(stocks_path, stocks_master)
    print(f"  종목 마스터: {stocks_path} ({len(stocks_master)}개)")

    # 3) themes.json 마스터 업데이트
    themes_path = os.path.join(DATA_DIR, "themes.json")
    themes_master = _load_json(themes_path)

    for theme, codes in theme_map.items():
        if theme not in themes_master:
            themes_master[theme] = {"종목": [], "활성일": []}
        entry = themes_master[theme]
        for code in codes:
            if code not in entry["종목"]:
                entry["종목"].append(code)
        if date_str not in entry["활성일"]:
            entry["활성일"].append(date_str)

    _save_json(themes_path, themes_master)
    print(f"  테마 마스터: {themes_path} ({len(themes_master)}개 테마)")

    return daily_path


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    app_key = os.environ.get("KIS_APP_KEY", "")
    app_secret = os.environ.get("KIS_APP_SECRET", "")
    is_mock = os.environ.get("KIS_IS_MOCK", "false").lower() == "true"
    gemini_key = os.environ.get("GEMINI_API_KEY", "")

    date_str = datetime.now().strftime("%Y-%m-%d")

    print(f"=== 대장주 분석 ({date_str}) ===\n")

    # ── 1. 주가 데이터 수집 (필수) ──
    print("[1/4] FinanceDataReader 주가 수집...")
    codes_info = fetch_rising_stocks()

    if not codes_info:
        print("조건을 만족하는 종목이 없습니다. (공휴일/장 미개장)")
        return

    print(f"    -> {len(codes_info)}개 종목 선정")

    # ── 2. 상한가 도달시간 (선택 - KIS API) ──
    upper_count = sum(1 for s in codes_info if s.get("_is_upper"))
    print(f"[2/4] 상한가 도달시간 조회 ({upper_count}개)...")
    if upper_count > 0 and app_key and app_secret:
        try:
            enrich_upper_limit_times(codes_info, app_key, app_secret, is_mock)
        except Exception as e:
            print(f"    KIS API 실패 (계속 진행): {e}")
    elif upper_count > 0:
        print("    KIS_APP_KEY 미설정 - 도달시간 생략")

    # ── 3. Gemini 테마 분류 (선택) ──
    theme_map = {}
    stock_reasons = {}
    if gemini_key:
        print("[3/4] Gemini 테마 분류...")
        # 기존 테마 목록 로드 (일관된 테마명 유지)
        themes_path = os.path.join(DATA_DIR, "themes.json")
        existing_themes = list(_load_json(themes_path).keys())
        if existing_themes:
            print(f"    기존 테마 {len(existing_themes)}개 참조")
        try:
            theme_map, stock_reasons = classify_themes(codes_info, gemini_key, existing_themes)
            for theme, codes in theme_map.items():
                print(f"    {theme}: {len(codes)}개")
        except Exception as e:
            print(f"    테마 분류 실패 (계속 진행): {e}")
    else:
        print("[3/4] GEMINI_API_KEY 미설정 - 테마 분류 생략")

    # ── 4. 점수 산출 & 저장 ──
    print("[4/4] 점수 산출...")

    sorted_by_vol = sorted(codes_info, key=lambda x: x.get("거래대금_백만", 0), reverse=True)
    for i, s in enumerate(sorted_by_vol, 1):
        s["거래대금_순위"] = i

    for s in codes_info:
        s["대장주_점수"] = score_leader(s)

    codes_info.sort(key=lambda x: x["대장주_점수"], reverse=True)

    # 내부 필드 제거
    for s in codes_info:
        s.pop("_is_upper", None)
        s.pop("_close", None)
        s.pop("거래대금_순위", None)

    # 결과 출력
    print(f"\n>> 대장주 {len(codes_info)}건 분석 완료")
    for s in codes_info[:10]:
        upper = f" [{s['상한가시간']}]" if s["상한가시간"] != "-" else ""
        print(f"  {s['종목코드']} {s['종목명']:<12s} {s['등락률']:>+7.2f}%{upper}  점수:{s['대장주_점수']:.1f}")

    # 데이터 축적
    print("\n>> 데이터 축적 중...")
    accumulate_data(codes_info, theme_map, date_str, stock_reasons)


if __name__ == "__main__":
    main()

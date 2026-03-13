"""
국내 대장주 분석 → JSON + Markdown 리포트 → Google Drive 업로드.

매 평일 장 마감 후 GitHub Actions에서 실행.
수집 흐름:
1. 등락률 순위 API → 상위 N개 선정
2. 종목별 상세 시세 조회 (시총, 거래대금, 거래량증가율, 상한가)
3. 상한가 종목만 분봉 API로 최초 도달시간 조회
4. 대장주 점수 산출 → JSON/MD 저장 → Google Drive 업로드
"""

import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests

from update_sheet import get_or_refresh_token

load_dotenv()

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

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


# ── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def _base_url(is_mock: bool = False) -> str:
    if is_mock:
        return "https://openapivts.koreainvestment.com:29443"
    return "https://openapi.koreainvestment.com:9443"


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
    min_advance: float = 0.0,
    top_n: int = 50,
) -> list[dict]:
    """등락률 순위 전체 조회 → min_advance% 이상 필터 → 상위 top_n개."""
    url = f"{_base_url(is_mock)}{FLUCTUATION_API}"
    all_rows: list[dict] = []
    tr_cont = ""

    while True:
        headers = _kis_headers(access_token, app_key, app_secret, FLUCTUATION_TR_ID, tr_cont)
        resp = requests.get(url, headers=headers, params=DEF_FLUCT_PARAMS)
        resp.raise_for_status()
        data = resp.json()

        chunk = data.get("output", [])
        if not chunk:
            break
        all_rows.extend(chunk)

        # 페이지네이션: 응답 헤더의 tr_cont 가 "M" 이면 다음 페이지 존재
        tr_cont = resp.headers.get("tr_cont", "")
        if tr_cont != "M":
            break
        tr_cont = "N"
        time.sleep(0.1)

    # 필터링
    result = []
    for row in all_rows:
        try:
            rate = float(row.get("prdy_ctrt", 0))
        except (ValueError, TypeError):
            rate = 0.0
        if rate >= min_advance:
            row["_rate"] = rate
            result.append(row)

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
    """개별 종목 현재가 시세 → 시총, 거래대금, 거래량증가율, 상한가, 현재가."""
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
    """분봉 API로 당일 고가 == 상한가인 최초 시간 탐색. 없으면 '-' 반환."""
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


# ── 4. 점수 산출 ──────────────────────────────────────────────────────────────

def score_leader(stock: dict, rank_cap: int = 100) -> float:
    """
    대장주 점수:
      - 시총 순위 역수: max 100점
      - 거래대금 순위 역수: max 100점
      - 등락률: 0~30% → 0~50점
      - 상한가 보너스: 도달 시 +15점, 09:30 이전 +5점 추가
    """
    score = 0.0

    rank_s = stock.get("시가총액_순위", 0)
    if rank_s and rank_s > 0:
        score += 100.0 / min(int(rank_s), rank_cap)

    rank_t = stock.get("거래대금_순위", 0)
    if rank_t and rank_t > 0:
        score += 100.0 / min(int(rank_t), rank_cap)

    rate = stock.get("등락률", 0.0)
    if rate > 0:
        score += min(rate * 2.0, 50.0)

    up_time = stock.get("상한가시간", "-")
    if up_time != "-":
        score += 15.0
        try:
            hh, mm = int(up_time[:2]), int(up_time[3:5])
            if hh < 9 or (hh == 9 and mm <= 30):
                score += 5.0
        except (ValueError, IndexError):
            pass

    return round(score, 2)


# ── 5. Markdown 리포트 ────────────────────────────────────────────────────────

def generate_markdown(stocks: list[dict], date_str: str) -> str:
    lines = [
        f"# 대장주 분석 ({date_str})",
        "",
    ]

    # 상한가 종목 섹션
    upper_stocks = [s for s in stocks if s.get("상한가시간", "-") != "-"]
    if upper_stocks:
        lines.append(f"## 상한가 종목 ({len(upper_stocks)}개)")
        lines.append("")
        lines.append("| 종목코드 | 종목명 | 등락률 | 거래대금(백만) | 시총(억) | 도달시간 | 점수 |")
        lines.append("|----------|--------|-------:|---------------:|---------:|:--------:|-----:|")
        for s in upper_stocks:
            lines.append(
                f"| {s['종목코드']} | {s['종목명']} "
                f"| {s['등락률']:+.2f}% "
                f"| {s['거래대금_백만']:,} "
                f"| {s['시가총액_억']:,} "
                f"| {s['상한가시간']} "
                f"| {s['대장주_점수']:.1f} |"
            )
        lines.append("")

    # 요약
    lines.append("## 요약")
    lines.append(f"- 분석 종목: {len(stocks)}개")
    lines.append(f"- 상한가 종목: {len(upper_stocks)}개")
    if stocks:
        lines.append(f"- 최고 점수: {stocks[0]['대장주_점수']:.1f} ({stocks[0]['종목명']})")
    lines.append("")

    # 전체 순위 테이블
    lines.append(f"## 전체 순위 (상위 {len(stocks)})")
    lines.append("")
    lines.append("| # | 종목코드 | 종목명 | 등락률 | 거래대금(백만) | 시총(억) | 거래량증가율 | 상한가 | 점수 |")
    lines.append("|--:|----------|--------|-------:|---------------:|---------:|------------:|:------:|-----:|")
    for i, s in enumerate(stocks, 1):
        upper_mark = s.get("상한가시간", "-")
        if upper_mark != "-":
            upper_mark = f"O ({upper_mark})"
        lines.append(
            f"| {i} "
            f"| {s['종목코드']} "
            f"| {s['종목명']} "
            f"| {s['등락률']:+.2f}% "
            f"| {s['거래대금_백만']:,} "
            f"| {s['시가총액_억']:,} "
            f"| {s['거래량증가율']:+.1f}% "
            f"| {upper_mark} "
            f"| {s['대장주_점수']:.1f} |"
        )
    lines.append("")
    return "\n".join(lines)


# ── 6. Google Drive 업로드 ────────────────────────────────────────────────────

def upload_to_drive(file_path: str, folder_id: str, creds_json: str) -> str:
    """파일을 Google Drive 폴더에 업로드. 업로드된 파일 ID 반환."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    scopes = ["https://www.googleapis.com/auth/drive.file"]
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    filename = os.path.basename(file_path)
    file_metadata = {"name": filename, "parents": [folder_id]}

    # MIME 타입 결정
    if file_path.endswith(".json"):
        mime = "application/json"
    elif file_path.endswith(".md"):
        mime = "text/markdown"
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
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

    if not app_key or not app_secret:
        print("오류: KIS_APP_KEY, KIS_APP_SECRET 환경변수를 설정하세요.")
        return

    date_str = datetime.now().strftime("%Y-%m-%d")
    date_suffix = datetime.now().strftime("%Y%m%d")

    print(f"=== 대장주 분석 ({date_str}) ===\n")

    # 1. 토큰
    print("[1/5] 토큰 발급...")
    access_token = get_or_refresh_token(app_key, app_secret, is_mock)

    # 2. 등락률 순위
    print("[2/5] 등락률 순위 조회 (상위 50)...")
    fluct = fetch_fluctuation_top(access_token, app_key, app_secret, is_mock, top_n=50)

    if not fluct:
        print("조건을 만족하는 종목이 없습니다. (공휴일/장 미개장)")
        return

    # 종목코드 추출
    codes_info = []
    for row in fluct:
        code = str(row.get("stck_shrn_iscd", "")).strip()
        if code and len(code) >= 5:
            codes_info.append({
                "종목코드": code,
                "종목명": str(row.get("hts_kor_isnm", "")).strip(),
                "등락률": row["_rate"],
                "현재가_순위": str(row.get("stck_prpr", "")).strip(),
            })
    print(f"    → {len(codes_info)}개 종목 선정")

    # 3. 종목별 상세 시세
    print(f"[3/5] 종목별 시세 조회 (시총/거래대금/거래량증가율)...")
    for item in codes_info:
        detail = fetch_stock_detail(access_token, app_key, app_secret, item["종목코드"], is_mock)
        item.update(detail)
        time.sleep(0.1)

    # 4. 상한가 종목 분봉 스캔
    upper_candidates = [
        s for s in codes_info
        if s.get("현재가") and s.get("상한가") and s["현재가"] == s["상한가"]
    ]
    print(f"[4/5] 상한가 종목 도달시간 조회 ({len(upper_candidates)}개)...")
    for s in upper_candidates:
        hit_time = fetch_upper_limit_time(
            access_token, app_key, app_secret,
            s["종목코드"], s["상한가"], is_mock,
        )
        s["상한가시간"] = hit_time
        print(f"    {s['종목코드']} {s['종목명']} → {hit_time}")
        time.sleep(0.1)

    # 상한가 아닌 종목은 '-'
    for s in codes_info:
        if "상한가시간" not in s:
            s["상한가시간"] = "-"

    # 5. 점수 산출 & 정렬
    print("[5/5] 점수 산출...")

    # 순위 계산 (내림차순)
    sorted_by_cap = sorted(codes_info, key=lambda x: x.get("시가총액_억", 0), reverse=True)
    for i, s in enumerate(sorted_by_cap, 1):
        s["시가총액_순위"] = i

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

    # JSON/MD 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, f"leaders_{date_suffix}.json")
    md_path = os.path.join(OUTPUT_DIR, f"leaders_{date_suffix}.md")

    save_keys = [
        "종목코드", "종목명", "등락률", "거래대금_백만", "시가총액_억",
        "거래량증가율", "상한가시간", "대장주_점수",
    ]
    save_data = [{k: s.get(k) for k in save_keys} for s in codes_info]
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)

    md_content = generate_markdown(codes_info, date_str)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"\n>> 저장: {json_path}")
    print(f">> 저장: {md_path}")

    # Google Drive 업로드
    if creds_json and folder_id:
        print("\n>> Google Drive 업로드 중...")
        upload_to_drive(json_path, folder_id, creds_json)
        upload_to_drive(md_path, folder_id, creds_json)
    else:
        print("\n>> Google Drive 환경변수 미설정 — 업로드 생략")


if __name__ == "__main__":
    main()

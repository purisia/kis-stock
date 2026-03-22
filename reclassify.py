"""기존 일별 데이터에 핀업 + 인포스탁 테마 분류 재실행."""
import json
import sys
import os

from analyze_leaders import (
    classify_themes_finup, fetch_infostock_data,
    accumulate_data, _load_json, DATA_DIR, DAILY_DIR,
)


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-18"

    daily_path = os.path.join(DAILY_DIR, f"{date_str}.json")
    if not os.path.exists(daily_path):
        print(f"{daily_path} 없음")
        return

    with open(daily_path, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    our_codes = {s["종목코드"] for s in stocks}
    code_name = {s["종목코드"]: s["종목명"] for s in stocks}

    print(f"== {date_str} 테마 재분류 ({len(stocks)}개 종목) ==")
    sys.stdout.flush()

    # 1) 핀업 테마 분류 (당일만 유효하지만 시도)
    theme_map = {}
    stock_reasons: dict[str, list[str]] = {}
    try:
        theme_map, stock_reasons_str = classify_themes_finup(stocks)
        # stock_reasons_str을 다시 리스트로 변환할 필요 없음 (그대로 사용)
        print(f"\n핀업: {len(theme_map)}개 테마 매칭")
    except Exception as e:
        print(f"핀업 실패: {e}")
        stock_reasons_str = {}

    # 2) InfoStock 테마 데이터 (날짜별 가능)
    print(f"\nInfoStock 데이터 수집 ({date_str})...")
    sys.stdout.flush()
    is_theme_map, is_descs = fetch_infostock_data(date_str)
    print(f"InfoStock: {len(is_theme_map)}개 테마, {len(is_descs)}개 설명")

    # 3) InfoStock 테마-종목을 우리 종목과 매칭하여 theme_map에 병합
    is_matched = 0
    for is_theme, is_codes in is_theme_map.items():
        matched_codes = our_codes & set(is_codes)
        if matched_codes:
            is_matched += 1
            if is_theme not in theme_map:
                theme_map[is_theme] = []
            for code in matched_codes:
                if code not in theme_map[is_theme]:
                    theme_map[is_theme].append(code)
                # stock_reasons 보강
                if code not in stock_reasons_str:
                    stock_reasons_str[code] = f"인포스탁 테마: {is_theme}"
                elif "인포스탁" not in stock_reasons_str[code]:
                    stock_reasons_str[code] += f", 인포스탁: {is_theme}"

    print(f"InfoStock 매칭: {is_matched}개 테마")

    # 4) 최종 결과 출력
    total_stocks = set()
    for codes in theme_map.values():
        total_stocks.update(codes)
    print(f"\n-> 최종 {len(theme_map)}개 테마, {len(total_stocks)}개 종목 매칭")
    for theme, codes in theme_map.items():
        names = [code_name.get(c, c) for c in codes[:5]]
        src = "핀+인" if theme in is_theme_map else "핀업"
        print(f"  [{src}] {theme}: {len(codes)}개 - {', '.join(names)}")
    sys.stdout.flush()

    # InfoStock 설명도 핀업 테마에 매칭
    # InfoStock 설명 dict에 핀업 테마명도 추가 (정확히 같은 이름이면)
    theme_descriptions = is_descs

    # 데이터 업데이트
    accumulate_data(stocks, theme_map, date_str, stock_reasons_str, theme_descriptions)
    print("완료!")


if __name__ == "__main__":
    main()

"""기존 일별 데이터에 Gemini 테마 분류만 재실행."""
import json
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# analyze_leaders.py의 함수 재사용
from analyze_leaders import classify_themes, accumulate_data, _load_json, DATA_DIR, DAILY_DIR

def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-03-17"
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not gemini_key:
        print("GEMINI_API_KEY 미설정")
        return

    daily_path = os.path.join(DAILY_DIR, f"{date_str}.json")
    if not os.path.exists(daily_path):
        print(f"{daily_path} 없음")
        return

    with open(daily_path, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    print(f"== {date_str} 테마 재분류 ({len(stocks)}개 종목) ==")
    sys.stdout.flush()

    # 기존 테마 로드
    themes_path = os.path.join(DATA_DIR, "themes.json")
    existing_themes = list(_load_json(themes_path).keys())
    print(f"기존 테마 {len(existing_themes)}개 참조")
    sys.stdout.flush()

    theme_map, stock_reasons = classify_themes(stocks, gemini_key, existing_themes)

    if not theme_map:
        print("테마 분류 실패")
        return

    for theme, codes in theme_map.items():
        print(f"  {theme}: {len(codes)}개")
    sys.stdout.flush()

    # 데이터 업데이트
    accumulate_data(stocks, theme_map, date_str, stock_reasons)
    print("완료!")

if __name__ == "__main__":
    main()

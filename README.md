# 한국투자증권 주식 시세 조회

GitHub Actions를 사용하여 코스피/코스닥 지수를 자동으로 조회하고 Google Sheets에 기록합니다.

## 설정 방법

### 1. GitHub Secrets 등록 (필수)

Repository > Settings > Secrets and variables > Actions > New repository secret

| Name | Value |
|------|-------|
| `KIS_APP_KEY` | 한국투자증권에서 발급받은 앱 키 |
| `KIS_APP_SECRET` | 한국투자증권에서 발급받은 앱 시크릿 |
| `GOOGLE_CREDENTIALS_JSON` | Google 서비스 계정 JSON 키 |
| `GOOGLE_SPREADSHEET_ID` | Google Spreadsheet ID |

### 2. Variables 등록 (선택)

Repository > Settings > Secrets and variables > Actions > Variables > New repository variable

| Name | Value | 기본값 |
|------|-------|--------|
| `KIS_IS_MOCK` | 모의투자 여부 | `false` |

## 실행 스케줄

매일 오전 8시(KST) 자동 실행 - 전일 코스피/코스닥 지수 수집

## 수동 실행

Actions > 코스피/코스닥 지수 수집 > Run workflow

## 로컬 테스트

```bash
# .env 파일 생성
cp .env.example .env
# .env 파일에 실제 키 입력

# 실행
pip install requests python-dotenv gspread google-auth
python update_sheet.py
```

## 한국투자증권 API 키 발급

1. https://apiportal.koreainvestment.com 접속
2. 회원가입 및 로그인
3. API 신청 > 앱 등록
4. 발급된 앱 키, 앱 시크릿 확인

# 한국투자증권 주식 시세 조회

GitHub Actions를 사용하여 주식 시세를 자동으로 조회합니다.

## 설정 방법

### 1. GitHub Secrets 등록 (필수)

Repository > Settings > Secrets and variables > Actions > New repository secret

| Name | Value |
|------|-------|
| `KIS_APP_KEY` | 한국투자증권에서 발급받은 앱 키 |
| `KIS_APP_SECRET` | 한국투자증권에서 발급받은 앱 시크릿 |

### 2. Variables 등록 (선택)

Repository > Settings > Secrets and variables > Actions > Variables > New repository variable

| Name | Value | 기본값 |
|------|-------|--------|
| `KIS_STOCK_CODES` | 조회할 종목코드 (쉼표 구분) | `005930` |
| `KIS_IS_MOCK` | 모의투자 여부 | `false` |

### 3. 종목 코드 예시

| 코드 | 종목명 |
|------|--------|
| 005930 | 삼성전자 |
| 000660 | SK하이닉스 |
| 035720 | 카카오 |
| 005380 | 현대차 |
| 035420 | NAVER |

## 실행 스케줄

평일(월-금) 다음 시간에 자동 실행:
- 09:30 (장 시작 후)
- 11:00
- 13:00
- 14:30
- 15:20 (장 마감 전)

## 수동 실행

Actions > 주식 시세 조회 > Run workflow

## 로컬 테스트

```bash
# .env 파일 생성
cp .env.example .env
# .env 파일에 실제 키 입력

# 실행
pip install requests python-dotenv
python kis_stock_price.py
```

## 한국투자증권 API 키 발급

1. https://apiportal.koreainvestment.com 접속
2. 회원가입 및 로그인
3. API 신청 > 앱 등록
4. 발급된 앱 키, 앱 시크릿 확인

# ❄️ Snowflake Intelligence Data Generator

> **Cortex Analyst, Cortex Search, Snowflake Intelligence를 위한 맞춤형 데모 인프라 자동 생성 도구**

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)

---

## 📋 목차

- [개요](#-개요)
- [빠른 시작](#-빠른-시작)
- [활용 가이드](#-활용-가이드)
- [주요 기능](#-주요-기능)
- [시스템 요구사항](#-시스템-요구사항)
- [설치 방법](#-설치-방법)
- [사용 방법](#-사용-방법)
- [설정 파일](#-설정-파일)
- [프로젝트 구조](#-프로젝트-구조)
- [제약 사항](#-제약-사항)
- [문제 해결](#-문제-해결)
- [라이선스](#-라이선스)

---

## ⚡ 빠른 시작

**5분 안에 데모 환경을 만들어보세요!**

### 🎬 데모 영상

전체 워크플로우를 영상으로 확인하세요:

📹 **[demo_walkthrough.mov](images/demo_walkthrough.mov)** (233MB)

> 💡 **영상 내용:** 고객 URL 입력부터 AI Agent 생성까지 전체 과정을 시연합니다.

---

### 📋 단계별 가이드

```
1️⃣ 고객 URL 입력      →  2️⃣ 데모 시나리오 선택  →  3️⃣ 옵션 선택  →  4️⃣ 생성 완료!
   (예: daisomall.co.kr)     (AI가 3개 생성)         (Analyst/Search/Agent)
```

| 단계 | 스크린샷 |
|------|---------|
| **Step 1:** 고객 정보 입력 | ![Step 1](images/step1_customer_info.png) |
| **Step 2:** 데모 시나리오 선택 | ![Step 2](images/step2_demo_selection.png) |
| **Step 3:** 인프라 옵션 구성 | ![Step 3](images/step3_configuration.png) |
| **Step 4:** 자동 생성 진행 | ![Step 4](images/step4_infrastructure_progress.png) |

> 📌 **결과물:** 4개 테이블 + Semantic View + Search Service + AI Agent가 자동 생성됩니다!

---

## 🎯 개요

Snowflake Intelligence Data Generator는 Snowflake의 AI 및 분석 기능을 시연하기 위한 **프로덕션 준비 데모 환경**을 자동으로 생성하는 엔터프라이즈급 도구입니다.

### 📸 앱 미리보기

![App Preview](images/step1_customer_info.png)

### 이런 분들에게 적합합니다:
- 💼 **솔루션 엔지니어** - 고객 미팅을 위한 맞춤형 데모 신속 생성
- 🎯 **영업 담당자** - 산업별 맞춤 시연 환경 구축
- 📊 **데이터 전문가** - Cortex 기능 테스트 및 개발
- 🎓 **교육 담당자** - 팀 교육용 실습 환경 생성

---

## 💡 활용 가이드

### 언제 이 프로젝트를 활용하세요?

#### 1️⃣ 고객사 맞춤 Demo 준비

고객 미팅이나 PoC를 위한 **맞춤형 데모 환경**이 필요할 때 활용하세요.

```
📌 시나리오 예시
├── 금융사 고객 미팅 예정
├── 고객사 홈페이지 URL 입력: https://customer-bank.com
├── AI가 금융 산업 컨텍스트 분석
├── 자동으로 금융 관련 테이블 구조 설계
│   ├── 고객 거래 내역 테이블
│   ├── 리스크 평가 테이블
│   └── 고객 세그먼트 테이블
└── 금융 산업에 적합한 질문 자동 생성
    ├── "지난 분기 대비 고위험 거래 비율은?"
    └── "VIP 고객 세그먼트의 평균 거래 금액은?"
```

**✅ 장점:**
- 고객 산업에 맞는 **현실적인 데모 데이터** 자동 생성
- 일반적인 "Sample Data" 대신 **비즈니스 컨텍스트가 있는 데이터**
- 미팅 준비 시간 단축 (수시간 → **10분 이내**)

---

#### 2️⃣ Snowflake Cortex 제품 Full Demo

Snowflake Cortex의 **모든 AI 기능을 통합 시연**하고자 할 때 활용하세요.

| 구성 요소 | 시연 가능한 기능 |
|----------|-----------------|
| **Cortex Analyst** | 자연어로 SQL 쿼리 생성, Semantic View 활용 |
| **Cortex Search** | 비정형 텍스트 의미 검색, 문서/지식베이스 검색 |
| **Snowflake Intelligence Agent** | Analyst + Search 통합 AI 에이전트 |
| **Custom Tools** | Agent에 연결된 도구 오케스트레이션 |

```
📌 시연 흐름 예시
1. Cortex Analyst로 "지난 달 매출 상위 10개 제품은?" 질문
   → AI가 자동으로 SQL 생성 및 실행

2. Cortex Search로 "반품 정책 관련 문서 찾아줘" 검색
   → 의미 기반으로 관련 문서 검색

3. Intelligence Agent로 "매출이 떨어진 제품의 고객 불만 사항은?" 질문
   → Analyst + Search 결합하여 종합 답변 생성
```

---

#### 3️⃣ 신규 팀원 온보딩 및 교육

Snowflake Cortex 기능을 **처음 접하는 팀원 교육용** 실습 환경으로 활용하세요.

| 교육 대상 | 활용 방법 |
|----------|----------|
| **신규 SE** | Cortex 제품 데모 연습 환경 구축 |
| **데이터 팀** | Semantic View 설계 패턴 학습 |
| **개발자** | Agent 및 Custom Tool 개발 참고용 |

**✅ 장점:**
- 안전한 **샌드박스 환경**에서 자유롭게 실습
- 다양한 산업/시나리오 데모 환경 **반복 생성 가능**
- 실수해도 쉽게 **초기화 및 재생성**

---

#### 4️⃣ PoC (Proof of Concept) 환경 신속 구축

고객 평가를 위한 **프로덕션과 유사한 환경**이 필요할 때 활용하세요.

```
📌 PoC 활용 예시
├── 고객이 직접 테스트할 수 있는 환경 제공
├── 실제 비즈니스 질문으로 테스트 가능
├── 데모 후 고객 피드백 반영하여 재생성
└── 성공적인 PoC 후 실제 데이터로 전환 용이
```

---

#### 5️⃣ 워크숍 및 핸즈온 랩

고객 또는 파트너 대상 **워크숍 실습 환경**으로 활용하세요.

| 워크숍 유형 | 활용 방법 |
|------------|----------|
| **AI/ML 워크숍** | 참가자별 독립된 데모 스키마 생성 |
| **데이터 분석 워크숍** | Cortex Analyst로 자연어 분석 실습 |
| **검색 솔루션 워크숍** | Cortex Search로 의미 검색 체험 |

**✅ 장점:**
- 참가자별 **격리된 환경** 제공 (타임스탬프 기반 스키마)
- 워크숍 종료 후 **간편한 정리** (스키마 DROP)
- **다국어 지원**으로 글로벌 워크숍 가능

---

### 📊 활용 시나리오 비교

| 시나리오 | 준비 시간 | 권장 레코드 수 | 추천 기능 |
|----------|----------|---------------|----------|
| **빠른 데모** | 5분 | 100-500 | Agent만 |
| **고객 미팅** | 10분 | 500-1,000 | 전체 |
| **PoC 환경** | 15분 | 1,000-5,000 | 전체 + 질문 검증 |
| **워크숍** | 10분/참가자 | 500-1,000 | Semantic View + Search |
| **팀 교육** | 5분 | 100-500 | 전체 |

---

## ✨ 주요 기능

### 🤖 AI 기반 생성
| 기능 | 설명 |
|------|------|
| **컨텍스트 인식 데모** | 회사 URL 분석으로 산업별 맞춤 데모 생성 |
| **Claude 4 Sonnet 통합** | 최신 LLM으로 현실적인 콘텐츠 생성 |
| **지능형 질문 생성** | 기본/중급/고급 수준의 12개 이상 질문 생성 |
| **다국어 지원** | 영어, 스페인어, 프랑스어, 독일어, 일본어, 중국어, 한국어 |

### 🏗️ 인프라 자동화
| 기능 | 설명 |
|------|------|
| **Semantic View** | Cortex Analyst용 Semantic View 자동 생성 |
| **Cortex Search Service** | 비정형 텍스트 검색 서비스 설정 |
| **Snowflake Intelligence Agent** | AI Agent 자동 생성 및 도구 연결 |
| **스마트 조인 키** | 테이블 간 70% 조인 오버랩 보장 |

### 📊 데이터 품질
- ✅ LLM 생성 Schema (현실적인 샘플 값 포함)
- ✅ 비즈니스 현실적인 데이터 (일반적인 "Type_A" 값 없음)
- ✅ 시계열 준비 완료 (지난 7일 타임스탬프)
- ✅ Star/Snowflake Schema 지원

---

## 💻 시스템 요구사항

| 항목 | 요구사항 |
|------|----------|
| **역할** | `ACCOUNTADMIN` (또는 동등 권한) |
| **Cortex** | Cortex 함수 활성화 필요 |
| **리전** | Cortex 지원 리전 ([지원 리전 확인](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability)) |
| **Edition** | Enterprise 이상 권장 |

---

## 🚀 설치 방법

Streamlit in Snowflake(SiS)로 배포합니다. Snowflake 내에서 직접 실행되며, 별도의 로컬 환경 구성이 필요 없습니다.

#### 1단계: 데이터베이스 및 인프라 설정

Snowsight SQL Worksheet에서 `Setup.sql` 스크립트를 실행합니다:

```sql
-- Setup.sql 파일 전체 실행
-- 또는 아래 핵심 명령어 순서대로 실행

USE ROLE ACCOUNTADMIN;

-- 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS DEMO_DB;
CREATE SCHEMA IF NOT EXISTS DEMO_DB.APPLICATIONS;
CREATE SCHEMA IF NOT EXISTS DEMO_DB.DEMO_DATA;

-- 히스토리 테이블 생성
CREATE TABLE IF NOT EXISTS DEMO_DB.APPLICATIONS.SI_GENERATOR_HISTORY (
    HISTORY_ID VARCHAR(50) PRIMARY KEY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    COMPANY_NAME VARCHAR(500),
    COMPANY_URL VARCHAR(1000),
    DEMO_TITLE VARCHAR(500),
    DEMO_DESCRIPTION VARCHAR(5000),
    SCHEMA_NAME VARCHAR(500),
    NUM_RECORDS INTEGER,
    LANGUAGE_CODE VARCHAR(10),
    TEAM_MEMBERS VARCHAR(1000),
    USE_CASES VARCHAR(5000),
    ENABLE_SEMANTIC_VIEW BOOLEAN,
    ENABLE_SEARCH_SERVICE BOOLEAN,
    ENABLE_AGENT BOOLEAN,
    ADVANCED_MODE BOOLEAN,
    TABLE_NAMES VARIANT,
    TARGET_QUESTIONS VARIANT,
    GENERATED_QUESTIONS VARIANT,
    DEMO_DATA_JSON VARIANT
);

-- Snowflake Intelligence용 데이터베이스
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;

-- Warehouse 생성
CREATE WAREHOUSE IF NOT EXISTS SI_DEMO_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- Cortex Cross-Region 활성화
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Stage 생성
USE SCHEMA DEMO_DB.APPLICATIONS;
CREATE OR REPLACE STAGE SI_DATA_GENERATOR_STAGE
    DIRECTORY = (ENABLE = TRUE);
```

#### 2단계: 파일 업로드

**Snowsight UI 사용:**
1. `Data` → `Databases` → `DEMO_DB` → `APPLICATIONS` → `Stages` 이동
2. `SI_DATA_GENERATOR_STAGE` 클릭
3. `+ Files` 버튼으로 아래 파일들 업로드:

```
필수 파일:
├── SI_Generator.py          # 메인 앱 (SiS 배포용)
├── demo_content.py          # 데모 콘텐츠 생성
├── errors.py                # 에러 처리
├── infrastructure.py        # 인프라 생성 로직
├── metrics.py               # 메트릭 추적
├── prompts.py               # LLM 프롬프트
├── styles.py                # UI 스타일
├── utils.py                 # 유틸리티 함수
└── environment.yml          # 의존성 정의
```

**또는 SnowSQL 사용:**
```bash
snowsql -a <account> -u <username> -r ACCOUNTADMIN

PUT file:///path/to/project/*.py @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/project/environment.yml @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

#### 3단계: Streamlit 앱 생성

```sql
USE SCHEMA DEMO_DB.APPLICATIONS;
USE WAREHOUSE SI_DEMO_WH;

CREATE OR REPLACE STREAMLIT SI_DATA_GENERATOR_APP
    ROOT_LOCATION = '@DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE'
    MAIN_FILE = 'SI_Generator.py'
    QUERY_WAREHOUSE = SI_DEMO_WH
    TITLE = 'Snowflake Agent Demo Data Generator';

GRANT USAGE ON STREAMLIT SI_DATA_GENERATOR_APP TO ROLE ACCOUNTADMIN;
```

#### 4단계: 앱 접속

Snowsight → `Streamlit` → `SI_DATA_GENERATOR_APP` 클릭

---

## 📖 사용 방법

### 4단계 워크플로우

앱은 직관적인 4단계 마법사로 구성되어 있습니다:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  1️⃣ 단계        │    │  2️⃣ 단계        │    │  3️⃣ 단계        │    │  4️⃣ 단계        │
│  Customer Info  │ ─→ │  Select Demo    │ ─→ │  Configure      │ ─→ │  Generate       │
│  고객 정보 입력  │    │  데모 시나리오   │    │  인프라 구성     │    │  생성           │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

### 1️⃣ 단계: 고객 정보 입력

고객 URL과 기본 정보를 입력하면 AI가 자동으로 산업을 분석하고 맞춤형 데모를 생성합니다.

![Step 1: Customer Information](images/step1_customer_info.png)

| 필드 | 설명 | 예시 |
|------|------|------|
| **Company URL** | 고객 웹사이트 URL | `https://www.daisomall.co.kr/` |
| **Team Members / Audience** | 데모 대상 청중 | `data team, CTO` |
| **Records per Table** | 테이블당 생성할 레코드 수 (20-10,000) | `1000` |
| **Specific Use Cases** | 특정 요구사항 (선택) | `Customer 360, 신제품 트렌드 분석` |
| **Content Language** | 생성될 콘텐츠 언어 | `Korean (한국어)` |

#### 🎯 Target Questions (고급 기능)

특정 질문에 답할 수 있는 데이터가 필요하다면, **Target Questions** 섹션을 활용하세요.

![Target Questions](images/step1_target_questions.png)

- 원하는 질문을 입력하면 AI가 해당 질문에 답할 수 있는 데이터 구조를 설계합니다
- 예시: "25-34세 연령대 사용자의 비율은?", "Q4에 가장 높은 매출 성장을 보인 제품은?"

---

### 2️⃣ 단계: 데모 시나리오 선택

AI가 3가지 맞춤형 데모 시나리오를 자동 생성합니다. 각 시나리오는 다음을 포함합니다:

![Step 2: Demo Selection](images/step2_demo_selection.png)

| 구성 요소 | 설명 |
|----------|------|
| **데모 제목** | 산업에 맞는 시나리오 이름 |
| **설명** | 데모의 상세 설명 |
| **Industry Focus** | 산업 분야 (예: 전자상거래/리테일) |
| **Business Value** | 비즈니스 가치 (예: 매출 증대, 고객 만족도 향상) |
| **Data Tables** | 생성될 테이블 구조 (Fact/Dimension 테이블) |

**📊 테이블 예시:**
- `FACT_ORDERS` - 주문 팩트 테이블
- `DIM_CUSTOMERS` - 고객 차원 테이블
- `DIM_PRODUCTS` - 상품 차원 테이블
- `CUSTOMER_REVIEWS` - 비정형 텍스트 데이터

---

### 3️⃣ 단계: 인프라 구성

생성할 인프라 옵션을 선택합니다.

![Step 3: Configuration](images/step3_configuration.png)

| 옵션 | 설명 |
|------|------|
| **Schema Name** | 자동 생성 (회사명_DEMO_타임스탬프) |
| **📊 Create Semantic View** | Cortex Analyst용 Semantic View 생성 |
| **🔍 Create Cortex Search Service** | 비정형 텍스트 검색 서비스 생성 |
| **🤖 Create AI Agent** | Snowflake Intelligence Agent 생성 |

> 💡 **Tip:** 모든 기능을 시연하려면 세 옵션 모두 선택하세요!

---

### 4️⃣ 단계: 생성

`🚀 Create Demo Infrastructure` 버튼을 클릭하면 자동으로 모든 인프라가 생성됩니다.

![Step 4: Infrastructure Progress](images/step4_infrastructure_progress.png)

**생성 과정:**
1. ✅ Schema 생성
2. ✅ Fact 테이블 생성 및 데이터 삽입
3. ✅ Dimension 테이블 생성 (70% 조인 오버랩)
4. ✅ 비정형 텍스트 테이블 생성
5. ✅ Semantic View 생성 (선택 시)
6. ✅ Cortex Search Service 생성 (선택 시)
7. ✅ AI Agent 생성 (선택 시)
8. ✅ 샘플 질문 자동 생성

> ⏱️ **예상 소요 시간:** 1,000 레코드 기준 약 2-3분

---

## ⚙️ 설정 파일

### `environment.yml`

Streamlit in Snowflake 의존성 정의 파일입니다:

```yaml
name: SI_DEMO_BUILDER
channels:
- snowflake
dependencies:
- streamlit=1.50.0
```

---

## 📁 프로젝트 구조

```
AI_Intelligence_Demo_Generator/
├── 📄 SI_Generator.py                   # 메인 앱 (SiS 배포용)
├── 📄 demo_content.py                   # 데모 콘텐츠 생성 로직
├── 📄 errors.py                         # 에러 처리 및 로깅
├── 📄 infrastructure.py                 # Snowflake 인프라 생성
├── 📄 metrics.py                        # 메트릭 수집 및 추적
├── 📄 prompts.py                        # LLM 프롬프트 템플릿
├── 📄 styles.py                         # Streamlit UI 스타일
├── 📄 utils.py                          # 유틸리티 함수
├── 📄 Setup.sql                         # Snowflake 초기 설정 SQL
├── 📄 environment.yml                   # SiS 의존성 정의
├── 📄 README.md                         # 이 문서
└── 📁 images/                           # README 미디어 파일
    ├── 🎬 demo_walkthrough.mov          # 전체 데모 워크스루 영상
    ├── step1_customer_info.png          # 1단계: 고객 정보 입력
    ├── step1_target_questions.png       # Target Questions 기능
    ├── step2_demo_selection.png         # 2단계: 데모 시나리오 선택
    ├── step3_configuration.png          # 3단계: 인프라 구성
    └── step4_infrastructure_progress.png # 4단계: 생성 진행
```

### 파일별 역할

| 파일 | 역할 |
|------|------|
| `SI_Generator.py` | Streamlit in Snowflake 메인 앱 |
| `demo_content.py` | LLM 기반 데모 시나리오 및 데이터 생성 |
| `infrastructure.py` | Semantic View, Search Service, Agent 생성 |
| `prompts.py` | Claude 4 Sonnet용 프롬프트 템플릿 |
| `utils.py` | 언어 설정, 공통 유틸리티 함수 |
| `errors.py` | 에러 코드 및 예외 처리 |
| `styles.py` | CSS 스타일 및 UI 컴포넌트 |
| `metrics.py` | 실행 시간 및 성능 메트릭 |

---

## ⚠️ 제약 사항

### Snowflake 관련

| 제약 | 설명 |
|------|------|
| **Cortex 리전** | Cortex 함수가 지원되는 리전에서만 동작 |
| **ACCOUNTADMIN** | Agent 생성에 ACCOUNTADMIN 권한 필요 |
| **Semantic View 이름** | `-` 특수문자 사용 불가 (자동으로 `_`로 변환) |
| **Cross-Region** | `CORTEX_ENABLED_CROSS_REGION` 설정 필요 |

### 데이터 관련

| 제약 | 설명 |
|------|------|
| **레코드 수** | 테이블당 최소 20개, 최대 10,000개 |
| **테이블 수** | 데모당 3-5개 구조화 테이블 + 1개 비구조화 테이블 |
| **조인 오버랩** | 테이블 간 70% 조인 키 오버랩 |

### 비용 관련

| 항목 | 예상 비용 |
|------|----------|
| **Cortex LLM** | Claude 4 Sonnet 호출당 크레딧 소비 |
| **Warehouse** | 데모 생성 중 웨어하우스 크레딧 소비 |
| **Storage** | 생성된 데이터 저장 비용 |

---

## 🔧 문제 해결

### 일반적인 오류

#### 1. `Cortex function not available`

```sql
-- Cortex Cross-Region 활성화
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

#### 2. `Permission denied: CREATE AGENT`

```sql
-- Agent 생성 권한 부여
USE ROLE ACCOUNTADMIN;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
```

#### 3. Semantic View 이름 오류 (특수문자)

회사 이름에 `-`가 포함된 경우 자동으로 `_`로 변환됩니다.
예: `Acme-Corp` → `ACME_CORP_SEMANTIC_VIEW_SEMANTIC_MODEL`

---

## 🧹 정리 (Cleanup)

생성된 데모 환경 삭제:

```sql
-- 데모 Schema 삭제
DROP SCHEMA IF EXISTS DEMO_DB.[COMPANY]_DEMO_[DATE];

-- Agent 삭제
DROP AGENT IF EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS.[AGENT_NAME];

-- 전체 초기화 (주의!)
DROP DATABASE IF EXISTS DEMO_DB;
DROP DATABASE IF EXISTS SNOWFLAKE_INTELLIGENCE;
```

---

## 📚 참고 자료

- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Cortex Search Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Snowflake Intelligence Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-intelligence)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

---

## 📄 라이선스

이 프로젝트는 내부 사용을 위해 개발되었습니다.

---

## 🤝 기여

문제 보고 또는 개선 제안은 이슈를 생성해 주세요.

---

**Made with ❄️ Snowflake & 🤖 Claude 4 Sonnet**


-- ================================================================================
-- SI Data Generator Setup Script
-- 
-- This script sets up the complete environment for the SI Data Generator
-- Streamlit application including database, permissions, stage for files,
-- and the Streamlit app itself.
--
-- Requirements:
-- - ACCOUNTADMIN role to create roles, databases, and integrations
-- - Application files uploaded to the stage (instructions below)
-- ================================================================================

USE ROLE ACCOUNTADMIN;

-- Set query tag for infrastructure deployment telemetry
ALTER SESSION SET QUERY_TAG = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"deploy_infrastructure"}}';

-- ================================================================================
-- 1. CREATE DATABASES AND SCHEMAS
-- ================================================================================

-- Create main demo database
CREATE DATABASE IF NOT EXISTS DEMO_DB
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_database"}}';

-- Create schema for the application
CREATE SCHEMA IF NOT EXISTS DEMO_DB.APPLICATIONS
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_schema"}}';

-- Create schema for demo data (this will be populated by the app)
CREATE SCHEMA IF NOT EXISTS DEMO_DB.DEMO_DATA
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_schema"}}';

-- ================================================================================
-- 1.5. CREATE HISTORY TRACKING TABLE
-- ================================================================================

-- Create history table to track all demo generations
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
)
COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_table"}}';



-- Create Snowflake Intelligence database and schema for agent discovery
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_database"}}';

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_schema"}}';

-- Grant usage to allow agent creation and discovery
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;

-- ================================================================================
-- 2. CREATE COMPUTE RESOURCES
-- ================================================================================

-- Set query tag for warehouse creation
ALTER SESSION SET QUERY_TAG = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_warehouse"}}';

-- Create warehouse for the application
CREATE WAREHOUSE IF NOT EXISTS SI_DEMO_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_warehouse"}}';

-- Create compute warehouse for Cortex operations
CREATE WAREHOUSE IF NOT EXISTS compute_wh
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_warehouse"}}';

-- ================================================================================
-- 3. ENABLE CORTEX AND VERIFY SETUP
-- ================================================================================
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ================================================================================
-- 4. CREATE STAGE FOR APPLICATION FILES
-- ================================================================================

-- Set query tag for application deployment
ALTER SESSION SET QUERY_TAG = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"deploy_application"}}';

-- Switch to the application schema
USE SCHEMA DEMO_DB.APPLICATIONS;

-- Create internal stage for Streamlit application files
CREATE OR REPLACE STAGE SI_DATA_GENERATOR_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_stage"}}';

-- Grant privileges on the stage
GRANT READ, WRITE ON STAGE SI_DATA_GENERATOR_STAGE TO ROLE ACCOUNTADMIN;


-- ================================================================================
-- 5. NETWORK POLICY & EXTERNAL ACCESS INTEGRATION
-- ================================================================================

-- Set query tag for network integration creation
ALTER SESSION SET QUERY_TAG = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_network_integration"}}';

-- NOTE: External Access Integration requires ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO_DB;
USE SCHEMA APPLICATIONS;

-- Create network rule for Naver API access
CREATE OR REPLACE NETWORK RULE DEMO_DB.APPLICATIONS.NAVER_API_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('openapi.naver.com:443')
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_network_rule"}}';

-- Create external access integration for Naver API
-- This integration is required for Custom Tools to call external APIs
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION NAVER_API_INTEGRATION
    ALLOWED_NETWORK_RULES = (DEMO_DB.APPLICATIONS.NAVER_API_NETWORK_RULE)
    ENABLED = TRUE
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_external_access_integration"}}';

-- Create additional integration for simple access (used by some procedures)
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION NAVER_API_SIMPLE_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (DEMO_DB.APPLICATIONS.NAVER_API_NETWORK_RULE)
    ENABLED = TRUE
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_external_access_integration"}}';

-- Grant usage on integrations to ACCOUNTADMIN
GRANT USAGE ON INTEGRATION NAVER_API_INTEGRATION TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION NAVER_API_SIMPLE_ACCESS_INTEGRATION TO ROLE ACCOUNTADMIN;

-- ================================================================================
-- 6. CUSTOM FUNCTIONS FOR CORTEX AGENT
-- ================================================================================

-- Set query tag for custom function creation
ALTER SESSION SET QUERY_TAG = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"create_custom_functions"}}';

-- NOTE: These functions require External Access Integration for Naver API
-- NAVER_API_INTEGRATION and NAVER_API_SIMPLE_ACCESS_INTEGRATION are created in Section 5

-- NAVER NEWS SEARCH FUNCTION
-- Searches Naver news articles using Naver Open API
CREATE OR REPLACE FUNCTION DEMO_DB.APPLICATIONS.NAVER_NEWS_SEARCH("QUERY" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'search_news'
EXTERNAL_ACCESS_INTEGRATIONS = (NAVER_API_INTEGRATION)
AS '
import requests
import json
from datetime import datetime

def search_news(query: str, start_date: str = None, end_date: str = None):
    """
    Search Naver news articles using Naver Open API
    Results are sorted by relevance (similarity) to search keyword
    
    Parameters:
    - query: Search keyword (required)
    - start_date: Start date filter in ''YYYY-MM-DD'' format (optional, use None or empty string)
    - end_date: End date filter in ''YYYY-MM-DD'' format (optional, use None or empty string)
    
    Returns:
    - JSON object with news articles
    """
    
    # Naver API credentials
    client_id = "dje4dWjcjqhKwdi2aOyU"
    client_secret = "_h2za7XAbE"
    
    # API endpoint
    url = "https://openapi.naver.com/v1/search/news.json"
    
    # Request headers
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    
    # Handle None and empty string for optional parameters
    if start_date in (None, '''', ''NULL'', ''null''):
        start_date = None
    if end_date in (None, '''', ''NULL'', ''null''):
        end_date = None
    
    # Parse date filters if provided
    start_datetime = None
    end_datetime = None
    
    if start_date:
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        except:
            pass
    
    if end_date:
        try:
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
            # Set to end of day
            end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
        except:
            pass
    
    try:
        all_items = []
        
        # Fetch up to 100 results (maximum per request)
        # If we need date filtering, we''ll fetch more and filter client-side
        max_results = 100
        
        params = {
            "query": query,
            "display": max_results,
            "start": 1,
            "sort": "sim"  # Fixed to relevance sort (관련도순)
        }
        
        # Make API request
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Extract items from response
        items = data.get("items", [])
        
        # Filter by date if date range is specified
        filtered_items = []
        for item in items:
            pub_date_str = item.get("pubDate", "")
            
            # Apply date filter if specified
            if start_datetime or end_datetime:
                try:
                    # Parse Naver''s date format: "Mon, 01 Jan 2024 10:30:00 +0900"
                    item_date = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %z")
                    item_date_naive = item_date.replace(tzinfo=None)
                    
                    # Check date range
                    if start_datetime and item_date_naive < start_datetime:
                        continue
                    if end_datetime and item_date_naive > end_datetime:
                        continue
                    
                    filtered_items.append(item)
                except:
                    # If date parsing fails, include the item
                    filtered_items.append(item)
            else:
                # No date filter, include all items
                filtered_items.append(item)
        
        # Convert to list of dictionaries for JSON output
        if not filtered_items:
            # Return empty result if no items found
            results = [{
                "title": "No results found",
                "original_link": "",
                "link": "",
                "description": "",
                "pub_date": ""
            }]
        else:
            results = []
            for item in filtered_items:
                results.append({
                    "title": item.get("title", ""),
                    "original_link": item.get("originallink", ""),
                    "link": item.get("link", ""),
                    "description": item.get("description", ""),
                    "pub_date": item.get("pubDate", "")
                })
        
        # Return JSON object
        return {
            "success": True,
            "count": len(results),
            "query": query,
            "start_date": start_date,
            "end_date": end_date,
            "items": results
        }
        
    except requests.exceptions.RequestException as e:
        # Handle API request errors
        return {
            "success": False,
            "error": f"API Error: {str(e)}",
            "query": query,
            "items": []
        }
    except Exception as e:
        # Handle other errors
        return {
            "success": False,
            "error": f"Error: {str(e)}",
            "query": query,
            "items": []
        }
';

-- NAVER SHOPPING COMPARE PRICE PROCEDURE
-- Compares product prices across different sellers on Naver Shopping
CREATE OR REPLACE PROCEDURE DEMO_DB.APPLICATIONS.NAVER_SHOPPING_COMPARE_PRICE("KEYWORD" VARCHAR, "PRODCUT_ID" VARCHAR DEFAULT null, "CATEGORY_FILTER" VARCHAR DEFAULT null, "TOP_N" NUMBER(38,0) DEFAULT 100)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('requests','snowflake-snowpark-python')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (NAVER_API_SIMPLE_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '

import json
from datetime import datetime, date
from typing import Dict, Any, List, Optional

import requests

CLIENT_ID = "BafjwZSJD5Chxkvvo0Of"
CLIENT_SECRET = "hz2J8Ue7kz"


NAVER_SHOP_SEARCH_URL = "https://openapi.naver.com/v1/search/shop"

def _clean_title(text: str) -> str:
    """
    상품명 정리: HTML 태그 제거 및 공백 정리
    """
    if not text:
        return ""
    
    # HTML 태그 제거
    import re
    text = re.sub(r''<[^>]+>'', '''', text)
    
    # 연속된 공백을 하나로 정리
    text = re.sub(r''\\s+'', '' '', text)
    
    return text.strip()

def search_naver_shopping(
    keyword: str,
    display: int = 100,
    start: int = 1,
    sort: str = "asc",
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    timeout: float = 10.0,
    debug: bool = True,
) -> dict:
    cid = client_id or "BafjwZSJD5Chxkvvo0Of"
    csec = client_secret or "hz2J8Ue7kz"

    headers = {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
        "Accept": "application/json",
        "User-Agent": "naver-price-compare/1.0",
    }
    params = {
        "query": keyword,
        "display": max(1, min(display, 100)),
        "start": max(1, min(start, 1000)),
        "sort": sort,
    }

    if debug:
        print(f"🔍 네이버 쇼핑 검색: ''{keyword}''")
        sort_desc = {
            "sim": "유사도순(키워드 정확도)",
            "date": "날짜순", 
            "asc": "가격 오름차순",
            "dsc": "가격 내림차순"
        }
        print(f"📊 요청 파라미터: display={params[''display'']}, start={params[''start'']}, sort={params[''sort'']}({sort_desc.get(params[''sort''], params[''sort''])})")

    resp = requests.get(NAVER_SHOP_SEARCH_URL, headers=headers, params=params, timeout=timeout)

    if resp.status_code != 200:
        # 실패 시 진단 정보 (비밀키는 마스킹)
        diag = {
            "status_code": resp.status_code,
            "reason": resp.reason,
            "request_url": resp.url,
            "sent_headers": {k: v if "Client-Secret" not in k else "******" + v[-4:] for k, v in headers.items()},
            "response_text": resp.text[:500],
        }
        if debug:
            print(json.dumps(diag, ensure_ascii=False, indent=2))

        if resp.status_code == 401:
            raise RuntimeError(
                "네이버 검색 API 인증 실패(401)입니다. "
                "1) 내 애플리케이션에서 ''검색 API'' 사용이 켜져 있는지, "
                "2) Client ID/Secret에 공백/개행이 없는지, "
                "3) 헤더가 프록시에서 제거되지 않는지(curl로 재현) 확인하세요."
            )
        elif resp.status_code == 403:
            raise RuntimeError(
                "네이버 검색 API 접근 금지(403)입니다. "
                "API 사용량 한도를 초과했거나 권한이 없습니다."
            )
        elif resp.status_code == 429:
            raise RuntimeError(
                "네이버 검색 API 호출 한도 초과(429)입니다. "
                "잠시 후 다시 시도해주세요."
            )
        resp.raise_for_status()

    result = resp.json()
    
    if debug:
        total_count = result.get(''total'', 0)
        items_count = len(result.get(''items'', []))
        print(f"✅ 검색 성공: 전체 {total_count:,}개 상품 중 {items_count}개 반환")

    return result

def filter_items_by_category(
    items: List[Dict[str, Any]], 
    category_filter: Optional[str] = None,
    debug: bool = True
) -> List[Dict[str, Any]]:
    """
    카테고리별로 상품을 필터링
    
    Args:
        items: 상품 리스트
        category_filter: 카테고리 필터 (예: "패션의류>남성의류>티셔츠")
        debug: 디버그 모드
    
    Returns:
        필터링된 상품 리스트
    """
    if not category_filter:
        return items
    

    filter_parts = [part.strip() for part in category_filter.split(''>'')]
    
    if debug:
        print(f"🔍 카테고리 필터 적용: {category_filter}")
        print(f"📋 필터 계층: {filter_parts}")
    
    filtered_items = []
    
    for item in items:

        categories = [
            item.get("category1", ""),
            item.get("category2", ""),
            item.get("category3", ""),
            item.get("category4", "")
        ]
        
        item_categories = [cat.strip() for cat in categories if cat and cat.strip()]
        

        match_count = 0
        for i, filter_part in enumerate(filter_parts):
            if i < len(item_categories):

                if filter_part.lower() in item_categories[i].lower():
                    match_count += 1
                else:
                    break
        

        if match_count == len(filter_parts):
            filtered_items.append(item)
            if debug and len(filtered_items) <= 3:
                print(f"✅ 매칭: {item.get(''title'', '''')[:50]}... -> {item_categories}")
    
    if debug:
        print(f"📊 필터링 결과: {len(items)}개 -> {len(filtered_items)}개")
    
    return filtered_items


def build_price_compare_result(
    keyword: str,
    items: List[Dict[str, Any]],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category_filter: Optional[str] = None
) -> Dict[str, Any]:
    """
    검색 결과(items)를 바탕으로 판매처(mallName)별 최저가 리스트를 구성.
    - 동일 판매처에서 여러 결과가 있을 때 최저가만 채택
    - productType==1(가격비교 카탈로그) 항목이 있으면 카탈로그 링크를 힌트로 포함
    - API 한계로 ''정확히 동일 스펙'' 보장은 불가 (제약 설명 참고)
    """
    today = date.today().isoformat()
    start_date = start_date or today
    end_date = end_date or today


    if category_filter:
        items = filter_items_by_category(items, category_filter, debug=True)
        if not items:
            return {
                "keyword": keyword,
                "category_filter": category_filter,
                "min_price": 0,
                "max_price": 0,
                "avg_price": 0,
                "seller_count": 0,
                "analysis_period": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "data": {
                    "source": "naver_search_api",
                    "sellers": [],
                    "catalog_hint": None,
                    "note": f"카테고리 ''{category_filter}''에 해당하는 상품이 없습니다."
                }
            }

    sellers: Dict[str, Dict[str, Any]] = {}
    catalog_hint = None
    
    print(f"📦 {len(items)}개 상품 데이터 처리 중...")
    

    if len(items) > 0:
        print(f"\\n📋 상위 5개 상품의 카테고리 정보:")
        for i, item in enumerate(items[:5]):
            title = _clean_title(item.get("title", ""))
            categories = [
                item.get("category1", ""),
                item.get("category2", ""),
                item.get("category3", ""),
                item.get("category4", "")
            ]

            categories = [cat.strip() for cat in categories if cat and cat.strip()]
            print(f"  {i+1}. {title[:50]}...")
            print(f"     카테고리: {categories}")
        print()

    for it in items:
        title_raw = it.get("title", "")
        title = _clean_title(title_raw)
        mall = it.get("mallName") or "네이버"
        try:
            price = int(it.get("lprice") or 0)
        except Exception:
            price = 0

        product_id = it.get("productId")
        product_type = it.get("productType")
        link = it.get("link")
        image = it.get("image")


        if str(product_type) == "1" and product_id and not catalog_hint:
            catalog_hint = {
                "product_id": product_id,

                "catalog_link_hint": f"https://search.shopping.naver.com/catalog/{product_id}"
            }


        current = sellers.get(mall)
        rec = {
            "seller": mall,
            "price": price,
            "title": _clean_title(title_raw),
            "product_id": product_id,
            "product_type": product_type,
            "link": link,
            "image": image,
            "brand": it.get("brand"),
            "maker": it.get("maker"),
            "categories": [it.get("category1"), it.get("category2"), it.get("category3"), it.get("category4")],
        }
        if current is None or (0 < price < current.get("price", 1 << 60)):
            sellers[mall] = rec

    seller_list = sorted(
        [v for v in sellers.values() if v.get("price", 0) > 0],
        key=lambda x: x["price"]
    )

    min_price = seller_list[0]["price"] if seller_list else 0
    max_price = max([v["price"] for v in seller_list]) if seller_list else 0
    avg_price = sum([v["price"] for v in seller_list]) / len(seller_list) if seller_list else 0

    print(f"💰 가격 분석 완료:")
    print(f"   - 판매처 수: {len(seller_list)}개")
    print(f"   - 최저가: {min_price:,}원")
    print(f"   - 최고가: {max_price:,}원") 
    print(f"   - 평균가: {avg_price:,.0f}원")

    result = {
        "keyword": keyword,
        "category_filter": category_filter,
        "min_price": min_price,
        "max_price": max_price,
        "avg_price": int(avg_price),
        "seller_count": len(seller_list),
        "analysis_period": {
            "start_date": start_date,
            "end_date": end_date
        },
        "timestamp": datetime.now().isoformat(),
        "status": "success",
        "data": {
            "source": "naver_search_api",
            "sellers": seller_list,
            "catalog_hint": catalog_hint,
            "note": (
                "공식 OpenAPI 결과를 키워드 정확도 기준으로 가져온 뒤 가격 오름차순으로 재정렬하여 판매처별 최저가를 집계한 값입니다. "
                "옵션/사이즈/모델 차이에 따라 동일 제품과 100% 일치하지 않을 수 있습니다."
            )
        }
    }
    return result

def run_compare(
    keyword: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    category_filter: Optional[str] = None,
    enable_price_sort: bool = True
) -> Dict[str, Any]:

    data = search_naver_shopping(
        keyword=keyword,
        display=100,
        start=1,
        sort="sim",
        client_id=client_id,
        client_secret=client_secret
    )

    items = data.get("items", [])
    

    if enable_price_sort and items:
        print(f"🔄 2단계: 키워드 정확도 상품들을 가격 오름차순으로 재정렬 중...")
        

        valid_items = []
        for item in items:
            try:
                price = int(item.get("lprice", 0))
                if price > 0:
                    item[''_sort_price''] = price
                    valid_items.append(item)
            except (ValueError, TypeError):
                continue
        

        valid_items.sort(key=lambda x: x[''_sort_price''])
        
        print(f"📊 가격 정렬 완료: {len(items)}개 → {len(valid_items)}개 (유효 가격 상품)")
        items = valid_items

    return build_price_compare_result(keyword, items, start_date, end_date, category_filter)


def run_compare_by_product_id(
    product_id: str,
    query: str = "커버낫 어센틱 로고 후디 집업",
    category_filter: Optional[str] = None,
    enable_price_sort: bool = True,
    debug: bool = True
) -> Dict[str, Any]:
    """
    상품 ID로 판매처별 가격 비교 (검색 키워드 활용)
    
    네이버 쇼핑에서 직접 카탈로그 접근이 제한되므로,
    유사한 키워드로 검색하여 판매처별 가격 정보를 수집합니다.
    """
    if debug:
        print(f"🎯 상품 ID {product_id}로 가격비교 실행 중...")
        print(f"🔍 검색 키워드: ''{query}''")
        if category_filter:
            print(f"📂 카테고리 필터: ''{category_filter}''")
        print(f"🔄 2단계 가격 정렬: {''✅ 활성화'' if enable_price_sort else ''❌ 비활성화''}")
        print("=" * 60)
    
    try:

        result = run_compare(query, category_filter=category_filter, enable_price_sort=enable_price_sort)
        
        if result.get(''status'') == ''success'':

            result[''product_id''] = product_id
            result[''search_method''] = ''keyword_based''
            result[''note''] = f"상품 ID {product_id}에 대해 키워드 ''{query}'' 검색 결과입니다."
            
            if debug:
                print(f"\\n💰 가격비교 결과:")
                print(f"   - 상품 ID: {product_id}")
                print(f"   - 검색 키워드: {query}")
                print(f"   - 판매처 수: {result[''seller_count'']}개")
                print(f"   - 최저가: {result[''min_price'']:,}원")
                print(f"   - 최고가: {result[''max_price'']:,}원")
                print(f"   - 평균가: {result[''avg_price'']:,}원")
        
        return result
        
    except Exception as e:
        return {
            ''status'': ''error'',
            ''error'': f''가격비교 실행 중 오류: {str(e)}'',
            ''product_id'': product_id,
            ''query'': query
        }


def main(keyword: str, product_id: str, category_filter:str, top_n: int = 100 ):
    """
    네이버 쇼핑 가격비교 메인 함수
    
    Args:
        keyword: 검색할 키워드
        product_id: 상품 ID
        top_n: 상위 N개 결과 표시 (기본값: 10)
    """
    print("🎯 네이버 쇼핑 가격비교 테스트 시작")
    print("=" * 60)
    print(f"🔍 검색 키워드: {keyword}")
    print(f"🆔 상품 ID: {product_id}")
    print(f"📊 상위 {top_n}개 결과 표시")
    print("=" * 60)


    category_filter = category_filter
    enable_price_sort = True
    
    try:
        print(f"🚀 상품 ID ''{product_id}'' 가격비교 실행 중...")
        print("-" * 50)
        

        out = run_compare_by_product_id(
            product_id=product_id, 
            query=keyword, 
            category_filter=category_filter, 
            enable_price_sort=enable_price_sort
        )
        
        print("\\n" + "="*60)
        print("🛍️  네이버 쇼핑 상품 ID 가격비교 결과")
        print("="*60)
        
        if out.get("status") == "success":
            print(f"🔍 상품 ID: {out.get(''product_id'', ''N/A'')}")
            print(f"🔍 검색어: {out[''keyword'']}")
            if out.get(''category_filter''):
                print(f"📂 카테고리 필터: {out[''category_filter'']}")
            print(f"💰 최저가: {out[''min_price'']:,}원")
            print(f"💰 최고가: {out[''max_price'']:,}원")
            print(f"💰 평균가: {out[''avg_price'']:,}원")
            print(f"🏪 판매처 수: {out[''seller_count'']}개")
            
            sellers = out.get("data", {}).get("sellers", [])
            if sellers:
                print(f"\\n📊 판매처별 최저가 TOP {top_n}:")
                print("-" * 80)
                print(f"{''순위'':<4} {''판매처'':<15} {''가격'':<12} {''상품명''}")
                print("-" * 80)
                

                for i, seller in enumerate(sellers[:top_n], 1):
                    price_str = f"{seller[''price'']:,}원"
                    title = seller[''title''][:40] + "..." if len(seller[''title'']) > 40 else seller[''title'']
                    print(f"{i:<4} {seller[''seller''][:15]:<15} {price_str:<12} {title}")
        else:
            print(f"❌ 오류: {out.get(''error'', ''알 수 없는 오류'')}")

        print(f"\\n📄 전체 JSON 결과:")
        print(json.dumps(out, ensure_ascii=False, indent=2))
        
        # 요청된 포맷으로 결과 반환
        if out.get("status") == "success":
            result = {
                ''keyword'': keyword,
                ''min_price'': out.get(''min_price'', 0),
                ''analysis_period'': out.get(''analysis_period'', {
                    ''start_date'': datetime.now().date().isoformat(),
                    ''end_date'': datetime.now().date().isoformat()
                }),
                ''timestamp'': datetime.now().isoformat(),
                ''status'': ''success'',
                ''data'': out.get(''data'', {})
            }
        else:
            result = {
                ''keyword'': keyword,
                ''min_price'': 0,
                ''analysis_period'': {
                    ''start_date'': datetime.now().date().isoformat(),
                    ''end_date'': datetime.now().date().isoformat()
                },
                ''timestamp'': datetime.now().isoformat(),
                ''status'': ''error'',
                ''data'': {''error'': out.get(''error'', ''알 수 없는 오류'')}
            }
        
        return result

    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        
        # 예외 발생 시에도 요청된 포맷으로 반환
        result = {
            ''keyword'': keyword,
            ''min_price'': 0,
            ''analysis_period'': {
                ''start_date'': datetime.now().date().isoformat(),
                ''end_date'': datetime.now().date().isoformat()
            },
            ''timestamp'': datetime.now().isoformat(),
            ''status'': ''error'',
            ''data'': {''error'': str(e)}
        }
        return result

';


-- NAVER SHOPPING TRENDS PROCEDURE
-- Analyzes shopping trends including price analysis and age demographics
CREATE OR REPLACE PROCEDURE DEMO_DB.APPLICATIONS.NAVER_SHOPPING_TRENDS("KEYWORD" VARCHAR, "MIN_PRICE" NUMBER(38,0) DEFAULT 5000, "START_DATE" VARCHAR DEFAULT null, "END_DATE" VARCHAR DEFAULT null, "TOP_N" NUMBER(38,0) DEFAULT 10)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('requests','snowflake-snowpark-python')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (NAVER_API_SIMPLE_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
import requests
import json
import urllib.parse
from datetime import datetime, timedelta

# 네이버 API 키 설정
# 재한님 id
CLIENT_ID = "BafjwZSJD5Chxkvvo0Of"
CLIENT_SECRET = "hz2J8Ue7kz"
#CLIENT_ID = "Uz0liEN_mhrgasgHblxX"
#CLIENT_SECRET = "QxNRfPqxfv"


# 카테고리 코드 매핑
CATEGORY_CODES = {
    "패션의류": "50000000",
    "화장품": "50000002", 
    "전자제품": "50000003",
    "가구": "50000004",
    "육아": "50000005",
    "식품": "50000006",
    "스포츠": "50000007",
    "생활": "50000008",
    "여행": "50000009",
    "자동차": "50000010"
}

# 키워드와 카테고리 매핑
KEYWORD_TO_CATEGORY = {
    "후드티": "패션의류", "맨투맨": "패션의류", "청바지": "패션의류", "원피스": "패션의류",
    "정장": "패션의류", "코트": "패션의류", "자켓": "패션의류", "운동복": "패션의류",
    "립스틱": "화장품", "파운데이션": "화장품", "마스카라": "화장품", "아이섀도": "화장품",
    "맥북": "전자제품", "아이폰": "전자제품", "갤럭시": "전자제품", "아이패드": "전자제품",
    "운동화": "스포츠", "구두": "패션의류", "부츠": "패션의류"
}

def calculate_default_dates():
    """기본 분석 기간 계산 (직전 3개월)"""
    today = datetime.now()
    
    # 직전 달의 마지막 날을 end_date로 설정
    if today.month == 1:
        end_date = f"{today.year - 1}-12-31"
    else:
        prev_month = today.month - 1
        year = today.year
        # 해당 월의 마지막 날 계산
        if prev_month in [1, 3, 5, 7, 8, 10, 12]:
            last_day = 31
        elif prev_month in [4, 6, 9, 11]:
            last_day = 30
        else:  # 2월
            last_day = 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28
        end_date = f"{year}-{prev_month:02d}-{last_day}"
    
    # 3개월 전을 start_date로 설정
    start_month = today.month - 3
    start_year = today.year
    if start_month <= 0:
        start_month += 12
        start_year -= 1
    start_date = f"{start_year}-{start_month:02d}-01"
    
    return start_date, end_date

def search_products(keyword, min_price, top_n):
    """네이버 쇼핑 검색 API로 상품 검색"""
    try:
        print(f"''{keyword}'' 상품 검색 중...")
        
        encoded_query = urllib.parse.quote(keyword)
        url = f"https://openapi.naver.com/v1/search/shop?query={encoded_query}&display={min(top_n * 2, 100)}&sort=sim"
        
        headers = {
            ''X-Naver-Client-Id'': CLIENT_ID,
            ''X-Naver-Client-Secret'': CLIENT_SECRET
        }
        
        print(f"API 요청: {url}")
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        items = data.get(''items'', [])
        
        print(f"검색 결과: 총 {data.get(''total'', 0):,}개 상품")
        
        if not items:
            return None
        
        # 최소 가격 이상 상품만 필터링
        valid_items = []
        for item in items:
            price = item.get(''lprice'', 0)
            if price and str(price).isdigit() and int(price) >= min_price:
                valid_items.append({
                    ''title'': item.get(''title'', '''').replace(''<b>'', '''').replace(''</b>'', ''''),
                    ''price'': int(price),
                    ''mall_name'': item.get(''mallName'', ''쇼핑몰 정보 없음''),
                    ''brand'': item.get(''brand'', ''''),
                    ''category'': item.get(''category1'', ''''),
                    ''link'': item.get(''link'', '''')
                })
        
        if not valid_items:
            print(f"{min_price:,}원 이상의 상품이 없습니다.")
            return None
        
        # 가격순 정렬
        valid_items.sort(key=lambda x: x[''price''])
        
        print(f"필터링된 상품: {len(valid_items)}개")
        
        return {
            ''query'': keyword,
            ''total_results'': data.get(''total'', 0),
            ''displayed_results'': len(valid_items),
            ''lowest_price'': valid_items[0],
            ''top_products'': valid_items[:top_n]
        }
        
    except Exception as e:
        print(f"상품 검색 오류: {str(e)}")
        return {''error'': f"상품 검색 오류: {str(e)}"}

def get_age_trends(keyword, start_date, end_date):
    """네이버 쇼핑인사이트 API로 연령별 트렌드 조회"""
    try:
        print(f"''{keyword}'' 연령별 트렌드 분석 중...")
        
        # 키워드로 카테고리 결정
        category_name = KEYWORD_TO_CATEGORY.get(keyword, "패션의류")
        category_code = CATEGORY_CODES.get(category_name)
        
        if not category_code:
            return {''error'': f"''{keyword}''의 카테고리를 찾을 수 없습니다."}
        
        print(f"카테고리: {category_name} ({category_code})")
        print(f"분석 기간: {start_date} ~ {end_date}")
        
        # API 요청 데이터
        request_data = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": "month",
            "category": category_code,
            "keyword": keyword,
            "device": "",
            "gender": "",
            "ages": ["10", "20", "30", "40", "50", "60"]
        }
        
        headers = {
            ''X-Naver-Client-Id'': CLIENT_ID,
            ''X-Naver-Client-Secret'': CLIENT_SECRET,
            ''Content-Type'': ''application/json''
        }
        
        url = "https://openapi.naver.com/v1/datalab/shopping/category/keyword/age"
        
        print(f"트렌드 API 요청 중...")
        
        response = requests.post(url, headers=headers, json=request_data, timeout=10)
        response.raise_for_status()
        
        trend_data = response.json()
        
        # 연령별 평균 계산
        if ''results'' in trend_data and trend_data[''results'']:
            result = trend_data[''results''][0]
            data_points = result.get(''data'', [])
            
            if data_points:
                # 연령별 데이터 그룹화 및 평균 계산
                age_totals = {}
                age_counts = {}
                
                for point in data_points:
                    age_group = point.get(''group'', '''')
                    ratio = point.get(''ratio'', 0)
                    if age_group:
                        if age_group not in age_totals:
                            age_totals[age_group] = 0
                            age_counts[age_group] = 0
                        age_totals[age_group] += ratio
                        age_counts[age_group] += 1
                
                # 평균 계산 및 정렬
                age_averages = {}
                for age in age_totals:
                    age_averages[age] = age_totals[age] / age_counts[age] if age_counts[age] > 0 else 0
                
                sorted_ages = sorted(age_averages.items(), key=lambda x: x[1], reverse=True)
                
                print(f"연령별 트렌드 분석 완료")
                
                return {
                    ''category'': category_name,
                    ''age_distribution'': [
                        {
                            ''rank'': i + 1,
                            ''age_group'': f"{age}대",
                            ''interest_ratio'': round(ratio, 1)
                        }
                        for i, (age, ratio) in enumerate(sorted_ages)
                    ],
                    ''primary_target'': {
                        ''age_group'': f"{sorted_ages[0][0]}대",
                        ''interest_ratio'': round(sorted_ages[0][1], 1)
                    } if sorted_ages else None
                }
        
        return {''error'': ''연령별 트렌드 데이터가 없습니다.''}
        
    except Exception as e:
        print(f"연령별 트렌드 분석 오류: {str(e)}")
        return {''error'': f"연령별 트렌드 분석 오류: {str(e)}"}

def main(session, keyword, min_price, start_date, end_date, top_n):
    """메인 프로시저 함수"""
    
    print(f"네이버 쇼핑 분석 시작: ''{keyword}''")
    print(f"   최저가 필터: {min_price:,}원")
    print(f"   상위 상품: {top_n}개")
    
    # 기본 날짜 설정
    if not start_date or not end_date:
        start_date, end_date = calculate_default_dates()
        print(f"   분석 기간: {start_date} ~ {end_date} (기본값)")
    else:
        print(f"   분석 기간: {start_date} ~ {end_date}")
    
    result = {
        ''keyword'': keyword,
        ''min_price'': min_price,
        ''analysis_period'': {
            ''start_date'': start_date,
            ''end_date'': end_date
        },
        ''timestamp'': datetime.now().isoformat(),
        ''status'': ''processing'',
        ''data'': {}
    }
    
    try:
        # 1. 상품 가격 분석
        print("\\n1단계: 상품 가격 분석")
        price_data = search_products(keyword, min_price, top_n)
        
        if price_data and ''error'' not in price_data:
            result[''data''][''price_analysis''] = price_data
            print(f"   최저가: {price_data[''lowest_price''][''price'']:,}원")
            print(f"   상품 수: {price_data[''displayed_results'']}개")
        else:
            result[''data''][''price_analysis''] = price_data or {''error'': ''가격 정보를 찾을 수 없습니다.''}
        
        # 2. 연령별 트렌드 분석
        print("\\n2단계: 연령별 트렌드 분석")
        age_trends = get_age_trends(keyword, start_date, end_date)
        
        if age_trends and ''error'' not in age_trends:
            result[''data''][''age_trends''] = age_trends
            if age_trends.get(''primary_target''):
                primary = age_trends[''primary_target'']
                print(f"   주요 타겟: {primary[''age_group'']} ({primary[''interest_ratio'']}%)")
        else:
            result[''data''][''age_trends''] = age_trends or {''error'': ''연령별 트렌드 데이터를 가져올 수 없습니다.''}
        
        # 3. 종합 결과
        result[''status''] = ''success''
        
        print(f"\\n분석 완료!")
        
        return result
        
    except Exception as e:
        result[''status''] = ''error''
        result[''error''] = str(e)
        print(f"\\n분석 오류: {str(e)}")
        return result

';

-- ================================================================================
-- 7. UPLOAD APPLICATION FILES TO STAGE
-- ================================================================================

/*
IMPORTANT: Before creating the Streamlit app, upload your application files to the stage.

METHOD 1: Using SnowSQL (Command Line)
--------------------------------------
snowsql -a <account> -u <username> -r ACCOUNTADMIN

-- Upload all files from your local directory to the stage
PUT file:///path/to/SI_Data_Generator-main/*.py @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- If you have subdirectories, upload them too (e.g., for any assets or configs)
-- PUT file:///path/to/SI_Data_Generator-main/assets/* @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE/assets AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

METHOD 2: Using Snowsight UI (Web Interface)
--------------------------------------------
1. Navigate to: Data » Databases » DEMO_DB » APPLICATIONS » Stages
2. Click on: SI_DATA_GENERATOR_STAGE
3. Click: "+ Files" button in the top-right
4. Upload all your .py files from the SI_Data_Generator-main directory
   Required files:
   - SI_Generator.py (this is your main file)
   - demo_content.py
   - errors.py
   - infrastructure.py
   - metrics.py
   - prompts.py
   - styles.py
   - utils.py
   - environment.yml (if needed for dependencies)

METHOD 3: Using Python Connector
--------------------------------
import snowflake.connector
from pathlib import Path

conn = snowflake.connector.connect(
    user='<username>',
    password='<password>',
    account='<account>',
    role='ACCOUNTADMIN'
)

cursor = conn.cursor()
cursor.execute("USE SCHEMA DEMO_DB.APPLICATIONS")

# Upload each file
files = Path('/path/to/SI_Data_Generator-main').glob('*.py')
for file in files:
    cursor.execute(f"PUT file://{file} @SI_DATA_GENERATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

conn.close()

VERIFICATION: List files in stage
----------------------------------
*/

-- After uploading, verify files are in the stage:
LIST @SI_DATA_GENERATOR_STAGE;

-- ================================================================================
-- 8. CREATE STREAMLIT APPLICATION
-- ================================================================================

-- Switch to the application schema and warehouse
USE SCHEMA DEMO_DB.APPLICATIONS;
USE WAREHOUSE SI_DEMO_WH;

-- Create the Streamlit application from stage
CREATE OR REPLACE STREAMLIT SI_DATA_GENERATOR_APP
    ROOT_LOCATION = '@DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE'
    MAIN_FILE = 'SI_Generator.py'
    QUERY_WAREHOUSE = SI_DEMO_WH
    COMMENT = '{"origin":"sf_sit-is","name":"si-data-generator","version":{"major":2,"minor":0},"attributes":{"is_quickstart":0,"source":"sql","action":"deploy_streamlit"}}'
    TITLE = 'Snowflake Agent Demo Data Generator';

-- Grant usage on the Streamlit app to ACCOUNTADMIN
GRANT USAGE ON STREAMLIT SI_DATA_GENERATOR_APP TO ROLE ACCOUNTADMIN;

-- ================================================================================
-- 9. ADDITIONAL SETUP FOR CORTEX SEARCH
-- ================================================================================

-- NOTE: No additional setup required for Cortex Search
-- Cortex Search services are created dynamically by the application when:
--   1. Users generate demo data through the Streamlit app
--   2. Enable the "Create Cortex Search Service" option
-- ACCOUNTADMIN role has all necessary privileges to create services
-- Cortex is already enabled via CORTEX_ENABLED_CROSS_REGION setting above

-- ================================================================================
-- 10. SETUP VALIDATION AND INFORMATION
-- ================================================================================

-- Set context for testing
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SI_DEMO_WH;
USE DATABASE DEMO_DB;

-- Show created objects
SHOW DATABASES LIKE 'DEMO_DB';
SHOW SCHEMAS IN DATABASE DEMO_DB;
SHOW WAREHOUSES LIKE 'SI_DEMO_WH';
SHOW STAGES IN SCHEMA DEMO_DB.APPLICATIONS;

-- Test Cortex function access
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-4-sonnet', 
    'Say "SI Data Generator setup is working!" and nothing else.'
) AS test_result;

-- ================================================================================
-- 11. POST-SETUP INSTRUCTIONS
-- ================================================================================

/*
================================================================================
POST-SETUP INSTRUCTIONS
================================================================================

1. FILE UPLOAD (REQUIRED BEFORE FIRST USE):
   - Upload all Python files to: @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE
   - Required files: SI_Generator.py, demo_content.py, errors.py, infrastructure.py,
                    metrics.py, prompts.py, styles.py, utils.py
   - Use one of the methods shown in section 5 above
   - Verify with: LIST @SI_DATA_GENERATOR_STAGE;

2. STREAMLIT APP ACCESS:
   - The Streamlit app is available at: DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_APP
   - Access URL: Go to Snowsight » Streamlit » SI_DATA_GENERATOR_APP
   - Or navigate directly to: https://<account>.snowflakecomputing.com/streamlit/DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_APP

3. UPDATING THE APPLICATION:
   - To update the app, simply re-upload the modified files to the stage:
     PUT file:///path/to/updated_file.py @DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   - The Streamlit app will automatically pick up the changes on next load
   - No need to recreate the Streamlit object

4. USER ACCESS:
   - All operations run with ACCOUNTADMIN privileges
   - No additional role grants needed for basic functionality

5. DEMO USAGE:
   - The app will create schemas under DEMO_DB with pattern: DEMO_DB.[COMPANY]_DEMO_[DATE]
   - Each demo creates tables, semantic views, and Cortex Search services
   - All demo data is isolated by schema for easy cleanup
   
6. AGENT CREATION:
   - Agents are created in SNOWFLAKE_INTELLIGENCE.AGENTS schema
   - Agent naming: [COMPANY]_[DATE]_AGENT (e.g., ACMECORP_20250131_AGENT)
   - Agents appear in Snowsight UI under: AI & ML » Agents
   - Agents persist after demo schema cleanup (manual deletion required if needed)
   - To delete an agent: DROP AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.[AGENT_NAME];

7. CUSTOMIZATION:
   - Modify the Streamlit app by editing Python files locally
   - Re-upload to stage to deploy changes
   - Add new demo templates by modifying the fallback demo ideas
   - Customize warehouse size based on expected usage

8. MONITORING:
   - Monitor warehouse usage and costs
   - Review generated schemas periodically for cleanup
   - Check Cortex function usage for cost optimization

9. PERMISSIONS:
   - All permissions handled by ACCOUNTADMIN role
   - No additional role management required

10. CLEANUP:
    To remove demo data, drop the individual demo schemas:
    DROP SCHEMA DEMO_DB.[COMPANY]_DEMO_[DATE];
    
    To remove agents (if needed):
    DROP AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.[COMPANY]_[DATE]_AGENT;
    
    To remove the stage and all files:
    DROP STAGE DEMO_DB.APPLICATIONS.SI_DATA_GENERATOR_STAGE;

================================================================================
SETUP COMPLETE - NEXT STEP: UPLOAD FILES TO STAGE (see section 5)
================================================================================
*/

-- Final verification
SELECT 'SI Data Generator setup completed successfully! 🎉' AS status,
       'Next step: Upload application files to @SI_DATA_GENERATOR_STAGE' AS next_action,
       CURRENT_ROLE() AS current_role,
       CURRENT_WAREHOUSE() AS current_warehouse,
       CURRENT_DATABASE() AS current_database,
       CURRENT_SCHEMA() AS current_schema;






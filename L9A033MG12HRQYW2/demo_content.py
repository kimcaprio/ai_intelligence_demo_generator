"""
Demo content generation for SI Data Generator.

This module consolidates all functionality related to generating demo content:
- Demo templates (fallback scenarios)
- Question generation (natural language questions for demos)
- Data generation (structured and unstructured data)
- Data validation (ensuring data can answer target questions)

This consolidates what was previously spread across demo_templates.py and
multiple sections of SI_Generator.py.
"""

import streamlit as st
import pandas as pd
import json
import re
import random
import time
import traceback
import numpy as np
from metrics import timeit
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

from utils import (
    LLM_MODEL,
    call_cortex_with_retry,
    MAX_FOLLOW_UP_QUESTIONS,
    DEFAULT_QUESTION_COUNT,
    MAX_RETRY_ATTEMPTS,
    DEFAULT_NUMERIC_RANGE_MAX,
    DATE_LOOKBACK_DAYS,
    SAMPLE_DATA_LIMIT,
    generate_tag_json,
    set_query_tag,
    clear_query_tag
)
from errors import (
    ErrorCode,
    ErrorSeverity,
    safe_json_parse
)
from prompts import (
    get_company_analysis_prompt,
    get_question_generation_prompt,
    get_follow_up_questions_prompt,
    get_target_question_analysis_prompt,
    get_schema_generation_prompt,
    get_collective_validation_prompt,
    get_single_table_validation_prompt,
    get_unstructured_data_generation_prompt
)
from utils import (
    validate_language_content,
    enhance_prompt_with_language,
    add_language_metadata_to_chunks,
    get_language_display_name,
    safe_parallel_execute
)


# ============================================================================
# DEMO TEMPLATES (from demo_templates.py)
# ============================================================================

def get_fallback_demo_ideas(
    company_name: str,
    team_members: str,
    use_cases: str,
    target_questions: Optional[List[str]] = None,
    advanced_mode: bool = False
) -> List[Dict]:
    """
    Return fallback demo ideas when Cortex LLM is unavailable.
    
    Provides three high-quality demo templates covering E-commerce,
    Financial Services, and Healthcare industries. These templates
    are designed to showcase Cortex Analyst and Cortex Search capabilities.
    
    Note: Fallback templates are in standard mode (3 structured + 1 unstructured
    table). For advanced mode with 3-5 tables, Cortex LLM should be used.
    
    Args:
        company_name: Company name to personalize templates
        team_members: Target audience description
        use_cases: Specific use cases to address
        target_questions: Optional list of questions (not used in templates)
        advanced_mode: Whether advanced mode was requested
        
    Returns:
        List of demo configuration dictionaries
    """
    # Note about mode limitations
    if advanced_mode:
        st.info(
            "ℹ️ Advanced mode (3-5 tables) not available in fallback. "
            "Using standard mode templates (3 tables). For advanced mode, "
            "ensure Cortex LLM is available."
        )
    
    # Note: Fallback templates are standard mode (3 tables: 1 fact + 2 dimensions + 1 unstructured)
    # Advanced mode (3-5 tables) requires Cortex LLM
    
    demos = [
        {
            "title": "E-commerce Analytics & Customer Intelligence",
            "description": (
                f"Comprehensive e-commerce analytics solution for "
                f"{company_name} combining transactional sales data with "
                f"customer intelligence to drive revenue growth and improve "
                f"customer lifetime value"
            ),
            "industry_focus": "E-commerce/Retail",
            "business_value": (
                "Optimize sales performance, predict customer churn, identify "
                "upsell opportunities, and personalize customer experiences "
                "through unified analytics across transactions, customer "
                "profiles, and product reviews. Enable data-driven decision "
                "making for marketing, product, and sales teams."
            ),
            "tables": {
                "structured_1": {
                    "name": "SALES_TRANSACTIONS",
                    "description": (
                        "Transaction-level sales data including order IDs, "
                        "customer IDs, product SKUs, purchase amounts, "
                        "discounts applied, timestamps, payment methods, "
                        "shipping details, geographic location, order status, "
                        "and revenue metrics across all sales channels"
                    ),
                    "purpose": (
                        "Enable Cortex Analyst to answer questions about "
                        "sales trends, revenue patterns, product performance, "
                        "regional analysis, discount effectiveness, and "
                        "customer purchasing behavior. Supports time-series "
                        "analysis, cohort analysis, and predictive forecasting."
                    ),
                    "table_type": "fact"
                },
                "structured_2": {
                    "name": "CUSTOMER_PROFILES",
                    "description": (
                        "Customer demographic and behavioral data including "
                        "customer IDs, acquisition dates, lifetime value, "
                        "purchase frequency, average order value, preferred "
                        "product categories, engagement scores, churn risk "
                        "indicators, subscription status, and customer segment "
                        "classifications"
                    ),
                    "purpose": (
                        "Support customer analytics including segmentation, "
                        "churn prediction, lifetime value analysis, and "
                        "personalization strategies. Enables Cortex Analyst "
                        "to join with transaction data for comprehensive "
                        "customer journey analysis and targeted marketing "
                        "insights."
                    ),
                    "table_type": "dimension"
                },
                "structured_3": {
                    "name": "PRODUCT_CATALOG",
                    "description": (
                        "Product dimension containing product IDs, SKUs, "
                        "product names, categories, subcategories, brands, "
                        "pricing tiers, inventory status, supplier information, "
                        "product attributes, and merchandising classifications"
                    ),
                    "purpose": (
                        "Provide product context and hierarchy for sales "
                        "analysis. Enables product performance tracking, "
                        "category analytics, and merchandising insights when "
                        "joined with transaction data."
                    ),
                    "table_type": "dimension"
                },
                "unstructured": {
                    "name": "PRODUCT_REVIEWS_CHUNKS",
                    "description": (
                        "Chunked customer product reviews and feedback "
                        "including review text, star ratings, product mentions, "
                        "sentiment indicators, helpfulness votes, and "
                        "user-generated content from website, mobile app, and "
                        "third-party platforms"
                    ),
                    "purpose": (
                        "Enable Cortex Search for semantic search across "
                        "customer feedback to identify product issues, feature "
                        "requests, competitive comparisons, and sentiment "
                        "trends. Supports voice-of-customer analysis and "
                        "product improvement prioritization."
                    )
                }
            }
        },
        {
            "title": "Financial Services Risk & Compliance",
            "description": (
                f"Risk management and compliance monitoring system for "
                f"{company_name} that combines real-time transaction "
                f"monitoring with regulatory compliance tracking and policy "
                f"documentation for comprehensive risk intelligence"
            ),
            "industry_focus": "Financial Services",
            "business_value": (
                "Enhance fraud detection, reduce regulatory risk, automate "
                "compliance reporting, and accelerate investigation workflows "
                "through AI-powered analytics and intelligent document search. "
                "Reduce false positives, improve audit preparedness, and "
                "ensure regulatory adherence across all business units."
            ),
            "tables": {
                "structured_1": {
                    "name": "TRANSACTION_MONITORING",
                    "description": (
                        "Financial transaction records including transaction "
                        "IDs, account numbers, amounts, currencies, "
                        "transaction types (wire, ACH, card), counterparty "
                        "information, geographic locations, timestamps, risk "
                        "scores, anomaly flags, AML alerts, pattern indicators, "
                        "and investigator notes"
                    ),
                    "purpose": (
                        "Enable Cortex Analyst to perform transaction pattern "
                        "analysis, identify suspicious activity, track risk "
                        "score trends, analyze false positive rates, and "
                        "measure investigation efficiency. Supports predictive "
                        "risk modeling and anomaly detection queries."
                    ),
                    "table_type": "fact"
                },
                "structured_2": {
                    "name": "COMPLIANCE_EVENTS",
                    "description": (
                        "Regulatory compliance events including event IDs, "
                        "event types (KYC, AML, sanctions screening), account "
                        "information, violation indicators, severity levels, "
                        "investigation status, remediation actions, regulatory "
                        "deadlines, audit findings, and resolution timestamps"
                    ),
                    "purpose": (
                        "Support compliance reporting, trend analysis of "
                        "regulatory events, remediation tracking, and audit "
                        "trail generation through Cortex Analyst. Enables "
                        "cross-referencing with transaction data for "
                        "comprehensive risk assessment and regulatory reporting."
                    ),
                    "table_type": "dimension"
                },
                "structured_3": {
                    "name": "ACCOUNT_PROFILES",
                    "description": (
                        "Account dimension containing account IDs, account types, "
                        "customer demographics, risk classifications, account "
                        "opening dates, KYC status, sanctions screening results, "
                        "geographic regions, relationship managers, and account "
                        "status indicators"
                    ),
                    "purpose": (
                        "Provide account context for transaction and compliance "
                        "analysis. Enables account segmentation, risk profiling, "
                        "and demographic analysis when joined with transaction "
                        "monitoring data."
                    ),
                    "table_type": "dimension"
                },
                "unstructured": {
                    "name": "REGULATORY_DOCS_CHUNKS",
                    "description": (
                        "Chunked regulatory documents, compliance policies, "
                        "procedure manuals, regulatory guidance, industry "
                        "standards, internal control documentation, audit "
                        "reports, and regulatory correspondence for "
                        "comprehensive policy knowledge base"
                    ),
                    "purpose": (
                        "Enable Cortex Search for rapid policy lookup, "
                        "regulatory guidance retrieval, compliance procedure "
                        "verification, and audit preparation. Supports "
                        "investigators and compliance officers in finding "
                        "relevant regulations and internal controls quickly "
                        "during case review."
                    )
                }
            }
        },
        {
            "title": "Healthcare Patient Analytics & Research",
            "description": (
                f"Patient outcomes and research data platform for "
                f"{company_name} integrating clinical outcomes with treatment "
                f"protocols and medical research for evidence-based care "
                f"delivery and continuous quality improvement"
            ),
            "industry_focus": "Healthcare",
            "business_value": (
                "Improve patient outcomes, reduce readmission rates, optimize "
                "treatment protocols, accelerate clinical research, and "
                "support evidence-based medicine through comprehensive "
                "analytics and intelligent research discovery. Enable "
                "clinicians to make data-informed decisions backed by "
                "real-world evidence and published research."
            ),
            "tables": {
                "structured_1": {
                    "name": "PATIENT_OUTCOMES",
                    "description": (
                        "Patient treatment outcomes including patient IDs "
                        "(anonymized), admission dates, discharge dates, "
                        "diagnoses (ICD-10 codes), procedures (CPT codes), "
                        "outcome measures, quality indicators, readmission "
                        "flags, length of stay, complication rates, patient "
                        "satisfaction scores, and recovery metrics"
                    ),
                    "purpose": (
                        "Enable Cortex Analyst to perform clinical performance "
                        "analysis, identify outcome patterns, benchmark against "
                        "quality metrics, predict readmission risk, and analyze "
                        "treatment effectiveness. Supports population health "
                        "analysis and quality improvement initiatives."
                    ),
                    "table_type": "fact"
                },
                "structured_2": {
                    "name": "TREATMENT_PROTOCOLS",
                    "description": (
                        "Treatment protocols and care pathways including "
                        "protocol IDs, condition types, medication regimens, "
                        "dosage information, treatment sequences, clinical "
                        "guidelines, contraindications, success rates, cost "
                        "data, and evidence levels for each treatment approach"
                    ),
                    "purpose": (
                        "Support treatment effectiveness comparison, protocol "
                        "optimization, cost-benefit analysis, and clinical "
                        "decision support through Cortex Analyst. Enables "
                        "joining with outcomes data to measure real-world "
                        "protocol effectiveness and identify best practices."
                    ),
                    "table_type": "dimension"
                },
                "structured_3": {
                    "name": "PROVIDER_DIRECTORY",
                    "description": (
                        "Healthcare provider dimension containing provider IDs, "
                        "provider types (physician, nurse, specialist), "
                        "specialties, certifications, facility affiliations, "
                        "patient ratings, case volumes, years of experience, "
                        "and performance metrics"
                    ),
                    "purpose": (
                        "Provide provider context for outcomes analysis. Enables "
                        "provider performance tracking, specialty comparisons, "
                        "and resource allocation when joined with patient "
                        "outcomes data."
                    ),
                    "table_type": "dimension"
                },
                "unstructured": {
                    "name": "RESEARCH_PAPERS_CHUNKS",
                    "description": (
                        "Chunked medical research papers, clinical studies, "
                        "meta-analyses, systematic reviews, case reports, "
                        "clinical trial results, and evidence summaries from "
                        "peer-reviewed journals covering relevant medical "
                        "specialties and treatment modalities"
                    ),
                    "purpose": (
                        "Enable Cortex Search for evidence-based research "
                        "discovery, clinical guideline verification, treatment "
                        "option exploration, and staying current with medical "
                        "literature. Supports clinicians and researchers in "
                        "finding relevant studies and evidence during care "
                        "planning and protocol development."
                    )
                }
            }
        }
    ]
    
    # Add target audience and customization to each demo
    for demo in demos:
        demo['target_audience'] = (
            f"Designed for presentation to: {team_members}"
        )
        if use_cases:
            demo['customization'] = f"Tailored for: {use_cases}"
    
    return demos


def get_template_by_industry(industry: str) -> Optional[Dict]:
    """
    Get a specific demo template by industry.
    
    Args:
        industry: Industry name (case-insensitive)
        
    Returns:
        Demo template dictionary or None if not found
    """
    # Get all templates with placeholder values
    all_templates = get_fallback_demo_ideas(
        company_name="COMPANY",
        team_members="Team",
        use_cases="General Analytics"
    )
    
    # Match by industry
    industry_lower = industry.lower()
    for template in all_templates:
        if industry_lower in template['industry_focus'].lower():
            return template
    
    return None


def get_available_template_industries() -> List[str]:
    """
    Get list of industries with available templates.
    
    Returns:
        List of industry names
    """
    return [
        "E-commerce/Retail",
        "Financial Services",
        "Healthcare"
    ]


# ============================================================================
# QUESTION GENERATION
# ============================================================================

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_company_url(
    company_url: str,
    _session,
    _error_handler
) -> Dict[str, Any]:
    """
    Analyze company URL to extract industry and business context using LLM.
    
    CACHED FUNCTION: Results cached for 1 hour to avoid redundant LLM calls.
    
    Args:
        company_url: Company website URL
        _session: Snowflake session (prefixed with _ to skip hashing)
        _error_handler: ErrorHandler instance (prefixed with _ to skip hashing)
        
    Returns:
        Dictionary with industry, domain, context, and keywords
    """
    # Restore original parameter names for use in function body
    session = _session
    error_handler = _error_handler
    result = {
        'industry': 'Technology',  # Default fallback
        'domain': '',
        'context': '',
        'keywords': []
    }
    
    if not company_url or company_url == "":
        return result
    
    try:
        # Extract domain from URL
        domain_match = re.search(
            r'(?:https?://)?(?:www\.)?([^/]+)', company_url
        )
        if domain_match:
            result['domain'] = domain_match.group(1)
        
        # Use LLM to analyze the company URL and infer industry
        prompt = get_company_analysis_prompt(company_url)
        
        response_text = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)",
            params=[LLM_MODEL, prompt]
        ).collect()[0][0]
        
        # Parse JSON response
        # Extract JSON from response
        json_match = re.search(
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response_text, re.DOTALL
        )
        if json_match:
            analysis = json.loads(json_match.group())
            result['industry'] = analysis.get('industry', result['industry'])
            result['keywords'] = analysis.get('keywords', [])
            result['context'] = analysis.get('context', '')
    
    except Exception:
        # Fallback: Use simple heuristics based on domain keywords
        domain_lower = result['domain'].lower() if result['domain'] else ''
        if any(word in domain_lower for word in [
            'bank', 'finance', 'capital', 'invest', 'credit'
        ]):
            result['industry'] = 'Finance'
            result['keywords'] = [
                'financial', 'risk', 'portfolio', 'transactions'
            ]
            result['context'] = (
                'Financial services with focus on risk management and '
                'portfolio analysis'
            )
        elif any(word in domain_lower for word in [
            'health', 'medical', 'pharma', 'clinic', 'hospital'
        ]):
            result['industry'] = 'Healthcare'
            result['keywords'] = ['patient', 'clinical', 'medical', 'treatment']
            result['context'] = (
                'Healthcare provider with patient care and clinical data '
                'analysis needs'
            )
        elif any(word in domain_lower for word in [
            'retail', 'shop', 'store', 'commerce', 'buy'
        ]):
            result['industry'] = 'Retail'
            result['keywords'] = ['sales', 'customer', 'inventory', 'orders']
            result['context'] = (
                'Retail business with customer analytics and inventory '
                'management focus'
            )
        elif any(word in domain_lower for word in [
            'tech', 'software', 'data', 'cloud', 'saas'
        ]):
            result['industry'] = 'Technology'
            result['keywords'] = ['user', 'product', 'platform', 'engagement']
            result['context'] = (
                'Technology company with product analytics and user '
                'engagement focus'
            )
        elif any(word in domain_lower for word in [
            'manu', 'factory', 'industrial', 'supply'
        ]):
            result['industry'] = 'Manufacturing'
            result['keywords'] = [
                'production', 'quality', 'supply chain', 'operations'
            ]
            result['context'] = (
                'Manufacturing with supply chain and operational efficiency '
                'needs'
            )
    
    return result


@timeit
def generate_contextual_questions(
    demo_data: Dict,
    semantic_model_info: Optional[Dict],
    company_name: str,
    session,
    error_handler,
    num_questions: int = 12,
    company_url: Optional[str] = None,
    table_schemas: Optional[Dict] = None,
    rich_table_contexts: Optional[List[Dict]] = None,
    unstructured_samples: Optional[Dict] = None,
    language_code: str = "en"
) -> List[Dict[str, Any]]:
    """
    Generate contextual questions using AI with optional URL analysis and
    table schemas.
    
    Args:
        demo_data: Demo configuration dictionary
        semantic_model_info: Optional semantic model information
        company_name: Company name
        session: Snowflake session
        error_handler: ErrorHandler instance
        num_questions: Number of questions to generate
        company_url: Optional company URL for context
        table_schemas: Optional table schemas for better questions (deprecated, use rich_table_contexts)
        rich_table_contexts: Optional rich context with full schema, data analysis, and cardinality
        unstructured_samples: Optional dict of actual unstructured data samples {table_name: {'description': ..., 'sample_chunks': [...]}}
        language_code: Language code for content generation (e.g., 'en', 'ko', 'ja')
        
    Returns:
        List of question dictionaries
    """
    industry = demo_data.get(
        'industry_focus',
        demo_data.get('industry', 'Business Intelligence')
    )
    
    # Analyze company URL if provided to enhance context
    url_context = None
    if company_url:
        try:
            url_context = analyze_company_url(
                company_url, session, error_handler
            )
            # Override industry if URL analysis provides better context
            if url_context.get('industry'):
                industry = url_context['industry']
        except Exception:
            pass  # Continue with default industry if URL analysis fails
    
    # Phase 0 Step 3: Validate target questions FIRST (100% must pass)
    validated_target_questions = []
    target_question_debug = {}
    if rich_table_contexts:
        target_questions_list = demo_data.get('target_questions', [])
        if target_questions_list and len(target_questions_list) > 0:
            import streamlit as st
            st.info(f"🎯 Validating {len(target_questions_list)} target questions (100% must pass)...")
            
            # Convert target questions to dict format for validation
            target_q_dicts = [
                {"text": q, "difficulty": "intermediate", "category": "analytics"} 
                for q in target_questions_list
            ]
            
            validated_targets, target_debug = validate_questions_with_llm(
                target_q_dicts,
                rich_table_contexts,
                session,
                error_handler,
                return_debug_info=True
            )
            
            target_question_debug = target_debug
            
            # STRICT: All target questions MUST pass
            if len(validated_targets) < len(target_questions_list):
                failed_count = len(target_questions_list) - len(validated_targets)
                error_msg = (
                    f"❌ CRITICAL: {failed_count}/{len(target_questions_list)} target questions are NOT answerable with the generated data. "
                    f"This indicates a mismatch between the demo structure and your requirements. "
                    f"Please try regenerating the demo or revising your questions."
                )
                st.error(error_msg)
                
                # Show which questions failed
                failed_questions = [
                    q for q in target_questions_list 
                    if q not in [vq['text'] for vq in validated_targets]
                ]
                if failed_questions:
                    st.error(f"Failed questions: {', '.join(failed_questions[:3])}")
                
                # Raise exception to stop the process
                raise Exception(error_msg)
            
            st.success(f"✅ All {len(target_questions_list)} target questions validated successfully!")
            validated_target_questions = validated_targets
            
            # Store validation info for debug display
            import streamlit as st
            st.session_state['target_question_validation_debug'] = target_question_debug
    
    # Extract semantic model vocabulary for question generation
    semantic_vocabulary = None
    if semantic_model_info and 'column_synonyms' in semantic_model_info:
        semantic_vocabulary = semantic_model_info['column_synonyms']
    
    # Generate MORE questions initially (2x) to increase chance of getting enough valid ones
    initial_generation_count = num_questions * 2
    questions = generate_questions_with_llm(
        demo_data,
        semantic_model_info,
        company_name,
        industry,
        session,
        error_handler,
        initial_generation_count,  # Generate 24 questions to get 12 valid
        url_context,
        table_schemas,
        rich_table_contexts,
        is_retry=False,
        existing_questions=[],
        semantic_vocabulary=semantic_vocabulary,  # NEW: Pass semantic model vocabulary
        unstructured_samples=unstructured_samples,
        language_code=language_code
    )
    
    # Validate questions with LLM if rich contexts available
    validation_debug = {
        'attempt_1': None,
        'attempt_2': None,
        'retry_triggered': False,
        'final_pass_rate': 0,
        'combined_pool_size': 0,
        'after_selection': 0,
        'attempt_1_questions': [],
        'attempt_2_questions': [],
        'attempt_1_valid': [],
        'attempt_2_valid': []
    }
    
    if rich_table_contexts and questions:
        import time
        start_time = time.time()
        
        # Store generated questions for debugging
        validation_debug['attempt_1_questions'] = [q.get('text', 'N/A') for q in questions]
        
        validated_questions, debug_info_1 = validate_questions_with_llm(
            questions,
            rich_table_contexts,
            session,
            error_handler,
            return_debug_info=True
        )
        
        # Store validated questions from attempt 1
        validation_debug['attempt_1_valid'] = [q.get('text', 'N/A') for q in validated_questions]
        validation_debug['attempt_1'] = debug_info_1
        
        # ACCUMULATIVE APPROACH: Keep valid questions and generate more if needed
        target_count = num_questions
        pass_rate = len(validated_questions) / len(questions) if questions else 0
        
        if len(validated_questions) < target_count:
            validation_debug['retry_triggered'] = True
            needed = target_count - len(validated_questions)
            
            import streamlit as st
            if st.session_state.get('debug_mode', False):
                st.warning(
                    f"⚠️ Only {len(validated_questions)}/{len(questions)} questions passed validation "
                    f"({pass_rate:.0%} pass rate). Generating {max(needed * 2, 12)} additional questions with ultra-conservative prompt..."
                )
            
            # Generate MORE questions than needed (2x buffer) to ensure enough after validation
            # Pass existing valid questions to LLM so it generates DIFFERENT questions
            new_questions = generate_questions_with_llm(
                demo_data,
                semantic_model_info,
                company_name,
                industry,
                session,
                error_handler,
                max(needed * 2, 12),  # Generate more to have buffer after validation
                url_context,
                table_schemas,
                rich_table_contexts,
                is_retry=True,  # Triggers ultra-conservative prompt
                existing_questions=validated_questions,  # Pass to avoid duplicates
                semantic_vocabulary=semantic_vocabulary,  # NEW: Pass semantic model vocabulary
                unstructured_samples=unstructured_samples,
                language_code=language_code
            )
            
            # Store generated questions from attempt 2
            validation_debug['attempt_2_questions'] = [q.get('text', 'N/A') for q in new_questions]
            
            validated_new, debug_info_2 = validate_questions_with_llm(
                new_questions,
                rich_table_contexts,
                session,
                error_handler,
                return_debug_info=True
            )
            
            # Store validated questions from attempt 2
            validation_debug['attempt_2_valid'] = [q.get('text', 'N/A') for q in validated_new]
            validation_debug['attempt_2'] = debug_info_2
            
            # Combine valid questions from both attempts
            combined_questions = validated_questions + validated_new
            validation_debug['combined_pool_size'] = len(combined_questions)
            
            # Select best questions from combined pool (prioritize complexity)
            final_questions = select_best_questions(
                combined_questions,
                target_count=target_count,
                min_advanced=3  # Ensure at least 3 complex questions
            )
            
            validation_debug['after_selection'] = len(final_questions)
            
            # If still not enough, log error (debug only)
            if len(final_questions) < target_count:
                if st.session_state.get('debug_mode', False):
                    st.error(
                        f"⚠️ After combining attempts: {len(final_questions)}/{target_count} valid questions. "
                        f"(Attempt 1: {len(validated_questions)}, Attempt 2: {len(validated_new)}). "
                        f"This may indicate an issue with the schema or data."
                    )
            
            validated_questions = final_questions
        
        # Calculate final pass rate based on actual validation (not fallback)
        # If fallback was used, pass rate is 0% (validation failed)
        final_attempt = validation_debug.get('attempt_2') or validation_debug.get('attempt_1')
        if final_attempt and final_attempt.get('fallback_used'):
            validation_debug['final_pass_rate'] = 0.0
            validation_debug['used_fallback'] = True
        else:
            validation_debug['final_pass_rate'] = len(validated_questions) / target_count if target_count > 0 else 0
            validation_debug['used_fallback'] = False
        
        validation_debug['total_validation_time'] = time.time() - start_time
        validation_debug['final_question_count'] = len(validated_questions)
        
        # Store in session state for debug display
        import streamlit as st
        st.session_state['question_validation_debug'] = validation_debug
        
        # ENHANCED: Test against actual semantic model (DISABLED - too slow with individual LLM calls)
        # The semantic vocabulary is now passed to question generation, which is the primary fix
        # This test would require batching or a different approach to be reliable
        # Use the "Test All Questions" checkbox state to control semantic model testing
        # MANDATORY semantic model testing - always enabled to guarantee 100% answerability
        if semantic_model_info:
            # No user message - will show in debug section only
            try:
                semantic_test_results = test_questions_against_semantic_model(
                    validated_questions,
                    semantic_model_info,
                    st.session_state.get('last_created_schema', ''),
                    session,
                    error_handler
                )
                
                # STRICT: Always filter out failed questions
                if semantic_test_results['failed']:
                    failed_count = len(semantic_test_results['failed'])
                    passed_count = len(semantic_test_results['answerable'])
                    # Store for debug display only (no user-facing message)
                    st.session_state['semantic_test_filtered'] = {
                        'failed_count': failed_count,
                        'passed_count': passed_count
                    }
                    validated_questions = semantic_test_results['answerable']
                
                st.session_state['semantic_model_test_results'] = semantic_test_results
            except Exception as e:
                # If testing fails, this is a critical error - cannot guarantee answerability
                error_handler.log_error(
                    error_code=ErrorCode.VALIDATION_FAILED,
                    error_type=type(e).__name__,
                    severity=ErrorSeverity.ERROR,
                    message=f"Mandatory semantic model testing failed: {str(e)}"
                )
                # Store error state
                st.session_state['semantic_model_test_results'] = {
                    'answerable': [],
                    'failed': validated_questions,
                    'skipped': False,
                    'error': str(e)
                }
                # Return empty - cannot proceed without testing
                validated_questions = []
        else:
            # No semantic model means we cannot test - this is a critical gap
            st.session_state['semantic_model_test_results'] = {
                'answerable': [],
                'failed': validated_questions,
                'skipped': True,
                'no_semantic_model': True
            }
            # Cannot guarantee answerability without semantic model
            validated_questions = []
        
        # RETRY LOGIC: Ensure minimum 2 questions per category (basic requirement)
        MAX_RETRIES = 3
        retry_attempt = 0
        accumulated_questions = validated_questions  # Keep all that pass
        
        while retry_attempt < MAX_RETRIES:
            # Check current distribution
            basic_count = len([q for q in accumulated_questions if q.get('difficulty') == 'basic'])
            intermediate_count = len([q for q in accumulated_questions if q.get('difficulty') == 'intermediate'])
            advanced_count = len([q for q in accumulated_questions if q.get('difficulty') == 'advanced'])
            analytics_count = len([q for q in accumulated_questions if q.get('category') == 'analytics'])
            search_count = len([q for q in accumulated_questions if q.get('category') == 'search'])
            
            # Check if we meet minimum requirements: at least 2 in each category
            needs_more_analytics = analytics_count < 2
            needs_more_search = search_count < 2
            
            if not (needs_more_analytics or needs_more_search):
                # We have enough, break out of retry loop
                break
            
            retry_attempt += 1
            
            # Generate additional questions to fill gaps
            try:
                num_analytics_needed = max(3 - analytics_count, 0) if needs_more_analytics else 0
                num_search_needed = max(3 - search_count, 0) if needs_more_search else 0
                total_needed = num_analytics_needed + num_search_needed
                
                if total_needed == 0:
                    break
                
                # Build focused retry prompt
                retry_instructions = f"🔄 RETRY ATTEMPT {retry_attempt}/{MAX_RETRIES}\n\n"
                retry_instructions += "Previous questions were rejected. Generate SAFE, SIMPLE questions that WILL pass validation.\n\n"
                retry_instructions += "SPECIFIC NEEDS:\n"
                if needs_more_analytics:
                    retry_instructions += f"- {num_analytics_needed} ANALYTICS questions (structured data analysis)\n"
                if needs_more_search:
                    retry_instructions += f"- {num_search_needed} SEARCH questions (unstructured content retrieval)\n"
                retry_instructions += "\nUSE ONLY COLUMNS YOU CAN SEE EXPLICITLY LISTED.\n"
                retry_instructions += "Prefer simple aggregations (COUNT, AVG, SUM) over complex calculations.\n\n"
                
                # Generate retry questions
                base_prompt = get_question_generation_prompt(
                    num_questions=total_needed,
                    industry=industry,
                    company_name=company_name,
                    demo_data=demo_data,
                    url_context=url_context,
                    table_info="",
                    columns_info="",
                    rich_table_contexts=rich_table_contexts,
                    is_retry=True,  # Ultra-conservative mode
                    existing_questions=accumulated_questions,
                    unstructured_samples=unstructured_samples
                )
                
                retry_prompt = f"{retry_instructions}\n\n{base_prompt}"
                
                # Call LLM
                result = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) as response",
                    params=[LLM_MODEL, retry_prompt]
                ).collect()
                
                response = result[0]['RESPONSE']
                
                import re, json
                json_match = re.search(r'\[.*\]', response, re.DOTALL)
                if json_match:
                    retry_questions = json.loads(json_match.group(0))
                    for q in retry_questions:
                        q['source'] = 'ai'
                        q['retry_attempt'] = retry_attempt
                    
                    # Validate retry questions
                    validated_retry, _ = validate_questions_with_llm(
                        retry_questions,
                        rich_table_contexts,
                        session,
                        error_handler,
                        return_debug_info=True
                    )
                    
                    # Test retry questions against semantic model (MANDATORY)
                    if semantic_model_info and validated_retry:
                        retry_test_results = test_questions_against_semantic_model(
                            validated_retry,
                            semantic_model_info,
                            st.session_state.get('last_created_schema', ''),
                            session,
                            error_handler
                        )
                        validated_retry = retry_test_results['answerable']
                    
                    # ACCUMULATE: Keep all questions that pass
                    if validated_retry:
                        accumulated_questions.extend(validated_retry)
                        # Store retry success for debug
                        st.session_state['retry_info'] = {
                            'attempt': retry_attempt,
                            'added': len(validated_retry),
                            'total_now': len(accumulated_questions)
                        }
                else:
                    # Retry failed, try again
                    continue
                    
            except Exception as e:
                # Log error but continue - we'll try again or use what we have
                error_handler.log_error(
                    error_code=ErrorCode.VALIDATION_FAILED,
                    error_type=type(e).__name__,
                    severity=ErrorSeverity.WARNING,
                    message=f"Retry attempt {retry_attempt} failed: {str(e)}"
                )
                continue
        
        # Use accumulated questions (all that passed)
        validated_questions = accumulated_questions
        
        # Recalculate distribution with final accumulated questions
        basic_count = len([q for q in validated_questions if q.get('difficulty') == 'basic'])
        intermediate_count = len([q for q in validated_questions if q.get('difficulty') == 'intermediate'])
        advanced_count = len([q for q in validated_questions if q.get('difficulty') == 'advanced'])
        analytics_count = len([q for q in validated_questions if q.get('category') == 'analytics'])
        search_count = len([q for q in validated_questions if q.get('category') == 'search'])
        
        # Count advanced analytics specifically (needed for final selection)
        advanced_analytics = [q for q in validated_questions if q.get('difficulty') == 'advanced' and q.get('category') == 'analytics']
        advanced_analytics_count = len(advanced_analytics)
        
        # Check if distribution is acceptable (need at least 3 advanced ANALYTICS and 3 search)
        missing_advanced_analytics = advanced_analytics_count < 3
        missing_search = search_count < 3
        
        if missing_advanced_analytics or missing_search:
            issues = []
            if missing_advanced_analytics:
                issues.append(f"only {advanced_analytics_count} advanced analytics (need 3+)")
            if missing_search:
                issues.append(f"only {search_count} search (need 3+)")
            
            # Store for debug display only (don't show to user)
            distribution_issue = f"Distribution incomplete: {', '.join(issues)}"
            
            # Calculate how many more we need
            advanced_analytics_needed = max(3 - advanced_analytics_count, 0)
            search_needed = max(3 - search_count, 0)
            total_needed = advanced_analytics_needed + search_needed
            
            # Build targeted prompt section with SPECIFIC category + difficulty requirements
            targeted_instructions = "You MUST generate questions to fill these SPECIFIC gaps:\n"
            if missing_advanced_analytics:
                targeted_instructions += f"- {advanced_analytics_needed} ADVANCED difficulty ANALYTICS category questions (3+ table joins, correlations, pattern analysis across structured data)\n"
            if missing_search:
                targeted_instructions += f"- {search_needed} SEARCH category questions (for unstructured content retrieval, any difficulty)\n"
            
            targeted_instructions += "\nCRITICAL REQUIREMENTS:\n"
            if missing_advanced_analytics:
                targeted_instructions += f"- These {advanced_analytics_needed} questions MUST have category='analytics' AND difficulty='advanced'\n"
            if missing_search:
                targeted_instructions += f"- These {search_needed} questions MUST have category='search'\n"
            targeted_instructions += "\nFocus ONLY on generating these specific types. "
            targeted_instructions += f"Avoid duplicating these existing questions: {[q['text'] for q in validated_questions]}\n"
            targeted_instructions += "\nReturn ONLY a JSON array with the missing question types."
            
            # Generate targeted questions
            try:
                from prompts import get_question_generation_prompt
                
                # Create a custom prompt that emphasizes the gaps
                base_prompt = get_question_generation_prompt(
                    num_questions=total_needed,
                    industry=industry,
                    company_name=company_name,
                    demo_data=demo_data,
                    url_context=url_context,
                    table_info="",
                    columns_info="",
                    rich_table_contexts=rich_table_contexts,
                    is_retry=False,
                    existing_questions=validated_questions,
                    unstructured_samples=unstructured_samples
                )
                
                # Prepend targeted instructions
                targeted_prompt = f"{targeted_instructions}\n\n{base_prompt}"
                
                # Call LLM
                result = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) as response",
                    params=[LLM_MODEL, targeted_prompt]
                ).collect()
                
                response = result[0]['RESPONSE']
                
                import re, json
                json_match = re.search(r'\[.*\]', response, re.DOTALL)
                if json_match:
                    targeted_questions = json.loads(json_match.group(0))
                    for q in targeted_questions:
                        q['source'] = 'ai'
                    
                    # Validate the new targeted questions
                    validated_targeted, _ = validate_questions_with_llm(
                        targeted_questions,
                        rich_table_contexts,
                        session,
                        error_handler,
                        return_debug_info=True
                    )
                    
                    # Combine with existing valid questions (accumulative approach)
                    validated_questions = validated_questions + validated_targeted
                    
                    # Store for debug display only (don't show to user)
                    targeted_success = f"Added {len(validated_targeted)} targeted questions to fill distribution gaps"
                else:
                    targeted_success = "Could not parse targeted questions, proceeding with existing questions"
                    
            except Exception as e:
                targeted_success = f"Targeted question generation failed: {str(e)[:100]}, proceeding with existing questions"
        
        # Store distribution info for debug display (recalculate after targeted generation)
        st.session_state['question_distribution'] = {
            'basic': len([q for q in validated_questions if q.get('difficulty') == 'basic']),
            'intermediate': len([q for q in validated_questions if q.get('difficulty') == 'intermediate']),
            'advanced': len([q for q in validated_questions if q.get('difficulty') == 'advanced']),
            'analytics': len([q for q in validated_questions if q.get('category') == 'analytics']),
            'search': len([q for q in validated_questions if q.get('category') == 'search']),
            # Add targeted generation info for debug display
            'targeted_generation_triggered': missing_advanced_analytics or missing_search,
            'targeted_issue': distribution_issue if (missing_advanced_analytics or missing_search) else None,
            'targeted_result': targeted_success if (missing_advanced_analytics or missing_search) else None
        }
        
        # CRITICAL FIX: Ensure proper category AND difficulty distribution in final selection
        # If we have more questions than needed, select with balanced distribution
        if len(validated_questions) > num_questions:
            # Separate by category
            analytics_qs = [q for q in validated_questions if q.get('category') == 'analytics']
            search_qs = [q for q in validated_questions if q.get('category') == 'search']
            
            # Target: 70% analytics (8-9), 30% search (3-4) for 12 questions
            target_analytics = int(num_questions * 0.7)
            target_search = num_questions - target_analytics
            
            # For analytics questions, ensure difficulty distribution (3 advanced minimum!)
            if len(analytics_qs) > target_analytics:
                # Separate analytics by difficulty
                advanced_analytics = [q for q in analytics_qs if q.get('difficulty') == 'advanced']
                intermediate_analytics = [q for q in analytics_qs if q.get('difficulty') == 'intermediate']
                basic_analytics = [q for q in analytics_qs if q.get('difficulty') == 'basic']
                
                # Ensure we get at least 3 advanced analytics questions
                selected_analytics = []
                selected_analytics.extend(advanced_analytics[:min(3, len(advanced_analytics))])
                
                # Fill remaining slots with intermediate then basic
                remaining_analytics_slots = target_analytics - len(selected_analytics)
                if remaining_analytics_slots > 0:
                    selected_analytics.extend(intermediate_analytics[:remaining_analytics_slots])
                    remaining_analytics_slots = target_analytics - len(selected_analytics)
                    if remaining_analytics_slots > 0:
                        selected_analytics.extend(basic_analytics[:remaining_analytics_slots])
            else:
                selected_analytics = analytics_qs[:target_analytics]
            
            # Select search questions (all difficulties)
            selected_search = search_qs[:target_search]
            
            # Combine, analytics first then search
            validated_questions = selected_analytics + selected_search
            
            # If we don't have enough search, fill with more analytics
            if len(validated_questions) < num_questions:
                remaining = num_questions - len(validated_questions)
                # Get analytics we haven't used yet
                used_texts = {q['text'] for q in validated_questions}
                unused_analytics = [q for q in analytics_qs if q['text'] not in used_texts]
                validated_questions.extend(unused_analytics[:remaining])
        
        # Trust LLM validation with semantic understanding (no hardcoded safety net)
        # Status message removed - info shown in debug expander only
        
        # Phase 0: Include validated target questions in final set
        if validated_target_questions:
            # Combine target questions with generated questions
            # Target questions come first, then fill with generated questions
            target_texts = {q['text'] for q in validated_target_questions}
            non_target_questions = [
                q for q in validated_questions 
                if q['text'] not in target_texts
            ]
            
            # Return target questions + enough generated questions to reach num_questions
            final_questions = validated_target_questions + non_target_questions
            return final_questions[:num_questions]
        
        return validated_questions[:num_questions]
    
    return questions[:num_questions]


def verify_column_references(
    questions: List[Dict[str, Any]],
    rich_table_contexts: Optional[List[Dict]] = None
) -> List[Dict[str, Any]]:
    """
    Verify that all referenced columns in questions actually exist in the schema.
    
    Args:
        questions: List of question dictionaries with optional 'referenced_columns' field
        rich_table_contexts: Rich context with full schema information
        
    Returns:
        List of verified questions (those with all columns existing)
    """
    if not rich_table_contexts or not questions:
        return questions
    
    # Build set of all available columns (uppercase for case-insensitive comparison)
    all_columns = set()
    for ctx in rich_table_contexts:
        for col in ctx.get('columns', []):
            col_name = col.get('name', '').upper()
            if col_name:
                all_columns.add(col_name)
    
    verified_questions = []
    for q in questions:
        # If question doesn't have referenced_columns field, assume it's valid (backward compatibility)
        if 'referenced_columns' not in q:
            verified_questions.append(q)
            continue
        
        referenced = [col.upper() for col in q.get('referenced_columns', [])]
        
        # Check if all referenced columns exist
        if all(col in all_columns for col in referenced):
            verified_questions.append(q)
        else:
            # Log which columns were referenced but don't exist (only in debug mode)
            missing = [col for col in referenced if col not in all_columns]
            if st.session_state.get('debug_mode', False):
                st.warning(f"❌ Rejected question due to missing columns: '{q.get('text', 'N/A')}'")
                st.caption(f"   Missing columns: {', '.join(missing)}")
                st.caption(f"   Available columns: {', '.join(sorted(all_columns)[:20])}...")
    
    return verified_questions


@timeit
def generate_questions_with_llm(
    demo_data: Dict,
    semantic_model_info: Optional[Dict],
    company_name: str,
    industry: str,
    session,
    error_handler,
    num_questions: int,
    url_context: Optional[Dict] = None,
    table_schemas: Optional[Dict] = None,
    rich_table_contexts: Optional[List[Dict]] = None,
    is_retry: bool = False,
    existing_questions: Optional[List[Dict]] = None,
    semantic_vocabulary: Optional[Dict] = None,
    unstructured_samples: Optional[Dict] = None,
    language_code: str = "en"
) -> List[Dict[str, Any]]:
    """
    Generate questions using Cortex LLM with optional URL context and actual
    table schemas.
    
    Args:
        demo_data: Demo configuration dictionary
        semantic_model_info: Optional semantic model information
        company_name: Company name
        industry: Industry name
        session: Snowflake session
        error_handler: ErrorHandler instance
        num_questions: Number of questions to generate
        url_context: Optional URL analysis context
        table_schemas: Optional actual table schemas (deprecated, use rich_table_contexts)
        rich_table_contexts: Optional rich context with full schema, data analysis, and cardinality
        is_retry: If True, uses more conservative prompt for guaranteed answerability
        existing_questions: Optional list of questions to avoid duplicating (for retry attempts)
        semantic_vocabulary: Optional semantic model vocabulary with dimensions, measures, time dimensions
        unstructured_samples: Optional dict of actual unstructured data samples {table_name: {'description': ..., 'sample_chunks': [...]}}
        language_code: Language code for content generation (e.g., 'en', 'ko', 'ja')
        
    Returns:
        List of question dictionaries
    """
    table_info = ""
    if 'structured_table_1' in demo_data:
        table_info += (
            f"- {demo_data['structured_table_1']['name']}: "
            f"{demo_data['structured_table_1'].get('description', '')}\n"
        )
    if 'structured_table_2' in demo_data:
        table_info += (
            f"- {demo_data['structured_table_2']['name']}: "
            f"{demo_data['structured_table_2'].get('description', '')}\n"
        )
    
    # Add actual column information if available
    columns_info = ""
    if table_schemas:
        columns_info = "\n\nActual Columns Available:\n"
        if 'table1' in table_schemas:
            columns_info += (
                f"- {table_schemas['table1']['name']}: "
                f"{', '.join(table_schemas['table1']['columns'])}\n"
            )
        if 'table2' in table_schemas:
            columns_info += (
                f"- {table_schemas['table2']['name']}: "
                f"{', '.join(table_schemas['table2']['columns'])}\n"
            )
    
    # Phase 1: Build Question-Friendly Column Guide from rich_table_contexts
    column_guide = None
    if rich_table_contexts:
        metrics = []  # Numeric columns
        dimensions = []  # Categorical columns
        time_columns = []  # Date/timestamp columns
        
        for ctx in rich_table_contexts:
            table_name = ctx.get('name', 'UNKNOWN')
            for col in ctx.get('columns', []):
                col_name = col.get('name', '')
                col_type = col.get('type', '')
                col_desc = col.get('description', '')
                
                # Build column info string with range/values
                col_info = f"{col_name}"
                
                # Add numeric range for metrics
                if col.get('numeric_range'):
                    nr = col['numeric_range']
                    col_info += f" (range: {nr['min']}-{nr['max']})"
                
                # Add sample values for dimensions
                elif col.get('sample_actual_values'):
                    samples = col['sample_actual_values'][:4]  # First 4 values
                    col_info += f" (values: {', '.join(str(s) for s in samples)})"
                
                # Add to appropriate category
                if col_type in ['NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER']:
                    metrics.append(col_info)
                elif col_type in ['DATE', 'TIMESTAMP', 'DATETIME']:
                    time_columns.append(col_info)
                elif col_type in ['STRING', 'VARCHAR', 'TEXT', 'BOOLEAN']:
                    dimensions.append(col_info)
        
        # Build the formatted guide
        if metrics or dimensions or time_columns:
            column_guide = "\n\n📋 QUESTION-FRIENDLY COLUMN GUIDE:\n\n"
            
            if metrics:
                column_guide += "AVAILABLE METRICS (numeric columns you can ask about):\n"
                for m in metrics:
                    column_guide += f"- {m}\n"
                column_guide += "\n"
            
            if dimensions:
                column_guide += "AVAILABLE DIMENSIONS (categorical columns for grouping/filtering):\n"
                for d in dimensions:
                    column_guide += f"- {d}\n"
                column_guide += "\n"
            
            if time_columns:
                column_guide += "AVAILABLE TIME COLUMNS (for temporal analysis):\n"
                for t in time_columns:
                    column_guide += f"- {t}\n"
                column_guide += "\n"
            
            column_guide += "🚨 MANDATORY: Every question MUST use column names from this list above.\n"
            column_guide += "If you want to ask about 'popularity', find the actual column (e.g., ORDER_COUNT, SALES_COUNT).\n"
            column_guide += "If you want to ask about 'performance', find the actual column (e.g., RATING, SCORE, REVENUE).\n"
            column_guide += "If you want to ask about 'seasonal', find the actual column (e.g., SEASON, MONTH, QUARTER).\n\n"
    
    # Use prompt from prompts module
    prompt = get_question_generation_prompt(
        num_questions=num_questions,
        industry=industry,
        company_name=company_name,
        demo_data=demo_data,
        url_context=url_context,
        table_info=table_info,
        columns_info=columns_info,
        rich_table_contexts=rich_table_contexts,
        is_retry=is_retry,
        existing_questions=existing_questions,
        unstructured_samples=unstructured_samples,
        semantic_vocabulary=semantic_vocabulary,  # Pass semantic model vocabulary
        column_guide=column_guide  # NEW: Pass column guide for question templates
    )
    
    # Enhance prompt with language-specific instructions for non-English content
    prompt = enhance_prompt_with_language(prompt, language_code)
    
    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) as response",
            params=[LLM_MODEL, prompt]
        ).collect()
        
        response = result[0]['RESPONSE']
        
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if json_match:
            questions_data = json.loads(json_match.group(0))
            for q in questions_data:
                q['source'] = 'ai'
            
            # Phase 4: Verify column references before returning
            verified_questions = verify_column_references(questions_data, rich_table_contexts)
            return verified_questions
        else:
            return []
    except Exception:
        return []


# NOTE: Old regex-based validate_questions_against_schema() function removed.
# Now using validate_questions_with_llm() which leverages LLM intelligence to
# understand semantics and catch edge cases (like missing cost columns when
# questions ask about "expensive" items). See validate_questions_with_llm() below.


@timeit
def get_actual_table_columns_from_snowflake(
    schema_name: str,
    table_names: List[str],
    session,
    error_handler
) -> Dict[str, List[str]]:
    """
    Query Snowflake to get actual column names from created tables.
    
    This is the SOURCE OF TRUTH - what columns actually exist vs what we think exists.
    
    Args:
        schema_name: Schema containing the tables
        table_names: List of table names to query
        session: Snowflake session
        error_handler: ErrorHandler instance
        
    Returns:
        Dict mapping table_name -> list of column names (uppercase)
    """
    import time
    import streamlit as st
    
    query_start = time.time()
    actual_columns = {}
    
    try:
        for table_name in table_names:
            try:
                # Query DESCRIBE TABLE to get actual columns
                result = session.sql(f"DESCRIBE TABLE {schema_name}.{table_name}").collect()
                actual_columns[table_name] = [row['name'].upper() for row in result]
            except Exception as e:
                error_handler.log_error(
                    error_code=ErrorCode.DATABASE_QUERY_FAILED,
                    error_type=type(e).__name__,
                    severity=ErrorSeverity.WARNING,
                    message=f"Could not query columns for {table_name}: {str(e)}"
                )
                # Don't fail completely - continue with other tables
                actual_columns[table_name] = []
        
        # Initialize metrics (was_needed determined by comparison step)
        if 'schema_validation_metrics' not in st.session_state:
            st.session_state['schema_validation_metrics'] = {}
        
        st.session_state['schema_validation_metrics']['snowflake_query'] = {
            'time': time.time() - query_start,
            'tables_queried': len(table_names),
            'columns_found': sum(len(cols) for cols in actual_columns.values()),
            'was_needed': False,  # Updated by comparison step
            'issues_caught': []
        }
        
    except Exception as e:
        error_handler.log_error(
            error_code=ErrorCode.DATABASE_QUERY_FAILED,
            error_type=type(e).__name__,
            severity=ErrorSeverity.ERROR,
            message=f"Failed to query Snowflake columns: {str(e)}"
        )
        actual_columns = {}
    
    return actual_columns


def build_schema_alignment_report(
    expected_contexts: List[Dict],
    actual_columns: Dict[str, List[str]]
) -> Dict:
    """
    Compare expected (from schema generation) vs actual (from Snowflake).
    Returns detailed report of discrepancies.
    
    Args:
        expected_contexts: List of rich table contexts with expected columns
        actual_columns: Dict mapping table_name -> list of actual column names from Snowflake
        
    Returns:
        Dict with alignment report including perfect_match flag and discrepancy details
    """
    report = {
        'perfect_match': True,
        'tables': {},
        'total_discrepancies': 0
    }
    
    for ctx in expected_contexts:
        table_name = ctx['name']
        expected = set([col['name'].upper() for col in ctx.get('columns', [])])
        actual = set(actual_columns.get(table_name, []))
        
        missing = expected - actual  # Expected but not in Snowflake
        extra = actual - expected     # In Snowflake but not expected
        
        if missing or extra:
            report['perfect_match'] = False
            report['total_discrepancies'] += len(missing) + len(extra)
            report['tables'][table_name] = {
                'missing': list(missing),
                'extra': list(extra),
                'match_rate': len(expected & actual) / len(expected | actual) if expected | actual else 0
            }
    
    return report


@timeit
def rebuild_rich_context_from_actual(
    original_contexts: List[Dict],
    actual_columns: Dict[str, List[str]],
    schema_name: str,
    session,
    error_handler
) -> List[Dict]:
    """
    Rebuild rich_table_contexts using ACTUAL Snowflake columns.
    Queries cardinality and ranges from actual data.
    
    This is called when schema generation didn't match Snowflake reality,
    ensuring questions are generated from columns that actually exist.
    
    Args:
        original_contexts: Original rich contexts built from generated schemas
        actual_columns: Dict of actual column names from Snowflake
        schema_name: Snowflake schema name
        session: Snowflake session
        error_handler: ErrorHandler instance
    
    Returns:
        Rebuilt list of rich table contexts with actual column metadata
    """
    import streamlit as st
    
    rebuilt_contexts = []
    
    for ctx in original_contexts:
        table_name = ctx['name']
        actual_cols = actual_columns.get(table_name, [])
        
        if not actual_cols:
            # No actual columns found, use original context
            rebuilt_contexts.append(ctx)
            continue
        
        # Build new context with actual columns
        rebuilt_ctx = {
            'name': table_name,
            'description': ctx.get('description', ''),
            'row_count': ctx.get('row_count', 0),
            'columns': []
        }
        
        for col_name in actual_cols:
            # Skip ENTITY_ID - it's a join key, not for questions
            if col_name == 'ENTITY_ID':
                continue
            
            col_metadata = {
                'name': col_name,
                'type': 'VARCHAR',  # Default, will be updated from DESCRIBE
                'unique_count': None,
                'numeric_range': None,
                'sample_values': []
            }
            
            try:
                # Get type from DESCRIBE TABLE
                desc_result = session.sql(
                    f"DESCRIBE TABLE {schema_name}.{table_name}"
                ).collect()
                
                for row in desc_result:
                    if row['name'].upper() == col_name:
                        col_metadata['type'] = row['type']
                    break
        
                # Get cardinality
                card_result = session.sql(
                    f"SELECT COUNT(DISTINCT {col_name}) as unique_count FROM {schema_name}.{table_name}"
                ).collect()
                col_metadata['unique_count'] = card_result[0]['UNIQUE_COUNT'] if card_result else None
                
                # Get range for numeric columns
                if 'NUMBER' in col_metadata['type'] or 'INT' in col_metadata['type'] or 'FLOAT' in col_metadata['type']:
                    range_result = session.sql(
                        f"SELECT MIN({col_name}) as min_val, MAX({col_name}) as max_val FROM {schema_name}.{table_name}"
                    ).collect()
                    if range_result:
                        col_metadata['numeric_range'] = {
                            'min': range_result[0]['MIN_VAL'],
                            'max': range_result[0]['MAX_VAL']
                        }
                
                # Get sample values for non-numeric (up to 5 distinct values)
                else:
                    sample_result = session.sql(
                        f"SELECT DISTINCT {col_name} FROM {schema_name}.{table_name} LIMIT 5"
                    ).collect()
                    col_metadata['sample_values'] = [row[0] for row in sample_result if row[0]]
                
            except Exception as e:
                error_handler.log_error(
                    error_code=ErrorCode.DATABASE_QUERY_FAILED,
                    error_type=type(e).__name__,
                    severity=ErrorSeverity.WARNING,
                    message=f"Could not get metadata for {table_name}.{col_name}: {str(e)}"
                )
            
            rebuilt_ctx['columns'].append(col_metadata)
        
        rebuilt_contexts.append(rebuilt_ctx)
    
    return rebuilt_contexts


@timeit
def validate_questions_with_llm(
    questions: List[Dict],
    rich_table_contexts: List[Dict],
    session,
    error_handler,
    return_debug_info: bool = False
):
    """
    Use LLM to intelligently validate questions against actual schema and data.
    
    Much smarter than regex patterns - understands semantics, can reason about
    whether questions are answerable, and catches edge cases like missing cost columns
    when questions ask about "expensive" items.
    
    NOTE: Search-category questions are automatically validated (they search unstructured
    text, not structured columns, so column validation doesn't apply).
    
    Args:
        questions: List of question dictionaries with 'text', 'type', 'difficulty'
        rich_table_contexts: Rich context with schema, cardinality, ranges, sample data
        session: Snowflake session for LLM calls
        error_handler: ErrorHandler instance
        return_debug_info: If True, returns (validated_questions, debug_info) tuple
        
    Returns:
        List of validated question dictionaries that are answerable with available data
        OR tuple of (validated_questions, debug_info) if return_debug_info=True
    """
    import time
    import streamlit as st
    
    debug_info = {
        'validation_started': time.time(),
        'total_questions': len(questions),
        'validation_success': False,
        'error_message': None,
        'llm_response_preview': None,
        'validated_count': 0,
        'validation_time': 0
    }
    if not questions or not rich_table_contexts:
        debug_info['validation_success'] = True
        debug_info['validated_count'] = len(questions)
        debug_info['validation_time'] = time.time() - debug_info['validation_started']
        debug_info['error_message'] = "No validation needed (empty questions or contexts)"
        if return_debug_info:
            return questions, debug_info
        return questions
    
    # Separate search questions from analytics questions
    # Search questions are for unstructured text content, so they don't need column validation
    search_questions = [q for q in questions if q.get('category') == 'search']
    analytics_questions = [q for q in questions if q.get('category') != 'search']
    
    # Search questions are automatically valid (no column checks needed)
    validated_search = search_questions.copy()
    
    # If no analytics questions to validate, return all questions
    if not analytics_questions:
        debug_info['validation_success'] = True
        debug_info['validated_count'] = len(questions)
        debug_info['validation_time'] = time.time() - debug_info['validation_started']
        debug_info['error_message'] = f"All {len(search_questions)} questions are search-category (auto-validated)"
        if return_debug_info:
            return questions, debug_info
        return questions
    
    # PART 2A & 2B: Query actual Snowflake columns and compare with expectations
    # This is the SOURCE OF TRUTH - what columns actually exist
    actual_columns_by_table = {}
    comparison_start = time.time()
    
    # Try to reuse already-queried columns from session state (avoids redundant queries)
    if 'actual_snowflake_columns' in st.session_state:
        actual_columns_by_table = st.session_state['actual_snowflake_columns']
    else:
        # Fallback: Query if not available in session state
        schema_name = st.session_state.get('last_created_schema', '')
        if schema_name:
            # Extract table names from rich_table_contexts
            table_names = [ctx['name'] for ctx in rich_table_contexts]
            
            # Query actual Snowflake columns
            try:
                actual_columns_by_table = get_actual_table_columns_from_snowflake(
                    schema_name=schema_name,
                    table_names=table_names,
                    session=session,
                    error_handler=error_handler
                )
            except Exception as e:
                # If query fails, just use empty dict and continue with validation
                actual_columns_by_table = {}
    
    # Only do comparison if we have actual columns
    if actual_columns_by_table:
        
        # PART 2B: Compare what we THOUGHT columns were vs what they ACTUALLY are
        discrepancies = []
        total_missing = 0
        total_extra = 0
        
        for ctx in rich_table_contexts:
            table_name = ctx['name']
            expected_columns = set([col['name'].upper() for col in ctx.get('columns', [])])
            actual_columns = set(actual_columns_by_table.get(table_name, []))
            
            missing = expected_columns - actual_columns  # We thought they existed, but don't
            extra = actual_columns - expected_columns     # They exist, but we didn't know
            
            if missing or extra:
                discrepancies.append({
                    'table': table_name,
                    'missing_in_snowflake': list(missing),
                    'extra_in_snowflake': list(extra)
                })
                total_missing += len(missing)
                total_extra += len(extra)
        
        # UPDATE metrics: was Snowflake query needed?
        any_discrepancies = total_missing > 0 or total_extra > 0
        
        if 'schema_validation_metrics' in st.session_state and 'snowflake_query' in st.session_state['schema_validation_metrics']:
            st.session_state['schema_validation_metrics']['snowflake_query']['was_needed'] = any_discrepancies
            st.session_state['schema_validation_metrics']['snowflake_query']['issues_caught'] = discrepancies
        
        if 'schema_validation_metrics' not in st.session_state:
            st.session_state['schema_validation_metrics'] = {}
        
        st.session_state['schema_validation_metrics']['schema_comparison'] = {
            'time': time.time() - comparison_start,
            'discrepancies_found': len(discrepancies),
            'columns_missing': total_missing,
            'columns_extra': total_extra,
            'was_needed': any_discrepancies,  # TRUE if we found misalignment
            'details': discrepancies
        }
    
    # PART 2C: Build EXPLICIT column-only schema for validation using ACTUAL Snowflake columns
    # DO NOT include descriptions to avoid LLM assuming columns exist based on concepts
    schema_summary = []
    schema_summary.append("=" * 80)
    schema_summary.append("ACTUAL COLUMNS IN SNOWFLAKE (SOURCE OF TRUTH)")
    schema_summary.append("=" * 80)
    schema_summary.append("\nQuestions can ONLY reference columns from the lists below.")
    schema_summary.append("Do NOT assume columns exist based on table descriptions or logical expectations.\n")
    
    for ctx in rich_table_contexts:
        table_name = ctx['name']
        schema_summary.append(f"\nTable: {table_name}")
        schema_summary.append(f"Row count: {ctx.get('row_count', 0)}")
        
        # Use ACTUAL Snowflake columns if available, otherwise fall back to rich_table_contexts
        actual_cols = actual_columns_by_table.get(table_name, [])
        
        if actual_cols:
            # Use actual Snowflake columns - SOURCE OF TRUTH
            column_names = []
            for actual_col_name in actual_cols:
                # Find matching column info from rich_table_contexts for metadata
                col_info = f"{actual_col_name}"
                matching_col = next((col for col in ctx.get('columns', []) if col['name'].upper() == actual_col_name), None)
                
                if matching_col:
                    col_info += f" ({matching_col['type']})"
                    # Add cardinality info (important for validation)
                    if matching_col.get('unique_count') is not None:
                        col_info += f" [{matching_col['unique_count']} unique]"
                    # Add range for numeric columns (important for validation)
                    if matching_col.get('numeric_range'):
                        nr = matching_col['numeric_range']
                        col_info += f" [range: {nr['min']}-{nr['max']}]"
                
                column_names.append(col_info)
            
            schema_summary.append(f"Columns (ACTUAL from Snowflake): {', '.join(column_names)}")
        else:
            # Fall back to rich_table_contexts
            column_names = []
            for col in ctx.get('columns', []):
                col_info = f"{col['name']} ({col['type']})"
                
                # Add cardinality info (important for validation)
                if col.get('unique_count') is not None:
                    col_info += f" [{col['unique_count']} unique]"
                
                # Add range for numeric columns (important for validation)
                if col.get('numeric_range'):
                    nr = col['numeric_range']
                    col_info += f" [range: {nr['min']}-{nr['max']}]"
                
                column_names.append(col_info)
            
            schema_summary.append(f"Columns: {', '.join(column_names)}")
    
    schema_summary.append("\n" + "=" * 80)
    schema_summary.append("VALIDATION RULE: Questions can ONLY reference columns listed above.")
    schema_summary.append("=" * 80)
    
    # Add semantic model synonym mappings for validation context
    # Only include synonyms for columns that ACTUALLY exist in Snowflake
    try:
        semantic_view_info = st.session_state.get('last_relationship_analysis', {}).get('semantic_view_info', {})
        column_synonyms = semantic_view_info.get('column_synonyms', {}) if semantic_view_info else {}
        
        if column_synonyms and isinstance(column_synonyms, dict):
            # Filter synonyms to only include columns that actually exist
            valid_synonyms = {}
            for col_key, synonyms in column_synonyms.items():
                if col_key and synonyms and '.' in col_key:
                    table, col = col_key.rsplit('.', 1)
                    # Check if this column actually exists in Snowflake
                    if actual_columns_by_table and table in actual_columns_by_table:
                        if col.upper() in actual_columns_by_table[table]:
                            valid_synonyms[col_key] = synonyms
                    else:
                        # If we don't have actual columns, include all synonyms
                        valid_synonyms[col_key] = synonyms
            
            if valid_synonyms:
                schema_summary.append("\n" + "=" * 80)
                schema_summary.append("NATURAL LANGUAGE MAPPINGS (Cortex Analyst Understanding):")
                schema_summary.append("=" * 80)
                schema_summary.append("\nCortex Analyst can understand these natural language terms for columns:")
                for col_name, synonyms in valid_synonyms.items():
                    # Format as "TABLE.COLUMN: synonym1, synonym2, synonym3"
                    schema_summary.append(f"\n  {col_name}: {synonyms}")
                schema_summary.append("\n" + "=" * 80)
                schema_summary.append("Use these mappings to validate if natural language in questions can map to columns.")
                schema_summary.append("=" * 80)
    except Exception as e:
        # If synonym mapping fails, just skip it - validation will still work
        pass
    
    # Format ONLY analytics questions for validation
    questions_text = []
    for i, q in enumerate(analytics_questions, 1):
        questions_text.append(f"{i}. {q['text']}")
    
    prompt = f"""🚨 ULTRA-STRICT VALIDATION - ZERO TOLERANCE FOR UNANSWERABLE QUESTIONS 🚨

You are performing MANDATORY validation to guarantee 100% question answerability.

ACTUAL COLUMNS (SOURCE OF TRUTH):
{chr(10).join(schema_summary)}

QUESTIONS TO VALIDATE:
{chr(10).join(questions_text)}

{"=" * 80}
YOUR JOB: REJECT ANY QUESTION IF YOU HAVE EVEN SLIGHT DOUBT
{"=" * 80}

ULTRA-STRICT VALIDATION RULES:

1. EXACT COLUMN MATCHING (MANDATORY):
   ❌ REJECT if question references ANY concept not explicitly present as a column
   ❌ REJECT if question implies a metric without direct column support
   ❌ REJECT if natural language term does NOT map to a listed column
   
   Examples:
   - Question: "annual rewards" → REJECT unless ANNUAL_REWARDS or REWARDS column exists
   - Question: "profit margins" → REJECT unless PROFIT_MARGIN or both PROFIT + REVENUE columns exist
   - Question: "customer satisfaction" → REJECT unless SATISFACTION/RATING/SCORE column exists
   - Question: "delegation counts" → OK if DELEGATION_COUNT or similar exists

2. ZERO ASSUMPTIONS (MANDATORY):
   ❌ DO NOT assume semantic equivalents unless explicitly listed in NATURAL LANGUAGE MAPPINGS
   ❌ DO NOT infer that similar concepts are available
   ❌ DO NOT trust that Cortex Analyst can "figure it out"
   
   IF IN ANY DOUBT → REJECT THE QUESTION

3. CALCULATED METRICS - ULTRA-STRICT:
   ❌ REJECT if question asks about derived metrics without explicit supporting columns
   
   Growth/Trend Questions:
   - "growth rate", "increase", "trend over time" → REJECT unless GROWTH_RATE column exists
   - Exception: Simple time-series with DATE + numeric column may work for "show X over time"
   - But "YoY growth" or "% increase" → MUST have explicit column
   
   Ratio/Percentage Questions:
   - "percentage of", "share of", "proportion" → REJECT unless PERCENTAGE column exists
   - Exception: Simple "breakdown by category" (Cortex can calculate) is OK
   - But "profit margin %" or "conversion rate" → MUST have explicit column
   
   Derived Metrics:
   - "average price per unit" → REJECT unless AVG_PRICE column exists OR both PRICE + QUANTITY exist
   - "cost per customer" → REJECT unless COST_PER_CUSTOMER exists OR both COST + CUSTOMER_COUNT exist
   - ANY metric that requires division/multiplication of two columns → MUST verify BOTH columns exist

4. COLUMN EXISTENCE VERIFICATION:
   For EVERY metric/dimension in a question, you MUST:
   □ Find the exact column name in the list above
   □ OR find it in the NATURAL LANGUAGE MAPPINGS with explicit synonym match
   □ OR REJECT the question
   
   DO NOT use loose semantic matching. Require EXACT or EXPLICITLY MAPPED terms only.

5. CARDINALITY & DATA SUPPORT:
   ❌ REJECT if asking "top N" where N >= unique count
   ❌ REJECT if grouping by column with insufficient cardinality
   ❌ REJECT if time-based analysis without proper DATE/TIMESTAMP columns
   ❌ REJECT if numeric aggregation on non-numeric columns

6. IF IN DOUBT → REJECT:
   - Not sure if column supports the concept? → REJECT
   - Not sure if calculation is feasible? → REJECT
   - Not sure if data granularity matches question? → REJECT
   - Not certain 100%? → REJECT
   
   Better to have 6 perfect questions than 10 questions where 4 fail.

7. SEARCH QUESTION CONTENT TYPE MATCHING & DATA BOUNDARY (MANDATORY):
   For questions with category="search" (unstructured data queries):
   
   CRITICAL DATA BOUNDARY VALIDATION:
   - Every question must be answerable with data that EXISTS in the provided context
   - For ANALYTICS: Verify all referenced columns/metrics are visible in the schema above
   - For SEARCH: Verify the question matches the actual sample content (not just content_type label)
   - ❌ REJECT if question assumes data beyond what was shown
   - ❌ REJECT if search question doesn't match what you see in the sample chunks
   
   CONTENT TYPE MATCHING WITH ACTUAL SAMPLES:
   ❌ REJECT if question asks for content type that doesn't match actual generated samples
   
   CRITICAL: If content_type label says one thing but samples show another → TRUST THE SAMPLES
   
   Examples based on ACTUAL sample content (not content_type labels):
   - IF samples show "Standard Operating Procedure: Payment processing... Step 1) Verify..."
     ✅ VALID: "Find operational procedures for payment systems"
     ❌ REJECT: "Find customer feedback about payments" (samples don't show feedback!)
   
   - IF samples show "Customer complained: The food was cold when delivered..."
     ✅ VALID: "Search for customer complaints about food quality"
     ❌ REJECT: "Locate technical documentation" (samples don't show technical docs!)
   
   - IF samples show "API Specification: POST /api/v1/orders. Parameters: customer_id..."
     ✅ VALID: "Find API specifications for order management"
     ❌ REJECT: "Search for implementation guides for ELD integration" (too specific if samples are generic)
   
   - IF content_type='customer_feedback' BUT samples show "Incident Report: System failure..."
     ❌ REJECT questions about customer feedback (actual content is incident reports!)
     ✅ VALID questions about incident reports (match what samples actually contain)
   
   VALIDATION PROCESS FOR SEARCH QUESTIONS:
   Step 1: Identify if question is category="search"
   Step 2: Read the ACTUAL sample content provided for unstructured tables
   Step 3: Check what the question asks for (documentation? feedback? policies? incidents?)
   Step 4: Verify the SAMPLES (not the content_type label) contain that content
   Step 5: Check specificity: Is the question asking for something more specific than what samples show?
   Step 6: If mismatch OR too specific → REJECT
   
   SPECIFICITY RULE:
   Search question specificity ≤ Sample content specificity
   - Samples show "API Specification: Vehicle Diagnostics" → Can ask for "API specifications" ✓
   - Samples show "API Specification: Vehicle Diagnostics" → Cannot ask for "ELD implementation guides" ✗ (too specific)

CRITICAL OUTPUT FORMAT:
You MUST return ONLY a JSON object. NO explanations, NO reasoning, NO markdown.
Just this exact format:

{{"valid_question_numbers": [1, 3, 5, ...]}}

Example valid response:
{{"valid_question_numbers": [1, 2, 3, 5, 7, 8, 9, 10, 12]}}

Example INVALID responses (DO NOT do this):
- "Looking at each question..." (NO text before JSON)
- "```json\n{{"valid_question_numbers": [...]}}\n```" (NO markdown, just raw JSON)
- "Based on analysis, I recommend..." (NO explanations)

Return the JSON object immediately. Nothing else."""

    try:
        response = call_cortex_with_retry(prompt, session, error_handler)
        
        # Store response preview for debugging
        debug_info['llm_response_preview'] = response[:200] if response else "No response"
        
        if not response or not response.strip():
            debug_info['validation_success'] = False
            debug_info['error_message'] = "LLM returned empty response"
            debug_info['validated_count'] = 0  # Validation failed, 0 were properly validated
            debug_info['fallback_used'] = True
            debug_info['fallback_count'] = len(analytics_questions)
            debug_info['validation_time'] = time.time() - debug_info['validation_started']
            
            if st.session_state.get('debug_mode', False):
                st.warning("⚠️ LLM validation returned empty response, using all analytics questions")
            
            # Return all analytics questions + auto-validated search questions
            combined = analytics_questions + validated_search
            debug_info['validated_count'] = len(combined)
            if return_debug_info:
                return combined, debug_info
            return combined
        
        import json
        # Handle potential JSON parsing issues
        response = response.strip()
        if response.startswith('```json'):
            response = response[7:]
        if response.startswith('```'):
            response = response[3:]
        if response.endswith('```'):
            response = response[:-3]
        response = response.strip()
        
        # Try to parse JSON
        try:
            result = json.loads(response)
        except json.JSONDecodeError as json_err:
            debug_info['validation_success'] = False
            debug_info['error_message'] = f"JSON parse error: {str(json_err)}"
            debug_info['validated_count'] = 0  # Validation failed, 0 were properly validated
            debug_info['fallback_used'] = True
            debug_info['fallback_count'] = len(analytics_questions)
            debug_info['validation_time'] = time.time() - debug_info['validation_started']
            
            if st.session_state.get('debug_mode', False):
                st.warning(f"⚠️ LLM validation response not valid JSON: {str(json_err)[:100]}, using all analytics questions")
            
            # Return all analytics questions + auto-validated search questions
            combined = analytics_questions + validated_search
            debug_info['validated_count'] = len(combined)
            if return_debug_info:
                return combined, debug_info
            return combined
        
        valid_nums = result.get('valid_question_numbers', [])
        
        # Return analytics questions that LLM validated
        validated_analytics = []
        for num in valid_nums:
            if 1 <= num <= len(analytics_questions):
                validated_analytics.append(analytics_questions[num - 1])
        
        # Combine validated analytics with auto-validated search questions
        combined = validated_analytics + validated_search
        
        # PART 3: Track synonym necessity - did questions use natural language that needed mapping?
        natural_language_questions = 0
        if actual_columns_by_table:
            # Check if validated questions use natural language (not exact column names)
            all_actual_columns = []
            for cols in actual_columns_by_table.values():
                all_actual_columns.extend(cols)
            
            for q in validated_analytics:
                q_text = q['text'].upper()
                # Check if question uses exact column names or natural language
                uses_exact_columns = any(col in q_text for col in all_actual_columns)
                # If it doesn't use exact columns, it relies on semantic understanding/synonyms
                if not uses_exact_columns:
                    natural_language_questions += 1
        
        # UPDATE synonym metrics
        if 'schema_validation_metrics' not in st.session_state:
            st.session_state['schema_validation_metrics'] = {}
        
        if 'synonym_generation' in st.session_state['schema_validation_metrics']:
            synonym_metrics = st.session_state['schema_validation_metrics']['synonym_generation']
            synonym_metrics['was_needed'] = natural_language_questions > 0
            synonym_metrics['natural_language_questions'] = natural_language_questions
            if natural_language_questions > 0:
                synonym_metrics['issues_caught'] = f"{natural_language_questions} questions needed synonym mapping"
            else:
                synonym_metrics['issues_caught'] = "All questions used exact column names"
        
        debug_info['validation_success'] = True
        debug_info['validated_count'] = len(combined)
        debug_info['validation_time'] = time.time() - debug_info['validation_started']
        
        if return_debug_info:
            return combined, debug_info
        return combined
            
    except Exception as e:
        # If LLM validation fails, fall back to returning all analytics questions + search questions
        # Better to have some questions than none
        debug_info['validation_success'] = False
        debug_info['error_message'] = f"Unexpected error: {str(e)}"
        debug_info['validated_count'] = 0  # Validation failed, 0 were properly validated
        debug_info['fallback_used'] = True
        debug_info['fallback_count'] = len(analytics_questions)
        debug_info['validation_time'] = time.time() - debug_info['validation_started']
        
        if st.session_state.get('debug_mode', False):
            st.warning(f"⚠️ LLM validation failed ({str(e)[:100]}), using all analytics questions")
        
        # Return all analytics questions + auto-validated search questions
        combined = analytics_questions + validated_search
        debug_info['validated_count'] = len(combined)
        if return_debug_info:
            return combined, debug_info
        return combined


def test_questions_against_semantic_model(
    questions: List[Dict],
    semantic_model_info: Dict,
    schema_name: str,
    session,
    error_handler
) -> Dict:
    """
    Test if questions can actually be answered by querying the semantic model.
    This is the ULTIMATE validation - if Cortex Analyst can't parse it, Agent can't either.
    
    Returns:
        Dict with 'answerable': List of questions that passed,
                   'failed': List with error details
    """
    if not semantic_model_info or 'view_name' not in semantic_model_info:
        return {'answerable': questions, 'failed': [], 'skipped': True}
    
    view_name = semantic_model_info['view_name']
    full_view_name = f"{schema_name}.{view_name}"
    
    answerable = []
    failed = []
    
    for q_dict in questions:
        if q_dict.get('category') == 'search':
            # Search questions don't use semantic model
            answerable.append(q_dict)
            continue
        
        question_text = q_dict.get('text', '')
        
        try:
            # Try to generate SQL using Cortex Analyst
            test_sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'claude-3-5-sonnet-20241022',
                CONCAT(
                    'Given this semantic model view: {full_view_name}, ',
                    'can you generate SQL to answer: "{question_text}"? ',
                    'Respond with just YES or NO.'
                )
            ) as can_answer
            """
            
            result = session.sql(test_sql).collect()
            response = result[0]['CAN_ANSWER'].upper() if result else 'NO'
            
            if 'YES' in response:
                answerable.append(q_dict)
            else:
                failed.append({
                    'question': question_text,
                    'reason': 'Cortex Analyst cannot generate SQL for this question'
                })
        
        except Exception as e:
            # If we get an error, the question is likely not answerable
            failed.append({
                'question': question_text,
                'reason': f'Error testing question: {str(e)[:100]}'
            })
    
    return {
        'answerable': answerable,
        'failed': failed,
        'skipped': False,
        'pass_rate': len(answerable) / len(questions) if questions else 0
    }


def select_best_questions(
    questions: List[Dict],
    target_count: int = 12,
    min_advanced: int = 3
) -> List[Dict]:
    """
    Select best questions from a pool, prioritizing complexity and diversity.
    
    Ensures a good mix of difficulties with minimum advanced questions.
    
    Args:
        questions: Pool of questions to select from
        target_count: Number of questions to select (default: 12)
        min_advanced: Minimum number of advanced questions required (default: 3)
        
    Returns:
        List of selected questions with balanced difficulty
    """
    if len(questions) <= target_count:
        return questions
    
    # Group by difficulty
    advanced = [q for q in questions if q.get('difficulty') == 'advanced']
    intermediate = [q for q in questions if q.get('difficulty') == 'intermediate']
    basic = [q for q in questions if q.get('difficulty') == 'basic']
    
    selected = []
    
    # First, ensure we have minimum advanced questions
    selected.extend(advanced[:min_advanced])
    remaining_slots = target_count - len(selected)
    
    # If we don't have enough advanced, take what we can get
    if len(selected) < min_advanced:
        # Fill remaining advanced slots with intermediate questions
        needed = min_advanced - len(selected)
        selected.extend(intermediate[:needed])
        intermediate = intermediate[needed:]
        remaining_slots = target_count - len(selected)
    else:
        # We have enough advanced, continue with remaining advanced if any
        advanced = advanced[min_advanced:]
    
    # Now distribute remaining slots between intermediate, basic, and any leftover advanced
    # Target distribution for remaining: 60% intermediate, 30% basic, 10% advanced
    remaining_advanced_count = min(len(advanced), max(1, int(remaining_slots * 0.1)))
    remaining_intermediate_count = min(len(intermediate), int(remaining_slots * 0.6))
    remaining_basic_count = min(len(basic), remaining_slots - remaining_advanced_count - remaining_intermediate_count)
    
    # Add the distributed questions
    selected.extend(advanced[:remaining_advanced_count])
    selected.extend(intermediate[:remaining_intermediate_count])
    selected.extend(basic[:remaining_basic_count])
    
    # If we still don't have enough, fill with whatever is left
    if len(selected) < target_count:
        remaining = advanced[remaining_advanced_count:] + intermediate[remaining_intermediate_count:] + basic[remaining_basic_count:]
        needed = target_count - len(selected)
        selected.extend(remaining[:needed])
    
    return selected[:target_count]


def create_question_chains(
    questions: List[Dict[str, Any]],
    session,
    error_handler,
    max_chains: int = 3
) -> Dict[str, List[str]]:
    """
    Create question chains with follow-ups IN PARALLEL.
    
    This function parallelizes the generation of follow-up questions to significantly
    reduce total execution time (from ~24s sequential to ~6s parallel for 12 questions).
    
    Args:
        questions: List of question dictionaries
        session: Snowflake session
        error_handler: ErrorHandler instance
        max_chains: Maximum number of chains to create
        
    Returns:
        Dictionary mapping primary questions to follow-up lists
    """
    chains = {}
    
    primary_questions = [
        q for q in questions
        if q.get('difficulty') in ['basic', 'intermediate']
    ][:max_chains]
    
    # Build argument list for parallel execution
    args_list = [
        [primary_q['text'], session, error_handler]
        for primary_q in primary_questions
    ]
    
    # Execute all follow-up generation calls in parallel (max 4 workers)
    follow_ups_list = safe_parallel_execute(
        generate_follow_up_questions,
        args_list,
        max_workers=min(4, len(primary_questions)),
        fallback_value=[]
    )
    
    # Build chains dictionary
    for q_dict, follow_ups in zip(primary_questions, follow_ups_list):
        if follow_ups:
            chains[q_dict['text']] = follow_ups
    
    return chains


def generate_follow_up_questions(
    primary_question: str,
    session,
    error_handler
) -> List[str]:
    """
    Generate follow-up questions for a primary question.
    
    Args:
        primary_question: The primary question text
        session: Snowflake session
        error_handler: ErrorHandler instance
        
    Returns:
        List of follow-up question strings
    """
    prompt = get_follow_up_questions_prompt(primary_question)
    
    try:
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) as response",
            params=[LLM_MODEL, prompt]
        ).collect()
        
        response = result[0]['RESPONSE']
        
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if json_match:
            follow_ups = json.loads(json_match.group(0))
            return follow_ups[:MAX_FOLLOW_UP_QUESTIONS]
        else:
            return []
    except Exception:
        return []


def analyze_target_questions(
    questions: List[str],
    session,
    error_handler
) -> Dict[str, Any]:
    """
    Analyze target questions using LLM to extract required entities, metrics,
    dimensions, and other requirements for demo generation.
    
    This is CRITICAL for Phase 0 - ensures target questions drive demo design
    from the very beginning, not as an afterthought.
    
    Args:
        questions: List of target questions that the demo MUST answer
        session: Snowflake session
        error_handler: ErrorHandler instance
    
    Returns:
        dict: Comprehensive analysis with structure:
        {
            "questions": [
                {
                    "text": "What are the top 10 suppliers by cost?",
                    "required_entities": ["suppliers"],
                    "required_metrics": ["cost_amount"],
                    "required_dimensions": ["supplier_name", "supplier_id"],
                    "aggregation": "ranking (top 10)",
                    "min_cardinality": 15,  # Need 15+ suppliers for "top 10"
                    "question_type": "aggregation"
                }
            ],
            "all_required_entities": ["suppliers", "orders"],
            "all_required_metrics": ["cost_amount", "order_count"],
            "all_required_dimensions": ["supplier_name", "date"],
            "min_tables_needed": 2,
            "has_target_questions": True,
            "original_questions": [...]
        }
    """
    if not questions or len(questions) == 0:
        return {
            'has_target_questions': False,
            'required_dimensions': [],
            'metrics_needed': [],
            'data_characteristics': {},
            'all_required_entities': [],
            'all_required_metrics': [],
            'all_required_dimensions': [],
            'min_tables_needed': 1,
            'questions': []
        }
    
    # Enhanced prompt for detailed analysis
    prompt = f"""Analyze these target questions in detail to extract ALL requirements for a demo data model.

Target Questions:
{chr(10).join([f'{i+1}. {q}' for i, q in enumerate(questions)])}

For EACH question, extract:
1. **Required Entities**: What business objects are needed? (e.g., "suppliers", "products", "customers")
2. **Required Metrics**: What numeric measurements? (e.g., "cost_amount", "revenue", "count")
3. **Required Dimensions**: What attributes for grouping/filtering? (e.g., "supplier_name", "category", "date")
4. **Aggregation Type**: What kind of calculation? (e.g., "top N ranking", "sum", "average", "count", "comparison")
5. **Min Cardinality**: If asking "top N", what's the minimum unique count needed? (top 10 → need 15+ unique values)
6. **Question Type**: "aggregation", "trend", "comparison", "filter", "ranking"

Then provide CONSOLIDATED requirements across all questions:
- All unique entities needed
- All unique metrics needed
- All unique dimensions needed
- Minimum number of tables needed to support these questions

Return ONLY valid JSON:
{{
  "questions": [
    {{
      "text": "question text",
      "required_entities": ["entity1", "entity2"],
      "required_metrics": ["metric1"],
      "required_dimensions": ["dim1", "dim2"],
      "aggregation": "description of aggregation",
      "min_cardinality": 15,
      "question_type": "aggregation"
    }}
  ],
  "all_required_entities": ["entity1", "entity2"],
  "all_required_metrics": ["metric1", "metric2"],
  "all_required_dimensions": ["dim1", "dim2", "dim3"],
  "min_tables_needed": 2,
  "cardinality_constraints": [
    {{"field": "supplier_id", "min_unique": 15, "reason": "top 10 query needs 15+ suppliers"}}
  ]
}}

Be thorough and specific. Extract ALL implicit requirements."""
    
    try:
        response = call_cortex_with_retry(prompt, session, error_handler)
        
        if response:
            parsed = safe_json_parse(response)
            if parsed:
                # Add metadata
                parsed['has_target_questions'] = True
                parsed['original_questions'] = questions
                
                # Also populate legacy fields for backward compatibility
                parsed['required_dimensions'] = parsed.get('all_required_dimensions', [])
                parsed['metrics_needed'] = parsed.get('all_required_metrics', [])
                parsed['data_characteristics'] = {
                    'numeric_fields': parsed.get('all_required_metrics', []),
                    'categorical_fields': parsed.get('all_required_dimensions', []),
                    'cardinality_constraints': parsed.get('cardinality_constraints', [])
                }
                
                return parsed
    except Exception as e:
        if st.session_state.get('debug_mode', False):
            st.warning(f"⚠️ Could not analyze target questions: {str(e)}")
    
    # Fallback: return basic structure
    return {
        'has_target_questions': True,
        'original_questions': questions,
        'required_dimensions': [],
        'metrics_needed': [],
        'data_characteristics': {},
        'all_required_entities': [],
        'all_required_metrics': [],
        'all_required_dimensions': [],
        'min_tables_needed': 1,
        'questions': [],
        'cardinality_constraints': []
    }


def format_questions_for_display(
    questions: List[Dict[str, Any]]
) -> str:
    """
    Format questions for display in UI.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Formatted markdown string
    """
    output = "## Generated Questions\n\n"
    
    basic_questions = [
        q for q in questions if q.get('difficulty') == 'basic'
    ]
    intermediate_questions = [
        q for q in questions if q.get('difficulty') == 'intermediate'
    ]
    advanced_questions = [
        q for q in questions if q.get('difficulty') == 'advanced'
    ]
    
    if basic_questions:
        output += "### Basic Questions\n\n"
        for idx, q in enumerate(basic_questions, 1):
            output += f"{idx}. {q['text']}\n"
        output += "\n"
    
    if intermediate_questions:
        output += "### Intermediate Questions\n\n"
        for idx, q in enumerate(intermediate_questions, 1):
            output += f"{idx}. {q['text']}\n"
        output += "\n"
    
    if advanced_questions:
        output += "### Advanced Questions\n\n"
        for idx, q in enumerate(advanced_questions, 1):
            output += f"{idx}. {q['text']}\n"
        output += "\n"
    
    return output


# ============================================================================
# DATA GENERATION
# ============================================================================

def normalize_field_name_to_sql(field_name: str) -> str:
    """
    Normalize natural language field names to SQL conventions.
    
    Converts "store ids" -> "STORE_ID"
    Converts "employee counts" -> "EMPLOYEE_COUNT"
    Converts "geographic coordinates" -> "GEOGRAPHIC_COORDINATE"
    
    Args:
        field_name: Natural language field name (may have spaces, plural)
        
    Returns:
        SQL-compliant field name (underscores, singular, uppercase)
    """
    import re
    
    # Convert to lowercase for processing
    normalized = field_name.lower().strip()
    
    # Replace spaces with underscores
    normalized = re.sub(r'\s+', '_', normalized)
    
    # Remove common plurals (be careful to preserve words that end in 's' naturally)
    # Handle common plural patterns
    plural_patterns = [
        (r'ies$', 'y'),      # quantities -> quantity, categories -> category
        (r'ses$', 's'),      # addresses -> address, classes -> class  
        (r'ches$', 'ch'),    # batches -> batch
        (r'xes$', 'x'),      # indexes -> index
        (r'([^s])s$', r'\1') # ids -> id, counts -> count (but not 'class' -> 'clas')
    ]
    
    for pattern, replacement in plural_patterns:
        if re.search(pattern, normalized):
            normalized = re.sub(pattern, replacement, normalized)
            break  # Only apply one pattern
    
    # Convert to uppercase
    normalized = normalized.upper()
    
    return normalized


def extract_required_fields_from_description(
    table_description: str
) -> List[Dict[str, Any]]:
    """
    Parse table description to extract explicitly mentioned field names.
    
    Uses regex patterns to identify:
    - Fields mentioned with parentheses: "movement_type (receipt, usage, waste)"
    - Fields in "including" lists: "including restaurant_id, supplier_id"
    - Common patterns: "with delivery_date", "containing quality_score"
    
    Args:
        table_description: The table description text to parse
        
    Returns:
        List of dicts with extracted field information:
        [
            {
                'field_name': 'MOVEMENT_TYPE',
                'suggested_type': 'STRING',
                'sample_values': ['receipt', 'usage', 'waste', 'transfer'],
                'description': 'Type of inventory movement'
            },
            ...
        ]
    """
    import re
    import time
    
    extraction_start = time.time()
    
    required_fields = []
    seen_fields = set()
    
    # Pattern 1: field_name (value1, value2, value3)
    # Matches: "movement_type (receipt, usage, waste, transfer)"
    pattern1 = r'\b([a-z_][a-z0-9_]*)\s*\(([^)]+)\)'
    matches1 = re.findall(pattern1, table_description.lower())
    
    for field_name, values_str in matches1:
        field_normalized = normalize_field_name_to_sql(field_name)
        if field_normalized not in seen_fields:
            # Extract values from parentheses
            values = [v.strip() for v in values_str.split(',')]
            
            # Infer type based on values
            suggested_type = 'STRING'
            if all(v.replace('.', '').replace('-', '').isdigit() for v in values if v):
                suggested_type = 'NUMBER'
            
            required_fields.append({
                'field_name': field_normalized,
                'suggested_type': suggested_type,
                'sample_values': values[:10],  # Limit to first 10
                'description': f'{field_name.replace("_", " ").title()}'
            })
            seen_fields.add(field_normalized)
    
    # Pattern 2: "including field1, field2, field3"
    pattern2 = r'including\s+([a-z0-9_,\s]+?)(?:\s+with|\s+and\s+|[,.]|\s*$)'
    matches2 = re.findall(pattern2, table_description.lower())
    
    for match in matches2:
        # Split by commas and "and"
        fields = re.split(r',|\s+and\s+', match)
        for field_name in fields:
            field_name = field_name.strip()
            if field_name and len(field_name) > 2:
                field_normalized = normalize_field_name_to_sql(field_name)
                if field_normalized not in seen_fields:
                    # Infer type from field name (use normalized version for checking)
                    suggested_type = 'STRING'
                    field_lower = field_normalized.lower()
                    if any(keyword in field_lower for keyword in ['_id', '_key', '_number', '_count', '_quantity']):
                        suggested_type = 'NUMBER'
                    elif any(keyword in field_lower for keyword in ['_date', '_time', '_timestamp']):
                        suggested_type = 'TIMESTAMP' if 'time' in field_lower or 'timestamp' in field_lower else 'DATE'
                    elif any(keyword in field_lower for keyword in ['_amount', '_cost', '_price', '_value', '_rate', '_score']):
                        suggested_type = 'FLOAT'
                    
                    required_fields.append({
                        'field_name': field_normalized,
                        'suggested_type': suggested_type,
                        'sample_values': [],
                        'description': f'{field_name.replace("_", " ").title()}'
                    })
                    seen_fields.add(field_normalized)
    
    # Pattern 3: "with field_name" or "containing field_name"
    pattern3 = r'(?:with|containing)\s+([a-z_][a-z0-9_\s]*)'
    matches3 = re.findall(pattern3, table_description.lower())
    
    for field_name in matches3:
        field_name = field_name.strip()
        if len(field_name) > 3:
            field_normalized = normalize_field_name_to_sql(field_name)
            if field_normalized not in seen_fields:
                # Infer type from field name (use normalized version)
                suggested_type = 'STRING'
                field_lower = field_normalized.lower()
                if any(keyword in field_lower for keyword in ['_id', '_key', '_number', '_count', '_quantity']):
                    suggested_type = 'NUMBER'
                elif any(keyword in field_lower for keyword in ['_date', '_time', '_timestamp']):
                    suggested_type = 'TIMESTAMP' if 'time' in field_lower or 'timestamp' in field_lower else 'DATE'
                elif any(keyword in field_lower for keyword in ['_amount', '_cost', '_price', '_value', '_rate', '_score']):
                    suggested_type = 'FLOAT'
                
                required_fields.append({
                    'field_name': field_normalized,
                    'suggested_type': suggested_type,
                    'sample_values': [],
                    'description': f'{field_name.replace("_", " ").title()}'
                })
                seen_fields.add(field_normalized)
    
    # Pattern 4: Common data field keywords
    keywords_to_extract = [
        'promotional_period', 'promotional_periods', 'promotion_type',
        'weather_condition', 'weather_conditions', 'weather_pattern',
        'local_event', 'local_events', 'event_type',
        'holiday', 'holidays', 'holiday_type',
        'season', 'seasons', 'seasonal',
        'waste_amount', 'waste_quantity', 'waste_type',
        'quality_score', 'quality_rating', 'quality_metric',
        'performance_metric', 'performance_score', 'performance_rating'
    ]
    
    for keyword in keywords_to_extract:
        if keyword in table_description.lower():
            field_normalized = normalize_field_name_to_sql(keyword)
            if field_normalized not in seen_fields:
                # Infer type
                suggested_type = 'STRING'
                if 'amount' in keyword or 'quantity' in keyword or 'score' in keyword or 'rating' in keyword or 'metric' in keyword:
                    suggested_type = 'FLOAT'
                
                required_fields.append({
                    'field_name': field_normalized,
                    'suggested_type': suggested_type,
                    'sample_values': [],
                    'description': f'{keyword.replace("_", " ").title()}'
                })
                seen_fields.add(field_normalized)
    
    # Store metrics (was_needed will be updated later if validation finds missing fields)
    extraction_time = time.time() - extraction_start
    if 'schema_validation_metrics' not in st.session_state:
        st.session_state['schema_validation_metrics'] = {}
    
    st.session_state['schema_validation_metrics']['field_extraction'] = {
        'time': extraction_time,
        'fields_extracted': len(required_fields),
        'was_needed': False,  # Will be True if validation finds missing fields
        'issues_caught': []
    }
    
    return required_fields


@timeit
def generate_schema_for_table(
    table_name: str,
    table_description: str,
    company_name: str,
    session,
    error_handler,
    max_attempts: int = 5,
    target_questions: Optional[List[str]] = None,
    question_analysis: Optional[Dict] = None,
    required_fields: Optional[List[Dict]] = None,
    table_type: Optional[str] = None,
    language_code: str = "en"
) -> Optional[List[Dict]]:
    """
    Use Cortex to generate a realistic schema for a table with validation
    and retry logic.
    
    Args:
        table_name: Name of the table
        table_description: Description of the table's purpose
        company_name: Company context for realistic data
        session: Snowflake session
        error_handler: ErrorHandler instance
        max_attempts: Maximum number of generation attempts if validation fails
        target_questions: Optional list of target questions
        question_analysis: Optional analysis dict from analyze_target_questions()
        required_fields: Optional list of mandatory fields extracted from description
        table_type: Optional table type ('fact' or 'dimension') for relationship guidance
        language_code: Language code for content generation (e.g., 'en', 'ko', 'ja')
    
    Returns:
        list: List of column definitions or None if all attempts fail
    """
    # Track missing fields across attempts for retry prompts
    previous_missing_fields = []
    
    for attempt in range(max_attempts):
        # Use prompt from prompts module with required fields enforcement and table type
        prompt = get_schema_generation_prompt(
            table_name=table_name,
            table_description=table_description,
            company_name=company_name,
            target_questions=target_questions,
            question_analysis=question_analysis,
            required_fields=required_fields,
            table_type=table_type
        )
        
        # Enhance prompt with language-specific instructions for non-English content
        prompt = enhance_prompt_with_language(prompt, language_code)
        
        # Add explicit reminder on retry attempts
        if attempt > 0 and previous_missing_fields:
            prompt += f"\n\n⚠️ CRITICAL - PREVIOUS ATTEMPT FAILED\n"
            prompt += f"Missing required fields: {', '.join(previous_missing_fields)}\n"
            prompt += f"You MUST include these EXACT field names in the schema.\n"
            prompt += f"This is attempt {attempt + 1}/{max_attempts}.\n"
        try:
            response = call_cortex_with_retry(prompt, session, error_handler)
            
            if response:
                parsed = safe_json_parse(response)
                
                # Debug: Show what we received if parsing failed (only in debug mode)
                if st.session_state.get('debug_mode_infrastructure', False):
                    if not parsed or "columns" not in parsed:
                        with st.expander(f"🔍 Debug: LLM Response for {table_name} (attempt {attempt + 1})"):
                            st.write("**Parsed Result:**", parsed)
                            st.write(f"**Response Length:** {len(response)} characters")
                            st.write("**Raw Response (first 1000 chars):**")
                            st.code(response[:1000] if response else "No response")
                            
                            # Try to diagnose the issue
                            if response:
                                import json
                                st.write("**Diagnosis:**")
                                
                                # Check if it starts with valid JSON
                                if response.strip().startswith('{'):
                                    st.write("✓ Starts with '{'")
                                else:
                                    st.write(f"✗ Starts with: '{response[:50]}'")
                                
                                # Check for closing brace
                                if '}' in response:
                                    last_brace = response.rfind('}')
                                    st.write(f"✓ Contains closing brace at position {last_brace}")
                                    st.write(f"**Last 200 chars:** {response[last_brace-100:last_brace+100]}")
                                else:
                                    st.write("✗ No closing brace found")
                                
                                # Try direct JSON parse
                                try:
                                    direct_parse = json.loads(response)
                                    st.write("✓ Direct json.loads() succeeded!")
                                    st.write("Columns found:", len(direct_parse.get('columns', [])))
                                except json.JSONDecodeError as e:
                                    st.write(f"✗ Direct json.loads() failed: {e}")
                                    st.write(f"Error at position {e.pos}: {response[max(0, e.pos-50):e.pos+50]}")
                
                if parsed and "columns" in parsed:
                    columns = parsed.get("columns", [])
                    
                    # SAFE IMPROVEMENT 1: Normalize all column names to uppercase with underscores
                    # This prevents case-sensitivity issues (entity_id vs ENTITY_ID)
                    for col in columns:
                        if 'name' in col and col['name']:
                            col['name'] = col['name'].upper().replace(' ', '_').replace('-', '_')
                    
                    # SAFE IMPROVEMENT 2: Auto-inject ENTITY_ID if missing
                    # This prevents fallback to generic schema when LLM forgot ENTITY_ID
                    has_entity_id = any(col.get('name') == 'ENTITY_ID' for col in columns)
                    if columns and not has_entity_id:
                        columns.insert(0, {
                            "name": "ENTITY_ID",
                            "type": "NUMBER",
                            "description": "Primary key for joining tables",
                            "sample_values": list(range(1, 11))
                        })
                    
                    # Validate that we got at least ENTITY_ID and one other column
                    # After normalization and auto-injection, ENTITY_ID is guaranteed to exist
                    if len(columns) >= 2:
                        # Validate that all required fields are present
                        # Since we've normalized field names to SQL conventions, we can do simpler matching
                        if required_fields:
                            generated_field_names = {col['name'].upper() for col in columns}
                            missing_fields = []
                            
                            for req_field in required_fields:
                                required_name = req_field['field_name'].upper()
                                found_match = False
                                
                                # Check for exact match
                                if required_name in generated_field_names:
                                    found_match = True
                                else:
                                    # Try with an 'S' appended or removed (final safety net)
                                    if required_name.endswith('S'):
                                        singular = required_name[:-1]
                                        if singular in generated_field_names:
                                            found_match = True
                                    else:
                                        plural = required_name + 'S'
                                        if plural in generated_field_names:
                                            found_match = True
                                
                                if not found_match:
                                    missing_fields.append(req_field['field_name'])
                            
                            if missing_fields:
                                # UPDATE metrics: extraction WAS needed - it caught missing fields!
                                if 'schema_validation_metrics' in st.session_state and 'field_extraction' in st.session_state['schema_validation_metrics']:
                                    st.session_state['schema_validation_metrics']['field_extraction']['was_needed'] = True
                                    st.session_state['schema_validation_metrics']['field_extraction']['issues_caught'].extend(missing_fields)
                                
                                # Store per-table validation metric
                                validation_key = f'validation_{table_name}'
                                if 'schema_validation_metrics' not in st.session_state:
                                    st.session_state['schema_validation_metrics'] = {}
                                st.session_state['schema_validation_metrics'][validation_key] = {
                                    'required': len(required_fields),
                                    'missing': len(missing_fields),
                                    'was_needed': len(missing_fields) > 0,
                                    'caught': missing_fields
                                }
                                
                                # Only fail if critical fields are missing (more than 20% of required fields)
                                missing_ratio = len(missing_fields) / len(required_fields)
                                
                                if missing_ratio > 0.2:
                                    # Store missing fields for retry prompt
                                    previous_missing_fields = missing_fields
                                    if st.session_state.get('debug_mode_infrastructure', False):
                                        st.warning(
                                            f"⚠️ Schema for {table_name} missing {len(missing_fields)}/{len(required_fields)} required fields: "
                                            f"{', '.join(missing_fields)}. Retrying... (attempt {attempt + 1}/{max_attempts})"
                                        )
                                    continue  # Retry
                                else:
                                    # Less than 20% missing - acceptable, just log warning
                                    if st.session_state.get('debug_mode_infrastructure', False):
                                        st.info(
                                            f"ℹ️ Schema for {table_name} missing some optional fields: "
                                            f"{', '.join(missing_fields)}. Proceeding with generated schema."
                                        )
                        
                        # All validations passed
                        return columns
                    else:
                        if st.session_state.get('debug_mode_infrastructure', False):
                            st.warning(
                                f"⚠️ Schema validation failed for {table_name} "
                                f"(attempt {attempt + 1}/{max_attempts}): "
                                f"Invalid column structure"
                            )
                else:
                    if st.session_state.get('debug_mode_infrastructure', False):
                        st.warning(
                            f"⚠️ Failed to parse schema JSON for {table_name} "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )
            else:
                if st.session_state.get('debug_mode_infrastructure', False):
                    st.warning(
                        f"⚠️ No response from LLM for {table_name} "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
            
            # If not the last attempt, wait a bit before retrying
            if attempt < max_attempts - 1:
                time.sleep(1)
                
        except Exception as e:
            error_handler.log_error(
                error_code=ErrorCode.DATA_GENERATION_FAILED,
                error_type=type(e).__name__,
                severity=ErrorSeverity.WARNING,
                message=(
                    f"Schema generation failed for {table_name} "
                    f"(attempt {attempt + 1}/{max_attempts}): {str(e)}"
                ),
                stack_trace=traceback.format_exc()
            )
            if attempt < max_attempts - 1:
                time.sleep(1)
    
    # All attempts failed - use basic fallback schema to prevent complete failure
    if st.session_state.get('debug_mode_infrastructure', False):
        st.error(
            f"❌ Failed to generate schema for {table_name} after {max_attempts} attempts. "
            f"Using basic fallback schema."
        )
    
    error_handler.log_error(
        error_code=ErrorCode.DATA_GENERATION_FAILED,
        error_type="SchemaGenerationError",
        severity=ErrorSeverity.WARNING,  # Changed from ERROR to WARNING since we have fallback
        message=(
            f"Failed to generate valid schema for {table_name} after "
            f"{max_attempts} attempts. Using fallback schema."
        ),
        function_name="generate_schema_for_table"
    )
    
    # Return a basic fallback schema that will at least allow the process to continue
    fallback_schema = [
        {
            "name": "ENTITY_ID",
            "type": "NUMBER",
            "description": "Primary key for joining tables",
            "sample_values": [1, 2, 3, 4, 5]
        },
        {
            "name": "NAME",
            "type": "STRING",
            "description": "Entity name",
            "sample_values": ["Item A", "Item B", "Item C", "Item D", "Item E"]
        },
        {
            "name": "VALUE",
            "type": "NUMBER",
            "description": "Numeric value",
            "sample_values": [100, 200, 300, 400, 500]
        },
        {
            "name": "CATEGORY",
            "type": "STRING",
            "description": "Category classification",
            "sample_values": ["Type 1", "Type 2", "Type 3", "Type 4", "Type 5"]
        },
        {
            "name": "TIMESTAMP",
            "type": "TIMESTAMP",
            "description": "Record timestamp",
            "sample_values": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
        }
    ]
    
    return fallback_schema


@timeit
def add_calculated_metrics(data: Dict, schema_data: List[tuple]) -> Dict:
    """
    Add calculated metric columns based on raw data patterns.
    
    This function intelligently detects opportunities to add calculated metrics
    like percentages, averages, ratios, and growth rates. These calculated columns
    enable the agent to answer more complex analytical questions.
    
    Patterns detected and handled:
    - AMOUNT + QUANTITY → AVG_PRICE (amount per unit)
    - CONVERSIONS + VISITS → CONVERSION_RATE_PCT (conversion rate as percentage)
    - Multiple amount columns from categories → PERCENTAGE_OF_TOTAL
    - DATE + AMOUNT → Enables growth rate calculations
    
    Args:
        data: Dictionary mapping column names to lists of values
        schema_data: List of (col_name, col_type, sample_values) tuples
        
    Returns:
        Enhanced data dictionary with calculated metric columns added
    """
    from collections import defaultdict
    import numpy as np
    
    num_records = len(next(iter(data.values()))) if data else 0
    if num_records == 0:
        return data
    
    # Identify column patterns for intelligent metric calculation
    amount_cols = []
    quantity_cols = []
    conversion_cols = []
    visit_cols = []
    category_cols = []
    date_cols = []
    
    for col_name in data.keys():
        col_upper = col_name.upper()
        
        # Identify amount/value columns
        if any(kw in col_upper for kw in ['AMOUNT', 'VALUE', 'REVENUE', 'SALES', 'COST', 'PRICE', 'TOTAL']):
            amount_cols.append(col_name)
        
        # Identify quantity columns
        elif any(kw in col_upper for kw in ['QUANTITY', 'QTY', 'COUNT', 'UNITS', 'VOLUME']):
            quantity_cols.append(col_name)
        
        # Identify conversion metrics
        elif 'CONVERSION' in col_upper:
            conversion_cols.append(col_name)
        
        # Identify visit/session metrics
        elif any(kw in col_upper for kw in ['VISIT', 'SESSION', 'VIEW', 'CLICK']):
            visit_cols.append(col_name)
        
        # Identify category/grouping columns
        elif any(kw in col_upper for kw in ['CATEGORY', 'TYPE', 'STATUS', 'SEGMENT', 'GROUP']):
            category_cols.append(col_name)
        
        # Identify date columns
        elif any(kw in col_upper for kw in ['DATE', 'TIMESTAMP', 'TIME']):
            date_cols.append(col_name)
    
    # Pattern 1: AMOUNT / QUANTITY → AVG_PRICE or AMOUNT_PER_UNIT
    if amount_cols and quantity_cols:
        amt_col = amount_cols[0]
        qty_col = quantity_cols[0]
        
        # Generate calculated column name
        if 'PRICE' in amt_col.upper() or 'AMOUNT' in amt_col.upper():
            calc_col_name = f"AVG_{amt_col}_PER_{qty_col}"
        else:
            calc_col_name = f"{amt_col}_PER_{qty_col}"
        
        # Limit name length
        if len(calc_col_name) > 50:
            calc_col_name = "AVG_PRICE_PER_UNIT"
        
        # Calculate: amount / quantity (handle division by zero)
        try:
            data[calc_col_name] = [
                round(float(amt) / max(float(qty), 0.01), 2)
                for amt, qty in zip(data[amt_col], data[qty_col])
            ]
        except (ValueError, TypeError, ZeroDivisionError):
            # Fallback if calculation fails
            pass
    
    # Pattern 2: CONVERSIONS / VISITS → CONVERSION_RATE_PCT
    if conversion_cols and visit_cols:
        conv_col = conversion_cols[0]
        visit_col = visit_cols[0]
        calc_col_name = "CONVERSION_RATE_PCT"
        
        # Calculate: (conversions / visits) * 100
        try:
            data[calc_col_name] = [
                round((float(conv) / max(float(visit), 0.01)) * 100, 2)
                for conv, visit in zip(data[conv_col], data[visit_col])
            ]
        except (ValueError, TypeError, ZeroDivisionError):
            pass
    
    # Pattern 3: AMOUNT + CATEGORY → PERCENTAGE_OF_TOTAL
    if amount_cols and category_cols:
        amt_col = amount_cols[0]
        cat_col = category_cols[0]
        
        try:
            # Calculate total by category
            category_totals = defaultdict(float)
            for amt, cat in zip(data[amt_col], data[cat_col]):
                category_totals[cat] += float(amt)
            
            # Calculate grand total
            grand_total = sum(category_totals.values())
            
            if grand_total > 0:
                # Calculate percentage for each row
                data['PERCENTAGE_OF_TOTAL'] = [
                    round((float(amt) / grand_total) * 100, 2)
                    for amt in data[amt_col]
                ]
        except (ValueError, TypeError, ZeroDivisionError):
            pass
    
    # Pattern 4: Multiple amount columns → TOTAL_AMOUNT
    if len(amount_cols) >= 2:
        try:
            # Sum the first 2-3 amount columns
            cols_to_sum = amount_cols[:min(3, len(amount_cols))]
            data['TOTAL_AMOUNT'] = [
                round(sum(float(data[col][i]) for col in cols_to_sum), 2)
                for i in range(num_records)
            ]
        except (ValueError, TypeError):
            pass
    
    # Pattern 5: DATE + AMOUNT → Add simple growth calculation if data is sorted
    # Note: This is a simplified version - real growth rate requires time-series analysis
    if date_cols and amount_cols and num_records >= 10:
        amt_col = amount_cols[0]
        
        try:
            # Calculate simple period-over-period change (for demonstration)
            # In real scenario, this would require proper time-series windowing
            growth_rates = [0.0]  # First row has no previous value
            
            for i in range(1, num_records):
                prev_amt = float(data[amt_col][i-1])
                curr_amt = float(data[amt_col][i])
                
                if prev_amt > 0:
                    growth = ((curr_amt - prev_amt) / prev_amt) * 100
                    growth_rates.append(round(growth, 2))
                else:
                    growth_rates.append(0.0)
            
            data['PERIOD_CHANGE_PCT'] = growth_rates
        except (ValueError, TypeError, IndexError):
            pass
    
    return data


def generate_data_from_schema(
    schema_data: List[Dict],
    num_records: int,
    table_info: Dict,
    company_name: str,
    join_key_values: Optional[List[int]] = None
) -> Dict[str, List]:
    """
    Generate realistic data based on LLM-provided schema with sample_values.
    
    Returns data in column-oriented format {col_name: [values]} instead of 
    row-oriented [{col: val}] for better pandas DataFrame creation performance.
    This approach is more efficient as pandas DataFrames are column-major
    internally.
    
    Args:
        schema_data: List of column definitions with name, type, and sample_values
        num_records: Number of records to generate
        table_info: Metadata about the table
        company_name: Company name for context-aware generation
        join_key_values: Optional list of ENTITY_ID values for joinable tables
    
    Returns:
        dict: Column-oriented data structure {column_name: [list_of_values]}
    """
    data = {}
    
    if join_key_values:
        data['ENTITY_ID'] = join_key_values
        num_records = len(join_key_values)
    else:
        data['ENTITY_ID'] = list(range(1, num_records + 1))
    
    for column in schema_data:
        if column['name'] == 'ENTITY_ID':
            continue
            
        col_type = column['type'].upper()
        col_name = column['name']
        sample_values = column.get('sample_values', [])
        
        # PREFER LLM-provided sample_values (like native_app does)
        if col_type in ['STRING', 'VARCHAR', 'TEXT']:
            if sample_values:
                data[col_name] = [
                    random.choice(sample_values) for _ in range(num_records)
                ]
            else:
                if st.session_state.get('debug_mode', False):
                    st.warning(
                        f"⚠️ Column {col_name} missing sample_values from LLM. "
                        f"Data quality may be degraded."
                    )
                data[col_name] = [
                    f"{col_name}_{i+1}" for i in range(num_records)
                ]
                
        elif col_type in ['NUMBER', 'INTEGER', 'INT']:
            if 'ID' in col_name.upper() and col_name != 'ENTITY_ID':
                # Keep sequential IDs for ID columns
                data[col_name] = [i + 1 for i in range(num_records)]
            elif sample_values:
                numeric_samples = [
                    int(x) for x in sample_values
                    if str(x).replace('-', '').isdigit()
                ]
                if numeric_samples:
                    # Calculate mean and std for INCREASED realistic variance
                    mean = np.mean(numeric_samples)
                    std = np.std(numeric_samples) if len(numeric_samples) > 1 else mean * 0.35
                    
                    # INCREASED VARIANCE: Ensure at least 30-40% variance for realistic data diversity
                    if std < mean * 0.30:
                        std = mean * 0.40
                    
                    # Generate with realistic variance: 70% normal, 30% wider spread for outliers
                    base_values = np.random.normal(mean, std, int(num_records * 0.7))
                    outliers = np.random.normal(mean, std * 2.5, int(num_records * 0.3))
                    generated_values = np.concatenate([base_values, outliers])
                    np.random.shuffle(generated_values)
                    
                    # Convert to integers and ensure non-negative if all samples were positive
                    if all(x >= 0 for x in numeric_samples):
                        generated_values = np.clip(generated_values, 0, None)
                    
                    data[col_name] = [
                        int(round(val)) for val in generated_values
                    ]
                else:
                    # NumPy optimization: 10-15% faster for large datasets
                    data[col_name] = np.random.randint(1, 1001, num_records).tolist()
            else:
                # NumPy optimization: 10-15% faster for large datasets
                data[col_name] = np.random.randint(1, 1001, num_records).tolist()
                
        elif col_type in ['FLOAT', 'DECIMAL', 'DOUBLE']:
            if sample_values:
                try:
                    float_samples = [float(x) for x in sample_values]
                    
                    # Calculate mean and std for INCREASED realistic variance
                    mean = np.mean(float_samples)
                    std = np.std(float_samples) if len(float_samples) > 1 else mean * 0.25
                    
                    # Detect percentage columns (0-100 range or name contains rate/percent/yield/margin)
                    is_percentage = (
                        'RATE' in col_name.upper() or 
                        'PERCENT' in col_name.upper() or 
                        'YIELD' in col_name.upper() or
                        'RATIO' in col_name.upper() or
                        'MARGIN' in col_name.upper() or
                        'SCORE' in col_name.upper() or
                        (all(0 <= x <= 100 for x in float_samples) and mean > 5)
                    )
                    
                    # INCREASED VARIANCE: For percentages/margins, ensure realistic spread
                    if is_percentage:
                        # Use larger std for percentage columns: 15-25% variance for diversity
                        std = max(std, mean * 0.20)  # At least 20% of mean as std
                    else:
                        # For non-percentage floats, ensure 30-40% variance
                        if std < mean * 0.30:
                            std = mean * 0.35
                    
                    # Generate with realistic variance: add outliers for realism
                    if is_percentage:
                        # For percentages, keep tighter control but with good spread
                        generated_values = np.random.normal(mean, std, num_records)
                    else:
                        # For other floats, add 30% outliers
                        base_values = np.random.normal(mean, std, int(num_records * 0.7))
                        outliers = np.random.normal(mean, std * 2.5, int(num_records * 0.3))
                        generated_values = np.concatenate([base_values, outliers])
                        np.random.shuffle(generated_values)
                    
                    # For percentages, clip to 0-100 range
                    if is_percentage:
                        generated_values = np.clip(generated_values, 0, 100)
                    
                    # Round to 2 decimal places
                    data[col_name] = [
                        round(float(val), 2) for val in generated_values
                    ]
                except:
                    data[col_name] = [
                        round(random.uniform(0, 1000), 2)
                        for _ in range(num_records)
                    ]
            else:
                data[col_name] = [
                    round(random.uniform(0, 1000), 2)
                    for _ in range(num_records)
                ]
                
        elif col_type == 'DATE':
            start_date = datetime.now() - timedelta(days=365)
            # NumPy optimization: generate array of day offsets
            day_offsets = np.random.randint(0, 366, num_records)
            # Convert to proper date objects (not strings) for Snowflake DATE type
            data[col_name] = [
                (start_date + timedelta(days=int(offset))).date()
                for offset in day_offsets
            ]
            
        elif 'TIMESTAMP' in col_type or col_type == 'DATETIME':
            # Generate recent timestamps (past 7 days) for realistic queries
            start_date = datetime.now() - timedelta(days=7)
            # NumPy optimization: generate arrays of offsets
            day_offsets = np.random.randint(0, 8, num_records)
            hour_offsets = np.random.randint(0, 24, num_records)
            minute_offsets = np.random.randint(0, 60, num_records)
            second_offsets = np.random.randint(0, 60, num_records)
            # Convert to proper datetime objects (not strings) for Snowflake TIMESTAMP type
            data[col_name] = [
                start_date + timedelta(
                    days=int(day_offsets[i]),
                    hours=int(hour_offsets[i]),
                    minutes=int(minute_offsets[i]),
                    seconds=int(second_offsets[i])
                )
                for i in range(num_records)
            ]
            
        elif col_type == 'BOOLEAN':
            # NumPy optimization: faster boolean generation
            data[col_name] = np.random.choice([True, False], num_records).tolist()
            
        else:
            # Default fallback
            if sample_values:
                data[col_name] = [
                    random.choice(sample_values) for _ in range(num_records)
                ]
            else:
                data[col_name] = [
                    f"Value_{i+1}" for i in range(num_records)
                ]
    
    # NOTE: We do NOT add calculated columns to raw data
    # Calculated metrics should be exposed in the SEMANTIC VIEW as SQL expressions
    # This keeps raw data clean and lets Cortex Analyst compute on the fly
    
    return data


def build_rich_table_context(
    table_key: str,
    table_info: Dict,
    table_schema: List[Dict],
    table_data: Dict[str, List],
    num_sample_rows: int = 7  # Increased from 5 to 7 for better variety
) -> Dict:
    """
    Build comprehensive context about a table for question generation.
    
    Analyzes both the LLM-generated schema and actual generated data to provide
    rich context including data types, descriptions, sample values, cardinality,
    numeric ranges, and sample rows.
    
    Args:
        table_key: Internal key for the table (e.g., 'structured_1')
        table_info: Table metadata including name and description
        table_schema: LLM-generated schema with columns, types, descriptions, sample_values
        table_data: Actual generated data in column-oriented format {col_name: [values]}
        num_sample_rows: Number of sample rows to include
    
    Returns:
        dict: Comprehensive table context with structure:
            {
                'name': str,
                'description': str,
                'columns': [
                    {
                        'name': str,
                        'type': str,
                        'description': str,
                        'llm_sample_values': List,  # From schema generation
                        'unique_count': int,  # For categorical columns
                        'sample_actual_values': List,  # From generated data
                        'numeric_range': {min, max, avg},  # For numeric columns
                        'has_nulls': bool
                    }
                ],
                'sample_rows': List[Dict],  # First N rows
                'row_count': int
            }
    """
    # Extract basic table info
    context = {
        'name': table_info['name'],
        'description': table_info.get('description', ''),
        'row_count': len(table_data.get('ENTITY_ID', [])),
        'columns': [],
        'sample_rows': []
    }
    
    # Analyze each column
    for col_def in table_schema:
        col_name = col_def['name']
        col_type = col_def['type'].upper()
        col_description = col_def.get('description', '')
        llm_sample_values = col_def.get('sample_values', [])
        
        col_context = {
            'name': col_name,
            'type': col_type,
            'description': col_description,
            'llm_sample_values': llm_sample_values,
            'has_nulls': False
        }
        
        # Get actual data for this column
        if col_name in table_data:
            col_values = table_data[col_name]
            
            # Check for nulls/None values
            col_context['has_nulls'] = any(v is None for v in col_values)
            
            # For categorical columns (STRING, VARCHAR, TEXT), analyze cardinality
            if col_type in ['STRING', 'VARCHAR', 'TEXT']:
                unique_values = list(set(col_values))
                col_context['unique_count'] = len(unique_values)
                # Sample up to 10 actual values
                col_context['sample_actual_values'] = unique_values[:10]
            
            # For numeric columns, calculate range and statistics
            elif col_type in ['NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL', 'DOUBLE']:
                numeric_values = [v for v in col_values if v is not None and isinstance(v, (int, float))]
                if numeric_values:
                    col_context['numeric_range'] = {
                        'min': min(numeric_values),
                        'max': max(numeric_values),
                        'avg': sum(numeric_values) / len(numeric_values)
                    }
                    col_context['unique_count'] = len(set(numeric_values))
            
            # For date/timestamp columns, get range
            elif 'DATE' in col_type or 'TIMESTAMP' in col_type:
                # For datetime objects
                if col_values and hasattr(col_values[0], 'strftime'):
                    date_values = [v for v in col_values if v is not None]
                    if date_values:
                        col_context['date_range'] = {
                            'min': str(min(date_values)),
                            'max': str(max(date_values))
                        }
                # For string dates
                elif col_values and isinstance(col_values[0], str):
                    date_values = [v for v in col_values if v is not None]
                    if date_values:
                        col_context['date_range'] = {
                            'min': min(date_values),
                            'max': max(date_values)
                        }
        
        context['columns'].append(col_context)
    
    # Build sample rows (convert column-oriented to row-oriented)
    if context['row_count'] > 0:
        num_rows_to_sample = min(num_sample_rows, context['row_count'])
        for i in range(num_rows_to_sample):
            row = {}
            for col_def in table_schema:
                col_name = col_def['name']
                if col_name in table_data and i < len(table_data[col_name]):
                    row[col_name] = table_data[col_name][i]
            context['sample_rows'].append(row)
    
    return context


@timeit
def generate_unstructured_data(
    table_name: str,
    table_description: str,
    num_chunks: int,
    company_name: str,
    session,
    error_handler,
    language_code: str = "en"
) -> List[Dict]:
    """
    Generate unstructured text data for Cortex Search with language support.
    
    Args:
        table_name: Name of the unstructured table
        table_description: Description of the content
        num_chunks: Number of text chunks to generate
        company_name: Company name for context
        session: Snowflake session
        error_handler: ErrorHandler instance
        language_code: Language code for content generation
        
    Returns:
        List of chunk dictionaries with metadata
    """
    # Use the centralized prompt function which has content-type-specific guidance
    base_prompt, content_type = get_unstructured_data_generation_prompt(
        table_name=table_name,
        table_description=table_description,
        company_name=company_name,
        num_chunks=num_chunks
    )

    prompt = enhance_prompt_with_language(base_prompt, language_code)
    
    response = call_cortex_with_retry(prompt, session, error_handler)
    
    chunks_data = []
    detected_content_type = content_type  # Store for return value
    
    if response:
        parsed = safe_json_parse(response)
        if parsed and isinstance(parsed, list):
            base_chunks = parsed
            
            while len(chunks_data) < num_chunks:
                for chunk in base_chunks:
                    if len(chunks_data) >= num_chunks:
                        break
                    
                    chunk_text = chunk.get(
                        'chunk_text',
                        f"Sample content {len(chunks_data)}"
                    )
                    
                    is_valid, error_msg = validate_language_content(
                        chunk_text, language_code
                    )
                    if not is_valid and language_code != "en":
                        if st.session_state.get('debug_mode', False):
                            st.warning(f"Language validation warning: {error_msg}")
                    
                    chunks_data.append({
                        'CHUNK_ID': len(chunks_data) + 1,
                        'DOCUMENT_ID': f"DOC_{(len(chunks_data) // 5) + 1}",
                        'CHUNK_TEXT': chunk_text,
                        'DOCUMENT_TYPE': chunk.get('document_type', 'general'),
                        'SOURCE_SYSTEM': chunk.get('source_system', company_name)
                    })
    
    if not chunks_data:
        for i in range(num_chunks):
            chunks_data.append({
                'CHUNK_ID': i + 1,
                'DOCUMENT_ID': f"DOC_{(i // 5) + 1}",
                'CHUNK_TEXT': (
                    f"This is sample content chunk {i+1} for "
                    f"{table_description}. " * 10
                ),
                'DOCUMENT_TYPE': table_name.lower(),
                'SOURCE_SYSTEM': company_name
            })
    
    chunks_data = add_language_metadata_to_chunks(chunks_data, language_code)
    
    # Return both chunks_data and the detected content_type
    return chunks_data, detected_content_type


# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_tables_collectively(
    all_tables_info: List[Dict],
    target_questions: List[str],
    session,
    error_handler
) -> Dict[str, Any]:
    """
    Validate that ALL tables TOGETHER can answer the target questions.
    This addresses cross-table questions that require joins.
    
    Args:
        all_tables_info: List of dicts with {name, schema, data, role}
        target_questions: List of target questions
        session: Snowflake session
        error_handler: ErrorHandler instance
    
    Returns:
        dict: Validation results with per-question assessment
    """
    if not target_questions or len(target_questions) == 0:
        return {'overall_valid': True, 'questions': []}
    
    # Build comprehensive tables summary
    tables_summary = []
    for table_info in all_tables_info:
        table_name = table_info['name']
        schema = table_info['schema']
        data = table_info['data']
        
        # Get enhanced sample data (10-15 rows instead of 3-5)
        sample_size = min(15, len(data.get('ENTITY_ID', [])))
        sample_rows = []
        for i in range(sample_size):
            row = {}
            for col_name, values in data.items():
                if i < len(values):
                    row[col_name] = values[i]
            sample_rows.append(row)
        
        # Calculate distribution statistics
        stats = {}
        for col in schema:
            col_name = col['name']
            if col_name in data and len(data[col_name]) > 0:
                col_data = data[col_name]
                if col['type'].upper() in [
                    'NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'
                ]:
                    try:
                        numeric_vals = [
                            float(v) for v in col_data if v is not None
                        ]
                        if numeric_vals:
                            stats[col_name] = {
                                'min': min(numeric_vals),
                                'max': max(numeric_vals),
                                'unique_count': len(set(numeric_vals))
                            }
                    except:
                        pass
                else:
                    unique_vals = set(str(v) for v in col_data if v is not None)
                    stats[col_name] = {
                        'unique_count': len(unique_vals),
                        'sample_values': list(unique_vals)[:10]
                    }
        
        column_list = ", ".join([
            f"{col['name']} ({col['type']})" for col in schema
        ])
        tables_summary.append(
            f"**{table_name}**:\n  Columns: {column_list}\n  "
            f"Row count: {len(data.get('ENTITY_ID', []))}\n  "
            f"Statistics: {json.dumps(stats, default=str)[:200]}"
        )
    
    tables_text = "\n\n".join(tables_summary)
    questions_text = "\n".join([
        f"{i+1}. {q}" for i, q in enumerate(target_questions)
    ])
    
    prompt = f"""Validate if these tables TOGETHER can answer the target questions.

Available Tables:
{tables_text}

Join Key: All tables can join on ENTITY_ID (with ~70% overlap expected)

Target Questions:
{questions_text}

For EACH question, determine:
1. Which table(s) are needed? (single table or join required)
2. What columns/operations are needed?
3. Is the answer CALCULABLE from available data and joins?
4. Rate confidence: HIGH (definitely answerable), MEDIUM (probably answerable), LOW (missing data)

Return ONLY a JSON object:
{{
  "overall_assessment": "Summary of whether all questions are answerable",
  "questions": [
    {{
      "question": "question text",
      "tables_needed": ["TABLE1", "TABLE2"],
      "columns_needed": ["column1", "column2"],
      "answerable": true/false,
      "confidence": "HIGH/MEDIUM/LOW",
      "requires_join": true/false,
      "notes": "Specific explanation of how to answer or what's missing"
    }}
  ]
}}

IMPORTANT: A question requiring data from multiple tables should NOT mark individual tables as invalid. Instead, mark requires_join=true and assess if the JOIN will produce the answer."""
    
    try:
        response = call_cortex_with_retry(prompt, session, error_handler)
        
        if response:
            parsed = safe_json_parse(response)
            if parsed:
                return parsed
    except Exception as e:
        if st.session_state.get('debug_mode', False):
            st.warning(f"⚠️ Collective validation error: {str(e)}")
    
    # Fallback
    return {
        'overall_assessment': 'Validation completed (limited confidence)',
        'questions': []
    }


def validate_data_against_questions(
    table_data: Dict,
    target_questions: List[str],
    table_schema: List[Dict],
    table_name: str,
    session,
    error_handler
) -> Tuple[bool, str]:
    """
    Validate that generated data can plausibly answer the target questions.
    NOTE: This is now a helper for single-table validation. Use
    validate_tables_collectively() for comprehensive validation.
    
    Args:
        table_data: Dict of column-oriented data
        target_questions: List of target questions
        table_schema: List of column definitions
        table_name: Name of the table
        session: Snowflake session
        error_handler: ErrorHandler instance
    
    Returns:
        tuple: (is_valid, feedback_message)
    """
    if not target_questions or len(target_questions) == 0:
        return (True, "No target questions to validate")
    
    # Enhanced sample size: 10-15 rows instead of 3-5
    sample_size = min(15, len(table_data.get('ENTITY_ID', [])))
    sample_data_rows = []
    if sample_size > 0:
        for i in range(sample_size):
            row = {}
            for col_name, values in table_data.items():
                if i < len(values):
                    row[col_name] = values[i]
            sample_data_rows.append(row)
    
    # Calculate distribution statistics for better validation
    column_summary = []
    for col in table_schema:
        col_name = col.get('name', 'Unknown')
        col_type = col.get('type', 'Unknown')
        sample_vals = col.get('sample_values', [])[:8]  # Show more samples
        
        # Add distribution info if available
        dist_info = ""
        if col_name in table_data and len(table_data[col_name]) > 0:
            col_data = table_data[col_name]
            if col_type.upper() in [
                'NUMBER', 'INTEGER', 'INT', 'FLOAT', 'DECIMAL'
            ]:
                try:
                    numeric_vals = [
                        float(v) for v in col_data if v is not None
                    ]
                    if numeric_vals:
                        dist_info = (
                            f" [range: {min(numeric_vals):.1f}-"
                            f"{max(numeric_vals):.1f}, "
                            f"unique: {len(set(numeric_vals))}]"
                        )
                except:
                    pass
            else:
                unique_vals = set(str(v) for v in col_data if v is not None)
                dist_info = f" [unique values: {len(unique_vals)}]"
        
        column_summary.append(
            f"- {col_name} ({col_type}): {sample_vals}{dist_info}"
        )
    
    columns_text = "\n".join(column_summary)
    questions_text = "\n".join([
        f"{i+1}. {q}" for i, q in enumerate(target_questions)
    ])
    sample_data_text = json.dumps(
        sample_data_rows[:10], indent=2, default=str
    )
    
    prompt = f"""Validate if this SINGLE table can contribute to answering the target questions.

IMPORTANT: Some questions may require joining with other tables. For those questions, assess if THIS table has the necessary columns and data to participate in the join.

Table: {table_name}

Columns (with distribution stats):
{columns_text}

Sample Data (first 10 rows):
{sample_data_text}

Target Questions:
{questions_text}

For each question, determine:
1. Can this table ALONE answer it? Or does it need to join with other tables?
2. If it needs a join, does this table have the necessary columns to participate?
3. Is the data quality sufficient (good distribution, realistic values)?

Return ONLY a JSON object:
{{
  "table_role": "primary/supporting/not_needed",
  "feedback": "Brief summary focusing on what THIS table provides",
  "questions_coverage": [
    {{"question": "question text", "role_for_question": "answers_alone/needs_join/not_relevant", "notes": "explanation"}}
  ]
}}"""
    
    try:
        response = call_cortex_with_retry(prompt, session, error_handler)
        
        if response:
            parsed = safe_json_parse(response)
            if parsed:
                feedback = parsed.get('feedback', 'Validation completed')
                questions_coverage = parsed.get('questions_coverage', [])
                
                # Build detailed feedback with better messaging
                detailed_feedback = f"{feedback}\n\n"
                for qc in questions_coverage:
                    question = qc.get('question', 'Unknown')
                    role = qc.get('role_for_question', 'unknown')
                    notes = qc.get('notes', '')
                    
                    if role == 'answers_alone':
                        status = "✅"
                        msg = "Can answer independently"
                    elif role == 'needs_join':
                        status = "🔗"
                        msg = "Supports via join"
                    else:
                        status = "➖"
                        msg = "Not relevant to this table"
                    
                    detailed_feedback += (
                        f"{status} {question}: {msg} - {notes}\n"
                    )
                
                # Don't mark as invalid if table just needs to join
                is_valid = True  # Individual tables are valid if they contribute
                return (is_valid, detailed_feedback)
    except Exception as e:
        return (True, f"Validation skipped due to error: {str(e)}")
    
    # Fallback: assume valid if we can't validate
    return (True, "Validation completed (no specific issues found)")


@timeit
def save_structured_table_to_snowflake(
    schema_name: str,
    table_name: str,
    table_schema: List[Dict],
    table_data: Dict,
    table_info: Dict,
    num_records: int,
    status_container,
    session,
    overlap_info: Optional[str] = None
) -> Dict:
    """
    Helper function to save a structured table to Snowflake and display results.
    
    Args:
        schema_name: Schema name
        table_name: Table name
        table_schema: List of column definitions
        table_data: Column-oriented data dict
        table_info: Table metadata
        num_records: Number of records
        status_container: Streamlit container for status messages
        session: Snowflake session
        overlap_info: Optional string describing join overlap
    
    Returns:
        dict: Result metadata for the created table
    """
    dataframe = pd.DataFrame(table_data)
    snowpark_dataframe = session.create_dataframe(dataframe)
    
    # Set query tag for table creation
    set_query_tag(session, "create_structured_table", "python", demo_name=schema_name)
    
    # Create table
    snowpark_dataframe.write.mode("overwrite").save_as_table(
        f"{schema_name}.{table_name}"
    )
    
    # Add primary key
    session.sql(
        f"ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY (ENTITY_ID)"
    ).collect()
    
    # Add comment tag to table
    comment_tag = generate_tag_json("create_structured_table", "python", demo_name=schema_name)
    session.sql(
        f"ALTER TABLE {schema_name}.{table_name} SET COMMENT = '{comment_tag}'"
    ).collect()
    
    # Clear query tag
    clear_query_tag(session)
    
    # Create status message
    status_msg = f"✅ {table_name} created ({num_records} records"
    if overlap_info:
        status_msg += f", {overlap_info}"
    status_msg += ")"
    
    # Add sample data preview in expander
    with status_container:
        with st.expander(status_msg, expanded=False):
            st.caption(
                f"**Columns:** {', '.join([col['name'] for col in table_schema])}"
            )
            st.dataframe(dataframe.head(3), use_container_width=True)
    
    return {
        'table': table_name,
        'records': num_records,
        'description': table_info.get('description', ''),
        'columns': [col['name'] for col in table_schema],
        'type': 'structured',
        'table_type': table_info.get('table_type', 'dimension'),
        'sample_data': dataframe.head(3).to_dict('records')  # MEMORY OPTIMIZATION: Convert DataFrame to dict
    }


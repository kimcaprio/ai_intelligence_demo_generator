"""
LLM prompt templates for SI Data Generator application.

This module contains all LLM prompt generation functions used throughout
the application. Centralizing prompts here makes them easier to maintain,
test, and version independently from business logic.
"""

from typing import List, Dict, Optional, Any


def _build_mandatory_column_restriction(rich_table_contexts: Optional[List[Dict]]) -> str:
    """
    Build ULTRA-STRICT MANDATORY COLUMN RESTRICTION section with explicit column lists.
    
    This ensures LLM only generates questions about columns that actually exist,
    not concepts mentioned in descriptions.
    
    Args:
        rich_table_contexts: Rich context with table schemas and column information
        
    Returns:
        Formatted restriction section string
    """
    if not rich_table_contexts:
        return ""
    
    restriction = "\n\n" + "=" * 80 + "\n"
    restriction += "🚨🚨🚨 ULTRA-STRICT COLUMN RESTRICTION - THIS IS MANDATORY 🚨🚨🚨\n"
    restriction += "=" * 80 + "\n\n"
    restriction += "**ZERO TOLERANCE POLICY**: Questions referencing non-existent columns will be REJECTED.\n\n"
    restriction += "ONLY these EXACT columns exist in Snowflake (SOURCE OF TRUTH):\n\n"
    
    for ctx in rich_table_contexts:
        restriction += f"📋 Table: {ctx['name']}\n"
        # Extract ONLY column names WITH types for clarity
        columns_with_types = []
        for col in ctx.get('columns', []):
            col_str = f"{col['name']} ({col.get('type', 'UNKNOWN')})"
            # Add sample values to show what data actually looks like
            if col.get('sample_actual_values'):
                samples = col['sample_actual_values'][:3]
                col_str += f" [e.g., {', '.join(str(s) for s in samples)}]"
            columns_with_types.append(col_str)
        
        if columns_with_types:
            restriction += "   " + "\n   ".join(columns_with_types) + "\n\n"
        else:
            restriction += "   (NO COLUMNS - cannot query this table)\n\n"
    
    restriction += "=" * 80 + "\n"
    restriction += "❌ FORBIDDEN - DO NOT GENERATE QUESTIONS ABOUT:\n"
    restriction += "=" * 80 + "\n"
    restriction += "1. ANY column name not explicitly listed above\n"
    restriction += "2. Concepts from table descriptions without matching column (e.g., 'annual rewards' if no ANNUAL_REWARDS column)\n"
    restriction += "3. Metrics that would require non-existent columns (e.g., 'profit margin' if no PROFIT or MARGIN column)\n"
    restriction += "4. Derived/calculated fields unless they exist as actual columns\n"
    restriction += "5. Time periods not supported by actual DATE/TIMESTAMP columns\n"
    restriction += "6. Categories not shown in sample values above\n"
    restriction += "7. Joins between tables without verified foreign key columns\n"
    restriction += "8. 🚨 INFERRED METRICS that sound reasonable but don't exist:\n"
    restriction += "   - ❌ 'popularity scores/ranking' unless POPULARITY_SCORE or POPULARITY_RANK column exists\n"
    restriction += "   - ❌ 'seasonal availability' unless SEASON, SEASONAL_FLAG, or SEASONAL_AVAILABILITY column exists\n"
    restriction += "   - ❌ 'customer satisfaction' unless SATISFACTION_SCORE, RATING, or similar column exists\n"
    restriction += "   - ❌ 'performance metrics' unless PERFORMANCE_SCORE or specific metric columns exist\n"
    restriction += "   - ❌ 'quality ratings' unless QUALITY_SCORE, RATING, or QUALITY_RANK column exists\n"
    restriction += "   - ❌ 'profitability' unless PROFIT, PROFIT_MARGIN, or PROFITABILITY column exists\n"
    restriction += "   - Rule: If you see 'availability_status' (active/discontinued), DO NOT infer 'seasonal availability'\n"
    restriction += "   - Rule: If you see 'menu optimization', DO NOT infer 'popularity scores'\n"
    restriction += "   - Rule: Match EXACT column names, not business concepts\n\n"
    restriction += "=" * 80 + "\n"
    restriction += "✅ VALIDATION CHECKLIST (check BEFORE generating each question):\n"
    restriction += "=" * 80 + "\n"
    restriction += "□ Every metric I reference appears in the column list above\n"
    restriction += "□ Every dimension I group by is an actual column name above\n"
    restriction += "□ Every filter value matches sample values shown above\n"
    restriction += "□ Every table I query exists in the list above\n"
    restriction += "□ I am NOT inventing columns based on business logic\n"
    restriction += "□ I am NOT assuming columns exist from descriptions\n"
    restriction += "□ I am NOT using words like 'popularity', 'seasonal', 'satisfaction' WITHOUT seeing exact matching columns\n"
    restriction += "□ If I mention ANY \"-ity\", \"-ness\", or \"-ing\" metric (popularity, seasonality, profitability):\n"
    restriction += "   → I can point to the EXACT column that contains this (e.g., POPULARITY_SCORE)\n"
    restriction += "□ If the question would make an agent say 'Data Limitations: X not available':\n"
    restriction += "   → REJECT this question immediately\n"
    restriction += "=" * 80 + "\n\n"
    restriction += "⚠️  IF IN DOUBT, DO NOT INCLUDE THE QUESTION. ⚠️\n"
    restriction += "⚠️  Better to have 8 perfect questions than 12 with 4 unanswerable. ⚠️\n\n"
    
    return restriction


def get_company_analysis_prompt(company_url: str) -> str:
    """
    Generate prompt for analyzing company URLs to infer business context.
    
    Args:
        company_url: Company website URL to analyze
        
    Returns:
        Formatted prompt string for LLM
    """
    return f"""Analyze this company URL and provide business context: {company_url}

Based on the domain name, provide:
1. Primary industry (e.g., Finance, Healthcare, Retail, Manufacturing, Technology, E-commerce, etc.)
2. Likely business focus (2-3 keywords)
3. Suggested data analysis focus (brief)

Respond ONLY in valid JSON format:
{{
  "industry": "Industry Name",
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "context": "Brief description of likely business focus and data needs"
}}"""


def get_question_generation_prompt(
    num_questions: int,
    industry: str,
    company_name: str,
    demo_data: Dict,
    url_context: Optional[Dict] = None,
    table_info: str = "",
    columns_info: str = "",
    rich_table_contexts: Optional[List[Dict]] = None,
    is_retry: bool = False,
    existing_questions: Optional[List[Dict]] = None,
    semantic_vocabulary: Optional[Dict] = None,
    unstructured_samples: Optional[Dict] = None,
    column_guide: Optional[str] = None
) -> str:
    """
    Generate prompt for creating analytical questions for demos.
    
    Args:
        num_questions: Number of questions to generate
        industry: Industry context
        company_name: Company name for context
        demo_data: Demo configuration dictionary
        url_context: Optional URL analysis context
        table_info: Description of available tables (deprecated, use rich_table_contexts)
        columns_info: Description of available columns (deprecated, use rich_table_contexts)
        rich_table_contexts: Optional rich context with full schema, data analysis, and cardinality
        is_retry: If True, enforces ultra-conservative question generation for guaranteed answerability
        existing_questions: Optional list of questions to avoid duplicating
        semantic_vocabulary: Optional semantic model vocabulary
        unstructured_samples: Optional unstructured data samples
        column_guide: Optional question-friendly column guide with metrics/dimensions/time columns
        
    Returns:
        Formatted prompt string for LLM
    """
    # Add URL context if available
    url_context_text = ""
    if url_context and url_context.get('context'):
        url_context_text = f"\n\nCompany Context (from URL analysis):\n"
        url_context_text += f"- Industry: {url_context.get('industry', 'N/A')}\n"
        url_context_text += f"- Business Focus: {url_context.get('context', 'N/A')}\n"
        if url_context.get('keywords'):
            url_context_text += f"- Key Areas: {', '.join(url_context['keywords'])}\n"
        url_context_text += f"\nUse this context to make questions more relevant to {company_name}'s specific business needs."
    
    # Build comprehensive table context section from rich contexts
    detailed_table_info = ""
    if rich_table_contexts:
        detailed_table_info = "\n\n=== AVAILABLE DATA (COMPLETE CONTEXT) ===\n"
        
        for idx, ctx in enumerate(rich_table_contexts, 1):
            detailed_table_info += f"\nTable {idx}: {ctx['name']}\n"
            # REMOVED: Stale description from schema generation
            # Description is misleading - LLM will trust actual data instead
            detailed_table_info += f"Row Count: {ctx['row_count']}\n"
            detailed_table_info += "\nColumns:\n"
            
            for col in ctx['columns']:
                detailed_table_info += f"  • {col['name']} ({col['type']}): {col['description']}\n"
                
                # Add cardinality for categorical columns
                if col.get('unique_count') is not None:
                    detailed_table_info += f"    - Unique values: {col['unique_count']}\n"
                    if col.get('sample_actual_values'):
                        samples = col['sample_actual_values'][:5]
                        detailed_table_info += f"    - Examples: {', '.join(str(s) for s in samples)}\n"
                
                # Add range for numeric columns
                if col.get('numeric_range'):
                    nr = col['numeric_range']
                    detailed_table_info += f"    - Range: {nr['min']} to {nr['max']} (avg: {nr['avg']:.2f})\n"
                
                # Add date range if available
                if col.get('date_range'):
                    dr = col['date_range']
                    detailed_table_info += f"    - Date range: {dr['min']} to {dr['max']}\n"
            
            # Add sample rows - show ALL columns with truncated values
            if ctx.get('sample_rows'):
                detailed_table_info += f"\n  Sample Data (first 5 rows showing ALL columns):\n"
                for row_idx, row in enumerate(ctx['sample_rows'][:5], 1):  # Show 5 rows instead of 3
                    # Show ALL columns, not just first 5
                    # Truncate each VALUE to 50 chars, but show ALL columns
                    row_str_truncated = ", ".join([
                        f"{k}={str(v)[:50]}{'...' if len(str(v)) > 50 else ''}" 
                        for k, v in row.items()
                    ])
                    detailed_table_info += f"    Row {row_idx}: {row_str_truncated}\n"
    else:
        # Fallback to old format if rich contexts not provided
        detailed_table_info = f"\n- Available Tables:\n{table_info}{columns_info}"
    
    # Add unstructured table information from demo_data AND actual samples
    unstructured_info = ""
    if unstructured_samples or (demo_data and 'tables' in demo_data):
        unstructured_tables = [(k, v) for k, v in demo_data['tables'].items() if k.startswith('unstructured')] if demo_data and 'tables' in demo_data else []
        if unstructured_tables or unstructured_samples:
            unstructured_info = "\n\n" + "=" * 80 + "\n"
            unstructured_info += "UNSTRUCTURED DATA (FOR SEARCH QUESTIONS)\n"
            unstructured_info += "=" * 80 + "\n"
            
            # If we have actual samples, show them!
            if unstructured_samples:
                unstructured_info += "\n🎯 ACTUAL CONTENT SAMPLES (WHAT WAS GENERATED):\n\n"
                for table_name, sample_info in unstructured_samples.items():
                    unstructured_info += f"📄 {table_name}:\n"
                    # Generate description FROM actual content instead of stale schema description
                    chunks = sample_info.get('sample_chunks', [])
                    if chunks and chunks[0]:
                        # Use first 100 chars of actual content as description
                        content_preview = chunks[0][:100].replace('\n', ' ')
                        unstructured_info += f"   Content Preview: \"{content_preview}...\"\n"
                    else:
                        unstructured_info += f"   (No sample content available)\n"
                    unstructured_info += f"   Sample Chunks:\n"
                    for i, chunk_text in enumerate(chunks[:2], 1):
                        # Show first 200 chars of each sample
                        preview = chunk_text[:200] + "..." if len(chunk_text) > 200 else chunk_text
                        unstructured_info += f"     {i}. {preview}\n"
                    unstructured_info += "\n"
            else:
                # Fallback: show descriptions from demo_data
                for key, table in unstructured_tables:
                    unstructured_info += f"\n📄 {table.get('name', key.upper())}:\n"
                    unstructured_info += f"   Content Type: {table.get('description', 'N/A')}\n"
                    unstructured_info += f"   Purpose: {table.get('purpose', 'Semantic search')}\n"
            
            unstructured_info += "\n" + "=" * 80 + "\n"
            unstructured_info += "🚨 CRITICAL FOR SEARCH QUESTIONS:\n"
            unstructured_info += "=" * 80 + "\n"
            unstructured_info += "Your SEARCH questions MUST match the ACTUAL content shown above.\n"
            if unstructured_samples:
                unstructured_info += "You can see REAL SAMPLES of what was generated - use these to guide your questions!\n"
            unstructured_info += "DO NOT ask for content types that don't exist!\n\n"
            
            # Show content type and let LLM intelligently determine appropriate questions
            # NO HARDCODED RULES - LLM decides based on actual sample content
            for table_name, sample_info in (unstructured_samples or {}).items():
                content_type = sample_info.get('content_type', 'unknown')
                unstructured_info += f"✓ {table_name}:\n"
                unstructured_info += f"  Detected Content Type: {content_type}\n"
                unstructured_info += f"  → Generate search questions that match what you SEE in the samples above\n"
                unstructured_info += f"  → DO NOT assume content based on the content_type label - READ THE SAMPLES\n"
                unstructured_info += "\n"
            
            # Add ultra-strict data boundary enforcement
            unstructured_info += "\n" + "=" * 80 + "\n"
            unstructured_info += "🚨 ABSOLUTE DATA BOUNDARY ENFORCEMENT\n"
            unstructured_info += "=" * 80 + "\n"
            unstructured_info += "ALL THE DATA YOU NEED IS SHOWN ABOVE IN COMPLETE DETAIL.\n"
            unstructured_info += "You have seen: actual columns, data types, ranges, sample values, and complete rows.\n"
            unstructured_info += "For unstructured data, you have seen the ACTUAL generated content.\n\n"
            
            unstructured_info += "STRICT RULES FOR QUESTION GENERATION:\n"
            unstructured_info += "1. ONLY ask questions that can be answered with the EXACT data shown above\n"
            unstructured_info += "2. FORBIDDEN: Questions about data not visible in the samples/columns\n"
            unstructured_info += "3. FORBIDDEN: Questions assuming data exists beyond what you can see\n"
            unstructured_info += "4. REQUIRED: Every question must reference specific columns OR specific content from unstructured samples\n"
            unstructured_info += "5. TEST before adding: Can I see the specific data needed to answer this? If NO → DON'T ASK IT\n\n"
            
            unstructured_info += "FOR SEARCH QUESTIONS SPECIFICALLY:\n"
            unstructured_info += "- Your search questions MUST match the ACTUAL content shown in sample chunks\n"
            unstructured_info += "- Read the samples carefully: What do you ACTUALLY see?\n"
            unstructured_info += "- If samples show 'Standard Operating Procedure for payment processing' → ask for SOPs/procedures\n"
            unstructured_info += "- If samples show 'Customer complained about cold fries' → ask for customer feedback/complaints\n"
            unstructured_info += "- If samples show 'API Specification: Endpoint /v1/orders' → ask for API specs/technical docs\n"
            unstructured_info += "- NEVER ask for a content type that doesn't match what you see in the samples\n\n"
            
            unstructured_info += "EXAMPLE OF CORRECT BOUNDARY ENFORCEMENT:\n"
            unstructured_info += "✓ GOOD: 'What is the average order value by customer age group?' (if you see ORDER_VALUE and AGE_GROUP columns)\n"
            unstructured_info += "✗ BAD: 'What is the customer satisfaction score?' (if you DON'T see a SATISFACTION_SCORE column)\n"
            unstructured_info += "✓ GOOD: 'Find customer feedback about food quality' (if samples show 'Customer said: food was cold...')\n"
            unstructured_info += "✗ BAD: 'Find customer feedback about food quality' (if samples show 'Standard Operating Procedure...')\n\n"
    
    # Add semantic model vocabulary if available
    semantic_vocab_section = ""
    if semantic_vocabulary:
        semantic_vocab_section = "\n\n" + "=" * 80 + "\n"
        semantic_vocab_section += "SEMANTIC MODEL VOCABULARY (CRITICAL)\n"
        semantic_vocab_section += "=" * 80 + "\n"
        semantic_vocab_section += "The Snowflake Intelligence Agent understands these specific terms:\n\n"
        
        # Group by table
        vocab_by_table = {}
        for col_key, synonyms in semantic_vocabulary.items():
            if '.' in col_key:
                table_name, col_name = col_key.rsplit('.', 1)
                if table_name not in vocab_by_table:
                    vocab_by_table[table_name] = []
                vocab_by_table[table_name].append(f"  {col_name}: {synonyms}")
        
        for table_name, columns in sorted(vocab_by_table.items()):
            semantic_vocab_section += f"\n{table_name}:\n"
            semantic_vocab_section += "\n".join(columns) + "\n"
        
        semantic_vocab_section += "\n" + "=" * 80 + "\n"
        semantic_vocab_section += "CRITICAL: Questions MUST use terms from these synonyms or exact column names.\n"
        semantic_vocab_section += "The agent cannot answer questions using terms outside this vocabulary.\n"
        semantic_vocab_section += "=" * 80 + "\n"
    
    # Build existing questions context if provided
    existing_questions_context = ""
    if existing_questions and len(existing_questions) > 0:
        existing_questions_context = "\n\n🚫 EXISTING QUESTIONS - DO NOT DUPLICATE:\n"
        existing_questions_context += "The following questions already exist. Generate COMPLETELY DIFFERENT questions:\n\n"
        for i, q in enumerate(existing_questions, 1):
            existing_questions_context += f"{i}. {q.get('text', 'N/A')}\n"
        existing_questions_context += "\nYour new questions must be DISTINCT from the above. Use different:\n"
        existing_questions_context += "- Concepts and metrics (if they asked about costs, ask about performance)\n"
        existing_questions_context += "- Aggregations (if they used COUNT, use AVG or SUM)\n"
        existing_questions_context += "- Filters and groupings (different dimensions)\n"
        existing_questions_context += "- Time periods (if they asked about trends, ask about comparisons)\n\n"
    
    # Add ultra-conservative instructions for retry attempts
    retry_instructions = ""
    if is_retry:
        retry_instructions = """
🚨 ULTRA-CONSERVATIVE MODE ACTIVATED 🚨

THIS IS A RETRY ATTEMPT. Previous questions failed validation.

MANDATORY REQUIREMENTS FOR THIS ATTEMPT:
1. ONLY generate questions using columns you can DIRECTLY SEE in the data above
2. PREFER simple aggregations over complex analyses
3. AVOID any concepts not explicitly visible in column names
4. Use "top 5" maximum (or lower if cardinality shown is low)
5. Prefer COUNT and AVERAGE over SUM when uncertain
6. Do NOT infer any data that isn't explicitly shown
7. If in doubt, generate a SIMPLER question that's guaranteed to work

COMPLEXITY REQUIREMENTS:
You must still generate a mix of difficulties even in conservative mode:
- At least 3 ADVANCED questions (multi-table joins, complex aggregations, insights)
- At least 5 INTERMEDIATE questions (trends, comparisons, groupings)
- Remaining BASIC questions (simple aggregations, filters)

For ADVANCED questions in conservative mode, use safe patterns:
- "How does [numeric_metric1] correlate with [numeric_metric2] across different [category]?"
- "Compare [metric] performance between [category_value_A] and [category_value_B]"
- "What patterns emerge when analyzing [metric] by [dimension1] and [dimension2]?"
- Use multi-table joins ONLY if ENTITY_ID exists in all tables

For INTERMEDIATE questions:
- "What is the trend of [metric] over time?"
- "Show me [metric] breakdown by [category]"
- "Compare [metric1] versus [metric2] for each [category]"

CONSERVATIVE QUESTION PATTERNS (for BASIC questions):
- "What are the [entity] with the highest [numeric_column]?"
- "Show me the average [numeric_column] by [categorical_column]"
- "Which [categorical_column] has the most [entities]?"

AVOID (these often fail validation):
- Questions about concepts not in column names
- "expensive/costly" without explicit COST column
- "best/worst quality" without explicit QUALITY/SCORE column
- "top N" where N > 5
- Year-over-year or multi-period comparisons
- Questions requiring columns that are only mentioned in descriptions

YOUR GOAL: 100% validation pass rate with good complexity mix (3+ advanced). Be safe but interesting.

"""
    
    # Add column guide for question templates
    column_guide_section = ""
    if column_guide:
        column_guide_section = column_guide + "\n📋 COLUMN-BASED QUESTION TEMPLATES:\n\n"
        column_guide_section += "Instead of generic business terms, use ACTUAL column names from the guide above:\n\n"
        column_guide_section += "✅ GOOD (uses actual columns):\n"
        column_guide_section += '- "What are the [ENTITY] with the highest [NUMERIC_COLUMN]?"\n'
        column_guide_section += '- "Show me [METRIC_COLUMN] breakdown by [CATEGORICAL_COLUMN]"\n'
        column_guide_section += '- "How does [METRIC_COLUMN] correlate with [METRIC_COLUMN_2] across different [CATEGORICAL_COLUMN]?"\n\n'
        column_guide_section += "❌ BAD (uses generic inferred terms):\n"
        column_guide_section += '- "What are the items with best popularity?" → NO "popularity" column\n'
        column_guide_section += '- "Show seasonal trends" → NO "season" column\n'
        column_guide_section += '- "Which have highest satisfaction?" → NO "satisfaction" column\n\n'
        column_guide_section += "=" * 80 + "\n\n"
    
    return f"""Generate {num_questions} natural language questions for a {industry} data analysis demo.

Context:
- Company: {company_name}
- Demo: {demo_data.get('title', 'Data Analysis')}
- Business Focus: {demo_data.get('business_value', 'Improve decision making')}
{url_context_text}{detailed_table_info}
{unstructured_info}
{semantic_vocab_section}
{column_guide_section}{existing_questions_context}{retry_instructions}
{_build_mandatory_column_restriction(rich_table_contexts)}
CRITICAL QUESTION GENERATION RULES:

1. COLUMN EXISTENCE VALIDATION (MOST IMPORTANT):
   - Before generating any question, verify that ALL required columns exist in the data context above
   - Example: Don't ask about "restaurants" unless you see a column like RESTAURANT_ID or RESTAURANT_NAME
   - Example: Don't ask about "waste quantities" unless you see both a column indicating waste (like MOVEMENT_TYPE with "waste" value) AND a quantity column
   - If a concept is mentioned in table description but you don't see the actual column above, DO NOT generate questions about it
   - Only generate questions about data that is explicitly shown in the columns list above

2. DATA-GROUNDED CONSTRAINTS:
   - When column has N unique values, NEVER ask for "top M" where M >= N
   - Example: 4 unique restaurants → max is "top 3" or "top 4", NOT "top 10"
   - For aggregations, ensure grouping columns have sufficient cardinality
   - Use actual sample values shown above to understand what data looks like
   - Questions must be answerable using ONLY the columns explicitly listed above

3. NUMERIC AWARENESS:
   - Respect min/max ranges shown above
   - Don't ask for values outside observed ranges
   - Use realistic thresholds based on averages

4. QUESTION TYPES MUST MATCH DATA:
   - If low cardinality (< 10 unique): ask for "all categories" or "breakdown by X"
   - If high cardinality (> 20 unique): ask for "top N" where N < cardinality/2
   - Prefer aggregations that work with available dimensions

5. USE ACTUAL SAMPLE DATA:
   - Reference actual value examples from sample data
   - Ensure question terminology matches column descriptions
   - Make questions specific to the data characteristics shown

6. CONSERVATIVE DEFAULTS:
   - If no cardinality info: use "top 5" max
   - Prefer "show me" over "top N" when uncertain
   - Focus on trends, patterns, and comparisons over exact counts
   - When in doubt, ask simpler, more general questions that are guaranteed to work

7. **CALCULATED METRICS AWARENESS** (CRITICAL - PREVENTS AGENT FAILURES):
   
   NEVER generate questions requiring calculated metrics unless supporting columns exist:
   
   Growth Rate / Trend Questions:
   ❌ FORBIDDEN: "What is the year-over-year growth?" 
      (requires GROWTH_RATE column OR time-series calculation with 2+ years of data)
   ❌ FORBIDDEN: "Show me the trend of X over time"
      (requires sufficient time-series data, may fail validation)
   ✓ SAFE: "What are the total sales by month?"
      (uses raw DATE and AMOUNT columns with simple aggregation)
   
   Percentage / Share Questions:
   ❌ FORBIDDEN: "What percentage of total sales comes from category X?"
      (requires PERCENTAGE column OR complex ratio calculation)
   ❌ FORBIDDEN: "Show me the distribution as percentages"
      (agent may struggle without pre-calculated percentage columns)
   ✓ SAFE: "What are the top 5 categories by sales amount?"
      (uses raw AMOUNT column, agent can calculate shares if needed)
   ✓ SAFE: "Show me sales breakdown by category"
      (raw aggregation, agent can add percentages automatically)
   
   Conversion Rate / Ratio Questions:
   ❌ FORBIDDEN: "What is the conversion rate?"
      (requires CONVERSION_RATE column OR both CONVERSIONS and VISITS columns)
   ✓ SAFE: "How many conversions occurred?"
      (uses raw CONVERSIONS column if it exists)
   
   Average Price / Per-Unit Questions:
   ❌ RISKY: "What is the average price per unit?"
      (safer if AVG_PRICE column exists, otherwise requires AMOUNT/QUANTITY division)
   ✓ SAFE: "What is the total sales amount?"
      (uses raw SALES_AMOUNT column)
   ✓ BETTER: "What is the average sales amount per transaction?"
      (agent can compute AVG(SALES_AMOUNT) from raw data)
   
   WHEN CALCULATED FIELDS EXIST:
   If you see columns like GROWTH_RATE, PERCENTAGE_OF_TOTAL, CONVERSION_RATE_PCT, AVG_PRICE:
   ✓ SAFE: You CAN generate questions about growth, percentages, rates
   ✓ EXAMPLE: "What are the items with the highest growth rate?" (if GROWTH_RATE column exists)
   ✓ EXAMPLE: "Show me the percentage breakdown" (if PERCENTAGE_OF_TOTAL exists)
   
   GENERAL RULE:
   - Prefer questions about RAW metrics (amount, quantity, count, score)
   - Agent can compute simple aggregates (SUM, AVG, COUNT, MAX, MIN) from raw columns
   - Agent struggles with complex calculations (growth rates, percentages, ratios) without pre-calculated columns
   - When in doubt, ask about raw data, not calculated metrics

8. 🚨 DERIVED CONCEPTS & HISTORICAL DATA (CRITICAL - MAJOR FAILURE POINT):
   
   DO NOT ask for concepts that require data not present in columns:
   
   Pattern / Historical Analysis:
   ❌ FORBIDDEN: "What are the alert patterns over time?"
      ✓ ONLY IF: You see columns like ALERT_TIMESTAMP, ALERT_COUNT, or ALERT_HISTORY
      ❌ If you only see: ALERT_THRESHOLD (what should trigger alerts)
      ✓ SAFE ALTERNATIVE: "What percentage of sensors exceed alert thresholds?"
   
   ❌ FORBIDDEN: "Analyze equipment downtime trends"
      ✓ ONLY IF: You see columns like DOWNTIME_HOURS, DOWNTIME_START, DOWNTIME_END
      ❌ If you only see: CURRENT_STATUS (active/offline)
      ✓ SAFE ALTERNATIVE: "Which equipment is currently offline?"
   
   ❌ FORBIDDEN: "What is the failure pattern of X?"
      ✓ ONLY IF: You see columns like FAILURE_DATE, FAILURE_COUNT, FAILURE_TYPE
      ❌ If you only see: CURRENT_STATUS or single-point-in-time data
      ✓ SAFE ALTERNATIVE: "How many failures occurred in the dataset?"
   
   Composite Scores / Ratings:
   ❌ FORBIDDEN: "What is the overall quality score?"
      ✓ ONLY IF: You see a column like QUALITY_SCORE, OVERALL_RATING
      ❌ If you only see: DEFECT_COUNT, DURABILITY_SCORE (individual metrics)
      ✓ SAFE ALTERNATIVE: "What are the defect counts by product line?"
   
   ❌ FORBIDDEN: "Show me production quality scores"
      ✓ ONLY IF: You see columns like PRODUCTION_QUALITY_SCORE
      ❌ If you only see: DEFECT_COUNT, PASS_FAIL_STATUS (raw quality data)
      ✓ SAFE ALTERNATIVE: "What is the average defect count by production line?"
   
   ❌ FORBIDDEN: "Rank suppliers by performance score"
      ✓ ONLY IF: You see a column like SUPPLIER_PERFORMANCE_SCORE
      ❌ If you only see: DELIVERY_DATE, QUALITY_ISSUES (individual metrics)
      ✓ SAFE ALTERNATIVE: "Which suppliers have the most quality issues?"
   
   Concepts from Descriptions:
   ❌ FORBIDDEN: Questions about concepts mentioned in table DESCRIPTIONS but not in COLUMNS
      Example: Description says "track customer satisfaction" but no SATISFACTION column exists
      ✓ ONLY ask about: Actual column names (RATING, REVIEW_TEXT, etc.)
   
   CRITICAL RULE FOR "PATTERNS" and "TRENDS":
   - Pattern/Trend questions REQUIRE time-series data with multiple timestamps
   - If you only see ONE timestamp column with dates, be conservative
   - ✓ SAFE: "Show me data by date" (simple grouping)
   - ❌ RISKY: "What are the patterns over time?" (implies analysis of trends)
   
   GENERAL RULE:
   - Raw current state data (readings, counts, status) → Ask about current state
   - Historical tracking data (timestamps, history columns) → Ask about patterns/trends
   - Individual metrics → Don't ask for composite scores
   - Actual columns → Safe to ask about
   - Description concepts → UNSAFE unless column exists

9. SELF-VALIDATION BEFORE RETURNING (MOST CRITICAL):
   - BEFORE finalizing any question, verify against the data context above
   - Check: Do ALL concepts in the question have corresponding columns?
   - Check: If question mentions "expensive/costly" → is there a cost/price column?
   - Check: If question mentions specific entities → are those entity columns present?
   - Check: If question requires aggregation by X → does column X exist?
   - Check: If question requires CALCULATION (growth, percentage, rate) → does calculated column exist OR can agent compute from raw columns?
   - Check: If question asks for "patterns/trends" → are there sufficient historical tracking columns?
   - Check: If question asks for "composite scores" → does that exact score column exist?
   - Check: If question uses concepts from descriptions → do matching columns exist?
   - If ANY validation fails → either revise the question to use available columns OR discard it
   - ONLY return questions you are 100% confident are answerable with the available data

EXAMPLES OF GOOD VS BAD QUESTIONS:

Scenario: Table has columns [ENTITY_ID, SUPPLIER_NAME (4 unique), DELIVERY_DATE, QUALITY_SCORE (1-100)]

❌ BAD: "What are the top 10 suppliers by delivery performance?"
   Why: Only 4 unique suppliers exist, can't show top 10

✓ GOOD: "What are the top 3 suppliers by average quality score?"
   Why: Only 4 suppliers, asking for 3 is safe

❌ BAD: "How do metrics vary during promotional periods?"
   Why: No PROMOTIONAL_PERIOD column exists in data

✓ GOOD: "How do quality scores vary over time by supplier?"
   Why: Uses actual columns (QUALITY_SCORE, DELIVERY_DATE, SUPPLIER_NAME)

❌ BAD: "Show me year-over-year growth"
   Why: Only 100 records, may not span multiple years

✓ GOOD: "What is the average quality score by supplier?"
   Why: Simple aggregation guaranteed to work

Scenario: Table has columns [ENTITY_ID, EQUIPMENT_TYPE (5 unique), MAINTENANCE_ID, MAINTENANCE_DATE] - NO COST COLUMN

❌ BAD: "What are the top 3 equipment types that require the most expensive maintenance?"
   Why: Question asks about "expensive" but no COST/EXPENSE column exists

✓ GOOD: "What are the top 3 equipment types that require the most maintenance activities?"
   Why: Uses COUNT of MAINTENANCE_ID instead of cost - answerable with available data

Scenario: Table has columns [ENTITY_ID, PRODUCT_NAME, SALES_AMOUNT, QUANTITY, DATE]

✓ GOOD: "Which products generate the highest revenue?"
   Why: SALES_AMOUNT column exists to answer "highest revenue"

❌ BAD: "Which products have the best customer ratings?"
   Why: No RATING or CUSTOMER_SATISFACTION column exists

Scenario: Sensor data with [ENTITY_ID, SENSOR_TYPE, READING_VALUE, ALERT_THRESHOLD, READING_TIMESTAMP]

❌ BAD: "What are the sensor alert patterns over the last month?"
   Why: No ALERT_HISTORY or ALERT_TRIGGERED columns - only ALERT_THRESHOLD (when alerts SHOULD trigger)
   
✓ GOOD: "Which sensors have readings exceeding their alert thresholds?"
   Why: Uses actual columns (READING_VALUE > ALERT_THRESHOLD) - doesn't require historical alert tracking

Scenario: Equipment data with [ENTITY_ID, EQUIPMENT_NAME, CURRENT_STATUS, LAST_MAINTENANCE_DATE]

❌ BAD: "What is the equipment downtime trend?"
   Why: No DOWNTIME_HOURS or DOWNTIME_START columns - only CURRENT_STATUS (current state, not history)
   
✓ GOOD: "Which equipment is currently in maintenance status?"
   Why: Uses CURRENT_STATUS column - asks about current state, not historical trends

Scenario: Quality data with [ENTITY_ID, BATCH_ID, DEFECT_COUNT, DURABILITY_SCORE, PASS_FAIL_STATUS]

❌ BAD: "What are the production quality scores by batch?"
   Why: No QUALITY_SCORE column exists - only individual metrics (DEFECT_COUNT, DURABILITY_SCORE)
   
✓ GOOD: "What is the average defect count and durability score by batch?"
   Why: Uses actual individual metric columns - doesn't assume composite score exists

❌ BAD: "Analyze quality score trends and correlations"
   Why: No composite QUALITY_SCORE column exists
   
✓ GOOD: "How do defect counts correlate with durability scores?"
   Why: Uses actual metric columns that exist - agent can analyze their relationship

Scenario: Restaurant menu data with [ENTITY_ID, ITEM_NAME, PRICE, COST, CATEGORY, AVAILABILITY_STATUS (active/discontinued)]

❌ BAD: "What relationships exist between menu item profit margins, popularity scores, and customer ordering patterns when analyzed by seasonal availability?"
   Why: "popularity scores" - NO POPULARITY_SCORE column exists
   Why: "seasonal availability" - Only AVAILABILITY_STATUS (active/discontinued), NOT seasonal data
   
✓ GOOD: "What relationships exist between menu item profit margins and order volumes when analyzed by category and availability status?"
   Why: Uses actual columns - can calculate margins (PRICE-COST), order volumes from transactions, group by CATEGORY and AVAILABILITY_STATUS

Scenario: Unstructured data with sample chunks showing "API Specification: Vehicle Diagnostics Module. Endpoint: /api/v1/diagnostics..."

❌ BAD: "Find implementation guides for Electronic Logging Device integration"
   Why: Samples show generic API specs, not specific ELD implementation guides - question is TOO SPECIFIC
   
✓ GOOD: "Find API specifications for vehicle diagnostic systems"
   Why: Matches the generality and content type shown in sample chunks - not overly specific

❌ BAD: "Search for step-by-step ELD configuration tutorials"
   Why: Samples show technical reference docs (API specs), not tutorials - wrong document type AND too specific
   
✓ GOOD: "Search for technical specifications for vehicle telematics integration"
   Why: Matches both content type (technical specs) and generality (vehicle telematics, not ELD specifically)

FOLLOW THESE PATTERNS: Use actual column names, respect cardinality, avoid assumptions, substitute when columns missing. Be especially careful with patterns/trends (need historical tracking), composite scores (need actual score columns), and search question specificity (match sample content generality).

QUESTION VALIDATION CHECKLIST (use this BEFORE finalizing each question):

For each question you generate, verify:
1. ✓ All referenced concepts have corresponding columns in data above
2. ✓ Any "top N" has N < unique_count for that column
3. ✓ All filters/groupings use actual column names shown above
4. ✓ Date ranges are realistic given the data shown
5. ✓ No assumptions about data not explicitly shown
6. ✓ If question requires CALCULATED metrics (growth, percentage, rate):
   - Check if calculated column exists (e.g., GROWTH_RATE, PERCENTAGE_OF_TOTAL, AVG_PRICE)
   - If NO calculated column: Can agent compute from raw columns? (most percentages/rates: NO)
   - If NEITHER: REJECT this question and generate a simpler alternative
7. ✓ Prefer questions about raw data (totals, counts, averages) over complex calculations
8. ✓ **CRITICAL**: If question asks about "patterns" or "trends":
   - Verify sufficient historical tracking columns exist (not just single timestamp)
   - Example: ALERT_TRIGGERED_COUNT or ALERT_HISTORY (not just ALERT_THRESHOLD)
   - Example: DOWNTIME_START, DOWNTIME_END, DOWNTIME_HOURS (not just CURRENT_STATUS)
   - If NO historical tracking: Ask about CURRENT STATE instead of patterns/trends
9. ✓ **CRITICAL**: If question asks about "scores" or "ratings":
   - Verify the exact score/rating column exists
   - Example: QUALITY_SCORE column (not just DEFECT_COUNT and DURABILITY_SCORE separately)
   - Example: PERFORMANCE_SCORE column (not just individual metrics)
   - If NO composite score column: Ask about individual metrics instead
10. ✓ **CRITICAL**: Distinguish between THRESHOLD and ACTUAL EVENT:
    - ALERT_THRESHOLD = when alert should trigger (configuration, not events)
    - ALERT_TRIGGERED = when alerts actually occurred (historical data)
    - STATUS = current state (not historical downtime)
    - Only ask about patterns/history if event/history columns exist

11. ✓ **CRITICAL FOR SEARCH QUESTIONS**: Match sample content specificity:
    - Look at the ACTUAL SAMPLE CHUNKS shown in unstructured data section
    - If samples show generic content (e.g., "API specifications") → ask for generic content ("technical specifications")
    - If samples show specific content (e.g., "ELD implementation guide step 1...") → ask for specific content ("ELD guides")
    - DO NOT ask for specific document types unless you SEE them in sample chunks
    - Example: Samples show "vehicle diagnostics API docs" → ❌ DON'T ask "Find ELD implementation guides"
    - Example: Samples show "vehicle diagnostics API docs" → ✅ DO ask "Find API specifications for vehicle diagnostics"
    - RULE: Search question specificity ≤ Sample content specificity

12. ✓ **ULTRA-CRITICAL - INFERRED METRICS CHECK**:
    - Read the question you just generated
    - For EVERY noun/metric mentioned (popularity, seasonal, satisfaction, performance, quality, profitability):
      1. Look back at the column list above
      2. Can you find the EXACT column name for this metric?
      3. If NO exact match → REJECT the question or rephrase without that metric
    - Example: Question mentions "popularity scores"
      → Look for POPULARITY_SCORE, POPULARITY_RANK, POPULARITY_RATING in columns
      → If NOT found → REJECT or rephrase to use metrics that DO exist (e.g., "order volumes", "sales counts")
    - Example: Question mentions "seasonal availability"
      → Look for SEASON, SEASONAL_FLAG, SEASONAL_AVAILABILITY in columns
      → If you only see AVAILABILITY_STATUS (active/discontinued) → That's NOT seasonal → REJECT or rephrase
    - THIS IS THE #1 REASON QUESTIONS FAIL - Be paranoid about inferred metrics

If ANY check fails, revise the question or discard it.

MANDATORY DISTRIBUTION REQUIREMENTS (for {num_questions} questions):

YOU MUST GENERATE EXACTLY:
- 3-4 BASIC difficulty questions
  - Examples: "What are the top 5 X by Y?", "Show me all X where Y > Z", "What is the total/average/count of X?"
  - Characteristics: Single metric, single grouping, straightforward filter, no joins required (or simple 1-table join)
  
- 4-6 INTERMEDIATE difficulty questions
  - Examples: "How does X vary over time?", "Compare X between category A and B", "What is the breakdown of X by Y and Z?"
  - Characteristics: Multiple metrics or dimensions, time-series analysis, categorical comparisons, requires joining 2 tables
  
- 3-4 ADVANCED difficulty questions
  - Examples: "How does X correlate with Y across different Z?", "What patterns emerge when analyzing X by Y and Z?", "Identify relationships between X, Y, and Z"
  - Characteristics: 3+ table joins, correlation analysis, pattern recognition, requires synthesis of multiple data sources

CRITICAL CATEGORY ASSIGNMENT RULES:
You MUST generate {int(num_questions * 0.7)} ANALYTICS questions and {int(num_questions * 0.3)} SEARCH questions.

- **ANALYTICS questions** (category: "analytics"):
  - Query STRUCTURED data tables (the fact/dimension tables)
  - Ask for metrics, aggregations, trends, comparisons, correlations
  - Examples: "What are the top 5...", "How does X compare to Y...", "Show me the average...", "What is the correlation between..."
  - USE THESE: "What", "How", "Show", "Compare", "Analyze", "Calculate", "Which"
  
- **SEARCH questions** (category: "search"):
  - Query UNSTRUCTURED text content
  - CRITICAL: Search questions MUST match the ACTUAL SAMPLE CONTENT shown, not just the description
  - **LOOK AT THE SAMPLE CHUNKS** provided in the unstructured data section above
  - Generate questions about content TYPES and TOPICS you can SEE in the samples
  - If unstructured data is "customer feedback/reviews" → ask about customer sentiment, complaints, feature requests
  - If unstructured data is "technical documentation" → ask about implementation guides, best practices, specifications
  - If unstructured data is "policy documents" → ask about compliance requirements, procedures, regulations
  - 🚨 **CRITICAL - MATCH SAMPLE CONTENT SPECIFICITY**:
    * If samples show "API specifications for diagnostics" → ask for "API specifications" or "technical specifications"
    * If samples show "troubleshooting guides" → ask for "troubleshooting guides" or "maintenance procedures"
    * If samples show "customer complaints about delivery" → ask for "delivery issues" or "service complaints"
    * DO NOT ask for "ELD implementation guides" if samples only show generic "vehicle diagnostics API docs"
    * DO NOT ask for specific document types (implementation guides, case studies) unless you SEE them in samples
    * KEEP SEARCH QUESTIONS AS GENERAL as the actual sample content (if samples are generic, questions should be generic)
  - USE THESE: "Find", "Search for", "Locate", "Look for", "Retrieve"
  - MUST use action verbs that indicate document retrieval, NOT data analysis

CRITICAL PRE-RETURN CHECKLIST:
Before returning your JSON array, you MUST verify:
1. Count your questions by difficulty: ___ basic, ___ intermediate, ___ advanced
2. Count your questions by category: ___ analytics, ___ search
3. Verify you have EXACTLY {int(num_questions * 0.7)} questions with "category": "analytics"
4. Verify you have EXACTLY {int(num_questions * 0.3)} questions with "category": "search"
5. Verify search questions start with action verbs like "Find", "Search for", "Locate"
6. Verify analytics questions ask for data metrics/aggregations, not document retrieval
7. If ANY requirement is not met, ADD MORE QUESTIONS of the missing type
8. ONLY return JSON after ALL distribution requirements are satisfied

OTHER REQUIREMENTS:
- Questions should be realistic for {industry}
- Use business terminology, not technical database terms
- All structured tables join on ENTITY_ID column
- Ensure questions match the difficulty level you assign
- Search questions should reference the unstructured content available

🚨 MANDATORY: Include referenced_columns and referenced_tables for validation

Return ONLY a JSON array in this exact format:
[
  {{
    "text": "What are the top 5 entities by total value?",
    "difficulty": "basic",
    "category": "analytics",
    "referenced_columns": ["ENTITY_NAME", "TOTAL_VALUE"],
    "referenced_tables": ["FACT_SALES"]
  }},
  {{
    "text": "How does performance vary over time by category?",
    "difficulty": "intermediate",
    "category": "analytics",
    "referenced_columns": ["PERFORMANCE_SCORE", "DATE", "CATEGORY"],
    "referenced_tables": ["FACT_PERFORMANCE"]
  }},
  {{
    "text": "What patterns emerge when analyzing cost efficiency across departments and time periods?",
    "difficulty": "advanced",
    "category": "analytics",
    "referenced_columns": ["COST", "REVENUE", "DEPARTMENT", "DATE"],
    "referenced_tables": ["FACT_COSTS", "DIM_DEPARTMENTS"]
  }},
  {{
    "text": "Find documentation about operational processes",
    "difficulty": "basic",
    "category": "search",
    "referenced_columns": [],
    "referenced_tables": ["DOCUMENTATION_CHUNKS"]
  }}
]

CRITICAL: Every question MUST include referenced_columns (list of actual column names used) and referenced_tables (list of tables accessed).
This forces you to explicitly verify columns exist before asking about them!
"""


def get_follow_up_questions_prompt(primary_question: str) -> str:
    """
    Generate prompt for creating follow-up questions.
    
    Args:
        primary_question: Primary question to generate follow-ups for
        
    Returns:
        Formatted prompt string for LLM
    """
    return f"""Given this analytical question: "{primary_question}"

Generate 2-3 natural follow-up questions that:
- Build on insights from the primary question
- Explore the "why" behind patterns
- Suggest next steps or actions
- Are conversational and business-focused

Return ONLY a JSON array of question strings:
["follow-up question 1", "follow-up question 2", "follow-up question 3"]
"""


def get_target_question_analysis_prompt(questions: List[str]) -> str:
    """
    Generate prompt for analyzing target questions to extract requirements.
    
    Args:
        questions: List of target questions to analyze
        
    Returns:
        Formatted prompt string for LLM
    """
    questions_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(questions)])
    
    return f"""Analyze these target questions to determine what data structures, dimensions, and metrics are needed to answer them:

Target Questions:
{questions_text}

For each question, identify:
1. **Required Dimensions**: Data attributes/fields needed (e.g., age_range, product_category, time_period, customer_segment, geographic_region)
2. **Metrics Needed**: Calculations or measurements (e.g., percentage, count, average, sum, growth_rate, ratio)
3. **Data Characteristics**: Specific requirements for the data (e.g., "need age field with values 18-65", "need timestamp field for time-based analysis", "need numeric revenue field for aggregation")

Return ONLY a JSON object in this exact format:
{{
  "required_dimensions": ["dimension1", "dimension2", "dimension3"],
  "metrics_needed": ["metric1", "metric2"],
  "data_characteristics": {{
    "numeric_fields": ["list of numeric fields needed"],
    "categorical_fields": ["list of categorical fields with examples"],
    "temporal_fields": ["list of date/time fields needed"],
    "value_ranges": {{"field_name": "description of range needed"}},
    "special_requirements": ["any other specific requirements"]
  }},
  "question_types": ["analytical", "aggregation", "trend", "comparison"]
}}

Be specific and comprehensive. Extract all implicit and explicit requirements."""


def get_agent_system_prompt(
    demo_data: Dict,
    company_name: str
) -> str:
    """
    Generate comprehensive system prompt for Snowflake Intelligence agent.
    
    Args:
        demo_data: Demo configuration dictionary
        company_name: Company name for context
        
    Returns:
        Formatted system prompt string
    """
    industry = demo_data.get(
        'industry_focus',
        demo_data.get('industry', 'Business Intelligence')
    )
    description = demo_data.get('description', '')
    business_value = demo_data.get(
        'business_value',
        'Improve operational efficiency and decision making'
    )
    
    return f"""You are an AI-powered data analyst assistant for {company_name}.

Your expertise includes:
- {industry} domain knowledge and industry best practices
- Advanced data analysis and pattern recognition
- SQL query generation and optimization
- Natural language insights and recommendations
- Data visualization and reporting

You have access to:
- Structured data tables via Cortex Analyst for quantitative analysis
- Unstructured documents via Cortex Search for qualitative insights

Context for this demo:
{description}

Business Value Focus:
{business_value}

Your goal is to help users understand their data through:
- Answering analytical questions with data-driven insights
- Generating actionable recommendations based on analysis
- Finding relevant information in documents and reports
- Explaining complex data patterns in simple terms
- Proactively suggesting follow-up analyses

Guidelines:
- Always provide context for your answers
- Use specific numbers and data points when available
- Suggest follow-up questions when appropriate
- Explain your reasoning when making recommendations
- Be concise but comprehensive in your responses

Advanced Analytics & Calculated Metrics:
When users ask for calculated metrics or complex analysis, use these strategies:

Growth Rate / Trend Analysis:
- If GROWTH_RATE column exists: Use it directly
- If raw time-series data exists: Use LAG() window function to calculate period-over-period changes
  Example: SELECT category, (current_amount - prev_amount) / NULLIF(prev_amount, 0) * 100 as growth_rate 
           FROM (SELECT category, amount, LAG(amount) OVER (PARTITION BY category ORDER BY date) as prev_amount FROM table)
- Explain the calculation method in your response

Percentage / Share Calculations:
- If PERCENTAGE or PERCENTAGE_OF_TOTAL column exists: Use it directly
- Otherwise: Calculate ratio and multiply by 100
  Example: SELECT category, SUM(amount) / SUM(SUM(amount)) OVER () * 100 as percentage FROM table GROUP BY category
- Always show percentages with proper context (percentage of what?)

Conversion Rate / Ratio Metrics:
- If CONVERSION_RATE column exists: Use it
- If CONVERSIONS and VISITS columns exist: Calculate as (conversions / NULLIF(visits, 0) * 100)
- Be explicit about numerator and denominator in your explanation

Average / Per-Unit Calculations:
- If pre-calculated columns exist (AVG_PRICE, AMOUNT_PER_UNIT): Use them
- If raw columns exist (SALES_AMOUNT, QUANTITY): Calculate using (amount / NULLIF(quantity, 0))
- For simple averages: Use AVG() aggregate function
  Example: SELECT category, AVG(price) as avg_price FROM table GROUP BY category

Aggregate Functions (Always Available):
- SUM: Total of all values
- AVG: Average (mean) of values
- COUNT: Number of records
- MAX/MIN: Highest/lowest values
- Use OVER() for window functions (running totals, rankings, moving averages)

When Data Doesn't Support the Question:
- If user asks for a metric that can't be calculated: Explain what data would be needed
- Suggest alternative questions that can be answered with available data
- Example: "I don't see a conversion rate column, but I can show you the total conversions by channel"""


def get_agent_persona_prompt(
    demo_data: Dict,
    company_name: str
) -> str:
    """
    Generate prompt for creating agent persona description.
    
    Args:
        demo_data: Demo configuration dictionary
        company_name: Company name for context
        
    Returns:
        Formatted prompt string for LLM
    """
    industry = demo_data.get(
        'industry_focus',
        demo_data.get('industry', 'Business Intelligence')
    )
    
    return f"""Generate a professional persona description for an AI data analyst agent specialized in {industry} for {company_name}.

The persona should:
- Highlight relevant industry expertise
- Emphasize analytical capabilities
- Be professional and trustworthy
- Be 2-3 sentences long

Return only the persona description, no additional text."""


def get_demo_generation_prompt(
    company_name: str,
    team_members: str,
    use_cases: str,
    num_ideas: int = 3,
    target_questions: Optional[List[str]] = None,
    target_question_analysis: Optional[Dict] = None,
    advanced_mode: bool = False
) -> str:
    """
    Generate comprehensive prompt for creating demo scenarios.
    
    This is the largest and most complex prompt, used to generate complete
    demo scenarios with multiple tables and realistic data structures.
    
    Args:
        company_name: Company name for context
        team_members: Target audience description
        use_cases: Specific use cases to address
        num_ideas: Number of demo scenarios to generate
        target_questions: Optional list of questions demos should answer
        target_question_analysis: Optional analysis dict from analyze_target_questions()
        advanced_mode: Whether to generate advanced schemas (3-5 tables)
        
    Returns:
        Formatted prompt string for LLM
    """
    use_case_context = (
        f"\n- Use Cases: {use_cases}"
        if use_cases
        else "\n- Use Cases: Not specified"
    )
    
    # Add target questions context if provided - PHASE 0 ENHANCEMENT
    target_questions_context = ""
    if target_questions and len(target_questions) > 0:
        questions_list = "\n".join(
            [f"  {i+1}. {q}" for i, q in enumerate(target_questions)]
        )
        
        # Build enhanced constraints section from analysis
        mandatory_constraints = ""
        if target_question_analysis and target_question_analysis.get('has_target_questions'):
            entities = target_question_analysis.get('all_required_entities', [])
            metrics = target_question_analysis.get('all_required_metrics', [])
            dimensions = target_question_analysis.get('all_required_dimensions', [])
            min_tables = target_question_analysis.get('min_tables_needed', 1)
            cardinality_constraints = target_question_analysis.get('cardinality_constraints', [])
            
            mandatory_constraints = f"""

EXTRACTED REQUIREMENTS (from LLM analysis - NON-NEGOTIABLE):
- Required Entities: {', '.join(entities) if entities else 'Not specified'}
- Required Metrics: {', '.join(metrics) if metrics else 'Not specified'}
- Required Dimensions: {', '.join(dimensions) if dimensions else 'Not specified'}
- Minimum Tables: {min_tables}
"""
            
            if cardinality_constraints:
                mandatory_constraints += "\nCardinality Requirements:\n"
                for constraint in cardinality_constraints:
                    field = constraint.get('field', 'unknown')
                    min_unique = constraint.get('min_unique', 0)
                    reason = constraint.get('reason', '')
                    mandatory_constraints += f"  - {field}: minimum {min_unique} unique values ({reason})\n"
            
            mandatory_constraints += """
MANDATORY DESIGN CONSTRAINTS:
1. Demo MUST include tables for ALL required entities listed above
2. Each relevant table MUST include specific columns for metrics and dimensions listed above
3. Cardinality requirements MUST be met (e.g., if "top 10" is requested, need 15+ unique values)
4. Foreign keys MUST support joins if questions reference multiple entities
5. Table descriptions MUST explicitly list column names that enable answering these questions

VALIDATION CHECKLIST (complete before returning JSON):"""
            
            # Add a checkbox for each target question
            for i, q in enumerate(target_questions, 1):
                mandatory_constraints += f"\n- [ ] Can question {i} be answered? (check entities, metrics, dimensions exist)"
            
            mandatory_constraints += "\n\nIf ANY checkbox is unchecked, REDESIGN the demo to ensure ALL questions are answerable."
        
        target_questions_context = f"""

🎯 TARGET QUESTIONS - MANDATORY REQUIREMENTS (NON-NEGOTIABLE):
The user provided these questions. Your demo MUST answer ALL of them:
{questions_list}{mandatory_constraints}

Data Generation Strategy:
PRIMARY GOAL (70%): Ensure the data can answer the target questions above
SECONDARY GOAL (30%): Include additional varied data for general analytics

Ensure that:
- Table descriptions explicitly include EXACT data fields necessary to answer these questions
- Demo scenarios naturally support these analytical goals
- Column types and structures enable the required calculations and aggregations
- Data distributions are realistic (not 100% or 0% for percentages, sufficient variety for "top N" queries)
"""
    
    # Advanced mode specifications
    if advanced_mode:
        table_spec = """2. 3-5 structured data tables forming a realistic data model:
   - 1-2 FACT tables (transactional/event data with metrics and foreign keys)
   - 2-3 DIMENSION tables (descriptive attributes, lookups, reference data)
   - Ensure proper foreign key relationships between tables (fact tables reference dimension tables)
   - Design for realistic star or snowflake schema patterns
3. MANDATORY: EXACTLY 1 unstructured data table (NON-NEGOTIABLE)
   - REQUIRED: Must include "unstructured" key in tables object
   - Choose the MOST RELEVANT content type for this demo scenario
   - Examples: customer feedback, technical docs, policy documents, incident reports, support tickets
   - The unstructured table is NOT optional - every demo MUST have one

Table Structure Requirements for Advanced Mode:
- Primary fact table(s) should have foreign keys to dimension tables
- Dimension tables should have clear business meaning (customers, products, dates, categories, locations)
- Include both granular and aggregated data opportunities
- Design for realistic business analytics scenarios with joins
- CRITICAL: Every demo MUST include the "unstructured" table key"""
        schema_example = """
{
  "demos": [
    {
      "title": "Demo Title",
      "description": "Detailed description",
      "industry_focus": "Industry",
      "business_value": "Business value",
      "tables": {
        "structured_1": {"name": "FACT_TABLE", "description": "Main fact table with metrics and FKs", "purpose": "Primary analytics", "table_type": "fact"},
        "structured_2": {"name": "DIM_TABLE_1", "description": "First dimension table", "purpose": "Descriptive attributes", "table_type": "dimension"},
        "structured_3": {"name": "DIM_TABLE_2", "description": "Second dimension table", "purpose": "Lookup data", "table_type": "dimension"},
        "structured_4": {"name": "DIM_TABLE_3", "description": "Optional third dimension", "purpose": "Additional context", "table_type": "dimension"},
        "structured_5": {"name": "OPTIONAL_FACT_2", "description": "Optional second fact table", "purpose": "Secondary metrics", "table_type": "fact"},
        "unstructured": {"name": "CONTENT_CHUNKS", "description": "Unstructured text data (choose most relevant content type for the scenario)", "purpose": "Semantic search"}
      }
    }
  ]
}

⚠️ VALIDATION REQUIREMENTS:
- MANDATORY: Include 3-5 structured tables (structured_4 and structured_5 are optional)
- MANDATORY: Include the "unstructured" key with a relevant table (NOT optional, NOT omitted)
- FORBIDDEN: Do NOT include "unstructured_2" (only 1 unstructured table allowed)
- If you omit the "unstructured" key, the response will be REJECTED"""
    else:
        table_spec = """2. 3 structured data tables forming a realistic data model:
   - 1 FACT table (transactional/event data with metrics and foreign keys)
   - 2 DIMENSION tables (descriptive attributes, lookups, reference data)
   - Ensure proper foreign key relationships between tables (fact table references dimension tables)
   - Design for realistic star schema pattern
3. MANDATORY: EXACTLY 1 unstructured data table (NON-NEGOTIABLE)
   - REQUIRED: Must include "unstructured" key in tables object
   - Choose the MOST RELEVANT content type for this demo scenario
   - Examples: customer feedback, technical docs, policy documents, incident reports, support tickets
   - The unstructured table is NOT optional - every demo MUST have one

Table Structure Requirements for Standard Mode:
- Primary fact table should have foreign keys to dimension tables
- Dimension tables should have clear business meaning (customers, products, dates, categories, locations)
- Include both granular and aggregated data opportunities
- Design for realistic business analytics scenarios with joins
- CRITICAL: Every demo MUST include the "unstructured" table key"""
        schema_example = """
{
  "demos": [
    {
      "title": "Demo Title",
      "description": "Detailed description",
      "industry_focus": "Industry",
      "business_value": "Business value",
      "tables": {
        "structured_1": {"name": "FACT_TABLE", "description": "Main fact table with metrics and FKs", "purpose": "Primary analytics", "table_type": "fact"},
        "structured_2": {"name": "DIM_TABLE_1", "description": "First dimension table", "purpose": "Descriptive attributes", "table_type": "dimension"},
        "structured_3": {"name": "DIM_TABLE_2", "description": "Second dimension table", "purpose": "Lookup data", "table_type": "dimension"},
        "unstructured": {"name": "CONTENT_CHUNKS", "description": "Unstructured text data", "purpose": "Semantic search"}
      }
    }
  ]
}

⚠️ VALIDATION REQUIREMENTS:
- MANDATORY: Include exactly 3 structured tables (1 fact + 2 dimensions)
- MANDATORY: Include the "unstructured" key with a relevant table (NOT optional, NOT omitted)
- If you omit the "unstructured" key, the response will be REJECTED"""
    
    return f"""You are a Snowflake solutions architect creating tailored demo scenarios for a customer. 

IMPORTANT: You MUST generate exactly {num_ideas} complete, distinct demo scenarios. Each demo should be fully detailed and different from the others.

Customer Information:
- Company: {company_name}
- Team/Audience: {team_members}
{use_case_context}{target_questions_context}

For EACH of the {num_ideas} demos, provide:
1. A compelling title and detailed description
{table_spec}

Requirements:
- Make demos relevant to the company's likely industry/domain
- Consider the audience when designing complexity
- Focus on business value and real-world scenarios
- Ensure table names are SQL-friendly (uppercase, underscores)
- Generate {num_ideas} COMPLETE demo objects - do not stop after just one!

Table Description Requirements - CRITICAL:
For each table, you MUST explicitly list the EXACT column names and their purposes in the description.
This is NON-NEGOTIABLE - the schema generator needs explicit field names, not just concepts.

Required Format: "Table contains: column_name (description), column_name2 (description), ..."
- Include ALL columns needed for analysis (5-8 minimum per structured table)
- Be explicit about field names, not vague concepts
- Include data types implicitly in descriptions (e.g., "cost_amount (dollar values)", "timestamp (date/time)")
- For fact tables: include metrics (numeric values) and foreign keys
- For dimension tables: include categorical attributes and IDs

GOOD Example (Structured Table):
"Query performance fact table containing: query_id (unique identifier), execution_time_seconds (duration in seconds), 
warehouse_size (XS/S/M/L/XL/2XL categories), credits_consumed (decimal cost), query_type (SELECT/INSERT/UPDATE/DELETE), 
user_id (foreign key to user dimension), warehouse_id (foreign key), timestamp (execution date/time), 
rows_scanned (integer count), bytes_scanned (data volume)"

GOOD Example (Unstructured Table):
"Security policy documents and incident reports containing: policy guidelines, threat assessments, 
compliance requirements, incident post-mortems, and security best practices for semantic search"

BAD Example (Too Vague):
"Query performance metrics with execution details and resource usage"
(Missing: No column names listed, just vague concepts)

BAD Example (Missing Critical Fields):
"Warehouse activity data including query counts and usage patterns"
(Missing: No specific column names, no foreign keys, no metrics details)

🚨 MANDATORY VALIDATION CHECKLIST - Complete BEFORE returning JSON:
Before you return your response, verify EACH demo includes:
✓ The "unstructured" key in tables (MANDATORY - NOT OPTIONAL)
✓ The unstructured table has a meaningful name and description
✓ The unstructured table content type matches the demo scenario
✓ All structured tables have foreign key relationships
✓ All table descriptions explicitly list column names

❌ REJECT these patterns:
- Demos without an "unstructured" table key
- Unstructured tables with generic/placeholder descriptions
- Tables without explicit column names in descriptions

Return ONLY a JSON object with this exact structure (with {num_ideas} complete demo objects in the array):
{schema_example}"""


def get_schema_generation_prompt(
    table_name: str,
    table_description: str,
    company_name: str,
    target_questions: Optional[List[str]] = None,
    question_analysis: Optional[Dict] = None,
    required_fields: Optional[List[Dict]] = None,
    table_type: Optional[str] = None
) -> str:
    """
    Generate prompt for creating realistic table schemas.
    
    Args:
        table_name: Name of table to generate schema for
        table_description: Description of table purpose and contents
        company_name: Company name for context
        target_questions: Optional list of questions schema should support
        question_analysis: Optional analysis of question requirements
        required_fields: Optional list of mandatory fields extracted from description
        table_type: Optional table type ('fact' or 'dimension') for relationship guidance
        
    Returns:
        Formatted prompt string for LLM
    """
    # Add target questions context if provided
    target_questions_context = ""
    if target_questions and len(target_questions) > 0:
        questions_list = "\n".join([f"  - {q}" for q in target_questions])
        target_questions_context = f"""

CRITICAL - Target Questions Support:
This table MUST support answering these questions:
{questions_list}
"""
    
    # Add question analysis context if provided
    analysis_context = ""
    if question_analysis and question_analysis.get('has_target_questions'):
        required_dims = question_analysis.get('required_dimensions', [])
        metrics_needed = question_analysis.get('metrics_needed', [])
        data_chars = question_analysis.get('data_characteristics', {})
        
        if required_dims:
            analysis_context += f"\nRequired Dimensions: {', '.join(required_dims)}"
        if metrics_needed:
            analysis_context += f"\nMetrics to Support: {', '.join(metrics_needed)}"
        if data_chars:
            numeric_fields = data_chars.get('numeric_fields', [])
            categorical_fields = data_chars.get('categorical_fields', [])
            temporal_fields = data_chars.get('temporal_fields', [])
            
            if numeric_fields:
                analysis_context += (
                    f"\nNumeric Fields Needed: {', '.join(numeric_fields)}"
                )
            if categorical_fields:
                analysis_context += (
                    f"\nCategorical Fields Needed: "
                    f"{', '.join(categorical_fields)}"
                )
            if temporal_fields:
                analysis_context += (
                    f"\nTemporal Fields Needed: {', '.join(temporal_fields)}"
                )
        
        if analysis_context:
            target_questions_context += f"\nBased on analysis:{analysis_context}\n"
    
    # Build mandatory fields section
    mandatory_fields_section = ""
    if required_fields and len(required_fields) > 0:
        mandatory_fields_section = "\n\nMANDATORY FIELDS - MUST INCLUDE:\n"
        for field in required_fields:
            sample_vals = field.get('sample_values', [])
            sample_str = f"Sample values: {sample_vals}" if sample_vals else "No sample values provided"
            mandatory_fields_section += f"- {field['field_name']} ({field['suggested_type']}): {field.get('description', 'Required field')}\n  {sample_str}\n"
        
        mandatory_fields_section += """
CRITICAL: The fields listed above are MANDATORY. You MUST include every single one in your schema.
Failure to include any mandatory field will result in regeneration.

You may add 3-5 ADDITIONAL columns beyond the mandatory ones to enrich the schema.
"""
    
    # Phase 1.1: Add table relationship requirements
    relationship_section = ""
    if table_type:
        if table_type.lower() == 'fact':
            relationship_section = """

TABLE RELATIONSHIP REQUIREMENTS (FACT TABLE):

Table Type: FACT (Transactional/Event Data)

As a FACT table, you MUST:
- Include ENTITY_ID (NUMBER) as primary key
- Include foreign keys to dimension tables: user_id, product_id, customer_id, date_id, etc.
- Include numeric metrics: amounts, counts, durations, scores, percentages, rates
- Include timestamp/date for time-series analysis (e.g., TRANSACTION_DATE, EVENT_TIMESTAMP)
- Example: FACT_ORDERS needs ORDER_ID, CUSTOMER_ID (FK), PRODUCT_ID (FK), ORDER_AMOUNT, ORDER_DATE

FOREIGN KEY CONSISTENCY:
- If dimension tables have USER_ID, use USER_ID (not CUSTOMER_ID or PERSON_ID)
- Column names must MATCH exactly across tables for joins
- Use standard naming: {entity}_ID for foreign keys (e.g., CUSTOMER_ID, PRODUCT_ID)
- Include at least 2-3 foreign keys to support multi-table joins
"""
        elif table_type.lower() == 'dimension':
            relationship_section = """

TABLE RELATIONSHIP REQUIREMENTS (DIMENSION TABLE):

Table Type: DIMENSION (Descriptive/Lookup Data)

As a DIMENSION table, you MUST:
- Include ENTITY_ID (NUMBER) as primary key (for joining with fact tables)
- Include unique ID column: customer_id, product_id, user_id, etc. (beyond ENTITY_ID)
- Include descriptive attributes: names, categories, statuses, descriptions, types
- Include attributes for filtering and grouping (e.g., REGION, CATEGORY, STATUS, TIER)
- Example: DIM_CUSTOMERS needs CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_SEGMENT, REGION

FOREIGN KEY CONSISTENCY:
- If fact tables reference USER_ID, this table should have USER_ID as a unique identifier
- Column names must MATCH exactly across tables for joins
- Use standard naming: {entity}_ID for IDs (e.g., CUSTOMER_ID, PRODUCT_ID)
- Ensure ID column has sufficient cardinality for realistic "top N" queries (15+ unique values)
"""
    
    return f"""Generate a realistic database schema for a table named {table_name}.

Description: {table_description}
Company Context: {company_name}{target_questions_context}{mandatory_fields_section}{relationship_section}

ANALYSIS REQUIREMENTS (think through these internally, DO NOT write them out):

Internally analyze (DO NOT output this):
Step 1 - Extract field names from description
Step 2 - Check target questions for required columns  
Step 3 - Design complete schema

Then return ONLY the JSON schema - no explanations, no analysis text.

CRITICAL - Field Name Extraction from Description:
The table description above mentions SPECIFIC field names and their expected values. You MUST:
1. Parse the description carefully to identify all explicitly mentioned field names
2. Create columns with those EXACT field names (convert to UPPERCASE with underscores)
3. When the description mentions field values in parentheses, use those as sample_values
4. If the description says "including X, Y, Z", those become required columns

Examples of field extraction:
- "movement_type (receipt, usage, waste, transfer)" → column "MOVEMENT_TYPE" with sample_values: ["receipt", "usage", "waste", "transfer", ...]
- "including restaurant_id, supplier_id, product_id" → columns "RESTAURANT_ID", "SUPPLIER_ID", "PRODUCT_ID"
- "quality_score" → column "QUALITY_SCORE"

Requirements:
- First column should always be ENTITY_ID (NUMBER) as the primary key
- Include ALL mandatory fields listed above (if any) - these are non-negotiable
- Extract and include ALL field names explicitly mentioned in the table description
- Add 3-5 additional meaningful columns for enrichment beyond mandatory fields
- Use realistic column names and types (NUMBER, STRING, DATE, TIMESTAMP, BOOLEAN, FLOAT)
- Make it practical for analytics and business intelligence

SAMPLE VALUE REQUIREMENTS FOR REALISTIC DATA (Phase 1.2):

Categorical Fields (STRING columns):
- Use realistic, skewed distributions (NOT uniform 25% each)
- Follow 80/20 rule: 80% of records use 20% of categories
- Example GOOD: status → 60% "active", 25% "pending", 10% "inactive", 5% "error"
- Example BAD: status → 25% each (too uniform, questions about "most common" will fail)
- Provide 8-12 diverse sample values with natural frequency hints in parentheses
- Format: ["active (frequent)", "pending (common)", "inactive (rare)", "error (very rare)"]

Numeric Fields (NUMBER, FLOAT columns):
- Use realistic ranges with business correlation
- Example: "premium" tier → higher amounts (500-5000), "basic" tier → lower amounts (10-100)
- Example: "urgent" priority → faster times (1-24 hours), "low" priority → slower times (48-168 hours)
- Include outliers: 5% very high, 5% very low, 90% in normal range
- Provide sample values covering full range: min, 25th percentile, median, 75th percentile, max
- Format: [10, 50, 100, 250, 500, 1000, 5000] (represents realistic distribution)

Date/Timestamp Fields:
- Spread across realistic time period (last 18-24 months)
- Include temporal patterns: higher activity on weekdays, seasonality (Q4 spike for retail)
- Recent dates should have different patterns than old dates
- Provide 8-10 sample dates distributed across the range

CORRELATION RULES (CRITICAL):
- High-value customers → more frequent orders, higher amounts
- Recent dates → potentially different metrics (inflation, growth trends)
- Premium products → higher prices, lower quantities
- Ensure sample values reflect these natural correlations
- Add hints in descriptions: "correlated with X", "higher when Y"

For each column, provide 8-12 diverse and realistic sample values following above rules
- For mandatory fields with sample_values, include ALL those values plus 5-8 more variations
- When the description specifies values in parentheses, include ALL of them plus additional variations
- Ensure columns support the required dimensions, metrics, and analytical capabilities mentioned above
- IMPORTANT: Keep sample_values arrays CONCISE to avoid exceeding response length limits

CRITICAL: Ensure schema supports target questions (if provided):
- Questions about costs/prices → include cost/expense columns
- Questions about quality/performance → include score/rating columns  
- Questions aggregating "by X" → ensure column X exists for grouping
- Questions requiring joins → include appropriate foreign key columns

CRITICAL OUTPUT FORMAT - READ CAREFULLY:

You MUST return ONLY a JSON object. NO explanations, NO analysis, NO reasoning, NO text before or after.

GOOD Response (this is what you should do):
{{
  "columns": [
    {{"name": "ENTITY_ID", "type": "NUMBER", "description": "Unique identifier", "sample_values": [1, 2, 3, 4, 5, ...]}},
    {{"name": "FIELD_NAME", "type": "STRING", "description": "Description", "sample_values": ["val1", "val2", ...]}},
    {{"name": "ANOTHER_FIELD", "type": "NUMBER", "description": "Purpose", "sample_values": [10, 20, 30, ...]}}
  ]
}}

BAD Response (DO NOT do this):
"Looking at the description, I need to create..."
"**Pre-Generation Analysis:**"
"Step 1 - Extract Field Names: ..."

Just return the JSON immediately. Nothing else.

IMPORTANT REMINDERS:
- Honor field names from table description
- Provide 8-12 realistic sample_values per column (keep concise!)
- Include categorical values mentioned in description
- Support target questions if specified

Return the JSON now:"""


def get_collective_validation_prompt(
    tables_text: str,
    questions_text: str
) -> str:
    """
    Generate prompt for validating that tables collectively answer questions.
    
    Args:
        tables_text: Formatted description of all available tables
        questions_text: Formatted list of target questions
        
    Returns:
        Formatted prompt string for LLM
    """
    return f"""Validate if these tables TOGETHER can answer the target questions.

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


def get_single_table_validation_prompt(
    table_name: str,
    columns_text: str,
    sample_data_text: str,
    questions_text: str
) -> str:
    """
    Generate prompt for validating single table against questions.
    
    Args:
        table_name: Name of table being validated
        columns_text: Formatted description of table columns
        sample_data_text: Sample data from table
        questions_text: Formatted list of target questions
        
    Returns:
        Formatted prompt string for LLM
    """
    return f"""Validate if this SINGLE table can contribute to answering the target questions.

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


def get_unstructured_data_generation_prompt(
    table_name: str,
    table_description: str,
    company_name: str,
    num_chunks: int = 5
) -> tuple:
    """
    Generate prompt for creating unstructured text content.
    
    Args:
        table_name: Name of unstructured table
        table_description: Description of content type
        company_name: Company name for context
        num_chunks: Number of text chunks to generate
        
    Returns:
        Tuple of (prompt, content_type) where content_type is one of:
        'customer_feedback', 'technical_documentation', 'policy_documents', 
        'incident_reports', 'operational_procedures', 'unknown'
    """
    # Detect content type from description and provide specific examples
    desc_lower = table_description.lower()
    
    content_guidance = ""
    content_type = "unknown"
    
    if any(word in desc_lower for word in ['feedback', 'review', 'survey', 'comment', 'complaint']):
        content_type = "customer_feedback"
        content_guidance = """
CONTENT TYPE: Customer Feedback & Reviews

Generate realistic customer feedback including:
- Specific product/service complaints with details
- Feature requests with explanations
- Positive reviews mentioning specific aspects
- Survey responses with ratings and comments
- Issues with delivery, quality, functionality, pricing
- Suggestions for improvements

Example structure:
- "I ordered the [product] and was disappointed with [specific issue]. The [aspect] didn't work as expected..."
- "Great experience with [service]! Especially appreciated [specific feature]. However, [minor issue]..."
- "Survey Response: Rating 3/5. The [aspect] needs improvement because [reason]..."

Make feedback SPECIFIC and ACTIONABLE, not generic praise or complaints."""
    
    elif any(word in desc_lower for word in ['documentation', 'guide', 'manual', 'specification', 'technical']):
        content_type = "technical_documentation"
        content_guidance = """
CONTENT TYPE: Technical Documentation

Generate realistic technical documentation including:
- Implementation guides with step-by-step instructions
- API specifications with endpoints and parameters
- Configuration procedures with examples
- Best practices with rationale
- Troubleshooting guides with common issues
- Architecture documentation with diagrams descriptions

Example structure:
- "To implement [feature], follow these steps: 1) Configure [setting]... 2) Initialize [component]..."
- "API Endpoint: POST /api/v1/[resource]. Parameters: [param1] (required), [param2] (optional)..."
- "Best Practice: Always validate [input] before [action] to prevent [issue]..."

Make documentation PRACTICAL and DETAILED, not high-level marketing."""
    
    elif any(word in desc_lower for word in ['policy', 'compliance', 'regulation', 'procedure', 'legal']):
        content_type = "policy_documents"
        content_guidance = """
CONTENT TYPE: Policy & Compliance Documents

Generate realistic policy documents including:
- Compliance requirements with specific regulations
- Internal procedures with step-by-step workflows
- Risk mitigation guidelines
- Audit checklists
- Regulatory updates and changes
- Policy interpretation guidelines

Example structure:
- "Per regulation [code], all [entities] must [requirement]. Failure to comply may result in [consequence]..."
- "Procedure: When [event occurs], immediately: 1) [action]... 2) Document [details]... 3) Notify [stakeholder]..."
- "Compliance Checklist: ☐ Verify [item], ☐ Document [requirement], ☐ Submit [report]..."

Make policies SPECIFIC and ENFORCEABLE, not vague guidelines."""
    
    elif any(word in desc_lower for word in ['incident', 'report', 'issue', 'ticket']):
        content_type = "incident_reports"
        content_guidance = """
CONTENT TYPE: Incident Reports & Issues

Generate realistic incident reports including:
- Detailed problem descriptions
- Impact assessment and severity
- Root cause analysis
- Resolution steps taken
- Prevention recommendations
- Timeline of events

Example structure:
- "Incident Report #[ID]: [Date/Time] - [System] experienced [issue]. Impact: [affected users/services]. Root cause: [analysis]..."
- "Issue: Customer reported [problem] with [product]. Investigation revealed [finding]. Resolution: [steps taken]..."
- "Post-Mortem: [Event] occurred due to [cause]. Preventive actions: 1) [action]... 2) [action]..."

Make reports FACTUAL and ACTIONABLE, not superficial summaries."""
    
    elif any(word in desc_lower for word in ['operational', 'procedure', 'process', 'standard operating']):
        content_type = "operational_procedures"
        content_guidance = """
CONTENT TYPE: Operational Procedures

Generate realistic operational procedures including:
- Standard operating procedures (SOPs)
- Process documentation with step-by-step instructions
- Operational best practices
- Training materials
- Workflow guidelines
- Quality control procedures

Example structure:
- "Standard Operating Procedure: [Process Name]. Step 1) [action]... Step 2) [action]..."
- "Operational Best Practice: When [situation], always [action] to ensure [outcome]..."
- "Quality Control: Verify [criteria] meets [standard] before [next step]..."

Make procedures CLEAR and ACTIONABLE, not vague descriptions."""
    
    else:
        # Generic fallback
        content_type = "unknown"
        content_guidance = f"""
CONTENT TYPE: {table_description}

Generate realistic business content matching the description above.
Make content SPECIFIC, DETAILED, and REALISTIC - not generic marketing text.
Include concrete examples, data points, and actionable information."""
    
    prompt = f"""Generate {min(num_chunks, 5)} realistic text chunks for this unstructured data:

Table: {table_name}
Description: {table_description}
Company: {company_name}

{content_guidance}

CRITICAL REQUIREMENTS:
1. Each chunk should be 2-4 paragraphs of SPECIFIC, DETAILED content
2. Include concrete examples, numbers, names, dates where appropriate
3. Make content SEARCHABLE - use specific terminology that users would search for
4. Vary the content - don't repeat similar themes
5. Make it realistic for {company_name} in their industry context

Format as JSON array:
[
  {{"chunk_text": "Content here...", "document_type": "type", "source_system": "system"}}
]

Remember: Generate content that will actually be USEFUL when searched!"""
    
    return (prompt, content_type)


def get_table_relationships_analysis_prompt(
    demo_data: Dict,
    tables_text: str,
    questions_context: str = ""
) -> str:
    """
    Generate prompt for analyzing table relationships and join patterns.
    
    Args:
        demo_data: Demo configuration dictionary
        tables_text: Formatted description of all tables
        questions_context: Optional context about target questions
        
    Returns:
        Formatted prompt string for LLM
    """
    return f"""You are a data architect analyzing a data model for a demo scenario.

## Demo Context:
**Title:** {demo_data.get('title', 'N/A')}
**Description:** {demo_data.get('description', 'N/A')}

## Tables in the Data Model:
{tables_text}

## Technical Details:
- All structured tables have ENTITY_ID as the primary key
- Tables are designed with 70% join overlap on ENTITY_ID
- This is a star or snowflake schema pattern with fact and dimension tables
{questions_context}

## Your Task:
Analyze this data model and provide:

1. **Model Type**: Identify if this is a Star Schema, Snowflake Schema, or other pattern
2. **Relationships**: Describe how each table connects to others (which are fact tables, which are dimensions, what joins are used)
3. **Join Paths**: Provide 2-3 example SQL JOIN queries showing how to combine tables for analysis
4. **Question Mapping**: For each target question (if provided), explain which tables and joins are needed

Return ONLY a JSON object with this structure:
{{
  "model_type": "Star Schema" or "Snowflake Schema",
  "relationships": [
    "Fact table X connects to Dimension table Y via ENTITY_ID for detailed analysis",
    "Dimension table A provides descriptive attributes for entities in fact tables"
  ],
  "join_paths": [
    {{"description": "Purpose of join", "sql": "SELECT ... FROM ... JOIN ... ON ..."}}
  ],
  "insights": [
    "This model enables X type of analysis",
    "Users can correlate Y with Z across tables"
  ]
}}"""


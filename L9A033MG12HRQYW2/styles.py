"""
UI styling and HTML template generation for SI Data Generator.

This module provides CSS styles and HTML template functions for consistent
UI rendering throughout the application, including step progress indicators,
cards, and information boxes.
"""

import streamlit as st
from typing import List, Dict


def get_main_css() -> str:
    """
    Return main application CSS styles.
    
    Provides comprehensive styling for the Snowflake Intelligence Data
    Generator application including responsive design, animations, and
    brand-consistent colors.
    
    Returns:
        CSS style string to be injected via st.markdown
    """
    return """
<style>
    /* Main container styling */
    .main > div {
        padding-top: 2rem;
        overflow-x: hidden;
    }
    
    /* Step progress indicator */
    .step-progress {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 24px;
        background: linear-gradient(135deg, #056fb7 0%, #29B5E8 100%);
        color: white;
        border-radius: 12px;
        margin-bottom: 32px;
        gap: 0;
    }
    
    .step-item {
        display: flex;
        flex-direction: column;
        align-items: center;
        min-width: 120px;
    }
    
    .step-number {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        background: rgba(255,255,255,0.2);
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: bold;
        font-size: 1.2rem;
        margin-bottom: 8px;
        color: rgba(255,255,255,0.6);
        transition: all 0.3s ease;
    }
    
    .step-number.active {
        background: white;
        color: #056fb7;
        width: 50px;
        height: 50px;
        font-size: 1.5rem;
        box-shadow: 0 0 20px rgba(255,255,255,0.8), 
                    0 0 40px rgba(255,255,255,0.4);
        transform: scale(1.1);
    }
    
    .step-number.completed {
        background: rgba(14,165,233,0.8);
        color: white;
    }
    
    .step-label {
        font-size: 0.9rem;
        text-align: center;
        color: rgba(255,255,255,0.7);
        transition: all 0.3s ease;
    }
    
    .step-item:has(.step-number.active) .step-label {
        font-weight: bold;
        font-size: 1rem;
        color: white;
    }
    
    .step-item:has(.step-number.completed) .step-label {
        color: white;
    }
    
    .step-connector {
        width: 100px;
        height: 2px;
        background: rgba(255,255,255,0.3);
        margin: 0 8px 28px 8px;
        flex-shrink: 0;
    }
    
    /* Card styling */
    .demo-card {
        border: 2px solid #d1d5db;
        border-radius: 12px;
        padding: 24px;
        background: white;
        transition: all 0.3s ease;
        min-height: 480px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        display: flex;
        flex-direction: column;
    }
    
    .demo-card:hover {
        border-color: #29B5E8;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.3);
        transform: translateY(-2px);
    }
    
    .demo-card-selected {
        border: 3px solid #29B5E8;
        background: #EBF8FF;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.3);
    }
    
    /* Progress indicators */
    .progress-item {
        padding: 16px;
        margin: 8px 0;
        border-left: 4px solid #29B5E8;
        background: #f8f9fa;
        border-radius: 4px;
        transition: all 0.3s ease;
    }
    
    .progress-item.success {
        border-left-color: #0EA5E9;
        background: #d4edda;
    }
    
    .progress-item.error {
        border-left-color: #DC3545;
        background: #f8d7da;
    }
    
    /* Info boxes */
    .info-box {
        background: linear-gradient(135deg, #056fb7 0%, #29B5E8 100%);
        color: white;
        padding: 16px;
        border-radius: 8px;
        margin: 8px 0;
    }
    
    .success-box {
        background: linear-gradient(135deg, #056fb7 0%, #29B5E8 100%);
        color: white;
        padding: 24px;
        border-radius: 10px;
        margin: 16px 0;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .step-progress {
            flex-direction: column;
        }
        .step-connector {
            display: none;
        }
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Button styling */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        padding: 8px 32px;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    /* Page header styling */
    .page-header {
        text-align: center;
        padding: 0rem 0;
    }
    
    .page-title {
        color: #29B5E8;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    
    .page-subtitle {
        color: #666;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* Selection box styling */
    .selection-box {
        background: #EBF8FF;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #29B5E8;
    }
    
    /* About page styling */
    .about-hero {
        background: linear-gradient(135deg, #056fb7 0%, #29B5E8 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 2rem;
    }
    
    .about-hero h3 {
        color: white;
        margin-top: 0;
    }
    
    .about-hero p {
        color: white;
        font-size: 1rem;
        line-height: 1.6;
    }
    
    /* Footer styling */
    .page-footer {
        text-align: center;
        color: #666;
        padding: 2rem 0;
        border-top: 1px solid #e0e0e0;
    }
    
    /* Loading spinner */
    .loading-spinner {
        display: inline-block;
        width: 20px;
        height: 20px;
        border: 3px solid rgba(255,255,255,0.3);
        border-top: 3px solid white;
        border-radius: 50%;
        animation: spin 1s linear infinite;
    }
    
    /* Results table styling */
    .results-table-item {
        font-size: 0.875rem;
        color: #6b7280;
    }
    
    .results-list {
        margin: 0;
        padding-left: 1.5rem;
        line-height: 1.8;
    }
    
    .results-list li {
        color: #374151;
    }
    
    /* Query card sections */
    .query-section {
        margin-bottom: 2rem;
    }
    
    .query-section-header {
        margin-bottom: 0.5rem;
        font-size: 1.25rem;
    }
    
    .query-section-desc {
        font-size: 0.875rem;
        color: #6b7280;
        font-style: italic;
        margin-bottom: 1rem;
    }
    
    .query-section ol li {
        color: #374151;
    }
    
    .query-section.analyst .query-section-header {
        color: #0ea5e9;
    }
    
    .query-section.search .query-section-header {
        color: #8b5cf6;
    }
    
    .query-section.intelligence .query-section-header {
        color: #06b6d4;
    }
    
    /* Difficulty badges */
    .difficulty-badge {
        font-weight: bold;
    }
    
    .difficulty-badge.basic {
        color: #10b981;
    }
    
    .difficulty-badge.intermediate {
        color: #f59e0b;
    }
    
    .difficulty-badge.advanced {
        color: #ef4444;
    }
    
    /* Value card with custom height */
    .value-card-full {
        max-height: none !important;
    }
    
    .value-card-content-full {
        max-height: none !important;
        overflow-y: visible !important;
    }
    
    /* Override default value-card max-height for query results */
    .value-card.query-results-card {
        max-height: none !important;
    }
    
    .value-card.query-results-card .value-card-content {
        max-height: none !important;
        overflow-y: visible !important;
    }
    
    /* Coverage section styling */
    .coverage-intro {
        margin-bottom: 1rem;
        color: #6b7280;
        font-size: 0.95rem;
    }
    
    .coverage-question-box {
        margin: 1.5rem 0;
        padding: 1rem;
        background: #f9fafb;
        border-left: 4px solid #29B5E8;
        border-radius: 4px;
    }
    
    .coverage-question-title {
        font-weight: 600;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    
    .coverage-feedback {
        margin-top: 0.75rem;
        font-size: 0.9rem;
    }
    
    .coverage-feedback-table {
        color: #056fb7;
        font-weight: bold;
    }
    
    .coverage-feedback-item {
        margin-left: 1rem;
        color: #374151;
    }
    
    .coverage-note {
        margin-top: 1rem;
        padding: 1rem;
        background: #ecfdf5;
        border-radius: 8px;
        border: 1px solid #a7f3d0;
    }
    
    .coverage-note-title {
        color: #065f46;
        font-weight: 500;
    }
    
    .coverage-note-text {
        color: #047857;
        font-size: 0.9rem;
        margin-top: 0.5rem;
    }
    
    /* Infrastructure cards */
    .infra-card {
        background: white;
        border: 2px solid #e5e7eb;
        border-radius: 12px;
        padding: 1.5rem;
        height: 100%;
        min-height: 350px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
    }
    
    .infra-card:hover {
        border-color: #29B5E8;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.2);
        transform: translateY(-2px);
    }
    
    .infra-card h3 {
        color: #056fb7;
        font-size: 1.1rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    .infra-stat {
        display: flex;
        align-items: center;
        padding: 0.75rem 0;
        border-bottom: 1px solid #f3f4f6;
    }
    
    .infra-stat:last-child {
        border-bottom: none;
    }
    
    .infra-stat-icon {
        font-size: 1.5rem;
        margin-right: 1rem;
        min-width: 40px;
        text-align: center;
    }
    
    .infra-stat-content {
        flex: 1;
    }
    
    .infra-stat-title {
        font-weight: 600;
        color: #1f2937;
        margin-bottom: 0.25rem;
    }
    
    .infra-stat-desc {
        font-size: 0.875rem;
        color: #6b7280;
    }
    
    /* Value cards */
    .value-card {
        background: white;
        border: 2px solid #e5e7eb;
        border-radius: 12px;
        padding: 1.5rem;
        height: 100%;
        min-height: 350px;
        max-height: 500px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
        overflow: hidden;
    }
    
    .value-card:hover {
        border-color: #29B5E8;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.2);
    }
    
    .value-card h2 {
        color: #056fb7;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #29B5E8;
        padding-bottom: 0.5rem;
        flex-shrink: 0;
    }
    
    .value-card-content {
        flex: 1;
        overflow-y: auto;
        padding-right: 0.5rem;
    }
    
    .value-card-content::-webkit-scrollbar {
        width: 6px;
    }
    
    .value-card-content::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 3px;
    }
    
    .value-card-content::-webkit-scrollbar-thumb {
        background: #29B5E8;
        border-radius: 3px;
    }
    
    .value-card-content::-webkit-scrollbar-thumb:hover {
        background: #056fb7;
    }
    
    /* Demo flow cards */
    .demo-flow-card {
        background: linear-gradient(135deg, #056fb7 0%, #29B5E8 100%);
        border-radius: 12px;
        padding: 1.5rem;
        color: white;
        display: flex;
        flex-direction: column;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        height: 100%;
        min-height: 100%;
    }
    
    .demo-flow-card h3 {
        color: white;
        margin-top: 0;
        margin-bottom: 1rem;
        font-size: 1.2rem;
        font-weight: 600;
    }
    
    .demo-flow-card p {
        color: white;
        font-size: 0.95rem;
        line-height: 1.6;
        margin-bottom: 0;
    }
    
    /* Equal height card containers */
    [data-testid="stHorizontalBlock"] {
        display: flex !important;
        align-items: stretch !important;
        gap: 1rem;
    }
    
    [data-testid="stHorizontalBlock"] > div {
        display: flex !important;
        flex: 1 !important;
    }
    
    [data-testid="stHorizontalBlock"] > div > div {
        width: 100% !important;
    }
    
    /* Demo flow card additional styles */
    .demo-flow-card .ask-text {
        color: #ffd700;
        font-weight: bold;
        margin-bottom: 1rem;
        font-style: italic;
        flex-shrink: 0;
        word-wrap: break-word;
    }
    
    .demo-flow-card .check-item {
        color: white;
        margin-bottom: 0.5rem;
        padding-left: 0;
        word-wrap: break-word;
    }
    
    .demo-flow-card .check-item::before {
        content: '✅ ';
    }
    
    /* Ensure equal height columns */
    [data-testid="column"] {
        display: flex;
        flex-direction: column;
    }
    
    [data-testid="column"] > div {
        height: 100%;
    }
    
    .value-card-content p,
    .value-card-content ol,
    .value-card-content ul {
        margin-bottom: 0.5rem;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .infra-card, .value-card {
            margin-bottom: 1rem;
        }
        .demo-flow-card {
            min-height: auto;
            margin-bottom: 1rem;
        }
        [data-testid="stHorizontalBlock"] {
            flex-direction: column;
        }
    }
    
    /* History page styles */
    .history-container {
        max-width: 1200px;
        margin: 0 auto;
        padding: 1rem;
    }
    
    .history-card {
        background: white;
        border: 2px solid #e5e7eb;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }
    
    .history-card:hover {
        border-color: #29B5E8;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.2);
        transform: translateY(-2px);
    }
    
    .history-header {
        display: flex;
        justify-content: space-between;
        align-items: start;
        margin-bottom: 1rem;
        padding-bottom: 1rem;
        border-bottom: 2px solid #f3f4f6;
    }
    
    .history-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #056fb7;
        margin-bottom: 0.5rem;
    }
    
    .history-timestamp {
        font-size: 0.875rem;
        color: #6b7280;
        font-style: italic;
    }
    
    .history-company {
        font-size: 1rem;
        color: #1f2937;
        font-weight: 500;
        margin-bottom: 0.25rem;
    }
    
    .history-details {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .history-detail-item {
        display: flex;
        flex-direction: column;
    }
    
    .history-detail-label {
        font-size: 0.75rem;
        color: #6b7280;
        text-transform: uppercase;
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    
    .history-detail-value {
        font-size: 0.95rem;
        color: #1f2937;
        font-weight: 500;
    }
    
    .history-badges {
        display: flex;
        gap: 0.5rem;
        flex-wrap: wrap;
        margin: 1rem 0;
    }
    
    .history-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
    }
    
    .history-badge.enabled {
        background: #d1fae5;
        color: #065f46;
    }
    
    .history-badge.disabled {
        background: #f3f4f6;
        color: #6b7280;
    }
    
    .history-badge.advanced {
        background: #fef3c7;
        color: #92400e;
    }
    
    .history-actions {
        display: flex;
        gap: 0.5rem;
        margin-top: 1rem;
    }
    
    .history-section {
        margin: 1rem 0;
        padding: 1rem;
        background: #f9fafb;
        border-radius: 8px;
    }
    
    .history-section-title {
        font-size: 0.875rem;
        font-weight: 600;
        color: #374151;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
    }
    
    .history-list {
        list-style: none;
        padding-left: 0;
        margin: 0;
    }
    
    .history-list li {
        padding: 0.25rem 0;
        color: #4b5563;
        font-size: 0.9rem;
    }
    
    .history-list li::before {
        content: '▸ ';
        color: #29B5E8;
        font-weight: bold;
    }
    
    .history-empty {
        text-align: center;
        padding: 3rem 1rem;
        color: #6b7280;
    }
    
    .history-empty-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }
    
    .history-pagination {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 1rem;
        margin: 2rem 0;
    }
    
    .export-button {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        border: none;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    
    .export-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
    }
    
    .reuse-button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        border: none;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    
    .reuse-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
    
    /* History page - darker text for disabled inputs */
    input:disabled, textarea:disabled {
        color: #1f2937 !important;
        opacity: 1 !important;
        -webkit-text-fill-color: #1f2937 !important;
    }
    
    /* Fix card overflow issues */
    .infra-card {
        overflow: auto;
        word-wrap: break-word;
    }
    
    .infra-stat-title, .infra-stat-desc {
        word-wrap: break-word;
        overflow-wrap: break-word;
        max-width: 100%;
    }
    
    .results-list {
        padding-left: 1.5rem;
        margin: 0;
    }
    
    .results-list li {
        word-wrap: break-word;
        overflow-wrap: break-word;
        max-width: 100%;
        line-height: 1.8;
    }
    
    .results-list li strong {
        display: inline-block;
        max-width: 100%;
        word-break: break-all;
    }
</style>
"""


def show_step_progress(current_step: int):
    """
    Display step progress indicator with visual feedback.
    
    Shows a multi-step progress indicator with completed, active, and
    pending states. Steps are connected with visual connectors.
    
    Args:
        current_step: Current step number (1-4)
    """
    steps = [
        {"num": 1, "label": "Customer Info"},
        {"num": 2, "label": "Select Demo"},
        {"num": 3, "label": "Configure"},
        {"num": 4, "label": "Generate"}
    ]
    
    html = '<div class="step-progress">'
    for i, step in enumerate(steps):
        status_class = ""
        if step["num"] < current_step:
            status_class = "completed"
        elif step["num"] == current_step:
            status_class = "active"
        
        # Add step item
        html += (
            f'<div class="step-item">'
            f'<div class="step-number {status_class}">{step["num"]}</div>'
            f'<div class="step-label">{step["label"]}</div>'
            f'</div>'
        )
        
        # Add connector AFTER step item (not inside it)
        if i < len(steps) - 1:
            html += '<div class="step-connector"></div>'
    
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_info_box(content: str):
    """
    Render an informational box with Snowflake styling.
    
    Args:
        content: HTML or text content to display in the box
    """
    html = f'<div class="info-box">{content}</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_success_box(content: str):
    """
    Render a success/completion box with Snowflake styling.
    
    Args:
        content: HTML or text content to display in the box
    """
    html = f'<div class="success-box">{content}</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_header(title: str, subtitle: str = ""):
    """
    Render application header with title and optional subtitle.
    
    Args:
        title: Main title text
        subtitle: Optional subtitle text
    """
    html = f"""
    <div class='page-header'>
        <h1 class='page-title'>
            ❄️ {title}
        </h1>
    """
    
    if subtitle:
        html += f"""
        <p class='page-subtitle'>
            {subtitle}
        </p>
        """
    
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_step_container(step_text: str):
    """
    Render a step container header.
    
    Args:
        step_text: Text to display in the step container
    """
    html = f"<div class='step-container'>{step_text}</div>"
    st.markdown(html, unsafe_allow_html=True)


def apply_main_styles():
    """
    Apply main application CSS styles.
    
    Should be called once at the beginning of the application to inject
    all necessary styles into the Streamlit app.
    """
    st.markdown(get_main_css(), unsafe_allow_html=True)


def render_selection_box(content: str):
    """
    Render a selection/info box with blue styling.
    
    Args:
        content: HTML or text content to display in the box
    """
    html = f'<div class="selection-box">{content}</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_about_hero(title: str, content: str):
    """
    Render the about page hero section.
    
    Args:
        title: Hero section title
        content: Hero section content text
    """
    html = f"""
    <div class="about-hero">
        <h3>{title}</h3>
        <p>{content}</p>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_page_footer(text: str):
    """
    Render page footer.
    
    Args:
        text: Footer text content
    """
    html = f'<div class="page-footer"><p>{text}</p></div>'
    st.markdown(html, unsafe_allow_html=True)


def render_loading_info(text: str):
    """
    Render info box with loading spinner.
    
    Args:
        text: Text to display next to spinner
    """
    html = f"""
    <div class='info-box' style='display: flex; align-items: center; gap: 12px;'>
        <div class='loading-spinner'></div>
        <span>{text}</span>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_demo_header(company_name: str, demo_title: str):
    """
    Render demo generation header.
    
    Args:
        company_name: Name of the company
        demo_title: Title of the demo
    """
    html = f"<h2 style='margin: 0; color: white;'>🎯 {company_name} Demo: {demo_title}</h2>"
    st.markdown(html, unsafe_allow_html=True)


def render_results_table_list(tables: List[Dict]) -> str:
    """
    Render list of created tables with descriptions.
    
    Args:
        tables: List of table dictionaries with 'table', 'type', and 'description' keys
        
    Returns:
        HTML string for the table list
    """
    items = []
    for table in tables:
        if table.get('type') == 'structured':
            desc = table.get('description', '')
            items.append(f"<li><strong>{table['table']}</strong><br/><span class='results-table-item'>{desc}</span></li>")
        elif table.get('type') == 'unstructured':
            # Show actual description, not generic placeholder!
            desc = table.get('description', 'Searchable text content for knowledge retrieval')
            items.append(f"<li><strong>{table['table']}</strong><br/><span class='results-table-item'>{desc}</span></li>")
    
    html = f"<ul class='results-list'>{''.join(items)}</ul>"
    return html


def render_query_results(analytics_questions: List[str], search_questions: List[str], intelligence_questions: Dict[str, List[str]]):
    """
    Render the unified query results card with all three sections.
    
    Args:
        analytics_questions: List of Cortex Analyst questions
        search_questions: List of Cortex Search questions
        intelligence_questions: Dict with 'basic', 'intermediate', 'advanced' question lists
    """
    html = "<div class='value-card query-results-card'><div class='value-card-content'>"
    
    # Cortex Analyst Section
    html += "<div class='query-section analyst'>"
    html += "<h3 class='query-section-header'>🔍 Cortex Analyst</h3>"
    html += "<div class='query-section-desc'>Natural language to SQL queries</div>"
    if analytics_questions:
        html += "<ol class='results-list'>"
        for q_text in analytics_questions[:3]:
            html += f'<li>"{q_text}"</li>'
        html += "</ol>"
    html += "</div>"
    
    # Cortex Search Section
    html += "<div class='query-section search'>"
    html += "<h3 class='query-section-header'>🔎 Cortex Search</h3>"
    html += "<div class='query-section-desc'>Semantic search for unstructured data</div>"
    if search_questions:
        html += "<ol class='results-list'>"
        for q_text in search_questions[:3]:
            html += f'<li>"{q_text}"</li>'
        html += "</ol>"
    html += "</div>"
    
    # Snowflake Intelligence Section
    html += "<div class='query-section intelligence'>"
    html += "<h3 class='query-section-header'>🤖 Snowflake Intelligence</h3>"
    html += "<div class='query-section-desc'>Multi-tool AI agent orchestration</div>"
    
    for difficulty, label, emoji in [('basic', 'Basic', '🟢'), ('intermediate', 'Intermediate', '🟡'), ('advanced', 'Advanced', '🔴')]:
        questions = intelligence_questions.get(difficulty, [])
        if questions:
            html += f"<div style='margin-bottom: 1rem;'><strong class='difficulty-badge {difficulty}'>{emoji} {label}:</strong>"
            html += "<ol style='margin: 0.5rem 0 0 1.5rem; padding-left: 0; line-height: 1.8;'>"
            # Show all questions for this difficulty level (no artificial limit)
            for q_text in questions:
                html += f'<li>"{q_text}"</li>'
            html += "</ol></div>"
    
    html += "</div></div></div>"
    st.markdown(html, unsafe_allow_html=True)


def render_infrastructure_results(
    results,
    selected_demo,
    company_name,
    schema_name,
    session
):
    """
    Render complete infrastructure results display including:
    - Demo header
    - Infrastructure summary
    - Tables created
    - Example queries
    - Target questions coverage
    - Demo flow
    - Next steps
    
    Args:
        results: List of result dictionaries from infrastructure creation
        selected_demo: Selected demo configuration dict
        company_name: Company name string
        schema_name: Schema name string
        session: Snowflake session for URL generation
    """
    import streamlit as st
    
    try:
        # Use results from session state that was stored during infrastructure creation
        # This is the same data source the debug expander uses
        saved_results = st.session_state.get('infrastructure_results', results)
        
        # If still empty, try the debug_results backup
        if not saved_results or len(saved_results) == 0:
            saved_results = st.session_state.get('debug_results', results)
        
        # If STILL empty, try getting from results parameter directly
        if not saved_results or len(saved_results) == 0:
            saved_results = results if results else []
        
        # Separate different types of objects for display with defensive coding
        semantic_views = [t for t in saved_results if isinstance(t, dict) and t.get('type') == 'semantic_view']
        search_services = [t for t in saved_results if isinstance(t, dict) and t.get('type') == 'search_service']
        agents = [t for t in saved_results if isinstance(t, dict) and t.get('type') == 'agent']
        structured_tables = [t for t in saved_results if isinstance(t, dict) and t.get('type') == 'structured']
        unstructured_tables = [t for t in saved_results if isinstance(t, dict) and t.get('type') == 'unstructured']
        
        # Combine structured and unstructured for record count
        regular_tables = structured_tables + unstructured_tables
        
        total_records = sum(t.get('records', 0) for t in regular_tables if isinstance(t.get('records'), int))
        
        # Display demo header in blue success box
        st.markdown(f"""
        <div class='success-box'>
            <h2 style='margin: 0; color: white;'>🎯 {company_name} Demo: {selected_demo.get('title', 'Demo')}</h2>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Infrastructure Created - Card Layout
        st.markdown("## 📊 Infrastructure Created")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Build semantic view stat HTML
            semantic_stat = f"""
            <div class='infra-stat'>
                <div class='infra-stat-icon'>🔗</div>
                <div class='infra-stat-content'>
                    <div class='infra-stat-title'>{len(semantic_views)} Semantic View</div>
                    <div class='infra-stat-desc'>AI-ready data relationships</div>
                </div>
            </div>
            """ if semantic_views else ""
            
            # Build search service stat HTML
            search_stat = f"""
            <div class='infra-stat'>
                <div class='infra-stat-icon'>🔍</div>
                <div class='infra-stat-content'>
                    <div class='infra-stat-title'>{len(search_services)} Cortex Search Service</div>
                    <div class='infra-stat-desc'>Intelligent document retrieval</div>
                </div>
            </div>
            """ if search_services else ""
            
            # Build agent stat HTML
            agent_stat = f"""
            <div class='infra-stat'>
                <div class='infra-stat-icon'>🤖</div>
                <div class='infra-stat-content'>
                    <div class='infra-stat-title'>{len(agents)} AI Agent</div>
                    <div class='infra-stat-desc'>Automated tools and capabilities</div>
                </div>
            </div>
            """ if agents else ""
            
            st.markdown(f"""
            <div class='infra-card'>
                <h3>🏗️ Infrastructure Summary</h3>
                <div class='infra-stat'>
                    <div class='infra-stat-icon'>📊</div>
                    <div class='infra-stat-content'>
                        <div class='infra-stat-title'>{len(structured_tables)} Structured Tables</div>
                        <div class='infra-stat-desc'>{total_records:,} records with ENTITY_ID PRIMARY KEY and 70% join overlap</div>
                    </div>
                </div>
                <div class='infra-stat'>
                    <div class='infra-stat-icon'>📄</div>
                    <div class='infra-stat-content'>
                        <div class='infra-stat-title'>{len(unstructured_tables)} Unstructured Table</div>
                        <div class='infra-stat-desc'>Text chunks for semantic search</div>
                    </div>
                </div>
                {semantic_stat}
                {search_stat}
                {agent_stat}
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            # Prepare tables for rendering (with truncation)
            try:
                tables_for_render = []
                for table in structured_tables:
                    if isinstance(table, dict):
                        table_copy = table.copy()
                        desc = table.get('description', '')
                        if len(desc) > 150:
                            table_copy['description'] = desc[:150] + "..."
                        tables_for_render.append(table_copy)
                
                for table in unstructured_tables:
                    if isinstance(table, dict):
                        tables_for_render.append(table)
                
                tables_html = render_results_table_list(tables_for_render)
                st.markdown(f"""
                <div class='infra-card'>
                    <h3>📋 Tables Created</h3>
                    {tables_html}
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error displaying tables: {str(e)}")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Single unified card for all example queries
        st.markdown("## 📋 Example Queries")
        
        # Get all generated questions
        all_questions = st.session_state.get('generated_questions', [])
        
        # Organize questions by category
        analytics_questions = []
        search_questions = []
        intelligence_questions = {'basic': [], 'intermediate': [], 'advanced': []}
        
        for q in all_questions:
            if not isinstance(q, dict):
                continue
                
            category = q.get('category', '')
            difficulty = q.get('difficulty', 'basic')
            text = q.get('text', '')
            
            if not text:
                continue
            
            if category == 'analytics':
                analytics_questions.append(text)
                # Also use for intelligence section
                if difficulty in intelligence_questions:
                    intelligence_questions[difficulty].append(text)
            elif category == 'search':
                search_questions.append(text)
                # Also use for intelligence section
                if difficulty in intelligence_questions:
                    intelligence_questions[difficulty].append(text)
        
        # Use the helper function to render query results
        render_query_results(analytics_questions, search_questions, intelligence_questions)
        
        # Target Questions Coverage Section
        if 'validation_results' in st.session_state and 'target_questions_for_display' in st.session_state:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("## 🎯 Target Questions Coverage")
            
            target_questions = st.session_state['target_questions_for_display']
            validation_results = st.session_state['validation_results']
            
            # Build coverage display
            coverage_html = "<div class='value-card'><div class='value-card-content'>"
            coverage_html += "<div class='coverage-intro'>"
            coverage_html += f"The generated data has been validated against your {len(target_questions)} target question(s):"
            coverage_html += "</div>"
            
            for idx, question in enumerate(target_questions, 1):
                coverage_html += "<div class='coverage-question-box'>"
                coverage_html += f"<div class='coverage-question-title'>Question {idx}: {question}</div>"
                
                # Show validation feedback for each table
                for table_name, validation_info in validation_results.items():
                    feedback = validation_info.get('feedback', '')
                    if feedback and feedback != "No target questions to validate":
                        coverage_html += "<div class='coverage-feedback'>"
                        coverage_html += f"<strong class='coverage-feedback-table'>{table_name}:</strong><br>"
                        # Convert feedback to HTML-safe format
                        feedback_lines = feedback.split('\n')
                        for line in feedback_lines:
                            if line.strip():
                                coverage_html += f"<div class='coverage-feedback-item'>{line}</div>"
                        coverage_html += "</div>"
                
                coverage_html += "</div>"
            
            coverage_html += "<div class='coverage-note'>"
            coverage_html += "<div class='coverage-note-title'>💡 Note:</div>"
            coverage_html += "<div class='coverage-note-text'>"
            coverage_html += "The data has been generated with your target questions in mind. You can now test these questions using Cortex Analyst or Snowflake Intelligence."
            coverage_html += "</div></div>"
            
            coverage_html += "</div></div>"
            
            st.markdown(coverage_html, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Demo flow with equal-height cards
        st.markdown("## 🚀 Step-by-Step Demo Flow")
        
        # Get generated questions for demo flow
        questions = st.session_state.get('generated_questions', [])
        analytics_questions_list = [q for q in questions if isinstance(q, dict) and q.get('category') == 'analytics']
        search_questions_list = [q for q in questions if isinstance(q, dict) and q.get('category') == 'search']
        
        # Get specific questions for each step with fallbacks
        step1_question = analytics_questions_list[0].get('text', "What are the top 5 performing entities and their key metrics?") if len(analytics_questions_list) > 0 else "What are the top 5 performing entities and their key metrics?"
        step2_question = analytics_questions_list[1].get('text', "What could be the reasons for these performance differences?") if len(analytics_questions_list) > 1 else "What could be the reasons for these performance differences?"
        step3_question = search_questions_list[0].get('text', "Find relevant best practices or recommendations") if len(search_questions_list) > 0 else "Find relevant best practices or recommendations"
        
        flow_col1, flow_col2, flow_col3 = st.columns(3)
        
        with flow_col1:
            st.markdown(f"""
<div class="demo-flow-card">
    <h3>Step 1: Structured Data Analysis</h3>
    <p class="flow-desc">Start with Cortex Analyst to analyze your structured tables</p>
    <div class="flow-example">
        <strong>Example Question:</strong><br>
        "{step1_question}"
    </div>
    <div class="flow-action">Use Cortex Analyst or Snowflake Intelligence</div>
</div>
""", unsafe_allow_html=True)
        
        with flow_col2:
            st.markdown(f"""
<div class="demo-flow-card">
    <h3>Step 2: AI Reasoning Follow-up</h3>
    <p class="flow-desc">Let the AI agent provide deeper insights and reasoning</p>
    <div class="flow-example">
        <strong>Follow-up Question:</strong><br>
        "{step2_question}"
    </div>
    <div class="flow-action">Continue conversation with Snowflake Intelligence</div>
</div>
""", unsafe_allow_html=True)
        
        with flow_col3:
            st.markdown(f"""
<div class="demo-flow-card">
    <h3>Step 3: Unstructured Knowledge Retrieval</h3>
    <p class="flow-desc">Search unstructured content for supporting documentation</p>
    <div class="flow-example">
        <strong>Example Search:</strong><br>
        "{step3_question}"
    </div>
    <div class="flow-action">Use Cortex Search via Agent</div>
</div>
""", unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("### 🎯 Next Steps")
        
        # Get first structured table name for example query
        first_table = next((r['table'] for r in saved_results if isinstance(r, dict) and r.get('type') == 'structured'), 'TABLE')
        
        # Get account info for Snowflake Intelligence URL
        try:
            account_result = session.sql("SELECT CURRENT_ACCOUNT_NAME() as account_locator, CURRENT_ACCOUNT() as account_name").collect()
            if account_result:
                account_locator = account_result[0]['ACCOUNT_LOCATOR']
                account_name = account_result[0]['ACCOUNT_NAME']
                intelligence_url = f"https://ai.snowflake.com/{account_locator}/{account_name}/"
            else:
                intelligence_url = "https://ai.snowflake.com"
        except:
            intelligence_url = "https://ai.snowflake.com"
        
        # Three columns in one row: Snowflake Intelligence, Query Your Data, Try Cortex Analyst
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Snowflake Intelligence (leftmost) - with glow border
            st.markdown("""
            <div style='
                text-align: center; 
                padding: 1.5rem; 
                background: white; 
                border: 2px solid #29B5E8; 
                border-radius: 12px; 
                box-shadow: 0 0 20px rgba(41, 181, 232, 0.3);
                transition: all 0.3s ease;
                margin-bottom: 1rem;
            '>
                <h3 style='color: #056fb7; margin: 0 0 0.75rem 0; font-size: 1.4rem; border-bottom: 2px solid #29B5E8; padding-bottom: 0.5rem;'>🤖 Snowflake Intelligence</h3>
                <p style='color: #6b7280; margin-bottom: 0; font-size: 1rem;'>Launch your AI-powered agent to interact with your demo data</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.link_button(
                "🤖 Use Your Agent",
                intelligence_url,
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            # Query Your Data (middle)
            st.markdown(f"""
            <div style='
                text-align: center; 
                padding: 1.5rem; 
                background: white; 
                border: 2px solid #e5e7eb; 
                border-radius: 12px; 
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                transition: all 0.3s ease;
                margin-bottom: 1rem;
            '>
                <h3 style='color: #056fb7; margin: 0 0 0.75rem 0; font-size: 1.4rem; border-bottom: 2px solid #29B5E8; padding-bottom: 0.5rem;'>📊 Query Your Data</h3>
                <p style='color: #6b7280; margin-bottom: 0.5rem; font-size: 1rem;'>Run SQL queries on your demo tables</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Get first structured table name
            first_table_name = structured_tables[0].get('table', 'TABLE') if structured_tables else "TABLE"
            
            # st.code(f"""USE SCHEMA {schema_name};
# SELECT * FROM {first_table_name} LIMIT 10;""", language="sql")
        
        with col3:
            # Try Cortex Analyst (rightmost)
            st.markdown("""
            <div style='
                text-align: center; 
                padding: 1.5rem; 
                background: white; 
                border: 2px solid #e5e7eb; 
                border-radius: 12px; 
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                transition: all 0.3s ease;
                margin-bottom: 1rem;
            '>
                <h3 style='color: #056fb7; margin: 0 0 0.75rem 0; font-size: 1.4rem; border-bottom: 2px solid #29B5E8; padding-bottom: 0.5rem;'>💡 Try Cortex Analyst</h3>
                <p style='color: #6b7280; margin-bottom: 0; font-size: 0.95rem;'>
                    • Ask natural language questions<br>
                    • Analyze trends and patterns<br>
                    • Generate insights automatically
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            st.session_state['last_created_schema'] = schema_name
            st.session_state['last_results'] = results
            
            # Set flag for reset button (don't rerun to avoid losing state)
            st.session_state['reset_button_shown'] = True
    
    except Exception as e:
        st.error(f"❌ Error displaying infrastructure: {str(e)}")
        import traceback
        with st.expander("🔍 View Full Error"):
            st.code(traceback.format_exc())


def render_demo_selection_ui(demo_ideas: List[Dict], template_label: str = ""):
    """
    Render the demo selection UI with tabs for each demo idea.
    
    Args:
        demo_ideas: List of demo configuration dictionaries
        template_label: Optional label to show if using templates
    """
    st.markdown(f"<br><div class='step-container'>Step 2: Select Your Demo Scenario{template_label}</div>", unsafe_allow_html=True)
    
    # Determine if we should show target indicator
    has_target_questions = bool(st.session_state.get('target_questions', []))
    using_cortex_demos = not st.session_state.get('used_fallback_demos', False)
    show_target_indicator = has_target_questions and using_cortex_demos
    
    # Create tab labels
    tab_labels = []
    for i, demo in enumerate(demo_ideas):
        if i == 0 and show_target_indicator:
            tab_labels.append(f"🎯 Target : Demo {i+1}: {demo['title'].split(':')[0]}")
        else:
            tab_labels.append(f"Demo {i+1}: {demo['title'].split(':')[0]}")
    
    # Create tabs for each demo idea
    tabs = st.tabs(tab_labels)
    
    for idx, (tab, demo) in enumerate(zip(tabs, demo_ideas)):
        with tab:
            # Wrap entire demo content in a bordered container (card)
            with st.container(border=True):
                st.subheader(demo['title'])
                st.write(demo['description'])
                
                # Industry focus
                if 'industry_focus' in demo:
                    st.info(f"🏭 **Industry Focus:** {demo['industry_focus']}")
                
                # Business value
                if 'business_value' in demo:
                    st.info(f"💼 **Business Value:** {demo['business_value']}")
                
                # Target audience
                if 'target_audience' in demo:
                    st.info(f"👥 {demo['target_audience']}")
                
                # Customization
                if 'customization' in demo:
                    st.info(f"🎯 {demo['customization']}")
                
                st.write("**📊 Data Tables:**")
                
                # Check if standard mode (3 structured + 1 unstructured) for 2-column layout
                has_structured_3 = 'structured_3' in demo['tables']
                has_structured_4 = 'structured_4' in demo['tables']
                has_structured_5 = 'structured_5' in demo['tables']
                has_unstructured_2 = 'unstructured_2' in demo['tables']
                
                # Standard mode: exactly 3 structured tables, use 2 columns x 2 rows
                is_standard_mode = has_structured_3 and not has_structured_4 and not has_structured_5 and not has_unstructured_2
                
                if is_standard_mode:
                    # Row 1: structured_1, structured_2
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Structured Table 1**")
                        st.write(f"🏷️ **{demo['tables']['structured_1']['name']}**")
                        st.caption(demo['tables']['structured_1']['description'])
                        if 'purpose' in demo['tables']['structured_1']:
                            st.caption(f"💡 {demo['tables']['structured_1']['purpose']}")
                        if 'table_type' in demo['tables']['structured_1']:
                            st.caption(f"📁 Type: {demo['tables']['structured_1']['table_type'].title()}")
                    
                    with col2:
                        st.write("**Structured Table 2**")
                        st.write(f"🏷️ **{demo['tables']['structured_2']['name']}**")
                        st.caption(demo['tables']['structured_2']['description'])
                        if 'purpose' in demo['tables']['structured_2']:
                            st.caption(f"💡 {demo['tables']['structured_2']['purpose']}")
                        if 'table_type' in demo['tables']['structured_2']:
                            st.caption(f"📁 Type: {demo['tables']['structured_2']['table_type'].title()}")
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    
                    # Row 2: structured_3, unstructured
                    col3, col4 = st.columns(2)
                    
                    with col3:
                        st.write("**Structured Table 3**")
                        st.write(f"🏷️ **{demo['tables']['structured_3']['name']}**")
                        st.caption(demo['tables']['structured_3']['description'])
                        if 'purpose' in demo['tables']['structured_3']:
                            st.caption(f"💡 {demo['tables']['structured_3']['purpose']}")
                        if 'table_type' in demo['tables']['structured_3']:
                            st.caption(f"📁 Type: {demo['tables']['structured_3']['table_type'].title()}")
                    
                    with col4:
                        if 'unstructured' in demo['tables']:
                            st.write("**Unstructured Table**")
                            st.write(f"🏷️ **{demo['tables']['unstructured']['name']}**")
                            st.caption(demo['tables']['unstructured']['description'])
                            if 'purpose' in demo['tables']['unstructured']:
                                st.caption(f"💡 {demo['tables']['unstructured']['purpose']}")
                        else:
                            st.write("**No Unstructured Table**")
                            st.caption("This demo uses only structured tables")
                
                else:
                    # Advanced mode: 3 columns for first row
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write("**Structured Table 1**")
                        st.write(f"🏷️ **{demo['tables']['structured_1']['name']}**")
                        st.caption(demo['tables']['structured_1']['description'])
                        if 'purpose' in demo['tables']['structured_1']:
                            st.caption(f"💡 {demo['tables']['structured_1']['purpose']}")
                        if 'table_type' in demo['tables']['structured_1']:
                            st.caption(f"📁 Type: {demo['tables']['structured_1']['table_type'].title()}")
                    
                    with col2:
                        st.write("**Structured Table 2**")
                        st.write(f"🏷️ **{demo['tables']['structured_2']['name']}**")
                        st.caption(demo['tables']['structured_2']['description'])
                        if 'purpose' in demo['tables']['structured_2']:
                            st.caption(f"💡 {demo['tables']['structured_2']['purpose']}")
                        if 'table_type' in demo['tables']['structured_2']:
                            st.caption(f"📁 Type: {demo['tables']['structured_2']['table_type'].title()}")
                    
                    with col3:
                        if 'unstructured' in demo['tables']:
                            st.write("**Unstructured Table**")
                            st.write(f"🏷️ **{demo['tables']['unstructured']['name']}**")
                            st.caption(demo['tables']['unstructured']['description'])
                            if 'purpose' in demo['tables']['unstructured']:
                                st.caption(f"💡 {demo['tables']['unstructured']['purpose']}")
                        else:
                            st.write("**No Unstructured Table**")
                            st.caption("This demo uses only structured tables")
                    
                    # Check if there are additional tables (advanced mode)
                    additional_tables = []
                    for i in range(3, 6):  # Check for structured_3, structured_4, structured_5
                        table_key = f'structured_{i}'
                        if table_key in demo['tables']:
                            additional_tables.append((f"Structured Table {i}", table_key, demo['tables'][table_key], True))
                    
                    # Check for second unstructured table
                    if 'unstructured_2' in demo['tables']:
                        additional_tables.append(("Unstructured Table 2", "unstructured_2", demo['tables']['unstructured_2'], False))
                    
                    # Display additional tables in a second row if they exist
                    if additional_tables:
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Create columns based on number of additional tables
                        num_additional = len(additional_tables)
                        if num_additional == 1:
                            col_additional = st.columns([1, 2, 1])
                            cols = [col_additional[1]]
                        elif num_additional == 2:
                            cols = st.columns(2)
                        else:
                            cols = st.columns(3)
                        
                        for col_idx, (table_label, table_key, table_info, is_structured) in enumerate(additional_tables):
                            if col_idx < len(cols):  # Ensure we don't exceed column count
                                with cols[col_idx]:
                                    st.write(f"**{table_label}**")
                                    st.write(f"🏷️ **{table_info['name']}**")
                                    st.caption(table_info['description'])
                                    if 'purpose' in table_info:
                                        st.caption(f"💡 {table_info['purpose']}")
                                    if is_structured and 'table_type' in table_info:
                                        st.caption(f"📁 Type: {table_info['table_type'].title()}")
                
                st.markdown("<br>", unsafe_allow_html=True)
                
            # Select button for this demo
            if st.button(f"🚀 Select Demo {idx+1}", key=f"select_demo_{idx}", type="primary"):
                st.session_state['selected_demo_idx'] = idx
                st.session_state['selected_demo'] = demo
                # Reset infrastructure flags when selecting a new demo
                st.session_state['infrastructure_started'] = False
                st.session_state['infrastructure_complete'] = False
                st.success(f"✅ Selected: {demo['title']}")
                st.rerun()


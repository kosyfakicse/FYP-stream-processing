Theme: Streamlit default
Layout: Wide layout

Sidebar: Custom sidebar with different section
Section 1: Database Status (Online/Offline)

Section 2: Navigation (Home, Query)

Section 3: Settings

Section 4: About / System Info

Page 1: Overview (Home)
Top Row: 
    1. Total Window Outputs
    2. Most Hashtag
    3. Total Contributors

Middle Row: 
    1. Top 5 Trending Hashtags (Horizontal Bar Chart)
    X: Hashtags Y: Count

Bottom Row:
    1. Latest 5 Processed Windows
    Hashtag     | Count | Attribution          | Time Window

Page 2: Query
    1. Backward Tab
    2. Forwward Tab 
    3. Temporal Tab
    4. Flow Tab
    5. Versioning Tab
    Can be Switch at the Query Tab

Tab 1: Backward Query
Input: 
    1. Enter Output ID
    2. Search Button

Result:
    1. Hashtag
    2. Total Count
    3. Window Range (Window ID)
    4. Chart
    5. Raw Data

Tab 2: Forward Query
Input:
    1. Enter source ID
    2. Search Button

Result:
    1. Success Message
    2. Data Table: Output ID, Hashtag, Total Count, Attribution JSON, Time Window

Tab 3: Temporal Lineage Query
Input:
    1. Dropdown Menu: Select a Hashtag

Result:
    1. Line Chart: X: Time, Y: Total Count
    2. Data Table

Tab 4: Flow Lineage Query
Input:
    1. Dropdown Menu: Select Run ID

Result:
    1. Data Table: Source Vertex, Target Vertex, Total Quantity

Tab 5: Versioning Query
Input:
    1. Select a Hashtag
    2. Select Window 1
    3. Select Window 2

Result:
    1. Grouped Bar Chart
    2. Different Table


Page 3: Insert Data

Top section: Dataset Preparation
    Tab 1: Upload File
        Schema Validation: Follow the ['user_id', 'text', 'timestamp'] and not empty space

    Tab 2: System Generate with Parameters
        Parameter Controls: Number of Rows, Number of  Users, Target Hashtags, Time Interval

    Tab 3: Full Custom Editor
        Interactive Data Editor: Allow user to add row and delete row and change some data

Middle Section: Dataset Preview & Validation
    Summary Metrics: 
        Total Records
        Unique Users
        Time Span
        Data Preview Table

Bottom Section: Pipeline Execution & Run ID Tracking
    Action Bar: Run Mode Toggle(Mock/Live Backend), Send button
    Execution Result: 
        Success Banner and show the Run ID
        Pipeline Status Indicator(Submitted / Processing / Completed)
        Quick Action Button


Page 4: TIN Network Topology (Not necessary)



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

Page 3: TIN Network Topology (Not necessary)


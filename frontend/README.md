# TIN Provenance Dashboard (Frontend)

Streamlit dashboard for the **Temporal Interaction Network (TIN)** provenance system.  
It visualises attribution results produced by the Java / Flink backend pipeline.

The dashboard communicates with the backend **only through the HTTP API** and never opens the DuckDB file directly.

---

## Pages

| Page | File | Description |
|------|------|-------------|
| Overview | `app.py` + `pages/1_Overview.py` | High-level metrics, top hashtags, latest windows |
| Query & Lineage | `pages/2_Query.py` | Backward, Forward, Temporal, Flow, and Versioning queries |
| TIN Network | `pages/3_TIN_Network.py` | Static architecture / topology diagram |
| Insert Data | `pages/4_Insert_Data.py` | Upload or generate a CSV and trigger a pipeline run |

---

## Project Structure

```
frontend/
├── app.py                     # Entry point
├── pages/
│   ├── 1_Overview.py
│   ├── 2_Query.py
│   ├── 3_TIN_Network.py
│   └── 4_Insert_Data.py
├── components/
│   ├── api_client.py          # HTTP client for /runs and /runs/{id}
│   ├── query_engine.py        # Query logic on DataFrames from the API
│   ├── charts.py              # Plotly chart helpers
│   ├── page_setup.py          # Shared page config
│   ├── sidebar.py             # Navigation + API status indicator
│   ├── data_utils.py          # CSV validation & mock tweet generator
│   └── parsers.py             # Attribution JSON parser
└── README.md
```

---

## Prerequisites

1. **Java backend** running on port 8000 (or the port you configure):
   ```bash
   cd ../backend
   mvn compile exec:exec "-DmainClass=com.fyp.ApiApplication"
   ```

2. **Python environment** with the required packages (see project `requirements.txt`).

---

## Configuration

Create a `.env` file in the `frontend` folder (or project root):

```env
BACKEND_API_URL=http://localhost:8000
```

---

## How to Run

```bash
cd frontend
streamlit run app.py
```

Open the URL shown in the terminal (usually `http://localhost:8501`).

---

## Typical Workflow

1. Start the Java API backend.
2. Start the Streamlit frontend.
3. Go to **Insert Data**, prepare or upload a CSV, and click **Send & Run Pipeline**.
4. Switch to **Overview** or **Query** to explore the results.
5. The sidebar shows **API Online** when the backend is reachable.

---

## Notes

- The dashboard and API can run at the same time because the dashboard never locks the DuckDB file.
- If the sidebar shows **Offline**, check that the Java process is running and that `BACKEND_API_URL` is correct.
- Non-CSV uploads on the Insert Data page are rejected with an error message.

# UAV Telemetry Analytics Pipeline

## Project Overview
An automated, end-to-end data pipeline designed to extract, process, analyze, and cluster ArduPilot UAV flight logs. The system implements a full data-driven cycle to identify anomalous flight patterns from raw hardware telemetry .

## Pipeline Architecture

### 1. Data Sources (Extract) 
* **Implementation:** An automated web scraper targeting the ArduPilot Discuss forum.
* **Tools:** `requests`, `BeautifulSoup`.
* **Process:** Searches troubleshooting threads for recently uploaded binary DataFlash logs (`.bin` files) and downloads them to a local staging directory.

### 2. Transform & Load (ETL) 
* **Implementation:** Binary log parsing and data structuring.
* **Tools:** `pymavlink`, `pandas`.
* **Process:** * Uses `pymavlink.DFReader` to decode `.bin` files.
    * Extracts critical telemetry packets (e.g., `ATT` for attitude/orientation, `BAT` for voltage/current metrics, `GPS` for spatial positioning).
    * Synchronizes disparate sensor frequencies using `TimeUS` timestamps.
    * Cleans noise, handles missing values, and loads the structured time-series data into a consolidated tabular format suitable for machine learning.

### 3. Exploratory Data Analysis (EDA) 
* **Implementation:** Statistical profiling of the extracted flight data.
* **Tools:** `pandas`, `seaborn`, `matplotlib`.
* **Process:** Generates feature distribution plots, correlation heatmaps, and assesses baseline trends (e.g., battery consumption rates versus altitude variations or motor outputs).

### 4. Clustering (Machine Learning) 
* **Implementation:** Unsupervised learning to detect flight behavior patterns.
* **Tools:** `scikit-learn` (K-Means).
* **Process:** Groups flight telemetry into distinct clusters based on variance in the data. This automatically separates normal autonomous missions from aggressive manual flights or hardware anomalies and crashes.

### 5. Business Intelligence (BI) & Visualization 
* **Implementation:** Visual interpretation of clustering results.
* **Tools:** `matplotlib`, `seaborn`.
* **Process:** Renders flight trajectories, anomaly detection reports, and visual summaries of the clusters to provide actionable insights into hardware health and pilot behavior.

### 6. Automation (DAG)
* **Implementation:** Workflow orchestration.
* **Tools:** Apache Airflow.
* **Process:** A defined Directed Acyclic Graph (DAG) that schedules and executes the entire pipeline sequentially. It simulates a daily batch process: triggering the scraper, parsing new logs, updating the dataset, running the clustering model, and generating updated BI visualizations.

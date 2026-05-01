import os
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


def perform_analysis(input_file, db_path="data/uav_analytics.db", output_dir="plots"):
    os.makedirs(output_dir, exist_ok=True)
    if not os.path.exists(input_file):
        print("Input file doesn't exist")
        return

    if not os.path.exists(db_path):
        print(f"Database {db_path} not found.")
        return

    print(f"Loading data from {db_path}...")
    try:
        conn = sqlite3.connect(db_path)
        # Load most recent data or entire table
        df = pd.read_sql_query("SELECT * FROM telemetry", conn)
        conn.close()
    except Exception as e:
        print(f"Failed to read from SQLite: {e}")
        return

    if df.empty:
        print("Dataset is empty.")
        return

    # EDA: Correlation Heatmap
    print("Generating EDA visualizations...")

    # Filter out columns that are mostly empty (e.g., > 90% NaN) to avoid noise in heatmap
    df_clean = df.dropna(axis=1, thresh=int(0.1 * len(df)))

    # Filter out columns with zero variance (constant values like all zeros)
    # These provide no information for correlation and cause blank spots in heatmaps
    cols_to_drop = [col for col in df_clean.columns if df_clean[col].nunique() <= 1]
    df_clean = df_clean.drop(columns=cols_to_drop)

    numeric_cols = df_clean.select_dtypes(include=["float64", "int64"]).columns
    corr_cols = [
        c for c in numeric_cols if c not in ["TimeUS", "time_boot_ms", "flight_id"]
    ]

    if len(corr_cols) > 1:
        plt.figure(figsize=(12, 10))
        sns.heatmap(df_clean[corr_cols].corr(), annot=False, cmap="coolwarm")  # pyright: ignore[reportCallIssue]
        plt.title("Telemetry Correlation Heatmap")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "correlation_heatmap.png"))
        plt.close()

    # Clustering (Machine Learning)
    # Select features for clustering
    features = []
    if "DesRoll" in df.columns and "Roll" in df.columns:
        df["Roll_Err"] = abs(df["DesRoll"] - df["Roll"])
        features.append("Roll_Err")
    if "DesPitch" in df.columns and "Pitch" in df.columns:
        df["Pitch_Err"] = abs(df["DesPitch"] - df["Pitch"])
        features.append("Pitch_Err")

    if not features:
        # Fallback features if specific ones not found
        print("Fetures not found, exiting")
        return

    print(f"Clustering based on features: {features}")
    # Drop rows with NaN in features
    X = df[features].dropna()
    if len(X) < 10:
        print("Not enough rows for clusterization")
        return

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=3, random_state=42, n_init="auto")
    df.loc[X.index, "Cluster"] = kmeans.fit_predict(X_scaled)

    # BI & Visualization
    if len(features) >= 2:
        plt.figure(figsize=(10, 8))
        sns.scatterplot(
            x=df[features[0]],
            y=df[features[1]],
            hue="Cluster",
            data=df,
            palette="viridis",
        )
        plt.title("Telemetry Clustering Analysis")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "clustering_scatter.png"))
        plt.close()
        print(f"Saved clustering scatter plot to {output_dir}/clustering_scatter.png")

    # Save clustered dataset
    clustered_file = input_file.replace(".csv", "_clustered.csv")
    df.to_csv(clustered_file, index=False)
    print(f"Saved clustered dataset to {clustered_file}")

    # NEW: Generate Per-Flight Health Summary
    print("Generating flight health summary report...")
    # Calculate the percentage of time each flight spent in each cluster
    health_summary = (
        pd.crosstab(df["flight_id"], df["Cluster"], normalize="index") * 100
    )
    health_summary.columns = [f"Cluster_{int(c)}_Pct" for c in health_summary.columns]

    # Identify the "Healthiest" cluster (the one with the lowest average error)
    # We'll assume Cluster 0 is usually the most stable, but let's be data-driven
    cluster_means = df.groupby("Cluster")[features].mean().sum(axis=1)
    stable_cluster = cluster_means.idxmin()

    health_summary["Health_Score"] = health_summary[
        f"Cluster_{int(stable_cluster)}_Pct"
    ]
    health_summary = health_summary.sort_values(by="Health_Score", ascending=False)

    summary_path = os.path.join(output_dir, "flight_health_summary.csv")
    health_summary.to_csv(summary_path)
    print(f"Saved flight health report to {summary_path}")

    # Stacked Bar Chart for Flight Composition
    plt.figure(figsize=(12, 6))
    # Plot only the percentage columns
    pct_cols = [c for c in health_summary.columns if "_Pct" in c]
    health_summary[pct_cols].plot(
        kind="bar", stacked=True, ax=plt.gca(), colormap="viridis"
    )

    plt.title("Flight Composition by Cluster")
    plt.xlabel("Flight ID")
    plt.ylabel("Percentage of Flight Time (%)")
    plt.legend(title="Clusters", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "flight_composition_bar.png"))
    plt.close()
    print(f"Saved stacked bar chart to {output_dir}/flight_composition_bar.png")

    # Print top 3 anomalies to console
    print("\n--- TOP 3 ANOMALOUS FLIGHTS (Lowest Health Score) ---")
    print(health_summary.tail(3)[["Health_Score"]])
    print("----------------------------------------------------\n")

    print("Analysis completed.")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    perform_analysis(
        input_file=os.path.join(base_dir, "data/processed/telemetry_dataset.csv"),
        output_dir=os.path.join(base_dir, "plots"),
    )

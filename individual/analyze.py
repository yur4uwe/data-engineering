import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


import sqlite3

def perform_analysis(db_path="data/uav_analytics.db", output_dir="plots"):
    os.makedirs(output_dir, exist_ok=True)

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
    numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
    corr_cols = [
        c for c in numeric_cols if c not in ["TimeUS", "time_boot_ms", "flight_id"]
    ]

    if len(corr_cols) > 1:
        plt.figure(figsize=(12, 10))
        sns.heatmap(df[corr_cols].corr(), annot=False, cmap="coolwarm")  # pyright: ignore[reportCallIssue]
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
        features = corr_cols[:3]

    if features:
        print(f"Clustering based on features: {features}")
        # Drop rows with NaN in features
        X = df[features].dropna()
        if len(X) > 10:
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
                print(
                    f"Saved clustering scatter plot to {output_dir}/clustering_scatter.png"
                )

            # Save clustered dataset
            clustered_file = input_file.replace(".csv", "_clustered.csv")
            df.to_csv(clustered_file, index=False)
            print(f"Saved clustered dataset to {clustered_file}")

    print("Analysis completed.")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    perform_analysis(
        input_file=os.path.join(base_dir, "data/processed/telemetry_dataset.csv"),
        output_dir=os.path.join(base_dir, "plots"),
    )

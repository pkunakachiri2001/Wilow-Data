import os
import pandas as pd
import joblib
import numpy as np

def test_model():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(os.path.dirname(base_dir), 'master_data.csv')
    
    print(f"Loading data from {data_path}...")
    df = pd.read_csv(data_path)
    print(f"Total rows: {len(df)}")
    
    columns_to_drop = ['Date', 'Time_sec']
    features_df = df.drop(columns=columns_to_drop, errors='ignore')
    features_df = features_df.ffill().bfill()
    print(f"Features: {list(features_df.columns)}")
    
    scaler_path = os.path.join(base_dir, 'scaler.joblib')
    model_path = os.path.join(base_dir, 'isolation_forest_model.joblib')
    
    print("Loading models...")
    scaler = joblib.load(scaler_path)
    model = joblib.load(model_path)
    
    print("Evaluating...")
    X_scaled = scaler.transform(features_df)
    
    # -1 is anomaly, 1 is normal
    predictions = model.predict(X_scaled)
    # Get anomaly scores. Lower scores mean more anomalous.
    scores = model.decision_function(X_scaled)
    
    print(f"Min score: {scores.min():.4f}")
    print(f"Max score: {scores.max():.4f}")
    
    # Calculate confidence based on standard sigmoid-like mapping or linear mapping
    # Isolation forest scores typically range roughly from -0.5 to +0.5.
    # We want negative scores (anomalies) to yield high confidence, and positive scores (normal) to yield low confidence.
    
    # Simple linear mapping:
    max_abs_score = max(abs(scores.min()), abs(scores.max()), 0.5)
    confidences = 50 - (scores * 50 / max_abs_score)
    
    anomalies_binary = (predictions == -1)
    anomalies_high_conf = (confidences > 90)
    
    print(f"Total flagged by model (predict == -1): {anomalies_binary.sum()} ({(anomalies_binary.sum()/len(df))*100:.2f}%)")
    print(f"Total flagged by >90% confidence threshold: {anomalies_high_conf.sum()} ({(anomalies_high_conf.sum()/len(df))*100:.2f}%)")
    
    if anomalies_high_conf.sum() > 0:
        print("\nSample high-confidence anomalies:")
        print(df[anomalies_high_conf].head())

if __name__ == "__main__":
    test_model()

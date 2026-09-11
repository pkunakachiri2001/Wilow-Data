import os
import glob
import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

def main():
    print("Starting Anomaly Detection Training Pipeline...")
    
    # Setup paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(base_dir), 'data_by_date')
    
    # 1. Load Data
    print(f"Searching for CSV files in {data_dir}...")
    csv_files = glob.glob(os.path.join(data_dir, '*', '*.csv'))
    
    if not csv_files:
        print("Error: No CSV files found.")
        return
        
    print(f"Found {len(csv_files)} CSV files. Loading data...")
    
    df_list = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            df_list.append(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")
            
    if not df_list:
        print("Error: No data loaded.")
        return
        
    full_df = pd.concat(df_list, ignore_index=True)
    print(f"Total data points loaded: {len(full_df)}")
    
    # 2. Preprocess Data
    # Drop Date and Time_sec as they are not sensor features
    columns_to_drop = ['Date', 'Time_sec']
    features_df = full_df.drop(columns=columns_to_drop, errors='ignore')
    
    print(f"Feature matrix shape: {features_df.shape}")
    print(f"Features used: {list(features_df.columns)}")
    
    # Handle any potential NaNs (forward fill then backward fill)
    features_df = features_df.ffill().bfill()
    
    # 3. Fit Scaler
    print("\nFitting StandardScaler...")
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features_df)
    
    # 4. Train Isolation Forest
    contamination_rate = 0.01 # 1% expected anomalies
    n_trees = 100
    
    print(f"\nTraining Isolation Forest (n_estimators={n_trees}, contamination={contamination_rate})...")
    model = IsolationForest(
        n_estimators=n_trees,
        contamination=contamination_rate,
        random_state=42,
        n_jobs=-1 # Use all available CPU cores
    )
    
    model.fit(scaled_features)
    
    # Optional: Get predictions on training data to see how many were flagged
    predictions = model.predict(scaled_features)
    num_anomalies = (predictions == -1).sum()
    print(f"Anomalies detected in training set: {num_anomalies} out of {len(predictions)} ({(num_anomalies/len(predictions))*100:.2f}%)")
    
    # 5. Save Models
    print("\nSaving scaler and model...")
    scaler_path = os.path.join(base_dir, 'scaler.joblib')
    model_path = os.path.join(base_dir, 'isolation_forest_model.joblib')
    
    joblib.dump(scaler, scaler_path)
    joblib.dump(model, model_path)
    
    print(f"Scaler saved to: {scaler_path}")
    print(f"Model saved to: {model_path}")
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()

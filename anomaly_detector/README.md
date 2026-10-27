# anomaly_detector — currently DISABLED

**Status: not connected to `server.py`.** Nothing in the running system loads these
files, scores incoming data, raises AI alerts or triggers event extraction.
The folder is kept so the architecture is ready when real data is available.

## Why it is off
The committed models (`isolation_forest_model.joblib`, `scaler.joblib`) were trained on
old data with the previous **19-feature** layout (Z/X acceleration + FFT only). They are
**stale**: they do not match the current 3-axis + displacement data and were not trained on
real data from the new ESP32 setup. Do not wire them back in.

## What is here
| File | Purpose |
|---|---|
| `train.py` | Loads every `data_by_date/*/*.csv`, drops `Date`/`Time_sec`, fits a `StandardScaler` + `IsolationForest` (100 trees, 1% contamination), writes both `.joblib` files here |
| `test_anomaly.py` | Scores `master_data.csv` with the saved model and prints anomaly stats |
| `*.joblib` | **Stale** models from the old schema (overwritten the next time `train.py` runs) |

## What the dashboard already supports (kept as is)
`templates/dashboard.html` still has the AI widgets. They sit idle until the server
sends AI fields:
- **AI Confidence gauge** shows `--` unless `/api/latest_status` contains `ai_confidence`
- **AI Safety Score** KPI shows *Not active* (health score uses alerts only)
- **Anomaly badge / alert text** uses `ai_confidence` on the `alert_notification` event if present

## How to enable it later (when there is real data)
1. Collect real data with the ESP32s running normally (healthy machine behaviour).
2. Install deps: `pip install scikit-learn joblib` (not in `requirements.txt` on purpose).
3. Retrain: `python anomaly_detector/train.py`. It uses all CSV columns except
   `Date`/`Time_sec`, so the new model will have **31 features** (Z/X/Y accel stats,
   displacement metrics, FFT peaks).
4. Check it: `python anomaly_detector/test_anomaly.py`.
5. Add a scoring step to `server.py` (it has none now). The old design was:
   - build the feature vector in **exactly the training column order** (`CSV_HEADERS[2:]`)
   - `score = model.decision_function(scaler.transform(X))[0]`
   - `confidence = 100 / (1 + exp(20 * score))`, anomaly if `confidence > 97`
   - put `ai_confidence` / `ai_status` into `latest_status` and `alert_notification`
   - optionally auto-save an event with a 10-minute cooldown
6. Gate it behind a flag (e.g. `AI_ANOMALY_ENABLED=false` by default) so it can be switched off.

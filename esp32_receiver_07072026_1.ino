#include <Arduino.h>
#include <math.h>
#include <WiFi.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>

const char* ssid = "Lapcare-4GMIFI-AE93";
const char* password = "12345678";
// Cloud server URL (Render) — replace with your actual Render app name
const char* serverUrl = "https://wilow-data.onrender.com/data";

// ============================================================
// ISRG Root X1 — Let's Encrypt root CA (used by Render.com)
// Valid until: 2035-06-04. Update if your build date is after that.
// Source: https://letsencrypt.org/certificates/
// ============================================================
static const char* root_ca = \
"-----BEGIN CERTIFICATE-----\n"
"MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw\n"
"TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh\n"
"cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4\n"
"WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu\n"
"ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY\n"
"MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoBggIBAK3oJHP0FDfzm54rVygc\n"
"h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+\n"
"0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U\n"
"A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW\n"
"T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH\n"
"B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC\n"
"B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv\n"
"KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn\n"
"OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn\n"
"jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw\n"
"qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI\n"
"rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV\n"
"HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq\n"
"hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL\n"
"ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ\n"
"3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK\n"
"NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5\n"
"ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur\n"
"TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwD\n"
"psbJKxm//GbrkiHxf0pSORgNFHUjTl0okeQCRMSKqoAOmHAhb3GCiZS0CPEEgmTZ\n"
"kHhHtxuKxXXb3mKqOB9bMcAkFGBHT5YwgjHq5LBuLzTFQFAm8Q1nNJLmTSjIGVP4\n"
"w2bHRuBR+J7QYRL0JAQ/pLjUDUMOFJJEt1pU0EE4TyAl5GN3VJuAb/x93TLv2Vo\n"
"h5OtSCVmNhRxvEXNGPSa6SH7Y7DkPLqcJWcLyYSuSmctiU6EaOi9k08KdEa1Caxp\n"
"UxHhSK+1+1VCZJqwuZGjmaTkV6bS1O7ztSmBMhBU4jqiFmHKn2j8EVLHlw==\n"
"-----END CERTIFICATE-----\n";

#define UART_RX_PIN 16
#define UART_TX_PIN 17

HardwareSerial S3_UART(2);

const float SAMPLE_RATE = 1000.0;
float X_Scale_Factor = 1.0;
float Z_Scale_Factor = 1.0;

#pragma pack(push, 1)
struct SensorPacket {
  uint8_t header1;
  uint8_t header2;
  uint32_t counter;
  float x_accel;
  float z_accel;
  uint8_t checksum;
};
#pragma pack(pop)

const unsigned long RECORD_MS = 5000;
const unsigned long PAUSE_MS  = 5000;

#define MAX_SAMPLES 5100
#define FFT_N       2048

// Shared FFT working arrays (protected by fftMutex, only one core analyzes at a time)
double vReal[FFT_N];
double vImag[FFT_N];

// Per-task context: each core has its own independent data buffer and statistics
struct TaskCtx {
  float         buf[MAX_SAMPLES]; // Z-axis buffer for FFT
  int           cnt;
  float         maxZ, minZ;
  double        meanZ, M2;
  float         maxX, minX;
  double        meanX;
  unsigned long samples;
  unsigned long recordStart;
  unsigned long recordStop;
};

static TaskCtx ctx1; // Core 1 data
static TaskCtx ctx2; // Core 0 data

// Synchronization
SemaphoreHandle_t uartMutex;   // Only one core reads UART at a time
SemaphoreHandle_t fftMutex;    // Only one core uses shared FFT arrays at a time
SemaphoreHandle_t serialMutex; // Prevents Serial output from interleaving

// Shared UART parser state (protected by uartMutex)
enum ParserState { WAIT_HEADER1, WAIT_HEADER2, READ_PAYLOAD };
ParserState uartState = WAIT_HEADER1;
uint8_t payloadBuf[13];
int payloadIdx = 0;

// ============================================================
// Statistics Helpers
// ============================================================
void resetCtx(TaskCtx &c) {
  c.cnt = 0; c.samples = 0;
  c.maxZ = 0; c.minZ = 0; c.meanZ = 0; c.M2 = 0;
  c.maxX = 0; c.minX = 0; c.meanX = 0;
}

void updateCtx(TaskCtx &c, float ax, float az) {
  if (c.samples == 0) { 
    c.maxZ = az; c.minZ = az; 
    c.maxX = ax; c.minX = ax; 
  } else { 
    if (az > c.maxZ) c.maxZ = az; 
    if (az < c.minZ) c.minZ = az; 
    if (ax > c.maxX) c.maxX = ax; 
    if (ax < c.minX) c.minX = ax; 
  }
  c.samples++;
  
  // Z-axis Welford's algorithm
  double dZ = az - c.meanZ;
  c.meanZ += dZ / c.samples;
  c.M2 += dZ * (az - c.meanZ);
  
  // X-axis mean
  c.meanX += (ax - c.meanX) / c.samples;
  
  if (c.cnt < MAX_SAMPLES) c.buf[c.cnt++] = az;
}

double calcSkewness(TaskCtx &c) {
  if (c.cnt < 3) return 0.0;
  double sd = sqrt(c.M2 / (c.samples - 1));
  if (sd == 0.0) return 0.0;
  double sum3 = 0.0;
  for (int i = 0; i < c.cnt; i++) { double z = (c.buf[i] - c.meanZ) / sd; sum3 += z*z*z; }
  return sum3 / c.cnt;
}

double calcKurtosis(TaskCtx &c) {
  if (c.cnt < 4) return 0.0;
  double sd = sqrt(c.M2 / (c.samples - 1));
  if (sd == 0.0) return 0.0;
  double sum4 = 0.0;
  for (int i = 0; i < c.cnt; i++) { double z = (c.buf[i] - c.meanZ) / sd; sum4 += z*z*z*z; }
  return (sum4 / c.cnt) - 3.0;
}

// ============================================================
// UART Reading — reads into a TaskCtx for durationMs
// Caller must hold uartMutex before calling
// ============================================================
void readUARTInto(TaskCtx &c, unsigned long durationMs) {
  uartState = WAIT_HEADER1; payloadIdx = 0;
  while (S3_UART.available()) S3_UART.read(); // flush stale bytes

  unsigned long t0 = millis();
  c.recordStart = t0;
  
  while (millis() - t0 < durationMs) {
    while (S3_UART.available()) {
      uint8_t b = S3_UART.read();
      switch (uartState) {
        case WAIT_HEADER1:
          if (b == 0xAA) uartState = WAIT_HEADER2;
          break;
        case WAIT_HEADER2:
          if      (b == 0xBB) { uartState = READ_PAYLOAD; payloadIdx = 0; }
          else if (b == 0xAA) uartState = WAIT_HEADER2;
          else                uartState = WAIT_HEADER1;
          break;
        case READ_PAYLOAD:
          payloadBuf[payloadIdx++] = b;
          if (payloadIdx == 13) {
            uint8_t chk = 0xAA + 0xBB;
            for (int i = 0; i < 12; i++) chk += payloadBuf[i];
            if (chk == payloadBuf[12]) {
              float ax, az;
              memcpy(&ax, &payloadBuf[4], sizeof(float));
              memcpy(&az, &payloadBuf[8], sizeof(float));
              
              ax *= X_Scale_Factor;
              az *= Z_Scale_Factor;
              
              updateCtx(c, ax, az);
            }
            uartState = WAIT_HEADER1;
          }
          break;
      }
    }
    vTaskDelay(1);
  }
  c.recordStop = millis();
}

// ============================================================
// FFT (Cooley-Tukey) — uses shared vReal/vImag
// Caller must hold fftMutex before calling
// ============================================================
void computeFFT() {
  int n = FFT_N, j = 0;
  for (int i = 1; i < n; i++) {
    int bit = n >> 1;
    while (j & bit) { j ^= bit; bit >>= 1; } j ^= bit;
    if (i < j) {
      double t = vReal[i]; vReal[i] = vReal[j]; vReal[j] = t;
      t = vImag[i]; vImag[i] = vImag[j]; vImag[j] = t;
    }
  }
  for (int len = 2; len <= n; len <<= 1) {
    double ang = -2.0 * PI / len, wlR = cos(ang), wlI = sin(ang);
    for (int i = 0; i < n; i += len) {
      double wR = 1.0, wI = 0.0;
      for (int k = 0; k < len/2; k++) {
        int e = i+k, o = i+k+len/2;
        double uR = vReal[e], uI = vImag[e];
        double tR = vReal[o]*wR - vImag[o]*wI, tI = vReal[o]*wI + vImag[o]*wR;
        vReal[e] = uR+tR; vImag[e] = uI+tI;
        vReal[o] = uR-tR; vImag[o] = uI-tI;
        double nwR = wR*wlR - wI*wlI; wI = wR*wlI + wI*wlR; wR = nwR;
      }
    }
  }
}

// ============================================================
// Analysis + Print — acquires fftMutex and serialMutex internally
// ============================================================
void analyzeAndPrint(TaskCtx &c, int coreID) {
  if (c.samples < 2) {
    xSemaphoreTake(serialMutex, portMAX_DELAY);
    Serial.printf("\n[Core %d] Not enough samples.\n", coreID);
    xSemaphoreGive(serialMutex);
    return;
  }

  double varianceZ = c.M2 / (c.samples - 1);
  double stdDevZ   = sqrt(varianceZ);
  double rmsZ      = sqrt(varianceZ + (c.meanZ * c.meanZ));
  double rangeZ    = c.maxZ - c.minZ;
  double skewnessZ = calcSkewness(c);
  double kurtosisZ = calcKurtosis(c);

  float peakFreq[5] = {}, peakMag[5] = {};

  xSemaphoreTake(fftMutex, portMAX_DELAY);
  if (c.cnt >= 16) {
    for (int i = 0; i < FFT_N; i++) {
      int idx = (long)i * (c.cnt - 1) / (FFT_N - 1);
      double s = c.buf[idx] - c.meanZ;
      double w = 0.5 * (1.0 - cos(2.0 * PI * i / (FFT_N - 1)));
      vReal[i] = s * w; vImag[i] = 0.0;
    }
    computeFFT();
    for (int k = 1; k < FFT_N/2; k++) {
      double freq = k * SAMPLE_RATE / FFT_N;
      double mag  = sqrt(vReal[k]*vReal[k] + vImag[k]*vImag[k]) / FFT_N;
      for (int p = 0; p < 5; p++) {
        if (mag > peakMag[p]) {
          for (int q = 4; q > p; q--) { peakMag[q] = peakMag[q-1]; peakFreq[q] = peakFreq[q-1]; }
          peakMag[p] = mag; peakFreq[p] = freq; break;
        }
      }
    }
  }
  xSemaphoreGive(fftMutex);

  double durationS = (c.recordStop - c.recordStart) / 1000.0;
  
  xSemaphoreTake(serialMutex, portMAX_DELAY);
  Serial.printf("\n=== METADATA ===\n");
  Serial.printf("Parameter,Value,Unit\n");
  Serial.printf("Device ID,ESP32_%d,-\n", coreID);
  Serial.printf("File number,0,-\n");
  Serial.printf("File name,ESP32_%d_LOCAL.csv,-\n", coreID);
  
  Serial.printf("\n=== RECORDING INFO ===\n");
  Serial.printf("Parameter,Value,Unit\n");
  Serial.printf("Record start,%lu,ms from boot\n", c.recordStart);
  Serial.printf("Record stop,%lu,ms from boot\n", c.recordStop);
  Serial.printf("Duration,%.3f,s\n", durationS);
  
  // Format H:MM:SS.mmm
  unsigned long durMs = c.recordStop - c.recordStart;
  int h = durMs / 3600000;
  durMs %= 3600000;
  int m = durMs / 60000;
  durMs %= 60000;
  int s = durMs / 1000;
  int ms = durMs % 1000;
  Serial.printf("Duration (H:MM:SS.mmm),%d:%02d:%02d.%03d,-\n", h, m, s, ms);
  
  Serial.printf("Total samples,%lu,samples\n", c.samples);
  Serial.printf("FFT samples used,%d,samples\n", c.cnt);
  Serial.printf("FFT coverage,100.0,%%\n");
  Serial.printf("Sample rate,%.3f,Hz\n", SAMPLE_RATE);
  Serial.printf("Nyquist frequency,%.3f,Hz\n", SAMPLE_RATE / 2.0);

  Serial.printf("\n=== Z-AXIS STATISTICS ===\n");
  Serial.printf("Parameter,Value,Unit\n");
  Serial.printf("Maximum Az,%.6f,m/s^2\n", c.maxZ);
  Serial.printf("Minimum Az,%.6f,m/s^2\n", c.minZ);
  Serial.printf("Range Az,%.6f,m/s^2\n", rangeZ);
  Serial.printf("Mean Az,%.6f,m/s^2\n", c.meanZ);
  Serial.printf("Variance Az,%.6f,(m/s^2)^2\n", varianceZ);
  Serial.printf("Std Dev Az,%.6f,m/s^2\n", stdDevZ);
  Serial.printf("RMS Az,%.6f,m/s^2\n", rmsZ);
  Serial.printf("Skewness Az,%.6f,-\n", skewnessZ);
  Serial.printf("Excess Kurtosis Az,%.6f,-\n", kurtosisZ);
  Serial.printf("Dominant Frequency,%.3f,Hz\n", peakFreq[0]);
  Serial.printf("Dominant Magnitude,%.6f,-\n", peakMag[0]);

  Serial.printf("\n=== X-AXIS STATISTICS ===\n");
  Serial.printf("Parameter,Value,Unit\n");
  Serial.printf("Maximum Ax,%.6f,m/s^2\n", c.maxX);
  Serial.printf("Minimum Ax,%.6f,m/s^2\n", c.minX);
  Serial.printf("Mean Ax,%.6f,m/s^2\n", c.meanX);

  Serial.printf("\n=== FFT PEAKS (Z-AXIS) ===\n");
  Serial.printf("Rank,Frequency (Hz),Magnitude\n");
  for (int i = 0; i < 5; i++) {
    Serial.printf("%d,%.3f,%.6f\n", i+1, peakFreq[i], peakMag[i]);
  }
  Serial.printf("=================================================\n\n");
  xSemaphoreGive(serialMutex);

  // ==========================================
  // HTTPS POST directly to Cloud Server (Render)
  // Retries up to 3 times (10s apart) to survive Render cold-starts
  // on first power-on. Normal operation never needs a retry since the
  // server stays awake as long as we send data every ~5 seconds.
  // ==========================================
  if (WiFi.status() == WL_CONNECTED) {
    char jsonPayload[1024];
    snprintf(jsonPayload, sizeof(jsonPayload),
        "{\"core\":%d,\"samples\":%lu,\"sample_rate\":%.3f,\"record_start\":%lu,\"record_stop\":%lu,"
        "\"max\":%.6f,\"min\":%.6f,\"mean\":%.6f,\"std_dev\":%.6f,\"skewness\":%.6f,\"kurtosis\":%.6f,"
        "\"max_x\":%.6f,\"min_x\":%.6f,\"mean_x\":%.6f,"
        "\"freq\":[%.3f,%.3f,%.3f,%.3f,%.3f],\"mag\":[%.6f,%.6f,%.6f,%.6f,%.6f]}",
        coreID, c.samples, SAMPLE_RATE, c.recordStart, c.recordStop,
        c.maxZ, c.minZ, c.meanZ, stdDevZ, skewnessZ, kurtosisZ,
        c.maxX, c.minX, c.meanX,
        peakFreq[0], peakFreq[1], peakFreq[2], peakFreq[3], peakFreq[4],
        peakMag[0], peakMag[1], peakMag[2], peakMag[3], peakMag[4]);

    const int MAX_RETRIES   = 3;
    const int RETRY_DELAY_S = 10;  // wait 10s between attempts (Render wakes in ~30s)
    bool posted = false;

    for (int attempt = 1; attempt <= MAX_RETRIES && !posted; attempt++) {
      WiFiClientSecure client;
      client.setInsecure();    // Bypass certificate validation (fixes Connection Refused without needing NTP time sync)

      HTTPClient http;
      http.begin(client, serverUrl);
      http.addHeader("Content-Type", "application/json");
      http.addHeader("X-API-Key", "babadasohue");  // matches ESP32_API_KEY on Render
      http.setTimeout(25000);       // 25s — longer than Render cold-start

      int code = http.POST(jsonPayload);
      http.end();

      xSemaphoreTake(serialMutex, portMAX_DELAY);
      if (code > 0) {
        Serial.printf("[Core %d] HTTPS POST OK (attempt %d/%d), Code: %d\n",
                      coreID, attempt, MAX_RETRIES, code);
        posted = true;
      } else {
        Serial.printf("[Core %d] HTTPS POST failed (attempt %d/%d): %s\n",
                      coreID, attempt, MAX_RETRIES,
                      http.errorToString(code).c_str());
        if (attempt < MAX_RETRIES) {
          Serial.printf("[Core %d] Render may be waking up — retrying in %ds...\n",
                        coreID, RETRY_DELAY_S);
        } else {
          Serial.printf("[Core %d] All retries exhausted — packet dropped.\n", coreID);
        }
      }
      xSemaphoreGive(serialMutex);

      if (!posted && attempt < MAX_RETRIES) {
        vTaskDelay(pdMS_TO_TICKS(RETRY_DELAY_S * 1000));
      }
    }
  } else {
    xSemaphoreTake(serialMutex, portMAX_DELAY);
    Serial.printf("[Core %d] WiFi Disconnected, skipping HTTPS POST\n", coreID);
    xSemaphoreGive(serialMutex);
  }
}

// ============================================================
// Task 1 — pinned to Core 1, starts recording immediately
// Timeline: RECORD(0-5s) → ANALYZE(5-10s) → repeat
// ============================================================
void task1Fn(void *pvParams) {
  TaskCtx *c = (TaskCtx*)pvParams;
  int coreID = xPortGetCoreID();

  while (true) {
    resetCtx(*c);
    xSemaphoreTake(uartMutex, portMAX_DELAY);
    readUARTInto(*c, RECORD_MS);
    xSemaphoreGive(uartMutex);

    analyzeAndPrint(*c, coreID);

    // Busy-wait for remainder of the PAUSE_MS window if analysis finished early
    vTaskDelay(pdMS_TO_TICKS(PAUSE_MS));
  }
}

// ============================================================
// Task 2 — pinned to Core 0, starts with a 5s delay
// Timeline: IDLE(0-5s) → RECORD(5-10s) → ANALYZE(10-15s) → repeat
// ============================================================
void task2Fn(void *pvParams) {
  TaskCtx *c = (TaskCtx*)pvParams;
  int coreID = xPortGetCoreID();

  // Stagger start: Core 1 records 0-5s while Core 0 waits
  vTaskDelay(pdMS_TO_TICKS(RECORD_MS));

  while (true) {
    resetCtx(*c);
    xSemaphoreTake(uartMutex, portMAX_DELAY);
    readUARTInto(*c, RECORD_MS);
    xSemaphoreGive(uartMutex);

    analyzeAndPrint(*c, coreID);

    vTaskDelay(pdMS_TO_TICKS(PAUSE_MS));
  }
}

TaskHandle_t task1Handle = NULL;
TaskHandle_t task2Handle = NULL;

void setup() {
  Serial.begin(115200);
  S3_UART.begin(500000, SERIAL_8N1, UART_RX_PIN, UART_TX_PIN);
  delay(2000);

  uartMutex   = xSemaphoreCreateMutex();
  fftMutex    = xSemaphoreCreateMutex();
  serialMutex = xSemaphoreCreateMutex();

  Serial.println("\nESP32 Dual-Core Receiver & Analyzer");
  
  Serial.print("Connecting to WiFi");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected.");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());

  Serial.println("Core 1: Records 0-5s, Analyzes 5-10s, repeat");
  Serial.println("Core 0: Idles 0-5s, Records 5-10s, Analyzes 10-15s, repeat");
  Serial.println("------------------------------------------------------\n");

  xTaskCreatePinnedToCore(task1Fn, "Task_Core1", 32000, &ctx1, 1, &task1Handle, 1);
  xTaskCreatePinnedToCore(task2Fn, "Task_Core0", 32000, &ctx2, 1, &task2Handle, 0);
}

void loop() {
  vTaskDelete(NULL); // Free the default Arduino task entirely
}

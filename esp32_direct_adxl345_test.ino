#include <Arduino.h>
#include <math.h>
#include <WiFi.h>
#include <HTTPClient.h>
#include <WiFiClientSecure.h>
#include <SPI.h>

const char* ssid = "Lapcare-4GMIFI-AE93";
const char* password = "12345678";
// Cloud server URL (Render)
const char* serverUrl = "https://wilow-data.onrender.com/data";

// ============================================================
// ISRG Root X1 — Let's Encrypt root CA (used by Render.com)
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

// ============================================================
// ADXL345 SPI Settings
// ============================================================
const int CS_PIN   = 5;
const int SCK_PIN  = 18;
const int MISO_PIN = 19;
const int MOSI_PIN = 23;

// ADXL345 Registers
const uint8_t BW_RATE     = 0x2C;
const uint8_t POWER_CTL   = 0x2D;
const uint8_t DATA_FORMAT = 0x31;
const uint8_t DATAX0      = 0x32;

// Full resolution mode is 4mg/LSB -> 0.004 g/LSB
// 0.004 * 9.80665 = 0.0392266 m/s^2 per LSB
const float SCALE_FACTOR = 0.0392266f;

const float SAMPLE_RATE = 1000.0;
const unsigned long RECORD_MS = 5000;
const unsigned long PAUSE_MS  = 5000;

#define MAX_SAMPLES 5100
#define FFT_N       2048

// Shared FFT working arrays (protected by fftMutex)
double vReal[FFT_N];
double vImag[FFT_N];

// Per-task context: each core has its own independent data buffer
struct TaskCtx {
  float         buf[MAX_SAMPLES];
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
SemaphoreHandle_t spiMutex;    // Only one core reads SPI at a time
SemaphoreHandle_t fftMutex;    // Only one core uses shared FFT arrays at a time
SemaphoreHandle_t serialMutex; // Prevents Serial output from interleaving

void writeRegister(uint8_t reg, uint8_t value) {
  SPI.beginTransaction(SPISettings(2000000, MSBFIRST, SPI_MODE3));
  digitalWrite(CS_PIN, LOW);
  SPI.transfer(reg); // Write, bit 7 = 0
  SPI.transfer(value);
  digitalWrite(CS_PIN, HIGH);
  SPI.endTransaction();
}

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
  
  double dZ = az - c.meanZ;
  c.meanZ += dZ / c.samples;
  c.M2 += dZ * (az - c.meanZ);
  
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
// SPI Reading — reads directly from ADXL345 for durationMs
// Caller must hold spiMutex before calling
// ============================================================
void readSPIInto(TaskCtx &c, unsigned long durationMs) {
  unsigned long t0 = millis();
  c.recordStart = t0;
  
  const unsigned long intervalUs = 1000000 / SAMPLE_RATE;
  unsigned long nextSampleTime = micros();

  // Settings for standard 2MHz SPI on ADXL345
  SPI.beginTransaction(SPISettings(2000000, MSBFIRST, SPI_MODE3));

  while (millis() - t0 < durationMs) {
    digitalWrite(CS_PIN, LOW);
    // 0x80 for Read, 0x40 for Multiple Bytes
    SPI.transfer(DATAX0 | 0x80 | 0x40);
    uint8_t x0 = SPI.transfer(0x00);
    uint8_t x1 = SPI.transfer(0x00);
    uint8_t y0 = SPI.transfer(0x00); // skip Y
    uint8_t y1 = SPI.transfer(0x00);
    uint8_t z0 = SPI.transfer(0x00);
    uint8_t z1 = SPI.transfer(0x00);
    digitalWrite(CS_PIN, HIGH);
    
    int16_t x_raw = x0 | (x1 << 8);
    int16_t z_raw = z0 | (z1 << 8);
    
    float ax = x_raw * SCALE_FACTOR;
    float az = z_raw * SCALE_FACTOR;
    
    updateCtx(c, ax, az);

    // Wait until it's time for the next sample
    while (micros() - nextSampleTime < intervalUs) {
      // Busy wait for precise 1000Hz sampling
    }
    nextSampleTime += intervalUs;

    // Feed the FreeRTOS watchdog every 100 samples to prevent crash
    if (c.samples % 100 == 0) {
      vTaskDelay(1); 
      nextSampleTime = micros(); // prevent accumulating delay debt
    }
  }
  
  SPI.endTransaction();
  c.recordStop = millis();
}

// ============================================================
// FFT (Cooley-Tukey)
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
// Analysis + Print + Upload
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
  
  xSemaphoreTake(serialMutex, portMAX_DELAY);
  Serial.printf("\n[Core %d] Done Analysis! Samples: %lu. Freq Peak 1: %.3f Hz\n", coreID, c.samples, peakFreq[0]);
  xSemaphoreGive(serialMutex);

  // HTTPS POST to Cloud Server
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
    const int RETRY_DELAY_S = 10;
    bool posted = false;

    for (int attempt = 1; attempt <= MAX_RETRIES && !posted; attempt++) {
      WiFiClientSecure client;
      client.setInsecure();    // Bypass certificate validation (fixes Connection Refused without needing NTP time sync)

      HTTPClient http;
      http.begin(client, serverUrl);
      http.addHeader("Content-Type", "application/json");
      http.addHeader("X-API-Key", "babadasohue");
      http.setTimeout(25000);

      int code = http.POST(jsonPayload);
      http.end();

      xSemaphoreTake(serialMutex, portMAX_DELAY);
      if (code > 0) {
        Serial.printf("[Core %d] HTTPS POST OK (attempt %d/%d), Code: %d\n", coreID, attempt, MAX_RETRIES, code);
        posted = true;
      } else {
        Serial.printf("[Core %d] HTTPS POST failed (attempt %d/%d): %s\n", coreID, attempt, MAX_RETRIES, http.errorToString(code).c_str());
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
// Core Tasks
// ============================================================
void task1Fn(void *pvParams) {
  TaskCtx *c = (TaskCtx*)pvParams;
  int coreID = xPortGetCoreID();

  while (true) {
    resetCtx(*c);
    xSemaphoreTake(spiMutex, portMAX_DELAY);
    readSPIInto(*c, RECORD_MS);
    xSemaphoreGive(spiMutex);

    analyzeAndPrint(*c, coreID);
    vTaskDelay(pdMS_TO_TICKS(PAUSE_MS));
  }
}

void task2Fn(void *pvParams) {
  TaskCtx *c = (TaskCtx*)pvParams;
  int coreID = xPortGetCoreID();

  vTaskDelay(pdMS_TO_TICKS(RECORD_MS));

  while (true) {
    resetCtx(*c);
    xSemaphoreTake(spiMutex, portMAX_DELAY);
    readSPIInto(*c, RECORD_MS);
    xSemaphoreGive(spiMutex);

    analyzeAndPrint(*c, coreID);
    vTaskDelay(pdMS_TO_TICKS(PAUSE_MS));
  }
}

TaskHandle_t task1Handle = NULL;
TaskHandle_t task2Handle = NULL;

void setup() {
  Serial.begin(115200);
  delay(2000);

  // Initialize SPI for ADXL345 directly connected to ESP32
  pinMode(CS_PIN, OUTPUT);
  digitalWrite(CS_PIN, HIGH);
  SPI.begin(SCK_PIN, MISO_PIN, MOSI_PIN, -1); // CS handled manually
  
  writeRegister(DATA_FORMAT, 0x0B); // ±16g, full resolution
  writeRegister(BW_RATE, 0x0F);     // 3200Hz output data rate
  writeRegister(POWER_CTL, 0x08);   // Measurement mode
  
  spiMutex    = xSemaphoreCreateMutex();
  fftMutex    = xSemaphoreCreateMutex();
  serialMutex = xSemaphoreCreateMutex();

  Serial.println("\nESP32 DIRECT ADXL345 (SPI) TEST (No Teensy)");
  
  Serial.print("Connecting to WiFi");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nWiFi connected.");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());

  xTaskCreatePinnedToCore(task1Fn, "Task_Core1", 32000, &ctx1, 1, &task1Handle, 1);
  xTaskCreatePinnedToCore(task2Fn, "Task_Core0", 32000, &ctx2, 1, &task2Handle, 0);
}

void loop() {
  vTaskDelete(NULL);
}

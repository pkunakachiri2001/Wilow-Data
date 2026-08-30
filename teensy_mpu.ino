#include <Wire.h>

const uint8_t MPU_ADDR = 0x68;

// MPU6050 Registers
const uint8_t PWR_MGMT_1   = 0x6B;
const uint8_t ACCEL_CONFIG = 0x1C;
const uint8_t CONFIG_REG   = 0x1A;
const uint8_t ACCEL_XOUT_H = 0x3B;
const uint8_t WHO_AM_I     = 0x75;

// 1000 Hz timer
elapsedMicros timer;
const uint32_t sampleInterval = 1000;

// ±16g conversion
// 2048 LSB/g
const float SCALE_FACTOR = 9.80665f / 2048.0f;

#pragma pack(push,1)
struct SensorPacket
{
    uint8_t header1;
    uint8_t header2;
    uint32_t counter;
    float x_accel;
    float z_accel;
    uint8_t checksum;
};
#pragma pack(pop)

SensorPacket packet;

uint32_t sampleCounter = 0;

void writeRegister(uint8_t reg, uint8_t value)
{
    Wire.beginTransmission(MPU_ADDR);
    Wire.write(reg);
    Wire.write(value);
    Wire.endTransmission();
}

uint8_t readRegister(uint8_t reg)
{
    Wire.beginTransmission(MPU_ADDR);
    Wire.write(reg);

    if (Wire.endTransmission(false) != 0)
        return 0xFF;

    if (Wire.requestFrom(MPU_ADDR, (uint8_t)1) != 1)
        return 0xFF;

    return Wire.read();
}

bool readAxes(float &x, float &z)
{
    Wire.beginTransmission(MPU_ADDR);
    Wire.write(ACCEL_XOUT_H);

    if (Wire.endTransmission(false) != 0)
        return false;

    if (Wire.requestFrom(MPU_ADDR, (uint8_t)6) != 6)
        return false;

    int16_t x_raw = (Wire.read() << 8) | Wire.read();

    Wire.read();   // Skip Y High
    Wire.read();   // Skip Y Low

    int16_t z_raw = (Wire.read() << 8) | Wire.read();

    x = x_raw * SCALE_FACTOR;
    z = z_raw * SCALE_FACTOR;

    return true;
}

void setup()
{
    Serial.begin(115200);
    Serial1.begin(500000);

    delay(2000);

    Wire.begin();

    // Uncomment after confirming everything works
    // Wire.setClock(400000);

    writeRegister(PWR_MGMT_1, 0x00);
    delay(100);

    writeRegister(ACCEL_CONFIG, 0x18);   // ±16g
    writeRegister(CONFIG_REG, 0x00);

    uint8_t id = readRegister(WHO_AM_I);

    Serial.println("--------------------------------");
    Serial.print("WHO_AM_I = 0x");
    Serial.println(id, HEX);

    if (id == 0x68 || id == 0x69 || id == 0x71)
        Serial.println("MPU6050 OK");
    else
        Serial.println("Unexpected WHO_AM_I");

    Serial.print("Packet Size = ");
    Serial.println(sizeof(SensorPacket));

    packet.header1 = 0xAA;
    packet.header2 = 0xBB;

    timer = 0;
}

void loop()
{
    if (timer >= sampleInterval)
    {
        timer -= sampleInterval;

        float x_accel;
        float z_accel;

        if (!readAxes(x_accel, z_accel))
            return;

        packet.counter = sampleCounter++;
        packet.x_accel = x_accel;
        packet.z_accel = z_accel;

        uint8_t *bytes = (uint8_t *)&packet;

        uint8_t checksum = 0;
        for (int i = 0; i < 14; i++)
            checksum += bytes[i];

        packet.checksum = checksum;

        // Send 15-byte packet to ESP32
        Serial1.write(bytes, sizeof(SensorPacket));

        // Optional heartbeat every 1000 samples (~1 second)
        if ((sampleCounter % 1000) == 0)
        {
            Serial.print("Samples Sent: ");
            Serial.println(sampleCounter);
        }
    }
}
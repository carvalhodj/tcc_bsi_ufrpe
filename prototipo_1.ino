#include <RTClib.h>
#include <Wire.h>
#include <AceButton.h>
#include <AdjustableButtonConfig.h>
#include <ButtonConfig.h>
#include "ACS712.h"
#include "SimpleTimer.h"
#include <ESP8266WiFi.h>
#include "ESP8266FtpServer.h"

#define LED_POWER   D6
#define BUTTON_PIN  D7

using namespace ace_button;

RTC_DS1307 rtc;
SimpleTimer timer;
ACS712 sensor(ACS712_30A, A0);
FtpServer ftp_srv;
AceButton button(BUTTON_PIN);

const char *ssid              = "JuaWMS";
const char *password          = "";
const char *ftp_user          = "juawms";
const char *ftp_password      = "juawms";
const double V                = 220;
const double treshold         = 1.0;
bool ligado                   = false;
//bool telemetria               = false;
bool servidor_ftp             = false;
int led_state                 = HIGH;
unsigned int interval         = 1000;
unsigned int previous_millis  = 0;
int timerId;
int timerId2;

void modo_telemetria();
void modo_sensor();
void blink_led();
void led_on();
void handle_event(AceButton*, uint8_t, uint8_t);

void setup() {
    pinMode(LED_POWER, OUTPUT);
    pinMode(BUTTON_PIN, INPUT_PULLUP);
    digitalWrite(LED_POWER, led_state);
    
    ButtonConfig* button_config = button.getButtonConfig();
    button_config->setEventHandler(handle_event);
    button_config->setFeature(ButtonConfig::kFeatureClick);
    button_config->setFeature(ButtonConfig::kFeatureLongPress);

   WiFi.mode(WIFI_AP);//Define o ESP8266 como Acess Point.
   WiFi.softAP(ssid, password);//Cria um WiFi de nome "NodeMCU" e sem senha.

  rtc.begin();
  // Setar no RTC a hora que este sketch foi compilado
   rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
  
    if (SPIFFS.begin())
    {
//        SPIFFS.format();
//        ftp_srv.begin(ftp_user, ftp_password);
        File f = SPIFFS.open("/coleta.csv", "a");
        f.println("<<< SISTEMA INICIADO >>>");
        f.close();
    }
//    
    Serial.println("Aguarde. Calibrando...");
    sensor.calibrate();
    Serial.println("Fim da calibração");

    timerId = timer.setInterval(1000, modo_sensor);
    timerId2 = timer.setInterval(1000, modo_telemetria);
    timer.disable(timerId2);
}

void loop() 
{
  button.check();
  timer.run();
  if (servidor_ftp)
    ftp_srv.handleFTP();
}

void modo_telemetria()
{
//  if (WiFi.status() != WL_CONNECTED)
//  {
//    WiFi.begin(ssid, password);
//    delay(2000);
//  }
  // Inicializando o servidor FTP
  if (!servidor_ftp)
  {
    ftp_srv.begin(ftp_user, ftp_password);
    servidor_ftp = true; 
  }
  blink_led();
}

void modo_sensor()
{
  float I = sensor.getCurrentAC(60);
  if (I > treshold && !ligado)
  {
    // Formato: TIMESTAMP,<ON/OFF>,CORRENTE(A)
    DateTime now = rtc.now();
    File f = SPIFFS.open("/coleta.csv", "a");
    f.print(now.year(), DEC);
    f.print("-");
    f.print(now.month(), DEC);
    f.print("-");
    f.print(now.day(), DEC);
    f.print(" ");
    f.print(now.hour(), DEC);
    f.print(":");
    f.print(now.minute(), DEC);
    f.print(":");
    f.print(now.second(), DEC);
    f.print(",");
    f.print("1");
    f.print(",");
    f.println(I);
    f.close();
    ligado = true;
  }
  else if (I < treshold && ligado)
  {
    DateTime now = rtc.now();
    File f = SPIFFS.open("/coleta.csv", "a");
    f.print(now.year(), DEC);
    f.print("-");
    f.print(now.month(), DEC);
    f.print("-");
    f.print(now.day(), DEC);
    f.print(" ");
    f.print(now.hour(), DEC);
    f.print(":");
    f.print(now.minute(), DEC);
    f.print(":");
    f.print(now.second(), DEC);
    f.print(",");
    f.print("0");
    f.print(",");
    f.println(I);
    f.close();
    ligado = false;
  }
  led_on();
}

void blink_led()
{
  unsigned long current_millis = millis();
  if(current_millis - previous_millis >= interval)
  {
    previous_millis = current_millis;   
    if (led_state == LOW)
      led_state = HIGH;  // Note that this switches the LED *off*
    else
      led_state = LOW;   // Note that this switches the LED *on*
    digitalWrite(LED_POWER, led_state);
  }
}

void led_on()
{
  if (!led_state)
  {
    led_state = HIGH;
    digitalWrite(LED_POWER, led_state);
  }
  
  if (servidor_ftp)
  {
    servidor_ftp = false;
//    WiFi.disconnect(true);
  }
}

void handle_event(AceButton* /* button */, uint8_t eventType, uint8_t buttonState)
{
  switch (eventType)
  {
    case AceButton::kEventClicked:
      timer.enable(timerId);
      timer.disable(timerId2);
      break;
    case AceButton::kEventLongPressed:
      timer.enable(timerId2);
      timer.disable(timerId);
      break;
  }
}


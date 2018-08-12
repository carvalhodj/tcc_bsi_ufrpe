#include <Wire.h>
#include <DS1307RTC.h>
#include <Time.h>
#include <TimeLib.h>
#include <TimeAlarms.h>
#include <AceButton.h>
#include <AdjustableButtonConfig.h>
#include <ButtonConfig.h>
#include "ACS712.h"
#include <ESP8266WiFi.h>
#include "ESP8266FtpServer.h"

#define LED_POWER   D3
#define BUTTON_PIN  D5

using namespace ace_button;

ACS712 sensor(ACS712_30A, A0);
//DS1307 rtc(D2, D1);
tmElements_t tm;
FtpServer ftp_srv;
AceButton button(BUTTON_PIN);

const char *ssid              = "JuaWMS";
const char *password          = "";
const char *ftp_user          = "juawms";
const char *ftp_password      = "juawms";
const double V                = 220;
const double treshold         = 1.0;
bool ligado                   = false;
bool telemetria               = false;
bool servidor_ftp             = false;
int led_state                 = HIGH;
unsigned int interval         = 1000;
unsigned int previous_millis  = 0;
AlarmId alarm_id_sensor;

void modo_telemetria();
void modo_sensor();
void blink_led();
void led_on();
void handle_event(AceButton*, uint8_t, uint8_t);

void setup() {
    pinMode(LED_POWER, OUTPUT);
    digitalWrite(LED_POWER, led_state);
    
    pinMode(BUTTON_PIN, INPUT_PULLUP);

    ButtonConfig* button_config = button.getButtonConfig();
    button_config->setEventHandler(handle_event);
    button_config->setFeature(ButtonConfig::kFeatureClick);
    button_config->setFeature(ButtonConfig::kFeatureLongPress);

    bool parse = false;
    bool config = false;

// get the date and time the compiler was run
//    if (getDate(__DATE__) && getTime(__TIME__)) 
//    {
//      parse = true;
//      // and configure the RTC with this info
//      if (RTC.write(tm))
//      {
//        config = true;
//      }
//    }
  
    Serial.begin(115200);
    //while (!Serial) ; // wait for Arduino Serial Monitor
    //delay(200);
    if (parse && config) 
    {
      Serial.print("DS1307 configured Time=");
      Serial.print(__TIME__);
      Serial.print(", Date=");
      Serial.println(__DATE__);
    }

    if (SPIFFS.begin())
    {
      Serial.println("SPIFFS opened!");
//    SPIFFS.format();
      ftp_srv.begin(ftp_user, ftp_password);
      File f = SPIFFS.open("/coleta.csv", "a");
      f.println("<<< SISTEMA INICIADO >>>");
      f.close();
    }
    
    Serial.println("Aguarde. Calibrando...");
    sensor.calibrate();
    Serial.println("Fim da calibração");

    alarm_id_sensor = Alarm.timerRepeat(1, modo_sensor);
}

void loop() 
{
  button.check();
  if (telemetria)
  {
    blink_led();
    modo_telemetria();
  }
  else
  {
    led_on();
  }
}

void modo_telemetria()
{
  // Desabilitando as chamadas da função de escrita no arquivo
  
  // Inicializando o servidor FTP
  if (!servidor_ftp)
  {
    ftp_srv.begin(ftp_user, ftp_password);
    servidor_ftp = true; 
  }
  ftp_srv.handleFTP();
  if (WiFi.status() != WL_CONNECTED)
    WiFi.begin(ssid, password); 
}

void modo_sensor()
{
  float I = sensor.getCurrentAC(60);
  if (I > treshold && !ligado)
  {
    // Formato: TIMESTAMP,<ON/OFF>,CORRENTE(A)
    File f = SPIFFS.open("/coleta.csv", "a");
//    f.print(rtc.getTimeStr());
//    f.print(",");
//    f.print(rtc.getDateStr(FORMAT_SHORT));
    f.print(",");
    f.print("1");
    f.print(",");
    f.println(I);
    f.close();
    ligado = true;
    Serial.println(String("Corrente = ") + I + " A");
  }
  else if (I < treshold && ligado)
  {
    File f = SPIFFS.open("/coleta.csv", "a");
//    f.print(rtc.getTimeStr());
//    f.print(",");
//    f.print(rtc.getDateStr(FORMAT_SHORT));
    f.print(",");
    f.print("0");
    f.print(",");
    f.println(I);
    f.close();
    ligado = false;
    Serial.println(String("Corrente = ") + I + " A");
  }
  Serial.print("Corrente (A): ");
  Serial.println(I);
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
    servidor_ftp = false;
}

void handle_event(AceButton* /* button */, uint8_t eventType, uint8_t buttonState)
{

  // Print out a message for all events.
  Serial.print(F("handleEvent(): eventType: "));
  Serial.print(eventType);
  Serial.print(F("; buttonState: "));
  Serial.println(buttonState);

  switch (eventType)
  {
    case AceButton::kEventClicked:
      telemetria = false;
      Alarm.enable(alarm_id_sensor);
      break;
    case AceButton::kEventLongPressed:
      telemetria = true;
      Alarm.disable(alarm_id_sensor);
//      modo_telemetria();
      break;
  }
}


#include <DS1307.h>
#include "ACS712.h"

#include <ESP8266WiFi.h>
#include "ESP8266FtpServer.h"

#define LED_POWER D3

ACS712 sensor(ACS712_30A, A0);
DS1307 rtc(D2, D1);
FtpServer ftp_srv;

const char *ssid          = "JuaWMS";
const char *password      = "juawms2018";
const float V             = 220;
const float treshold      = 1.0;
bool ligado               = false;

unsigned int tempo_atual  = millis();

void setup() {
    pinMode(LED_POWER, OUTPUT);
    digitalWrite(LED_POWER, HIGH);
     //Aciona o relogio
    rtc.halt(false);
    
    //As linhas abaixo setam a data e hora do modulo
    //e podem ser comentada apos a primeira utilizacao
//    rtc.setDOW(FRIDAY);      //Define o dia da semana
//    rtc.setTime(22, 49, 0);     //Define o horario
//    rtc.setDate(27, 7, 2018);   //Define o dia, mes e ano
    
    //Definicoes do pino SQW/Out
    rtc.setSQWRate(SQW_RATE_1);
    rtc.enableSQW(true);
    
    Serial.begin(115200);

    // Configurando o access point
    WiFi.softAP(ssid, password);
    IPAddress myIP = WiFi.softAPIP();
    Serial.print("AP IP address: ");
    Serial.println(myIP);
    
    if (SPIFFS.begin())
    {
      Serial.println("SPIFFS opened!");
//      SPIFFS.format();
      ftp_srv.begin("juawms", "juawms");
      File f = SPIFFS.open("/coleta.csv", "a");
      f.println("<<< SISTEMA INICIADO >>>");
      f.close();
    }
    Serial.println("Aguarde. Calibrando...");
    sensor.calibrate();
    Serial.println("Fim da calibração");
}

void loop() {
    ftp_srv.handleFTP();
    
    if (millis() - tempo_atual >= 1000)
    {
      float I = sensor.getCurrentAC(60);
      if (I > treshold && !ligado)
      {
        File f = SPIFFS.open("/coleta.csv", "a");
        f.print(rtc.getTimeStr());
        f.print(",");
        f.print(rtc.getDateStr(FORMAT_SHORT));
        f.print(",");
        f.println("1");
        f.close();
        tempo_atual = millis();
        ligado = true;
        Serial.println(String("Corrente = ") + I + " A");
      }
      else if (I < treshold && ligado)
      {
        File f = SPIFFS.open("/coleta.csv", "a");
        f.print(rtc.getTimeStr());
        f.print(",");
        f.print(rtc.getDateStr(FORMAT_SHORT));
        f.print(",");
        f.println("0");
        f.close();
        tempo_atual = millis();
        ligado = false;
        Serial.println(String("Corrente = ") + I + " A");
      }
      else
      {
        tempo_atual = millis();
      }
      Serial.print("Corrente (A): ");
      Serial.println(I);
    }
}


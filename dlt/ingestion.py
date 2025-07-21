import dlt
from dlt.sources.helpers.rest_client import RESTClient
from datetime import datetime, timedelta
import time
import logging

# Configurar logging do DLT
logger = logging.getLogger(__name__)

API_KEY = "0dc3b5990e776e55b24bd7518b245eca"
HISTORY_API_URL = "https://history.openweathermap.org/data/2.5/history/city"

CITIES = [
    # Região Sul
    {"name": "Porto Alegre", "lat": -30.0346, "lon": -51.2177},
    {"name": "Curitiba", "lat": -25.4289, "lon": -49.2671},
    {"name": "Florianópolis", "lat": -27.5969, "lon": -48.5495},
    {"name": "Pelotas", "lat": -31.7719, "lon": -52.3428},
    {"name": "Caxias do Sul", "lat": -29.1686, "lon": -51.1794}
]

def fetch_historical_weather_data():
    client = RESTClient(base_url=HISTORY_API_URL)
    
    total_cities = len(CITIES)
    total_days = 3
    
    logger.info("🚀 Iniciando ingestão de dados climáticos históricos")
    logger.info(f"📊 Total de cidades: {total_cities}")
    logger.info(f"📅 Período: {total_days} dias (dados diários)")
    logger.info(f"🔗 API: OpenWeatherMap History")
    logger.info("-" * 60)
    
    for city_index, city in enumerate(CITIES, 1):
        try:
            logger.info(f"🏙️  [{city_index}/{total_cities}] Processando {city['name']}...")
            
            city_data_count = 0
            
            # Para dados diários, você pode buscar períodos maiores
            for day_offset in range(total_days):
                try:
                    target_date = datetime.now() - timedelta(days=day_offset)
                    
                    # Para dados diários, usa o dia inteiro
                    start_timestamp = int(target_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
                    end_timestamp = int(target_date.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())
                    
                    params = {
                        "lat": city["lat"],
                        "lon": city["lon"],
                        "type": "daily",  # Mudou de "hour" para "daily"
                        "start": start_timestamp,
                        "end": end_timestamp,
                        "appid": API_KEY
                    }
                    
                    # Log a cada 30 dias para não poluir muito
                    if day_offset % 30 == 0:
                        logger.info(f"   📅 Buscando dados de {target_date.strftime('%Y-%m-%d')} (dia {day_offset + 1}/{total_days})")
                    
                    data = client.get("", params=params).json()
                    
                    if data.get("cod") == "200" and "list" in data:
                        for daily_data in data["list"]:
                            city_data_count += 1
                            yield {
                                "city_name": city["name"],
                                "city_id": data.get("city_id"),
                                "dt": daily_data["dt"],
                                "dt_iso": datetime.fromtimestamp(daily_data["dt"]).strftime("%Y-%m-%d %H:%M:%S"),
                                "date": datetime.fromtimestamp(daily_data["dt"]).strftime("%Y-%m-%d"),
                                "main": daily_data["main"],
                                "wind": daily_data["wind"],
                                "clouds": daily_data["clouds"],
                                "weather": daily_data["weather"],
                                "rain": daily_data.get("rain", {}),
                                "snow": daily_data.get("snow", {}),
                                "calctime": data.get("calctime"),
                                "cnt": data.get("cnt")
                            }
                    else:
                        error_msg = data.get('message', 'Erro desconhecido')
                        logger.warning(f"   ⚠️  Erro na resposta para {city['name']} - {target_date.strftime('%Y-%m-%d')}: {error_msg}")
                        
                        # Se for erro de período fora do range, para de buscar essa cidade
                        if "out of allowed range" in error_msg:
                            logger.warning(f"   🛑 Parando busca para {city['name']} - dados fora do período disponível")
                            break
                    
                    time.sleep(0.01)  # Rate limiting
                    
                except Exception as day_error:
                    logger.error(f"   ❌ Erro ao processar dia {day_offset + 1} para {city['name']}: {str(day_error)}")
                    continue  # Continua para o próximo dia
            
            logger.info(f"   ✅ {city['name']} concluído - {city_data_count} registros coletados")
            
        except Exception as city_error:
            logger.error(f"❌ Erro crítico ao processar cidade {city['name']}: {str(city_error)}")
            continue  # Continua para a próxima cidade
    
    logger.info("-" * 60)
    logger.info("🎉 Processamento concluído!")

# Configuração do pipeline
pipeline = dlt.pipeline(
    pipeline_name="dados_clima_historico",
    destination="postgres",
    dataset_name="dados_climaticos"
)

@dlt.resource(
    name="dados_clima_historico_daily", 
    write_disposition="merge", 
    primary_key=["city_name", "date"]
)
def weather_resource():
    yield from fetch_historical_weather_data()

if __name__ == "__main__":
    logger.info("🚀 Iniciando pipeline DLT...")
    load_info = pipeline.run(weather_resource())
    logger.info("✅ Pipeline concluído!")
    logger.info(f"📊 Informações do carregamento: {load_info}")
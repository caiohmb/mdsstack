import dlt
from dlt.sources.helpers.rest_client import RESTClient
from datetime import datetime, timedelta

# Configurações (poderiam vir de dlt.config)
API_URL = "https://archive-api.open-meteo.com/v1/archive"
CITIES = [
    {"name": "Porto Alegre", "lat": -30.0346, "lon": -51.2177},
    # ...adicione outras cidades
]
DAYS_BACK = 30

def fetch_weather_data():
    delay = 3
    end_date = datetime.now() - timedelta(days=delay)
    start_date = end_date - timedelta(days=DAYS_BACK)
    client = RESTClient(base_url=API_URL)
    for city in CITIES:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "America/Sao_Paulo"
        }
        data = client.get("", params=params).json()
        for i, date in enumerate(data["daily"]["time"]):
            yield {
                "city": city["name"],
                "date": date,
                "temp_max": data["daily"]["temperature_2m_max"][i],
                "temp_min": data["daily"]["temperature_2m_min"][i],
                "precipitation": data["daily"]["precipitation_sum"][i]
            }

@dlt.resource(write_disposition="merge", primary_key=["city", "date"])
def weather_resource():
    yield from fetch_weather_data()

pipeline = dlt.pipeline(
    pipeline_name="clima_rs",
    destination="postgres",
    dataset_name="dados_climaticos"
)

if __name__ == "__main__":
    load_info = pipeline.run(weather_resource())
    print(load_info)
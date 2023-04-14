from airflow.utils.task_group import TaskGroup
from include.process_city_weather_taskgroup import process_city_weather_taskgroup

PROCESS_CITIES_WEATHER_GROUP_ID="process_cities_weather"

def process_cities_weather_taskgroup(dag, cities):
    with TaskGroup(group_id = PROCESS_CITIES_WEATHER_GROUP_ID, dag = dag) as process_cities_weather:
        for city in cities:
            process_city_weather_taskgroup(dag, PROCESS_CITIES_WEATHER_GROUP_ID, f"process_{ city['name'] }_weather", city)
                    
    return process_cities_weather


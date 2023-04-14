# Configurations
Airflow uses CITIES variable to get the list of cities: 
```
[ 
    {"name":"Lviv", "lat": 49.841952, "lon": 24.0315921},
    {"name":"Kyiv", "lat": 50.4500336, "lon": 30.5241361},
    {"name":"Kharkiv", "lat": 49.9923181, "lon": 36.2310146},
    {"name":"Odesa", "lat": 46.4825, "lon": 30.7233},
    {"name":"Zhmerynka", "lat": 49.0354593, "lon": 28.1147317},
    {"name":"Ternopil", "lat": 49.5557716, "lon": 25.591886}
]
```
Add new city to the list for which to collect data.
# Weather Data Pipeline

![image](https://user-images.githubusercontent.com/25819135/232031151-9987e8ea-8805-4033-bb6a-f3757a8f051f.png)

# Results
![image](https://user-images.githubusercontent.com/25819135/232031420-25c6fb6c-a672-4384-914b-f7f6bc5b1e4f.png)
Note, that for the catchup pipeline the 'extract_history_data' task was run, while for today's pipeline 'extract_current_data' task ran   
![image](https://user-images.githubusercontent.com/25819135/232031508-c4966cc3-d95e-41a0-82bd-c07c6fbda7f1.png)



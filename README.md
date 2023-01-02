# ETL-Spotify

<img src="https://user-images.githubusercontent.com/110300201/210246234-424de5bf-8808-46cc-8e1c-648f3cf755ca.png" width=75% height=75%>

This ETL takes data from Spotify's API and loads it into a SQLite database.   
The python script is scheduled by Task Scheduler from Windows.

In production, we could run this script in the cloud with an orchestration platform such as Apache Airflow.   
We could also use PostgreSQL which is more suitable for production.

# Apache-Airflow-DAGs

Collection of Apache Airflow DAGs

This is a place I document my journey of praticising the use of Apache Airflow.

<h4>Gdynia Air Quality</h4>
Using the <a href="https://www.iqair.com">iQAir API</a> I gather some stats about Gdynia (Poland):
- location coordinates
- timestamp and current pollution level in US aqius
- timestamp and current temperature in Celsius
After the API request, the data is transformed and sent to a PostgresSQL database

<i>AirQualityETL.py</i>

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import pymysql
import smtplib
from email.mime.text import MIMEText
import os

# ---------- CONFIGURATION ----------
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

CITIES = ["Karachi", "Peshawar", "Multan", "Sukkur", "Hyderabad"]

# ---------- TASK 1: Read FAO CSV ----------
def read_fao_csv(**context):
    csv_path = "/opt/airflow/dags/pak-flood-events-fao-eve.csv"

    try:
        df = pd.read_csv(csv_path)

        print("✅ CSV loaded! Rows:", len(df))
        print("Columns:", df.columns.tolist())

        context['ti'].xcom_push(
            key='fao_data',
            value=df.to_json()
        )

        return "CSV read successful"

    except Exception as e:
        print("❌ CSV Error:", e)
        raise e


# ---------- TASK 2: Fetch Weather ----------
def fetch_weather_data(**context):

    weather_results = []

    for city in CITIES:

        url = (
            f"http://api.openweathermap.org/data/2.5/weather?"
            f"q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
        )

        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 200:

                data = response.json()

                weather_results.append({
                    'city': city,
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'rainfall': data.get('rain', {}).get('1h', 0),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

                print(f"✅ {city} weather done")

            else:
                print(f"❌ {city} failed: {response.status_code}")

        except Exception as e:
            print(f"❌ {city} error: {e}")

    context['ti'].xcom_push(
        key='weather_data',
        value=weather_results
    )

    return "Weather fetch complete"


# ---------- TASK 3: Validate Weather Data ----------
def validate_weather_data(**context):

    ti = context['ti']

    weather_data = ti.xcom_pull(
        key='weather_data',
        task_ids='fetch_weather_data'
    )

    if not weather_data:
        raise ValueError("❌ Weather data empty! Validation failed.")

    expected_cities = set(CITIES)

    received_cities = set([
        item['city'] for item in weather_data
    ])

    missing_cities = expected_cities - received_cities

    if missing_cities:
        print(f"⚠️ Warning: Missing cities - {missing_cities}")
    else:
        print("✅ All cities data received.")

    for record in weather_data:

        temp = record.get('temperature')
        humidity = record.get('humidity')

        if temp is None or temp < -20 or temp > 60:
            raise ValueError(
                f"❌ Invalid temperature for {record['city']}: {temp}"
            )

        if humidity is None or humidity < 0 or humidity > 100:
            raise ValueError(
                f"❌ Invalid humidity for {record['city']}: {humidity}"
            )

    print("✅ Validation passed.")

    context['ti'].xcom_push(
        key='validated_data',
        value=weather_data
    )

    return "Validation complete"


# ---------- TASK 4: Calculate Risk ----------
def calculate_risk_score(**context):

    ti = context['ti']

    fao_json = ti.xcom_pull(
        key='fao_data',
        task_ids='read_fao_csv'
    )

    weather_data = ti.xcom_pull(
        key='validated_data',
        task_ids='validate_weather_data'
    )

    if not fao_json or not weather_data:
        raise ValueError("Missing data!")

    weather_df = pd.DataFrame(weather_data)

    for idx, row in weather_df.iterrows():

        risk_score = 0

        # Temperature
        if row['temperature'] > 35:
            risk_score += 3
        elif row['temperature'] > 30:
            risk_score += 2

        # Humidity
        if row['humidity'] > 80:
            risk_score += 3
        elif row['humidity'] > 70:
            risk_score += 2

        # Rainfall
        if row['rainfall'] > 5:
            risk_score += 4
        elif row['rainfall'] > 0:
            risk_score += 2

        weather_df.at[idx, 'risk_score'] = risk_score

        if risk_score >= 7:
            weather_df.at[idx, 'risk_level'] = 'HIGH'
        elif risk_score >= 4:
            weather_df.at[idx, 'risk_level'] = 'MEDIUM'
        else:
            weather_df.at[idx, 'risk_level'] = 'LOW'

    context['ti'].xcom_push(
        key='results',
        value=weather_df.to_json()
    )

    print("✅ Risk calculated")

    print(
        weather_df[
            [
                'city',
                'temperature',
                'humidity',
                'risk_score',
                'risk_level'
            ]
        ]
    )

    return "Risk calculation complete"


# ---------- TASK 5: Store in MySQL ----------
def store_in_mysql(**context):

    ti = context['ti']

    results_json = ti.xcom_pull(
        key='results',
        task_ids='calculate_risk_score'
    )

    if not results_json:
        raise ValueError("No results to store!")

    results_df = pd.read_json(results_json)

    connection = pymysql.connect(
        host='mysql',
        user='root',
        password=MYSQL_PASSWORD,
        database='flood_db',
        port=3306
    )

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flood_risk (
            id INT AUTO_INCREMENT PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            humidity FLOAT,
            rainfall FLOAT,
            risk_score INT,
            risk_level VARCHAR(20),
            timestamp DATETIME
        )
    """)

    for _, row in results_df.iterrows():

        cursor.execute("""
            INSERT INTO flood_risk (
                city,
                temperature,
                humidity,
                rainfall,
                risk_score,
                risk_level,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['city'],
            row['temperature'],
            row['humidity'],
            row['rainfall'],
            row['risk_score'],
            row['risk_level'],
            row['timestamp']
        ))

    connection.commit()

    cursor.close()
    connection.close()

    print(f"✅ {len(results_df)} records stored in MySQL!")

    return "Storage complete"


# ---------- TASK 6: Generate Quality Report ----------
def generate_quality_report(**context):

    ti = context['ti']

    results_json = ti.xcom_pull(
        key='results',
        task_ids='calculate_risk_score'
    )

    if not results_json:
        print("❌ No results found!")
        return

    df = pd.read_json(results_json)

    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_cities': len(df),
        'high_risk_count': len(df[df['risk_level'] == 'HIGH']),
        'medium_risk_count': len(df[df['risk_level'] == 'MEDIUM']),
        'low_risk_count': len(df[df['risk_level'] == 'LOW']),
        'avg_temperature': round(df['temperature'].mean(), 2),
        'avg_humidity': round(df['humidity'].mean(), 2),
        'avg_rainfall': round(df['rainfall'].mean(), 2),
        'avg_risk_score': round(df['risk_score'].mean(), 2)
    }

    report_df = pd.DataFrame([report])

    report_path = "/opt/airflow/data/quality_report.csv"

    if os.path.exists(report_path):

        existing = pd.read_csv(report_path)

        final = pd.concat(
            [existing, report_df],
            ignore_index=True
        )

        final.to_csv(report_path, index=False)

    else:
        report_df.to_csv(report_path, index=False)

    print(f"✅ Quality report saved at {report_path}")

    print(report_df.to_string(index=False))

    return "Quality report generated"


# ---------- TASK 7: Prepare Email ----------
def prepare_email(**context):

    ti = context['ti']

    results_json = ti.xcom_pull(
        key='results',
        task_ids='calculate_risk_score'
    )

    if not results_json:

        context['ti'].xcom_push(
            key='email_body',
            value="No data"
        )

        return

    df = pd.read_json(results_json)

    high_risk = df[df['risk_level'] == 'HIGH']

    if not high_risk.empty:

        body = f"""
        <h2>Daily Flood Risk Report - Pakistan</h2>

        <p>
        <b>High Risk Cities:</b>
        {', '.join(high_risk['city'].tolist())}
        </p>

        <p>
        <b>Average Risk Score:</b>
        {df['risk_score'].mean():.2f}
        </p>

        <p>Please take necessary precautions.</p>
        """

    else:
        body = "<p>No high flood risk detected today.</p>"

    context['ti'].xcom_push(
        key='email_body',
        value=body
    )


# ---------- TASK 8: Send Email ----------
def send_email_via_smtplib(**context):

    ti = context['ti']

    email_html = ti.xcom_pull(
        key='email_body',
        task_ids='prepare_email_content'
    )

    if not email_html:
        email_html = "<p>No report generated.</p>"

    sender = EMAIL_SENDER
    password = EMAIL_PASSWORD
    receiver = EMAIL_SENDER

    msg = MIMEText(email_html, 'html')

    msg['Subject'] = '🌊 Daily Flood Risk Report'
    msg['From'] = sender
    msg['To'] = receiver

    try:

        with smtplib.SMTP('smtp.gmail.com', 587) as server:

            server.starttls()

            server.login(sender, password)

            server.send_message(msg)

        print("✅ Email sent successfully!")

    except Exception as e:
        print(f"❌ Email failed: {e}")
        raise e


# ---------- DAG Definition ----------
default_args = {
    'owner': 'flood_monitor',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='flood_risk_monitor_pakistan',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=False
)

# ---------- Tasks ----------
t1 = PythonOperator(
    task_id='read_fao_csv',
    python_callable=read_fao_csv,
    dag=dag
)

t2 = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)

t_validate = PythonOperator(
    task_id='validate_weather_data',
    python_callable=validate_weather_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='calculate_risk_score',
    python_callable=calculate_risk_score,
    dag=dag
)

t4 = PythonOperator(
    task_id='store_in_mysql',
    python_callable=store_in_mysql,
    dag=dag
)

t_quality = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

t_prep_email = PythonOperator(
    task_id='prepare_email_content',
    python_callable=prepare_email,
    dag=dag
)

t_send_email = PythonOperator(
    task_id='send_email_direct',
    python_callable=send_email_via_smtplib,
    dag=dag
)

# ---------- Dependencies ----------
t1 >> t2 >> t_validate >> t3 >> t4 >> t_quality >> t_prep_email >> t_send_email
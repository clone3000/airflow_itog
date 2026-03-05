from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd

def create_mart_users():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS mart_user_activity;
            
            CREATE TABLE mart_user_activity AS
            SELECT 
                user_id,
                COUNT(*) as session_count,
                ROUND(AVG(session_duration)) as avg_session_min,
                ROUND(SUM(session_duration)) as total_time_min,
                ROUND(AVG(array_length(string_to_array(pages_visited, ','), 1))) as avg_pages,
                MAX(pages_visited) as last_pages,
                MIN(start_time) as first_visit,
                MAX(end_time) as last_visit
            FROM user_sessions
            GROUP BY user_id
            ORDER BY session_count DESC;
        """))
    
    df = pd.read_sql("SELECT COUNT(*) as cnt FROM mart_user_activity", engine)
    print(f"Витрина пользователей: {df['cnt'][0]} записей")
    
    engine.dispose()

def create_mart_support():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS mart_support_stats;
            
            CREATE TABLE mart_support_stats AS
            SELECT 
                status,
                COUNT(*) as ticket_count,
                ROUND(CAST(AVG(resolution_hours) AS numeric), 1) as avg_resolution_hours,
                MIN(resolution_hours) as min_hours,
                MAX(resolution_hours) as max_hours
            FROM support_tickets
            GROUP BY status
            ORDER BY ticket_count DESC;
        """))
        
        conn.execute(text("""
            DROP TABLE IF EXISTS mart_issue_types;
            
            CREATE TABLE mart_issue_types AS
            SELECT 
                issue_type,
                COUNT(*) as ticket_count,
                SUM(CASE WHEN status IN ('resolved', 'closed') THEN 1 ELSE 0 END) as resolved,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open,
                ROUND(CAST(AVG(CASE WHEN status IN ('resolved', 'closed') THEN resolution_hours END) AS numeric), 1) as avg_resolution
            FROM support_tickets
            GROUP BY issue_type
            ORDER BY ticket_count DESC;
        """))
    
    df1 = pd.read_sql("SELECT COUNT(*) as cnt FROM mart_support_stats", engine)
    df2 = pd.read_sql("SELECT COUNT(*) as cnt FROM mart_issue_types", engine)
    print(f"Витрина поддержки: {df1['cnt'][0]} статусов, {df2['cnt'][0]} типов проблем")
    
    engine.dispose()

def check_marts():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    marts = ['mart_user_activity', 'mart_support_stats', 'mart_issue_types']
    
    print("\nПроверка витрин:")
    with engine.connect() as conn:
        for mart in marts:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {mart}"))
                count = result.scalar()
                print(f"  {mart}: {count} записей")
            except:
                print(f"  {mart}: не создана")
    
    try:
        top = pd.read_sql("""
            SELECT user_id, session_count, total_time_min 
            FROM mart_user_activity 
            LIMIT 5
        """, engine)
        print("\nТоп пользователей:")
        for _, row in top.iterrows():
            print(f"  {row['user_id']}: {row['session_count']} сессий, {row['total_time_min']} мин")
    except:
        pass
    
    try:
        stats = pd.read_sql("SELECT * FROM mart_support_stats", engine)
        print("\nСтатусы тикетов:")
        for _, row in stats.iterrows():
            print(f"  {row['status']}: {row['ticket_count']} шт, ср.{row['avg_resolution_hours']} ч")
    except:
        pass
    
    engine.dispose()

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'analytics_marts',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    mart1 = PythonOperator(
        task_id='create_user_activity_mart',
        python_callable=create_mart_users
    )
    
    mart2 = PythonOperator(
        task_id='create_support_mart',
        python_callable=create_mart_support
    )
    
    check = PythonOperator(
        task_id='check_marts',
        python_callable=check_marts
    )
    
    start >> [mart1, mart2] >> check >> end
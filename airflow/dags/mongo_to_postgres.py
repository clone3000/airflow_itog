from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import pandas as pd

def create_tables():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR(100) PRIMARY KEY,
                user_id VARCHAR(50),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                device VARCHAR(20),
                pages_visited TEXT,
                actions TEXT,
                session_duration INT,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_logs (
                event_id VARCHAR(100) PRIMARY KEY,
                timestamp TIMESTAMP,
                event_type VARCHAR(50),
                details TEXT,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                ticket_id VARCHAR(100) PRIMARY KEY,
                user_id VARCHAR(50),
                status VARCHAR(20),
                issue_type VARCHAR(50),
                message_count INT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                resolution_hours FLOAT,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_recommendations (
                user_id VARCHAR(50) PRIMARY KEY,
                recommended_products TEXT,
                product_count INT,
                last_updated TIMESTAMP,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS moderation_queue (
                review_id VARCHAR(100) PRIMARY KEY,
                user_id VARCHAR(50),
                product_id VARCHAR(50),
                review_text TEXT,
                rating INT,
                moderation_status VARCHAR(20),
                flags TEXT,
                submitted_at TIMESTAMP,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS replication_log (
                log_id SERIAL PRIMARY KEY,
                collection_name VARCHAR(50),
                records_copied INT,
                copy_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    
    engine.dispose()
    print("Таблицы созданы")

def copy_sessions():
    mongo = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    db = mongo['etl_project']
    sessions = list(db.user_sessions.find({}, {'_id': 0}))
    
    if not sessions:
        print("Нет сессий")
        return
    
    data = []
    for s in sessions:
        start = pd.to_datetime(s['start_time'].replace('Z', '+00:00'))
        end = pd.to_datetime(s['end_time'].replace('Z', '+00:00'))
        
        data.append({
            'session_id': s['session_id'],
            'user_id': s['user_id'],
            'start_time': start,
            'end_time': end,
            'device': s.get('device', 'unknown'),
            'pages_visited': ','.join(s.get('pages_visited', [])),
            'actions': ','.join(s.get('actions', [])),
            'session_duration': int((end - start).total_seconds() / 60)
        })
    
    df = pd.DataFrame(data)
    
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM user_sessions"))
    
    df.to_sql('user_sessions', pg, if_exists='append', index=False)
    
    with pg.begin() as conn:
        conn.execute(
            text("INSERT INTO replication_log (collection_name, records_copied) VALUES (:name, :count)"),
            {'name': 'user_sessions', 'count': len(df)}
        )
    
    print(f"Сессий: {len(df)}")
    mongo.close()
    pg.dispose()

def copy_events():
    mongo = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    db = mongo['etl_project']
    events = list(db.event_logs.find({}, {'_id': 0}))
    
    if not events:
        print("Нет событий")
        return
    
    data = []
    for e in events:
        data.append({
            'event_id': e['event_id'],
            'timestamp': pd.to_datetime(e['timestamp'].replace('Z', '+00:00')),
            'event_type': e['event_type'],
            'details': str(e.get('details', {}))
        })
    
    df = pd.DataFrame(data)
    
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM event_logs"))
    
    df.to_sql('event_logs', pg, if_exists='append', index=False)
    
    with pg.begin() as conn:
        conn.execute(
            text("INSERT INTO replication_log (collection_name, records_copied) VALUES (:name, :count)"),
            {'name': 'event_logs', 'count': len(df)}
        )
    
    print(f"Событий: {len(df)}")
    mongo.close()
    pg.dispose()

def copy_tickets():
    mongo = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    db = mongo['etl_project']
    tickets = list(db.support_tickets.find({}, {'_id': 0}))
    
    if not tickets:
        print("Нет тикетов")
        return
    
    data = []
    for t in tickets:
        created = pd.to_datetime(t['created_at'].replace('Z', '+00:00'))
        updated = pd.to_datetime(t['updated_at'].replace('Z', '+00:00'))
        
        data.append({
            'ticket_id': t['ticket_id'],
            'user_id': t['user_id'],
            'status': t['status'],
            'issue_type': t['issue_type'],
            'message_count': len(t.get('messages', [])),
            'created_at': created,
            'updated_at': updated,
            'resolution_hours': round((updated - created).total_seconds() / 3600, 2)
        })
    
    df = pd.DataFrame(data)
    
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM support_tickets"))
    
    df.to_sql('support_tickets', pg, if_exists='append', index=False)
    
    with pg.begin() as conn:
        conn.execute(
            text("INSERT INTO replication_log (collection_name, records_copied) VALUES (:name, :count)"),
            {'name': 'support_tickets', 'count': len(df)}
        )
    
    print(f"Тикетов: {len(df)}")
    mongo.close()
    pg.dispose()

def copy_recommendations():
    mongo = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    db = mongo['etl_project']
    recs = list(db.user_recommendations.find({}, {'_id': 0}))
    
    if not recs:
        print("Нет рекомендаций")
        return
    
    data = []
    for r in recs:
        data.append({
            'user_id': r['user_id'],
            'recommended_products': ','.join(r.get('recommended_products', [])),
            'product_count': len(r.get('recommended_products', [])),
            'last_updated': pd.to_datetime(r['last_updated'].replace('Z', '+00:00'))
        })
    
    df = pd.DataFrame(data)
    
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM user_recommendations"))
    
    df.to_sql('user_recommendations', pg, if_exists='append', index=False)
    
    with pg.begin() as conn:
        conn.execute(
            text("INSERT INTO replication_log (collection_name, records_copied) VALUES (:name, :count)"),
            {'name': 'user_recommendations', 'count': len(df)}
        )
    
    print(f"Рекомендаций: {len(df)}")
    mongo.close()
    pg.dispose()

def copy_reviews():
    mongo = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    db = mongo['etl_project']
    reviews = list(db.moderation_queue.find({}, {'_id': 0}))
    
    if not reviews:
        print("Нет отзывов")
        return
    
    data = []
    for r in reviews:
        data.append({
            'review_id': r['review_id'],
            'user_id': r['user_id'],
            'product_id': r['product_id'],
            'review_text': r['review_text'],
            'rating': r['rating'],
            'moderation_status': r['moderation_status'],
            'flags': ','.join(r.get('flags', [])),
            'submitted_at': pd.to_datetime(r['submitted_at'].replace('Z', '+00:00'))
        })
    
    df = pd.DataFrame(data)
    
    with pg.begin() as conn:
        conn.execute(text("DELETE FROM moderation_queue"))
    
    df.to_sql('moderation_queue', pg, if_exists='append', index=False)
    
    with pg.begin() as conn:
        conn.execute(
            text("INSERT INTO replication_log (collection_name, records_copied) VALUES (:name, :count)"),
            {'name': 'moderation_queue', 'count': len(df)}
        )
    
    print(f"Отзывов: {len(df)}")
    mongo.close()
    pg.dispose()

def check_copy():
    pg = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    tables = ['user_sessions', 'event_logs', 'support_tickets', 
              'user_recommendations', 'moderation_queue']
    
    print("\nДанные в PostgreSQL:")
    with pg.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table}: {count}")
        
        result = conn.execute(text("SELECT * FROM replication_log ORDER BY copy_time DESC LIMIT 5"))
        logs = result.fetchall()
        print("\nПоследние копирования:")
        for log in logs:
            print(f"  {log[1]}: {log[2]} записей")
    
    pg.dispose()

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'mongo_to_postgres',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    create = PythonOperator(task_id='create_tables', python_callable=create_tables)
    
    t1 = PythonOperator(task_id='copy_sessions', python_callable=copy_sessions)
    t2 = PythonOperator(task_id='copy_events', python_callable=copy_events)
    t3 = PythonOperator(task_id='copy_tickets', python_callable=copy_tickets)
    t4 = PythonOperator(task_id='copy_recommendations', python_callable=copy_recommendations)
    t5 = PythonOperator(task_id='copy_reviews', python_callable=copy_reviews)
    
    check = PythonOperator(task_id='check_copy', python_callable=check_copy)
    
    start >> create >> [t1, t2, t3, t4, t5] >> check >> end
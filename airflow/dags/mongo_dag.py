from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import random
import uuid

def generate_sessions():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    db.user_sessions.delete_many({})
    
    devices = ['mobile', 'desktop', 'tablet']
    pages = ['/home', '/products', '/cart', '/profile', '/support', '/payment']
    actions = ['login', 'view', 'add', 'remove', 'logout', 'search']
    
    sessions = []
    for i in range(200):
        user_id = f"user_{random.randint(1, 50)}"
        start = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        end = start + timedelta(minutes=random.randint(1, 60))
        
        session = {
            "session_id": f"sess_{uuid.uuid4().hex[:8]}",
            "user_id": user_id,
            "start_time": start.isoformat() + "Z",
            "end_time": end.isoformat() + "Z",
            "pages_visited": random.sample(pages, random.randint(1, 5)),
            "device": random.choice(devices),
            "actions": random.sample(actions, random.randint(2, 5))
        }
        sessions.append(session)
    
    if sessions:
        db.user_sessions.insert_many(sessions)
    
    print(f"Сессий: {len(sessions)}")
    client.close()

def generate_events():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    db.event_logs.delete_many({})
    
    event_types = ['click', 'scroll', 'view', 'error', 'purchase']
    
    events = []
    for i in range(500):
        event = {
            "event_id": f"evt_{uuid.uuid4().hex[:8]}",
            "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30),
                                                    hours=random.randint(0, 23))).isoformat() + "Z",
            "event_type": random.choice(event_types),
            "details": {"page": random.choice(['/home', '/products', '/cart'])}
        }
        events.append(event)
    
    if events:
        db.event_logs.insert_many(events)
    
    print(f"Событий: {len(events)}")
    client.close()

def generate_tickets():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    db.support_tickets.delete_many({})
    
    statuses = ['open', 'in_progress', 'resolved', 'closed']
    issue_types = ['payment', 'technical', 'account', 'product', 'delivery']
    
    tickets = []
    for i in range(50):
        created = datetime.now() - timedelta(days=random.randint(0, 20))
        
        messages = []
        for j in range(random.randint(1, 4)):
            msg_time = created + timedelta(hours=j * random.randint(1, 24))
            messages.append({
                "sender": random.choice(['user', 'support']),
                "message": f"Сообщение {j+1}",
                "timestamp": msg_time.isoformat() + "Z"
            })
        
        ticket = {
            "ticket_id": f"ticket_{uuid.uuid4().hex[:8]}",
            "user_id": f"user_{random.randint(1, 50)}",
            "status": random.choice(statuses),
            "issue_type": random.choice(issue_types),
            "messages": messages,
            "created_at": created.isoformat() + "Z",
            "updated_at": messages[-1]['timestamp'] if messages else created.isoformat() + "Z"
        }
        tickets.append(ticket)
    
    if tickets:
        db.support_tickets.insert_many(tickets)
    
    print(f"Тикетов: {len(tickets)}")
    client.close()

def generate_recommendations():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    db.user_recommendations.delete_many({})
    
    products = [f"prod_{i}" for i in range(100, 200)]
    recommendations = []
    
    for user_id in range(1, 51):
        rec = {
            "user_id": f"user_{user_id}",
            "recommended_products": random.sample(products, random.randint(3, 6)),
            "last_updated": (datetime.now() - timedelta(hours=random.randint(1, 48))).isoformat() + "Z"
        }
        recommendations.append(rec)
    
    if recommendations:
        db.user_recommendations.insert_many(recommendations)
    
    print(f"Рекомендаций: {len(recommendations)}")
    client.close()

def generate_reviews():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    db.moderation_queue.delete_many({})
    
    statuses = ['pending', 'approved', 'rejected']
    ratings = [1, 2, 3, 4, 5]
    reviews = []
    
    for i in range(30):
        review = {
            "review_id": f"rev_{uuid.uuid4().hex[:8]}",
            "user_id": f"user_{random.randint(1, 50)}",
            "product_id": f"prod_{random.randint(100, 199)}",
            "review_text": f"Отзыв {i}",
            "rating": random.choice(ratings),
            "moderation_status": random.choice(statuses),
            "flags": random.sample(['images', 'links', 'spam'], random.randint(0, 2)),
            "submitted_at": (datetime.now() - timedelta(days=random.randint(0, 15))).isoformat() + "Z"
        }
        reviews.append(review)
    
    if reviews:
        db.moderation_queue.insert_many(reviews)
    
    print(f"Отзывов: {len(reviews)}")
    client.close()

def check_data():
    client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
    db = client['etl_project']
    
    collections = ['user_sessions', 'event_logs', 'support_tickets', 
                   'user_recommendations', 'moderation_queue']
    
    print("\nДанные в MongoDB:")
    for coll in collections:
        count = db[coll].count_documents({})
        print(f"  {coll}: {count}")
    
    client.close()

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'mongo_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id='gen_sessions', python_callable=generate_sessions)
    t2 = PythonOperator(task_id='gen_events', python_callable=generate_events)
    t3 = PythonOperator(task_id='gen_tickets', python_callable=generate_tickets)
    t4 = PythonOperator(task_id='gen_recommendations', python_callable=generate_recommendations)
    t5 = PythonOperator(task_id='gen_reviews', python_callable=generate_reviews)
    t6 = PythonOperator(task_id='check_data', python_callable=check_data)
    
    [t1, t2, t3, t4, t5] >> t6
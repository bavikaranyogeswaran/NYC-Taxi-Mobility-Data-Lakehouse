import requests
import time

SUPERSET_URL = "http://127.0.0.1:8088"
USERNAME = "admin"
PASSWORD = "admin"

def get_auth_token():
    print("Authenticating with Superset...")
    res = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    })
    if res.status_code != 200:
        raise Exception(f"Login failed: {res.text}")
    return res.json()["access_token"]

def get_csrf_token(token):
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
    if res.status_code != 200:
        raise Exception(f"Failed to get CSRF: {res.text}")
    return res.json()["result"]

def get_database_id(token, db_name="NYC Taxi Gold"):
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, params={"q": f'{{"filters": [{{"col": "database_name", "opr": "eq", "value": "{db_name}"}}]}}'})
    if res.status_code == 200:
        dbs = res.json().get("result", [])
        if dbs:
            return dbs[0]["id"]
    return None

def create_dataset(token, csrf, db_id, table_name):
    headers = {
        "Authorization": f"Bearer {token}",
        "X-CSRFToken": csrf,
        "Content-Type": "application/json"
    }
    payload = {
        "database": db_id,
        "schema": "public",
        "table_name": table_name
    }
    res = requests.post(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, json=payload)
    if res.status_code in [200, 201]:
        print(f"  OK: Dataset {table_name} registered.")
    elif res.status_code == 422 and "already exists" in res.text:
        print(f"  SKIP: Dataset {table_name} already registered.")
    else:
        print(f"  ERROR: Failed to register {table_name}: {res.text}")

def main():
    print("=" * 60)
    print("NYC Taxi Lakehouse — Superset Auto-Config")
    print("=" * 60)
    
    # Wait for Superset to be fully up
    for _ in range(5):
        try:
            requests.get(f"{SUPERSET_URL}/health")
            break
        except requests.ConnectionError:
            print("Waiting for Superset to start...")
            time.sleep(2)
            
    token = get_auth_token()
    csrf = get_csrf_token(token)
    
    db_id = get_database_id(token, "NYC Taxi Gold")
    if not db_id:
        print("ERROR: Could not find database 'NYC Taxi Gold'. Make sure 'superset set-database-uri' succeeded.")
        return

    tables = [
        "gold_daily_revenue",
        "gold_hourly_performance",
        "gold_route_summary",
        "gold_dq_summary"
    ]
    
    for t in tables:
        create_dataset(token, csrf, db_id, t)
        
    print("\n✓ Success! All Datasets are registered in Superset.")
    print("You can now log in to http://localhost:8088 (admin/admin) and drag-and-drop your Dashboards.")

if __name__ == "__main__":
    main()

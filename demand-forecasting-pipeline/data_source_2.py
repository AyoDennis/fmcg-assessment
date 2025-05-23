import json
import random
from data_source_1 import fake


def simulate_api_data(n=1000):
    return [
        {
            "supplier_uuid": fake.uuid4(),
            "status": random.choice(["active", "inactive", "pending"]),
            "compliance_score": round(random.uniform(60, 100), 1),
            "last_inspection": fake.date_between(start_date='-1y', end_date='today').isoformat()
        }
        for _ in range(n)
    ]

api_data = simulate_api_data()
with open("api_suppliers.json", "w") as f:
    json.dump(api_data, f, indent=2)
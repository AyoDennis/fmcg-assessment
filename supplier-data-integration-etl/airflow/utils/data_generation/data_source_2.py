import json
import logging
import random

from faker import Faker

fake = Faker()
logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


def simulate_api_data(n=1000):
    """
    This function created json data to silumate api source
    """
    return [
        {
            "supplier_uuid": fake.uuid4(),
            "status": random.choice(["active", "inactive", "pending"]),
            "compliance_score": round(random.uniform(60, 100), 1),
            "last_inspection": fake.date_between(
                start_date='-1y', end_date='today').isoformat()
        }
        for _ in range(n)
    ]


api_data = simulate_api_data()
with open("../dags/api_suppliers.json", "w") as f:
    json.dump(api_data, f, indent=2)

logging.info("json api data created")

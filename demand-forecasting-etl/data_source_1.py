import logging
import random

import pandas as pd
from faker import Faker

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)

fake = Faker()
Faker.seed(42)
logging.info("faker instantiated")

def generate_supplier_data(n=1000):
    """
    This functions generates random supplier data
    """
    data = []
    for _ in range(n):
        data.append({
            "supplier_id": fake.uuid4(),
            "supplier_name": fake.company(),
            "contact_name": fake.name(),
            "email": fake.company_email(),
            "country": fake.country(),
            "phone": fake.phone_number(),
            "performance_score": round(random.uniform(1.0, 5.0), 2),
            "last_audit_date": fake.date_between(start_date='-2y', end_date='today'),
            "preferred_supplier": random.choice(["Yes", "No"])
        })
    return pd.DataFrame(data)

df = generate_supplier_data(1000)

logging.info("data generated")

df.to_csv("./demand-forecasting-etl/mock_supplier_data.csv", index=False)


logging.info("csv created")

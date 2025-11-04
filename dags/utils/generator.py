from faker import Faker
from uuid import uuid4
from datetime import datetime
import random

fake = Faker()

def generate_data():
    """
    Generates a single synthetic student record.
    """
    return {
        'student_id': str(uuid4()),
        'full_name': fake.name(),
        'date_of_birth': fake.date_time_between(start_date='-18y', end_date='-5y'),
        'grade_level': random.randint(1, 12),
        'student_email': fake.email()
    }
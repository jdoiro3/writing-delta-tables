from faker import Faker
import typing as ty
import pyarrow as pa
import random


faker = Faker()

class Person(ty.TypedDict):
    id: int
    age: int
    name: str
    address: str
    random_data: str


person_schema = pa.schema([
    ("id", pa.int32()),
    ("age", pa.int32()),
    ("name", pa.string()),
    ("address", pa.string()),
    ("random_data", pa.string())
])


def new_person(id: int) -> Person:
    return Person(
        id=id,
        age=random.randint(1, 100),
        name=faker.name(),
        address=faker.address(),
        random_data=""
    )
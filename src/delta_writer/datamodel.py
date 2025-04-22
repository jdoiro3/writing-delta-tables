from faker import Faker
import typing as ty
import pyarrow as pa
import random


faker = Faker()

class Person(ty.TypedDict):
    id: int
    id_batch: int
    age: int
    name: str
    address: str
    random_data: str


person_schema = pa.schema([
    ("id", pa.int32()),
    ("id_batch", pa.int32()),
    ("age", pa.int32()),
    ("name", pa.string()),
    ("address", pa.string()),
    ("random_data", pa.string())
])


def get_batch(id: int, num_batches: int) -> int:
    return id % num_batches


def new_person(id: int, num_batches: int = 100) -> Person:
    return Person(
        id=id,
        id_batch=get_batch(id, num_batches),
        age=random.randint(1, 100),
        name=faker.name(),
        address=faker.address(),
        random_data=""
    )
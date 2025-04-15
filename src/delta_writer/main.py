import deltalake
import typing as ty
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import random
import os
from multiprogress import progress_bar, MultiProgress
import typer
from enum import Enum
from functools import partial
from random import choice
from string import ascii_uppercase
from pathlib import Path
from delta_writer.writing import DeltaWriter
from delta_writer.utils import split, RepeatTimer
from delta_writer.datamodel import Person, person_schema, new_person


def pipeline(file: Path, table: Path | str):
    written = 0
    not_written = 0
    with DeltaWriter[Person](deltalake.DeltaTable(table), person_schema, "age", "id") as w:
        pq_f = pq.ParquetFile(file)
        for i in range(pq_f.num_row_groups):
            ps = list(w.not_written(pq_f.read_row_group(i)))
            if ps:
                for p in progress_bar(
                    ps, 
                    id=f"{file.name}{i}", 
                    desc=f"Processing {file.name} and row group {i+1}:", 
                    total=len(ps)
                ):
                    # Note: We randomly fail 5% of the time, simulating a person not 
                    # being able to be processed for some reason.
                    if random.randrange(100) < .05:
                        not_written += 1
                    else:
                        p["random_data"] = "".join(
                            choice(ascii_uppercase) 
                            for _ in range(random.randint(100, 10_000))
                        )
                        w.write(p)
                        written += 1
    return written, not_written


class Executer(Enum):
    Threads = "threads"
    Processes = "processes"


def process(people_files: ty.Iterable[Path], executer: Executer = Executer.Processes) -> int:
    n: int = os.cpu_count() or 1
    exec = ProcessPoolExecutor(max_workers=n) if executer.value == "processes" else ThreadPoolExecutor()

    path = "people"
    table = (
        deltalake.DeltaTable(path)
        if deltalake.DeltaTable.is_deltatable(path) else 
        deltalake.DeltaTable.create(
            path, 
            schema=person_schema, 
            partition_by=["age"],
            configuration = {"delta.logRetentionDuration": "0 days"}
        )
    )

    def optimize():
        """
        Checkpoints allow for efficient table queries and 
        compact merges small parquet files into larger one.
        """
        table.create_checkpoint()
        print(table.optimize.compact())
        table.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)

    optimize_thread = RepeatTimer(20.0, optimize)
    optimize_thread.start()

    writes, not_written = 0, 0
    with exec as e, MultiProgress():
        for w, nw in list(e.map(partial(pipeline, table=path), people_files)):
            writes += w
            not_written += nw

    optimize_thread.cancel()
    optimize()

    print(f"Written: {writes}, Not written: {not_written}")
    num_rows = table.to_pyarrow_table(columns=["id"]).num_rows
    print(f"Number of rows delta table: {num_rows}")

    return num_rows


def get_new_people(ids: list[int], input_dir: Path = Path()) -> Path:
    where = input_dir / f"ids_{ids[0]}-{ids[-1]}.parquet"
    with pq.ParquetWriter(where, schema=person_schema) as w:
        people = []
        for i in progress_bar(ids, desc="Creating people...", total=len(ids)):
            people.append(new_person(i))
            if len(people) == 10_000:
                w.write_batch(pa.RecordBatch.from_pylist(people, schema=person_schema))
                people = []
        w.write_batch(pa.RecordBatch.from_pylist(people, schema=person_schema))
        return where


def process_people(num_p: int = 100_000, executer: Executer = Executer.Processes.value) -> int:
    cpus = os.cpu_count() or 1
    input_dir = Path("input")
    if not input_dir.exists():
        input_dir.mkdir(parents=True)
    with ProcessPoolExecutor() as p, MultiProgress():
        files = list(p.map(partial(get_new_people, input_dir=input_dir), split(range(num_p), cpus)))
    return process(files, executer)


def run():
    typer.run(process_people)


if __name__ == "__main__":
    run()
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
from delta_writer.datamodel import Person, person_schema, new_person, get_batch
import pyarrow.compute as pc
import json


PARTITION_COL: ty.Final = "id_batch"


def pipeline(file: Path, table: Path | str):
    written = 0
    not_written = 0
    with DeltaWriter[Person](
        deltalake.DeltaTable(table), 
        person_schema, 
        PARTITION_COL, 
        "id", 
        50_000
    ) as w:
        pq_f = pq.ParquetFile(file, memory_map=True)
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


def add_tag(tags_table: deltalake.DeltaTable, table: deltalake.DeltaTable, tag: str) -> None:
    metrics = (
        tags_table.merge(
            source=pa.table({"tag": [tag], "version": [table.history(1)[0]["version"]]}),
            predicate="target.tag = source.tag",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update(updates={"tag": "source.tag", "version": "source.version"})
        .when_not_matched_insert(updates={"tag": "source.tag", "version": "source.version"})
        .execute()
    )
    print(metrics)


def process_people(people_files: ty.Iterable[Path], tag: str, executer: Executer = Executer.Processes) -> int:
    n: int = os.cpu_count() or 1
    exec = ProcessPoolExecutor(max_workers=n) if executer.value == "processes" else ThreadPoolExecutor()

    path = "people"
    table = (
        deltalake.DeltaTable(path)
        if deltalake.DeltaTable.is_deltatable(path) else 
        deltalake.DeltaTable.create(
            path, 
            schema=person_schema, 
            partition_by=[PARTITION_COL],
            configuration = {"delta.logRetentionDuration": "0 days"}
        )
    )
    tags_table = "tags"
    tags_table = (
        deltalake.DeltaTable(tags_table)
        if deltalake.DeltaTable.is_deltatable(tags_table) else 
        deltalake.DeltaTable.create(
            tags_table, 
            schema=pa.schema([("tag", pa.string()), ("version", pa.int32())]), 
        )
    )
    print(len(table.files()))

    def optimize():
        """
        Checkpoints allow for efficient table queries and 
        compact merges small parquet files into larger ones.
        """
        table.create_checkpoint()
        print(table.optimize.compact())
        print(len(table.files()))

    optimize_thread = RepeatTimer(60.0, optimize)
    optimize_thread.start()

    writes, not_written = 0, 0
    with exec as e, MultiProgress():
        for w, nw in list(e.map(partial(pipeline, table=path), people_files)):
            writes += w
            not_written += nw

    optimize_thread.cancel()

    table = deltalake.DeltaTable(path)
    num_rows = table.to_pyarrow_table(columns=["id"]).num_rows
    print(f"Written: {writes}, Not written: {not_written}")
    print(f"Number of rows delta table: {num_rows}")
    add_tag(tags_table, table, tag)
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
    

app = typer.Typer()


@app.command()
def process(tag: str, num_p: int = 100_000, executer: Executer = Executer.Processes.value) -> int:
    cpus = os.cpu_count() or 1
    input_dir = Path("input")
    if not input_dir.exists():
        input_dir.mkdir(parents=True)
    with ProcessPoolExecutor() as p, MultiProgress():
        files = list(p.map(partial(get_new_people, input_dir=input_dir), split(range(num_p), cpus)))
    return process_people(files, tag, executer)


@app.command()
def get(id: int, version: int | None = None, tag: str | None = None) -> None:
    if tag and version:
        raise ValueError(f"Both version and tag provided.")
    if tag:
        tag_to_versions: list[dict[str, int]] = deltalake.DeltaTable("tags").to_pyarrow_table().to_pylist()
        version_and_tag = [tv for tv in tag_to_versions if tv["tag"] == tag]
        if not version_and_tag:
            raise ValueError(tag)
        assert len(version_and_tag) == 1, f"Got too many records: {version_and_tag}..."
        version = version_and_tag[0]["version"]
    recs =deltalake.DeltaTable("people", version=version).to_pyarrow_table(
        partitions=[(PARTITION_COL, "=", str(get_batch(id, 100)))],
        filters=pc.field("id") == id
    ).to_pylist()
    assert len(recs) == 1
    print(json.dumps(Person(**recs[0]), indent=4))


def run():
    app()


if __name__ == "__main__":
    run()
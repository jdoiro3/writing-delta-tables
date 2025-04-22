import deltalake
import typing as ty
import pyarrow as pa
import tempfile
import pyarrow.ipc as ipc
import pyarrow.compute as pc


T = ty.TypeVar("T", bound=dict)


class DiskBuffer(ty.Generic[T]):
    def __init__(self, schema: pa.Schema, arrow_record_batch_size: int):
        self.schema = schema
        self._arrow_record_batch_size = arrow_record_batch_size
        self.reset()

    def reset(self) -> None:
        self._tempf = tempfile.NamedTemporaryFile(prefix="delta-writer", suffix=".arrow")
        self._arrow_file = ipc.new_file(pa.OSFile(self._tempf.name, mode="wb"), self.schema)
        self._arrow_record_batch_size = self._arrow_record_batch_size
        self._arrow_buffer: list[T] = []

    def put(self, rec: T) -> None:
        self._arrow_buffer.append(rec)
        if len(self._arrow_buffer) == self._arrow_record_batch_size:
            self._arrow_file.write(pa.RecordBatch.from_pylist(self._arrow_buffer, schema=self.schema))
            self._arrow_buffer = []

    def flush(self, write: ty.Callable[[pa.Table], None]) -> int:
        self._arrow_file.write(pa.RecordBatch.from_pylist(self._arrow_buffer, schema=self.schema))
        self._arrow_file.close()
        table: pa.Table = ipc.open_file(pa.memory_map(self._tempf.name, mode="rb")).read_all()
        if table.num_rows > 0:
            write(table)
        self._tempf.close()
        self.reset()
        return table.num_rows


class DeltaWriter(ty.Generic[T]):
    def __init__(
            self, 
            delta_table: deltalake.DeltaTable,
            schema: pa.Schema,
            partition_col: str,
            id_col: str,
            recs_per_commit: int,
            max_recs_in_memory: int = 5_000
    ):
        self.partition_col = partition_col
        self.id_col = id_col
        self._buffer = DiskBuffer[T](schema, max_recs_in_memory)
        self.delta_table = delta_table
        self.recs_per_commit = recs_per_commit
        self._writes = 0
        
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._buffer.flush(self._w)
        self._writes = 0

    def not_written(self, data: ty.Iterable[T] | pa.Table) -> ty.Generator[T, None, None]:
        if isinstance(data, pa.Table):
            data: ty.Iterable[T] = data.to_pylist()
        batches = [rec[self.partition_col] for rec in data]
        ids = [rec[self.id_col] for rec in data]
        written_ids = self.delta_table.to_pyarrow_table(
            columns=[self.id_col],
            partitions=[(self.partition_col, "in", [str(b) for b in batches])], 
            filters=pc.field(self.id_col).isin(ids)
        ).to_pydict()[self.id_col]
        not_written_ids = set(list(ids)) - set(written_ids)
        return (rec for rec in data if rec[self.id_col] in not_written_ids)

    def _w(self, table: pa.Table):
        deltalake.write_deltalake(
            self.delta_table.table_uri, 
            table, 
            partition_by=[self.partition_col], 
            mode="append"
        )

    def write(self, rec: T):
        self._buffer.put(rec)
        self._writes += 1
        if self._writes == self.recs_per_commit:
            self._buffer.flush(self._w)
            self._writes = 0

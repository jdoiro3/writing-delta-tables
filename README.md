# Run

```shell
uv run process-people
```

## With `memray`

If using `memray` use threads so the memory profiling is accurate.
```shell
uv run memray run src/delta_writer/main.py --executer threads --num-p 500_000
```
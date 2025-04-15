import typing as ty
from threading import Timer

T = ty.TypeVar("T")

def split(a: list[T], n: int) -> list[list[T]]:
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
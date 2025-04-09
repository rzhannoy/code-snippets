'''
Execute any functions with any kwargs concurrently using threads or processes.
Each `ExecConf.func` is run against each of its `ExecConf.kwargs_sets`.
Results will be tracked either via `kwargs_sets[id_field]`, in which case it must be unique,
or via `ExecConf.func.__name__` plus random string if `id_field` is not provided.

Usage:

```
def add(a: int, b: int) -> int:
    return a + b

def square(a: int) -> int:
    return a * a

results = execute_concurrently(4, [
    ExecConf(func=add, kwargs_sets=[{'a': 1, 'b': 2}, {'a': 3, 'b': 4}, {'a': 5, 'b': 6}]),
    ExecConf(func=square, kwargs_sets=[{'a': 1}, {'a': 2}, {'a': 3}]),
])

for k, v in results.data.items():
    print(k, v)
```
'''

import concurrent.futures
from enum import Enum
from typing import Callable, Any

from loguru import logger
from pydantic import BaseModel

from core.utils import gen_random_string


class ExecutorType(int, Enum):
    THREAD = 1
    PROCESS = 2


class ExecConf(BaseModel):
    func: Callable
    kwargs_sets: list[dict[str, Any]]
    id_field: str | None = None


class ExecutionResultsItem(BaseModel):
    result: Any = None
    error: str | None = None


class ExecutionResults(BaseModel):
    data: dict[str, ExecutionResultsItem]


def _get_id_value(kwargs: dict[str, Any], conf: ExecConf) -> Any:
    return kwargs[conf.id_field] if conf.id_field else f"{conf.func.__name__}__{gen_random_string(12)}"


def execute_concurrently(
    n_workers: int,
    exec_confs: list[ExecConf],
    timeout: int | float | None = None, # in seconds
    executor_type: ExecutorType = ExecutorType.THREAD,
    executor_kwargs: dict[str, Any] | None = None
) -> ExecutionResults:
    results = {}

    Executor = {
        ExecutorType.THREAD: concurrent.futures.ThreadPoolExecutor,
        ExecutorType.PROCESS: concurrent.futures.ProcessPoolExecutor,
    }[executor_type]

    if not executor_kwargs:
        executor_kwargs = {}

    executor_kwargs['max_workers'] = n_workers

    with Executor(**executor_kwargs) as executor:
        futures = {}
        for conf in exec_confs:
            futures |= {
                executor.submit(conf.func, **kwargs): _get_id_value(kwargs, conf)
                for kwargs in conf.kwargs_sets
            }

        for future in concurrent.futures.as_completed(futures, timeout=timeout):
            id_value = futures[future]
            results[id_value] = {'result': None, 'error': None}

            try:
                results[id_value]['result'] = future.result()

            except Exception as e:
                logger.error(f"Error executing `{id_value}`: {e}")
                results[id_value]['error'] = f"{e}"

    return ExecutionResults.model_validate({'data': results})

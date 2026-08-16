"""Dataset profiles: the data-handler + DB + table group for an FL task.

Fixed per dataset (not per node, not per run). A config names a dataset; the renderer
pulls the matching profile. Mirrors the committed env files (e.g. mnist1.env).

Under 1:1:1 each node has its own Postgres DB, so train/test table names stay constant
across nodes (same name, separate DBs = isolated). The shared-operator fallback would
need per-node table names; left as a future hook.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetProfile:
    name: str               # registry key, e.g. "mnist"
    module_name: str        # MODULE_NAME, data handler class
    module_file: str        # MODULE_FILE, file under TRAINING_APPLICATION_DIR
    logical_database: str   # LOGICAL_DATABASE
    train_table: str        # TRAIN_TABLE
    test_table: str         # TEST_TABLE


DATASETS: dict[str, DatasetProfile] = {
    "mnist": DatasetProfile(
        name="mnist",
        module_name="MnistDataHandler",
        module_file="custom_data_handler.py",
        logical_database="mnist_fl",
        train_table="mnist_train",
        test_table="mnist_test",
    ),
}


def get_dataset(name: str) -> DatasetProfile:
    if name not in DATASETS:
        raise KeyError(
            f"unknown dataset '{name}'. Known: {sorted(DATASETS)}. "
            f"Add a DatasetProfile to harness/datasets.py."
        )
    return DATASETS[name]

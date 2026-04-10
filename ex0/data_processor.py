from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class for all data processors."""

    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Checks whether the input data are appropriate for the processor."""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Processes and stores the input data. Raises Exception if invalid."""
        pass

    def output(self) -> tuple[int, str]:
        """Extracts the oldest piece of data and its processing rank."""
        if not self._storage:
            raise IndexError("Storage is empty")
        # Pop the oldest item (index 0)
        oldest_data = self._storage.pop(0)
        current_rank = self._rank
        self._rank += 1
        return (current_rank, oldest_data)


class NumericProcessor(DataProcessor):
    """Processes int, float, and lists of ints/floats."""

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            return all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
        else:
            self._storage.append(str(data))


class TextProcessor(DataProcessor):
    """Processes str and lists of strings."""

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
        else:
            self._storage.append(str(data))


class LogProcessor(DataProcessor):
    """Processes dict of string key-value pairs, and lists of those
    dicts."""

    def validate(self, data: Any) -> bool:

        def is_valid_dict(d: Any) -> bool:
            return isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in d.items())

        if is_valid_dict(data):
            return True
        if isinstance(data, list):
            return all(is_valid_dict(x) for x in data)
        return False

    def ingest(
            self,
            data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(log_dict: dict[str, str]) -> str:
            level = log_dict.get('log_level', 'UNKNOWN')
            msg = log_dict.get('log_message', '')
            return f"{level}: {msg}"

        if isinstance(data, list):
            for item in data:
                self._storage.append(format_log(item))
        else:
            self._storage.append(format_log(data))


if __name__ == "__main__":
    print("=== Code Nexus Data Processor ===")

    print("\nTesting Numeric Processor...")
    num_proc = NumericProcessor()
    print(f"Trying to validate input '42': {num_proc.validate(42)}")
    print(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")  # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")

    num_list: list[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_list}")
    num_proc.ingest(num_list)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"Numeric value {rank}: {val}")

    print("\nTesting Text Processor...")
    text_proc = TextProcessor()
    print(f"Trying to validate input 'Hello': {text_proc.validate('Hello')}")
    print(f"Trying to validate input '42': {text_proc.validate(42)}")

    text_list = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {text_list}")
    text_proc.ingest(text_list)
    print("Extracting 1 value...")
    rank, val = text_proc.output()
    print(f"Text value {rank}: {val}")

    print("\nTesting Log Processor...")
    log_proc = LogProcessor()
    print(
        "Trying to validate input "
        f"'{{\'log_level\': \'INFO\', \'log_message\': \'ok\'}}': "
        f"{log_proc.validate({'log_level': 'INFO', 'log_message': 'ok'})}")
    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")

    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_data}")
    log_proc.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"Log entry {rank}: {val}")

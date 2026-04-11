from abc import ABC, abstractmethod
import typing
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


class DataStream:
    """Manages a stream of data and routes it to registered processors."""

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Registers a new processor to handle data in the stream."""
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        """Routes each element to the first capable processor."""
        for item in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            if not handled:
                print(
                    f"DataStream error - Can't process element "
                    f"in stream: {item}")

    def print_processors_stats(self) -> None:
        """Prints statistics for all registered processors."""
        print("== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            remaining = len(proc._storage)
            total = remaining + proc._rank

            proc_name = proc.__class__.__name__.replace(
                "Processor", " Processor")
            print(
                f"{proc_name}: total {total} items processed, "
                f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)

    stream.print_processors_stats()

    print("\nRegistering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("\nSend the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    stream.print_processors_stats()

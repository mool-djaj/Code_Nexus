import typing
from typing import Any


class DataProcessor(typing.Protocol):
    """Protocol defining the required behaviors and attributes for
    processors."""
    _storage: list[str]
    _rank: int

    def validate(self, data: Any) -> bool:
        ...

    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        ...


class ExportPlugin(typing.Protocol):
    """Protocol defining the required behaviors for export plugins."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class NumericProcessor:
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

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

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("Storage is empty")
        oldest_data = self._storage.pop(0)
        current_rank = self._rank
        self._rank += 1
        return (current_rank, oldest_data)


class TextProcessor:
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

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

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("Storage is empty")
        oldest_data = self._storage.pop(0)
        current_rank = self._rank
        self._rank += 1
        return (current_rank, oldest_data)


class LogProcessor:
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

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

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("Storage is empty")
        oldest_data = self._storage.pop(0)
        current_rank = self._rank
        self._rank += 1
        return (current_rank, oldest_data)


class CSVExportPlugin:
    """Formats data as a comma-separated string."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("CSV Output:")
        formatted_csv = ",".join([item[1] for item in data])
        print(formatted_csv)


class JSONExportPlugin:
    """Formats data as a JSON dictionary string based on rank."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("JSON Output:")
        entries: list[str] = []
        for rank, value in data:
            escaped = value.replace('\\', '\\\\').replace('"', '\\"')
            entries.append(f'"item_{rank}": "{escaped}"')
        print("{" + ", ".join(entries) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
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

    def output_pipeline(
            self, nb: int, plugin: ExportPlugin) -> None:
        """Extracts elements ONE PROCESSOR AT A TIME and sends to
        plugin."""
        for proc in self._processors:
            collected_data: list[tuple[int, str]] = []

            for _ in range(nb):
                if not proc._storage:
                    break
                collected_data.append(proc.output())

            if collected_data:
                plugin.process_output(collected_data)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors\n")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch_1 = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch_1}\n")
    stream.process_stream(batch_1)

    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a "
          "CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)

    stream.print_processors_stats()

    batch_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR',
          'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print(f"\nSend another batch of data: {batch_2}")
    stream.process_stream(batch_2)

    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a "
          "JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)

    stream.print_processors_stats()

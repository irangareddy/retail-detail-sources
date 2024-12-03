"""Module defining the DatabaseCursor protocol."""

from collections.abc import Mapping
from typing import Any, Protocol


class DatabaseCursor(Protocol):
    """Protocol defining the required interface for database cursors."""

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        """Execute a SQL statement with optional parameters."""
        ...

    def executemany(self, sql: str, seq_of_parameters: list[Mapping[str, Any]]) -> None:
        """Execute the same SQL statement with a sequence of parameters."""
        ...

    def fetchall(self) -> list[str]:
        """Fetch all rows of a query result."""
        ...

    def fetchone(self) -> None:
        """Fetch the next row of a query result."""
        ...

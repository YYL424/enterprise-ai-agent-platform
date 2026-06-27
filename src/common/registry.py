"""Global service registry / dependency injection container."""

from __future__ import annotations

from typing import Any, Dict, Type, TypeVar, Optional

T = TypeVar("T")

class ServiceRegistry:
    """Lightweight service locator for cross-module dependency injection."""

    def __init__(self) -> None:
        self._services: Dict[str, Any] = {}
        self._instances: Dict[str, Any] = {}

    def register(self, name: str, factory: Any, singleton: bool = True) -> None:
        self._services[name] = (factory, singleton)
        if name in self._instances:
            del self._instances[name]

    def get(self, name: str, expected_type: Optional[Type[T]] = None) -> T:
        if name not in self._services:
            raise KeyError(f"Service '{name}' not registered")

        factory, singleton = self._services[name]
        if singleton:
            if name not in self._instances:
                self._instances[name] = factory() if callable(factory) else factory
            instance = self._instances[name]
        else:
            instance = factory() if callable(factory) else factory

        if expected_type is not None and not isinstance(instance, expected_type):
            raise TypeError(f"Service '{name}' is not of type {expected_type}")

        return instance

# Global singleton
registry = ServiceRegistry()

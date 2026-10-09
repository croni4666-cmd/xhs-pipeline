# -*- coding: utf-8 -*-
"""
xhs-pipeline Resilience & Operational Governance Module (Phase 4)
Provides industrial-grade request budgeting, token-bucket rate limiting,
exponential backoff retry, circuit breaking, and controlled driver failover.

Guarantees Taleb-style anti-ruin defense:
- Never exceed API / scraping request quotas (hard budget boundary).
- Back off exponentially on rate limiting (HTTP 429) rather than hammering.
- Trip circuit breaker on repeated authentication or anti-bot blocks to protect accounts.
- Gracefully failover to secondary drivers upon circuit trip.
"""

import time
import asyncio
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Callable, Awaitable

from .contracts import (
    PipelineError,
    RateLimitError,
    AuthenticationError,
    BudgetExceededError,
    CircuitBreakerOpenError
)


# =====================================================================
# Request Budget & Rate Limiting
# =====================================================================

class RequestBudgetManager:
    """
    Manages cumulative request limits and per-minute rate limits
    to strictly bound execution and prevent uncontrolled scraping loops.
    """

    def __init__(self, max_budget: int = 100, rate_limit_per_min: int = 30):
        self.max_budget = max_budget
        self.rate_limit_per_min = rate_limit_per_min
        self.used_requests = 0
        self.timestamps: List[float] = []

    def can_request(self) -> bool:
        return self.used_requests < self.max_budget

    async def acquire(self):
        """
        Acquires permission to execute a request.
        Raises BudgetExceededError if cumulative budget is exhausted.
        Throttles if per-minute rate limit is reached.
        """
        if self.used_requests >= self.max_budget:
            raise BudgetExceededError(
                f"Request budget exhausted: {self.used_requests}/{self.max_budget} requests used."
            )

        now = time.time()
        # Clean timestamps older than 60 seconds
        self.timestamps = [t for t in self.timestamps if now - t < 60.0]

        if len(self.timestamps) >= self.rate_limit_per_min:
            oldest = self.timestamps[0]
            sleep_needed = max(0.1, 60.0 - (now - oldest) + random.uniform(0.1, 0.5))
            await asyncio.sleep(sleep_needed)

        self.used_requests += 1
        self.timestamps.append(time.time())

    def get_stats(self) -> Dict[str, Any]:
        return {
            "max_budget": self.max_budget,
            "used_requests": self.used_requests,
            "remaining_budget": max(0, self.max_budget - self.used_requests),
            "recent_requests_last_minute": len(self.timestamps)
        }


# =====================================================================
# Exponential Backoff Retry
# =====================================================================

class ExponentialBackoff:
    """
    Retries asynchronous operations with jittered exponential backoff
    specifically handling RateLimitError and transient network hiccups.
    """

    def __init__(
        self,
        initial_delay: float = 1.0,
        factor: float = 2.0,
        max_delay: float = 15.0,
        max_retries: int = 3
    ):
        self.initial_delay = initial_delay
        self.factor = factor
        self.max_delay = max_delay
        self.max_retries = max_retries

    async def execute(self, fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        delay = self.initial_delay
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return await fn(*args, **kwargs)
            except RateLimitError as e:
                last_exception = e
                if attempt == self.max_retries:
                    raise
                jitter = random.uniform(0.1, 0.5)
                wait_time = min(self.max_delay, delay + jitter)
                await asyncio.sleep(wait_time)
                delay *= self.factor
            except Exception:
                raise

        raise last_exception if last_exception else PipelineError("Exhausted backoff retries")


# =====================================================================
# Circuit Breaker & Driver Failover
# =====================================================================

class CircuitState(str, Enum):
    CLOSED = "closed"        # Normal operation: all calls allowed
    OPEN = "open"            # Tripped: all calls immediately blocked
    HALF_OPEN = "half_open"  # Trial recovery: one test call allowed


class CircuitBreaker:
    """
    Guards a specific driver against repeated failures.
    Trips open when consecutive failures hit threshold to prevent platform account bans.
    """

    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.consecutive_failures = 0
        self.state = CircuitState.CLOSED
        self.last_state_change: float = time.time()

    def can_execute(self) -> bool:
        now = time.time()
        if self.state == CircuitState.OPEN:
            if now - self.last_state_change > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.last_state_change = now
                return True
            return False
        return True

    def record_success(self):
        self.consecutive_failures = 0
        if self.state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
            self.state = CircuitState.CLOSED
            self.last_state_change = time.time()

    def record_failure(self):
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.last_state_change = time.time()

    def get_status(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "consecutive_failures": self.consecutive_failures,
            "failure_threshold": self.failure_threshold
        }


class DriverFallbackRouter:
    """
    Coordinates primary and fallback drivers with autonomous circuit breaking.
    When the primary driver's circuit trips, automatically falls back to secondary driver.
    """

    def __init__(self, primary_driver: str, fallback_driver: Optional[str] = None):
        self.primary_driver = primary_driver
        self.fallback_driver = fallback_driver
        self.breakers: Dict[str, CircuitBreaker] = {
            primary_driver: CircuitBreaker(name=primary_driver)
        }
        if fallback_driver:
            self.breakers[fallback_driver] = CircuitBreaker(name=fallback_driver)
        self.active_driver_name = primary_driver
        self.switch_history: List[Dict[str, Any]] = []

    def get_circuit_breaker(self, driver_name: str) -> CircuitBreaker:
        if driver_name not in self.breakers:
            self.breakers[driver_name] = CircuitBreaker(name=driver_name)
        return self.breakers[driver_name]

    def select_healthy_driver(self) -> str:
        """
        Returns the best healthy driver. If primary is open and fallback is available,
        dynamically routes to fallback.
        """
        primary_cb = self.get_circuit_breaker(self.primary_driver)
        if primary_cb.can_execute():
            self.active_driver_name = self.primary_driver
            return self.primary_driver

        if self.fallback_driver:
            fallback_cb = self.get_circuit_breaker(self.fallback_driver)
            if fallback_cb.can_execute():
                if self.active_driver_name != self.fallback_driver:
                    self.switch_history.append({
                        "from": self.active_driver_name,
                        "to": self.fallback_driver,
                        "reason": f"Primary circuit breaker {self.primary_driver} is OPEN",
                        "timestamp": time.time()
                    })
                self.active_driver_name = self.fallback_driver
                return self.fallback_driver

        raise CircuitBreakerOpenError(
            f"All available drivers ({self.primary_driver}, {self.fallback_driver}) have OPEN circuit breakers."
        )

"""Backend-neutral runtime contracts; no content or crawler dependencies."""
from dataclasses import dataclass
from enum import Enum
from ..contracts import DriverError


class SessionState(str, Enum):
    UNKNOWN = 'unknown'
    READY = 'ready'
    AUTH_REQUIRED = 'auth_required'
    CHALLENGE = 'challenge'
    RATE_LIMITED = 'rate_limited'
    ACCESS_RESTRICTED = 'access_restricted'
    NETWORK_ERROR = 'network_error'


PAUSE_STATES = frozenset({SessionState.AUTH_REQUIRED, SessionState.CHALLENGE,
                         SessionState.RATE_LIMITED, SessionState.ACCESS_RESTRICTED})


class ControlPausedError(DriverError):
    def __init__(self, state):
        self.state = SessionState(state)
        super().__init__('Online task paused: ' + self.state.value + '; manual acknowledgment required')


@dataclass(frozen=True)
class RuntimeCapabilities:
    session_probe: bool = False
    request_hooks: bool = False
    risk_events: bool = False

    @property
    def supported(self):
        return self.session_probe and self.request_hooks and self.risk_events


@dataclass(frozen=True)
class RequestPolicy:
    max_per_task: int = 20
    per_minute: int = 6
    max_per_day: int = 100

    def __post_init__(self):
        if any(type(value) is not int for value in (self.max_per_task, self.per_minute, self.max_per_day)):
            raise ValueError('Request policy values must be integers')
        if not 0 <= self.max_per_task <= 100000 or not 0 <= self.max_per_day <= 100000 or not 1 <= self.per_minute <= 100000:
            raise ValueError('Request policy values are outside supported bounds')

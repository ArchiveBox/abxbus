#!/usr/bin/env -S uv run python
"""Run: uv run python examples/simple.py"""

import asyncio
from typing import Any, Literal

from pydantic import BaseModel

from abxbus import BaseEvent, EventBus


class RegisterUserResult(BaseModel):
    user_id: str
    welcome_email_sent: bool


class RegisterUserEvent(BaseEvent[RegisterUserResult]):
    email: str
    plan: Literal['free', 'pro']
    event_result_type: Any = RegisterUserResult


class AuditEvent(BaseEvent[None]):
    message: str


def short_id(event_id: str) -> str:
    return event_id[-8:]


async def main() -> None:
    bus = EventBus(name='SimpleExampleBus')

    try:
        # 1) Observe every event via wildcard registration.
        def on_wildcard(event: BaseEvent[Any]) -> None:
            print(f'[wildcard] {event.event_type}#{short_id(event.event_id)}')

        bus.on('*', on_wildcard)

        # 2) Register a typed class handler.
        async def on_register_user(event: RegisterUserEvent) -> RegisterUserResult:
            print(f'[class handler] Creating account for {event.email} ({event.plan})')
            return RegisterUserResult(
                user_id=f'user_{event.email.split("@", maxsplit=1)[0]}',
                welcome_email_sent=True,
            )

        bus.on(RegisterUserEvent, on_register_user)

        # 3) Register by string event type.
        def on_audit(event: BaseEvent[Any]) -> None:
            print(f'[string handler] Audit log: {getattr(event, "message", "<missing>")}')

        bus.on('AuditEvent', on_audit)

        # 4) Intentionally return an invalid shape for runtime result validation.
        def on_register_user_invalid(_event: BaseEvent[Any]) -> object:
            return {'user_id': 123, 'welcome_email_sent': 'yes'}

        bus.on('RegisterUserEvent', on_register_user_invalid)

        # Dispatch a simple event handled by string registration.
        await bus.emit(AuditEvent(message='Starting simple abxbus example')).now()

        # Dispatch typed event; one handler is valid, one is invalid.
        register_event = bus.emit(
            RegisterUserEvent(
                email='ada@example.com',
                plan='pro',
            )
        )
        await register_event.now()

        print('\nRegisterUserEvent handler outcomes:')
        for result in register_event.event_results.values():
            if result.status == 'completed':
                print(f'- {result.handler_name}: completed -> {result.result!r}')
                continue
            if result.status == 'error':
                message = str(result.error) if result.error is not None else 'unknown error'
                print(f'- {result.handler_name}: error -> {message}')
                continue
            print(f'- {result.handler_name}: {result.status}')

        first_valid = await register_event.event_result(raise_if_any=False, raise_if_none=False)
        all_errors = [result.error for result in register_event.event_results.values() if result.error is not None]

        print(f'\nFirst valid parsed result: {first_valid!r}')
        print(f'Total event errors: {len(all_errors)}')
        for index, error in enumerate(all_errors, start=1):
            print(f'  {index}. {error}')

        await bus.wait_until_idle()
        print('\n=== bus.log_tree() ===')
        print(bus.log_tree())
    finally:
        await bus.destroy(clear=True)


if __name__ == '__main__':
    asyncio.run(main())

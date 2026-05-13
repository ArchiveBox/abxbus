import inspect
from typing import Any

from abxbus import BaseEvent, EventBus, events_suck


class CreateUserEvent(BaseEvent[str]):
    id: str | None = None
    name: str
    age: int
    nickname: str | None = None


class UpdateUserEvent(BaseEvent[bool]):
    id: str
    name: str | None = None
    age: int | None = None
    source: str | None = None


class SomeLegacyImperativeClass:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def create(self, id: str | None, name: str, age: int) -> str:
        self.calls.append(('create', {'id': id, 'name': name, 'age': age}))
        return f'{name}-{age}'

    def update(self, id: str, name: str | None = None, age: int | None = None, **extra: Any) -> bool:
        self.calls.append(('update', {'id': id, 'name': name, 'age': age, **extra}))
        return bool(id)


def ping_user(user_id: str) -> str:
    return f'pong:{user_id}'


async def test_events_suck_wrap_emits_and_returns_first_result():
    bus = EventBus('EventsSuckBus')
    seen_payloads: list[dict[str, Any]] = []

    async def on_create(event: CreateUserEvent) -> str:
        seen_payloads.append(
            {
                'id': event.id,
                'name': event.name,
                'age': event.age,
                'nickname': event.nickname,
            }
        )
        return 'user-123'

    async def on_update(event: UpdateUserEvent) -> bool:
        seen_payloads.append(
            {
                'id': event.id,
                'name': event.name,
                'age': event.age,
                'source': event.source,
            }
        )
        return event.age == 46

    bus.on(CreateUserEvent, on_create)
    bus.on(UpdateUserEvent, on_update)

    MySDKClient = events_suck.wrap(
        'MySDKClient',
        {
            'create': CreateUserEvent,
            'update': UpdateUserEvent,
        },
    )
    client = MySDKClient(bus=bus)

    created_id = await client.create(name='bob', age=45, nickname='bobby')
    updated = await client.update(id=created_id, age=46, source='sync')

    assert created_id == 'user-123'
    assert updated is True
    assert seen_payloads == [
        {'id': None, 'name': 'bob', 'age': 45, 'nickname': 'bobby'},
        {'id': created_id, 'name': None, 'age': 46, 'source': 'sync'},
    ]

    await bus.destroy(clear=True)


def test_events_suck_wrap_builds_typed_method_signature():
    TestClient = events_suck.wrap('TestClient', {'create': CreateUserEvent})
    signature = inspect.signature(TestClient.create)
    params = signature.parameters

    assert list(params) == ['self', 'id', 'name', 'age', 'nickname', 'extra']
    assert params['id'].annotation == str | None
    assert params['id'].default is None
    assert params['name'].annotation is str
    assert params['name'].default is inspect.Parameter.empty
    assert params['age'].annotation is int
    assert params['nickname'].annotation == str | None
    assert params['nickname'].default is None
    assert params['extra'].kind == inspect.Parameter.VAR_KEYWORD
    assert signature.return_annotation is str


async def test_events_suck_make_events_and_make_handler_runtime_binding():
    events = events_suck.make_events(
        {
            'FooBarAPICreateEvent': SomeLegacyImperativeClass.create,
            'FooBarAPIUpdateEvent': SomeLegacyImperativeClass.update,
            'FooBarAPIPingEvent': ping_user,
        }
    )
    FooBarAPICreateEvent = events.FooBarAPICreateEvent
    FooBarAPIUpdateEvent = events.FooBarAPIUpdateEvent
    FooBarAPIPingEvent = events.FooBarAPIPingEvent

    assert FooBarAPICreateEvent.model_fields['id'].annotation == str | None
    assert FooBarAPICreateEvent.model_fields['name'].annotation is str
    assert FooBarAPICreateEvent.model_fields['age'].annotation is int
    assert FooBarAPICreateEvent.model_fields['event_result_type'].default is str

    bus = EventBus('LegacyBus')
    impl = SomeLegacyImperativeClass()
    bus.on(FooBarAPICreateEvent, events_suck.make_handler(impl.create))
    bus.on(FooBarAPIUpdateEvent, events_suck.make_handler(impl.update))
    bus.on(FooBarAPIPingEvent, events_suck.make_handler(ping_user))

    create_result = await bus.emit(FooBarAPICreateEvent(name='bob', age=45)).event_result()
    update_result = await bus.emit(
        FooBarAPIUpdateEvent(id='4ddee2b7-782f-7bbf-84ff-6aad2693982e', age=46, source='sync')
    ).event_result()
    ping_result = await bus.emit(FooBarAPIPingEvent(user_id='e692b6cb-ae63-773b-8557-3218f7ce5ced')).event_result()

    assert create_result == 'bob-45'
    assert update_result is True
    assert ping_result == 'pong:e692b6cb-ae63-773b-8557-3218f7ce5ced'
    assert impl.calls == [
        ('create', {'id': None, 'name': 'bob', 'age': 45}),
        ('update', {'id': '4ddee2b7-782f-7bbf-84ff-6aad2693982e', 'name': None, 'age': 46, 'source': 'sync'}),
    ]

    await bus.destroy(clear=True)

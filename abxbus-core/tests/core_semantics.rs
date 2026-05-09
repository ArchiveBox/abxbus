use std::process::Command;
use std::thread;
use std::time::Duration;

use abxbus_core::{
    eligible_results, event_lock_resource, BusDefaults, BusRecord, Core, CorePatch,
    EventConcurrency, EventEnvelope, EventRecord, EventStatus, HandlerCompletion,
    HandlerConcurrency, HandlerOutcome, HandlerRecord, HostHandlerOutcome, HostToCoreMessage,
    LockAcquire, LockManager, LockResource, ProtocolEnvelope, ResultStatus, RouteStep,
    CORE_PROTOCOL_VERSION,
};
use serde_json::{json, Map, Value};

fn bus(id: &str, label: &str, defaults: BusDefaults) -> BusRecord {
    BusRecord {
        bus_id: id.to_string(),
        name: id.to_string(),
        label: label.to_string(),
        defaults,
        host_id: "host".to_string(),
        max_history_size: Some(100),
        max_history_drop: false,
    }
}

fn handler(id: &str, bus_id: &str, event_pattern: &str, handler_seq: usize) -> HandlerRecord {
    HandlerRecord {
        handler_id: id.to_string(),
        bus_id: bus_id.to_string(),
        host_id: "host".to_string(),
        event_pattern: event_pattern.to_string(),
        handler_name: format!("handler_{handler_seq}"),
        handler_file_path: None,
        handler_registered_at: "2026-05-08T00:00:00Z".to_string(),
        handler_timeout: None,
        handler_slow_timeout: None,
        handler_concurrency: None,
        handler_completion: None,
    }
}

fn event(id: &str, event_type: &str) -> EventRecord {
    EventRecord {
        event_id: id.to_string(),
        event_type: event_type.to_string(),
        event_version: "0.0.1".to_string(),
        payload: json!({ "field": "value" }),
        event_result_schema: None,
        event_timeout: None,
        event_slow_timeout: None,
        event_concurrency: None,
        event_handler_timeout: None,
        event_handler_slow_timeout: None,
        event_handler_concurrency: None,
        event_handler_completion: None,
        event_parent_id: None,
        event_emitted_by_result_id: None,
        event_emitted_by_handler_id: None,
        event_blocks_parent_completion: false,
        event_path: Vec::new(),
        event_created_at: "2026-05-08T00:00:00Z".to_string(),
        event_started_at: None,
        event_completed_at: None,
        event_status: EventStatus::Pending,
        completed_result_snapshots: Default::default(),
    }
}

#[test]
fn defaults_resolve_at_route_processing_without_mutating_event_fields() {
    let mut defaults = BusDefaults::default();
    defaults.event_concurrency = EventConcurrency::Parallel;
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;
    defaults.event_handler_completion = HandlerCompletion::First;

    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "TestEvent", 0));
    core.insert_handler(handler("handler-b", "bus-a", "TestEvent", 1));

    let route_id = core
        .emit_to_bus(event("event-a", "TestEvent"), "bus-a")
        .unwrap();
    core.create_missing_result_rows(&route_id).unwrap();

    let stored_event = core.store.events.get("event-a").unwrap();
    assert_eq!(stored_event.event_concurrency, None);
    assert_eq!(stored_event.event_handler_concurrency, None);
    assert_eq!(stored_event.event_handler_completion, None);

    let bus = core.store.buses.get("bus-a").unwrap();
    assert_eq!(
        event_lock_resource(EventConcurrency::Parallel, &bus.bus_id),
        None
    );
    assert_eq!(eligible_results(&core.store, &route_id).len(), 2);
}

#[test]
fn handler_timeout_precedence_matches_python_and_ts() {
    let mut defaults = BusDefaults::default();
    defaults.event_timeout = Some(0.2);
    defaults.event_handler_timeout = None;
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("default-handler", "bus-a", "TimeoutEvent", 0));
    let mut overridden = handler("overridden-handler", "bus-a", "TimeoutEvent", 1);
    overridden.handler_timeout = Some(0.12);
    core.insert_handler(overridden);

    let mut event_record = event("event-a", "TimeoutEvent");
    event_record.event_timeout = Some(0.2);
    event_record.event_handler_timeout = Some(0.05);
    let route_id = core.emit_to_bus(event_record, "bus-a").unwrap();
    core.create_missing_result_rows(&route_id).unwrap();
    let default_result = core
        .store
        .route_results(&route_id)
        .find(|result| result.handler_id == "default-handler")
        .unwrap();
    assert_eq!(default_result.timeout, Some(0.05));
    let overridden_result = core
        .store
        .route_results(&route_id)
        .find(|result| result.handler_id == "overridden-handler")
        .unwrap();
    assert_eq!(overridden_result.timeout, Some(0.12));

    let mut tighter_event_record = event("event-b", "TimeoutEvent");
    tighter_event_record.event_timeout = Some(0.08);
    tighter_event_record.event_handler_timeout = Some(0.2);
    let tighter_route_id = core.emit_to_bus(tighter_event_record, "bus-a").unwrap();
    core.create_missing_result_rows(&tighter_route_id).unwrap();
    for result in core.store.route_results(&tighter_route_id) {
        assert_eq!(result.timeout, Some(0.08));
    }
}

#[test]
fn explicit_event_overrides_beat_processing_bus_defaults() {
    let mut defaults = BusDefaults::default();
    defaults.event_concurrency = EventConcurrency::Parallel;
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;

    let mut record = event("event-a", "OverrideEvent");
    record.event_concurrency = Some(EventConcurrency::BusSerial);
    record.event_handler_concurrency = Some(HandlerConcurrency::Serial);

    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "OverrideEvent", 0));
    core.insert_handler(handler("handler-b", "bus-a", "OverrideEvent", 1));

    let route_id = core.emit_to_bus(record, "bus-a").unwrap();
    core.create_missing_result_rows(&route_id).unwrap();

    let resource = abxbus_core::event_lock_resource_for_route(
        core.store.events.get("event-a").unwrap(),
        core.store.buses.get("bus-a").unwrap(),
    );
    assert_eq!(resource, Some(LockResource::EventBus("bus-a".to_string())));
    assert_eq!(eligible_results(&core.store, &route_id).len(), 1);
}

#[test]
fn import_bus_snapshot_resumes_pending_serial_results_in_core_order() {
    let mut core = Core::default();
    let bus_record = bus("bus-a", "BusA#0001", BusDefaults::default());
    let handler_a = handler("handler-a", "bus-a", "ResumeEvent", 0);
    let handler_b = handler("handler-b", "bus-a", "ResumeEvent", 1);

    core.handle_host_message_for_host(
        "host",
        HostToCoreMessage::ImportBusSnapshot {
            bus: bus_record,
            handlers: vec![handler_a, handler_b],
            events: vec![
                json!({
                    "event_id": "event-a",
                    "event_type": "ResumeEvent",
                    "event_version": "0.0.1",
                    "event_created_at": "2026-05-08T00:00:00Z",
                    "event_status": "pending",
                    "event_results": {
                        "handler-a": {
                            "id": "result-a",
                            "status": "completed",
                            "event_id": "event-a",
                            "handler_id": "handler-a",
                            "result": "seeded",
                            "event_children": []
                        },
                        "handler-b": {
                            "id": "result-b",
                            "status": "pending",
                            "event_id": "event-a",
                            "handler_id": "handler-b",
                            "event_children": []
                        }
                    }
                }),
                json!({
                    "event_id": "event-b",
                    "event_type": "ResumeEvent",
                    "event_version": "0.0.1",
                    "event_created_at": "2026-05-08T00:00:01Z",
                    "event_status": "pending"
                }),
            ],
            pending_event_ids: vec!["event-a".to_string(), "event-b".to_string()],
        },
    )
    .unwrap();

    let first = core.process_next_route_for_bus("bus-a").unwrap().unwrap();
    let RouteStep::InvokeHandlers { invocations, .. } = first else {
        panic!("expected imported pending result to invoke first");
    };
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].event_id, "event-a");
    assert_eq!(invocations[0].handler_id, "handler-b");

    core.accept_handler_outcome(
        "result-b",
        &invocations[0].invocation_id,
        invocations[0].fence,
        HandlerOutcome::Completed {
            value: json!("h2:e1"),
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    core.process_next_route_for_bus("bus-a").unwrap();

    let second = core.process_next_route_for_bus("bus-a").unwrap().unwrap();
    let RouteStep::InvokeHandlers { invocations, .. } = second else {
        panic!("expected next imported queued event to invoke after event-a");
    };
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].event_id, "event-b");
    assert_eq!(invocations[0].handler_id, "handler-a");
}

#[test]
fn lock_manager_preserves_fifo_waiter_order() {
    let mut locks = LockManager::default();
    let resource = Some(LockResource::Custom("resource".to_string()));

    let first = locks.acquire(resource.clone(), "owner-1".to_string(), None);
    assert!(matches!(first, Some(LockAcquire::Granted(_))));

    assert_eq!(
        locks.acquire(resource.clone(), "owner-2".to_string(), None),
        Some(LockAcquire::Waiting { position: 0 })
    );
    assert_eq!(
        locks.acquire(resource.clone(), "owner-3".to_string(), None),
        Some(LockAcquire::Waiting { position: 1 })
    );
    assert_eq!(
        locks.acquire(resource.clone(), "owner-2".to_string(), None),
        Some(LockAcquire::Waiting { position: 0 })
    );

    let LockAcquire::Granted(first_lease) = first.unwrap() else {
        unreachable!();
    };
    locks.release(&first_lease).unwrap();

    assert_eq!(
        locks.acquire(resource.clone(), "owner-3".to_string(), None),
        Some(LockAcquire::Waiting { position: 1 })
    );
    let second = locks.acquire(resource, "owner-2".to_string(), None);
    assert!(matches!(second, Some(LockAcquire::Granted(_))));
}

#[test]
fn serial_route_pause_blocks_overshoot_during_queue_jump() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_bus(bus("bus-b", "BusB#0002", BusDefaults::default()));
    core.insert_handler(handler("parent-1", "bus-a", "ParentEvent", 0));
    core.insert_handler(handler("parent-2", "bus-a", "ParentEvent", 1));
    core.insert_handler(handler("child-a", "bus-a", "ChildEvent", 0));
    core.insert_handler(handler("child-b", "bus-b", "ChildEvent", 0));

    let parent_route = core
        .emit_to_bus(event("parent", "ParentEvent"), "bus-a")
        .unwrap();
    let parent_invocation = core
        .start_eligible_results(&parent_route)
        .unwrap()
        .remove(0);

    let mut child = event("child", "ChildEvent");
    child.event_parent_id = Some("parent".to_string());
    child.event_emitted_by_handler_id = Some(parent_invocation.handler_id.clone());
    child.event_path = vec!["BusB#0002".to_string(), "BusA#0001".to_string()];
    let child_route_a = core.emit_to_bus(child, "bus-a").unwrap();
    let child_route_b = core
        .emit_to_bus(event("child", "ChildEvent"), "bus-b")
        .unwrap();

    core.mark_child_blocks_parent_completion(&parent_invocation.invocation_id, "child")
        .unwrap();
    core.pause_serial_route_for_invocation(&parent_invocation.invocation_id)
        .unwrap();

    assert!(eligible_results(&core.store, &parent_route).is_empty());
    assert_eq!(
        core.routes_for_event_path_order("child"),
        vec![child_route_b, child_route_a]
    );
    assert!(
        core.store
            .events
            .get("child")
            .unwrap()
            .event_blocks_parent_completion
    );
    assert_eq!(
        core.store
            .results
            .get(&parent_invocation.result_id)
            .unwrap()
            .event_children,
        vec!["child".to_string()]
    );
}

#[test]
fn queue_jump_api_marks_blocking_child_pauses_parent_and_borrows_initiating_event_lock() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_handler(handler("parent-1", "bus-a", "ParentEvent", 0));
    core.insert_handler(handler("parent-2", "bus-a", "ParentEvent", 1));
    core.insert_handler(handler("child-a", "bus-a", "ChildEvent", 0));

    let parent_route = core
        .emit_to_bus(event("parent", "ParentEvent"), "bus-a")
        .unwrap();
    let RouteStep::InvokeHandlers {
        invocations: parent_invocations,
        ..
    } = core.process_route_step(&parent_route, None).unwrap()
    else {
        panic!("parent should start first handler");
    };
    let parent_invocation = parent_invocations.first().unwrap();

    let mut child = event("child", "ChildEvent");
    child.event_parent_id = Some("parent".to_string());
    child.event_emitted_by_handler_id = Some(parent_invocation.handler_id.clone());
    child.event_path = vec!["BusA#0001".to_string()];
    let child_route = core.emit_to_bus(child, "bus-a").unwrap();

    let steps = core
        .process_queue_jump_event(&parent_invocation.invocation_id, "child", true, true)
        .unwrap();
    let child_invocations: Vec<_> = steps
        .iter()
        .flat_map(|step| match step {
            RouteStep::InvokeHandlers { invocations, .. } => invocations.clone(),
            _ => Vec::new(),
        })
        .collect();

    assert_eq!(child_invocations.len(), 1);
    let child_invocation = child_invocations.first().unwrap();
    core.accept_handler_outcome(
        &child_invocation.result_id,
        &child_invocation.invocation_id,
        child_invocation.fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    let RouteStep::RouteCompleted {
        event_completed, ..
    } = core.process_route_step(&child_route, None).unwrap()
    else {
        panic!("borrowed child route should complete without reacquiring the parent event lock");
    };
    assert!(event_completed);
    assert_eq!(
        core.store.events.get("child").unwrap().event_status,
        EventStatus::Completed
    );

    core.accept_handler_outcome(
        &parent_invocation.result_id,
        &parent_invocation.invocation_id,
        parent_invocation.fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.store
            .routes
            .get(&parent_route)
            .unwrap()
            .serial_handler_paused_by
            .as_deref(),
        None
    );
    assert!(
        core.store
            .events
            .get("child")
            .unwrap()
            .event_blocks_parent_completion
    );
    assert_eq!(
        core.store
            .results
            .get(&parent_invocation.result_id)
            .unwrap()
            .event_children,
        vec!["child".to_string()]
    );
}

#[test]
fn queue_jump_does_not_leave_bus_serial_waiter_ahead_of_fifo_queue() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_handler(handler("event1-handler", "bus-a", "Event1", 0));
    core.insert_handler(handler("event2-handler", "bus-a", "Event2", 0));
    core.insert_handler(handler("event3-handler", "bus-a", "Event3", 0));
    core.insert_handler(handler("child-a-handler", "bus-a", "ChildA", 0));
    core.insert_handler(handler("child-b-handler", "bus-a", "ChildB", 0));
    core.insert_handler(handler("child-c-handler", "bus-a", "ChildC", 0));

    let event1_route = core
        .emit_to_bus(event("event1", "Event1"), "bus-a")
        .unwrap();
    let event2_route = core
        .emit_to_bus(event("event2", "Event2"), "bus-a")
        .unwrap();
    let event3_route = core
        .emit_to_bus(event("event3", "Event3"), "bus-a")
        .unwrap();

    let RouteStep::InvokeHandlers {
        invocations: event1_invocations,
        ..
    } = core.process_route_step(&event1_route, None).unwrap()
    else {
        panic!("event1 should start");
    };
    let event1_invocation = event1_invocations.first().unwrap().clone();

    for (id, ty) in [
        ("child-a", "ChildA"),
        ("child-b", "ChildB"),
        ("child-c", "ChildC"),
    ] {
        let mut child = event(id, ty);
        child.event_parent_id = Some("event1".to_string());
        child.event_emitted_by_handler_id = Some(event1_invocation.handler_id.clone());
        child.event_path = vec!["BusA#0001".to_string()];
        core.emit_to_bus(child, "bus-a").unwrap();
    }

    let child_b_steps = core
        .process_queue_jump_event(&event1_invocation.invocation_id, "child-b", true, true)
        .unwrap();
    let child_b_invocation = child_b_steps
        .iter()
        .find_map(|step| match step {
            RouteStep::InvokeHandlers { invocations, .. } => invocations.first().cloned(),
            _ => None,
        })
        .expect("child-b should queue-jump");
    core.accept_handler_outcome(
        &child_b_invocation.result_id,
        &child_b_invocation.invocation_id,
        child_b_invocation.fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    let child_b_route = core
        .store
        .results
        .get(&child_b_invocation.result_id)
        .unwrap()
        .route_id
        .clone();
    assert!(matches!(
        core.process_route_step(&child_b_route, None).unwrap(),
        RouteStep::RouteCompleted { .. }
    ));

    core.accept_handler_outcome(
        &event1_invocation.result_id,
        &event1_invocation.invocation_id,
        event1_invocation.fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    assert!(matches!(
        core.process_route_step(&event1_route, None).unwrap(),
        RouteStep::RouteCompleted { .. }
    ));

    let step = core.process_available_routes_for_bus("bus-a").unwrap();
    let started_events = step
        .iter()
        .flat_map(|step| match step {
            RouteStep::InvokeHandlers { invocations, .. } => invocations
                .iter()
                .map(|invocation| invocation.event_id.as_str())
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();
    assert_eq!(started_events, vec!["event2"]);
    let event2_invocation = step
        .iter()
        .find_map(|step| match step {
            RouteStep::InvokeHandlers { invocations, .. } => invocations.first().cloned(),
            _ => None,
        })
        .expect("event2 should start after event1");
    core.accept_handler_outcome(
        &event2_invocation.result_id,
        &event2_invocation.invocation_id,
        event2_invocation.fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    assert!(matches!(
        core.process_route_step(&event2_route, None).unwrap(),
        RouteStep::RouteCompleted { .. }
    ));
    let remaining_steps = core.process_available_routes_for_bus("bus-a").unwrap();
    assert!(
        remaining_steps.iter().any(|step| matches!(
            step,
            RouteStep::InvokeHandlers { invocations, .. }
                if invocations.iter().any(|invocation| invocation.event_id == "event3")
        )),
        "event3 route should not be hidden behind a stale bus-serial waiter"
    );

    assert!(core.store.routes.contains_key(&event2_route));
    assert!(core.store.routes.contains_key(&event3_route));
}

#[test]
fn first_mode_treats_falsy_values_as_winners_but_not_null_or_event_refs() {
    let mut defaults = BusDefaults::default();
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;
    defaults.event_handler_completion = HandlerCompletion::First;

    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "FirstEvent", 0));
    core.insert_handler(handler("handler-b", "bus-a", "FirstEvent", 1));

    let route_id = core
        .emit_to_bus(event("event-a", "FirstEvent"), "bus-a")
        .unwrap();
    let invocations = core.start_eligible_results(&route_id).unwrap();
    assert_eq!(invocations.len(), 2);

    core.accept_handler_outcome(
        &invocations[0].result_id,
        &invocations[0].invocation_id,
        invocations[0].fence,
        HandlerOutcome::Completed {
            value: Value::Null,
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.store
            .results
            .get(&invocations[1].result_id)
            .unwrap()
            .status,
        ResultStatus::Started
    );

    core.accept_handler_outcome(
        &invocations[1].result_id,
        &invocations[1].invocation_id,
        invocations[1].fence,
        HandlerOutcome::Completed {
            value: json!(false),
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();
    assert_eq!(
        core.store
            .results
            .get(&invocations[1].result_id)
            .unwrap()
            .status,
        ResultStatus::Completed
    );
}

#[test]
fn first_mode_aborts_started_losers_and_cancels_pending_losers() {
    let mut defaults = BusDefaults::default();
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;
    defaults.event_handler_completion = HandlerCompletion::First;

    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "FirstEvent", 0));
    core.insert_handler(handler("handler-b", "bus-a", "FirstEvent", 1));
    let mut pending_handler = handler("handler-c", "bus-a", "FirstEvent", 2);
    pending_handler.handler_concurrency = Some(HandlerConcurrency::Serial);
    core.insert_handler(pending_handler);

    let route_id = core
        .emit_to_bus(event("event-a", "FirstEvent"), "bus-a")
        .unwrap();
    core.create_missing_result_rows(&route_id).unwrap();
    let result_ids = core.store.results_by_route.get(&route_id).unwrap().clone();
    let pending_loser_id = result_ids
        .iter()
        .find(|result_id| core.store.results[*result_id].handler_id == "handler-c")
        .unwrap()
        .clone();
    let invocations = core.start_eligible_results(&route_id).unwrap();
    let winner = invocations
        .iter()
        .find(|invocation| core.store.results[&invocation.result_id].handler_id == "handler-a")
        .unwrap();
    let started_loser = invocations
        .iter()
        .find(|invocation| core.store.results[&invocation.result_id].handler_id == "handler-b")
        .unwrap();

    core.accept_handler_outcome(
        &winner.result_id,
        &winner.invocation_id,
        winner.fence,
        HandlerOutcome::Completed {
            value: json!("winner"),
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();

    let started = core.store.results.get(&started_loser.result_id).unwrap();
    assert_eq!(started.status, ResultStatus::Cancelled);
    assert_eq!(
        started.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerAborted
    );
    let pending = core.store.results.get(&pending_loser_id).unwrap();
    assert_eq!(pending.status, ResultStatus::Cancelled);
    assert_eq!(
        pending.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerCancelled
    );
}

#[test]
fn core_deadlines_reject_late_host_outcomes_and_commit_timeout_state() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    let mut timed_handler = handler("handler-a", "bus-a", "TimeoutEvent", 0);
    timed_handler.handler_timeout = Some(0.001);
    core.insert_handler(timed_handler);

    let route_id = core
        .emit_to_bus(event("event-a", "TimeoutEvent"), "bus-a")
        .unwrap();
    let invocation = core.start_eligible_results(&route_id).unwrap().remove(0);
    assert!(invocation.deadline_at.is_some());

    thread::sleep(Duration::from_millis(5));
    let expired = core.expire_timed_out_invocations();
    assert_eq!(expired, vec![invocation.result_id.clone()]);

    let stale = core.accept_handler_outcome(
        &invocation.result_id,
        &invocation.invocation_id,
        invocation.fence,
        HandlerOutcome::Completed {
            value: json!("late"),
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    );
    assert_eq!(
        stale,
        Err(abxbus_core::CoreErrorState::StaleInvocationOutcome)
    );

    let result = core.store.results.get(&invocation.result_id).unwrap();
    assert_eq!(result.status, ResultStatus::Error);
    assert_eq!(
        result.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerTimeout
    );
}

#[test]
fn event_timeout_aborts_started_handlers_cancels_pending_and_completes_event() {
    let mut defaults = BusDefaults::default();
    defaults.event_timeout = Some(0.001);
    defaults.event_handler_timeout = None;
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "HardTimeoutEvent", 0));
    core.insert_handler(handler("handler-b", "bus-a", "HardTimeoutEvent", 1));

    let route_id = core
        .emit_to_bus(event("event-a", "HardTimeoutEvent"), "bus-a")
        .unwrap();
    let RouteStep::InvokeHandlers { invocations, .. } =
        core.process_route_step(&route_id, None).unwrap()
    else {
        panic!("expected first handler invocation");
    };
    assert_eq!(invocations.len(), 1);

    thread::sleep(Duration::from_millis(5));
    assert_eq!(core.expire_timed_out_events(), vec!["event-a".to_string()]);

    let first = core.store.results.get(&invocations[0].result_id).unwrap();
    assert_eq!(first.status, ResultStatus::Error);
    assert_eq!(
        first.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerAborted
    );
    let second = core
        .store
        .route_results(&route_id)
        .find(|result| result.handler_id == "handler-b")
        .unwrap();
    assert_eq!(second.status, ResultStatus::Cancelled);
    assert_eq!(
        core.store.events.get("event-a").unwrap().event_status,
        EventStatus::Completed
    );

    let mut messages = Vec::new();
    core.drain_host_invocations("host", Some("bus-a"), None, &mut messages);
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            abxbus_core::CoreToHostMessage::CancelInvocation {
                invocation_id,
                reason: abxbus_core::CancelReason::EventTimeout,
            } if invocation_id == &invocations[0].invocation_id
        )
    }));
}

#[test]
fn parent_timeout_cancels_awaited_children_but_not_unawaited_children() {
    let mut defaults = BusDefaults::default();
    defaults.event_timeout = None;
    defaults.event_handler_timeout = None;
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("parent-handler", "bus-a", "ParentTimeoutEvent", 0));
    core.insert_handler(handler(
        "awaited-child-handler",
        "bus-a",
        "AwaitedChildEvent",
        0,
    ));
    core.insert_handler(handler(
        "unawaited-child-handler",
        "bus-a",
        "UnawaitedChildEvent",
        0,
    ));

    let mut parent_event = event("parent", "ParentTimeoutEvent");
    parent_event.event_timeout = Some(0.001);
    let parent_route = core.emit_to_bus(parent_event, "bus-a").unwrap();
    let RouteStep::InvokeHandlers {
        invocations: parent_invocations,
        ..
    } = core.process_route_step(&parent_route, None).unwrap()
    else {
        panic!("expected parent invocation");
    };
    let parent_invocation = parent_invocations.first().unwrap();

    let mut awaited_child = event("awaited-child", "AwaitedChildEvent");
    awaited_child.event_parent_id = Some("parent".to_string());
    awaited_child.event_emitted_by_handler_id = Some(parent_invocation.handler_id.clone());
    awaited_child.event_path = vec!["BusA#0001".to_string()];
    core.emit_to_bus(awaited_child, "bus-a").unwrap();
    let awaited_steps = core
        .process_queue_jump_event(
            &parent_invocation.invocation_id,
            "awaited-child",
            true,
            true,
        )
        .unwrap();
    let awaited_invocation = awaited_steps
        .iter()
        .find_map(|step| match step {
            RouteStep::InvokeHandlers { invocations, .. } => invocations.first().cloned(),
            _ => None,
        })
        .expect("awaited child should queue-jump and start");

    let mut unawaited_child = event("unawaited-child", "UnawaitedChildEvent");
    unawaited_child.event_parent_id = Some("parent".to_string());
    unawaited_child.event_emitted_by_handler_id = Some(parent_invocation.handler_id.clone());
    unawaited_child.event_path = vec!["BusA#0001".to_string()];
    core.emit_to_bus(unawaited_child, "bus-a").unwrap();

    thread::sleep(Duration::from_millis(5));
    assert_eq!(core.expire_timed_out_events(), vec!["parent".to_string()]);

    assert_eq!(
        core.store.events.get("parent").unwrap().event_status,
        EventStatus::Completed
    );
    assert_eq!(
        core.store.events.get("awaited-child").unwrap().event_status,
        EventStatus::Completed
    );
    assert_eq!(
        core.store
            .events
            .get("unawaited-child")
            .unwrap()
            .event_status,
        EventStatus::Pending
    );

    let awaited_result = core
        .store
        .results
        .get(&awaited_invocation.result_id)
        .unwrap();
    assert_eq!(awaited_result.status, ResultStatus::Error);
    assert_eq!(
        awaited_result.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerAborted
    );

    let mut messages = Vec::new();
    core.drain_host_invocations("host", Some("bus-a"), None, &mut messages);
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            abxbus_core::CoreToHostMessage::CancelInvocation {
                invocation_id,
                reason: abxbus_core::CancelReason::ParentTimeout,
            } if invocation_id == &awaited_invocation.invocation_id
        )
    }));
}

#[test]
fn slow_handler_warning_is_emitted_by_core_timer_state_once() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    let mut slow_handler = handler("handler-a", "bus-a", "SlowWarningEvent", 0);
    slow_handler.handler_timeout = Some(1.0);
    slow_handler.handler_slow_timeout = Some(0.001);
    core.insert_handler(slow_handler);

    let route_id = core
        .emit_to_bus(event("event-a", "SlowWarningEvent"), "bus-a")
        .unwrap();
    let invocation = core.start_eligible_results(&route_id).unwrap().remove(0);

    thread::sleep(Duration::from_millis(5));
    assert_eq!(
        core.emit_due_slow_warnings(),
        vec![invocation.result_id.clone()]
    );
    assert!(core.emit_due_slow_warnings().is_empty());
    let patches = core.drain_patches();
    assert!(patches.iter().any(|patch| {
        matches!(
            patch,
            CorePatch::ResultSlowWarning {
                result_id,
                invocation_id,
                ..
            } if result_id == &invocation.result_id
                && invocation_id == &invocation.invocation_id
        )
    }));
}

#[test]
fn slow_event_warning_is_emitted_by_core_timer_state_once() {
    let mut defaults = BusDefaults::default();
    defaults.event_timeout = Some(1.0);
    defaults.event_slow_timeout = Some(0.001);
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "SlowEventWarningEvent", 0));

    let route_id = core
        .emit_to_bus(event("event-a", "SlowEventWarningEvent"), "bus-a")
        .unwrap();
    let RouteStep::InvokeHandlers { .. } = core.process_route_step(&route_id, None).unwrap() else {
        panic!("expected handler invocation to start event");
    };

    assert_eq!(
        core.store.events.get("event-a").unwrap().event_slow_timeout,
        None
    );
    thread::sleep(Duration::from_millis(5));
    assert_eq!(
        core.emit_due_event_slow_warnings(),
        vec!["event-a".to_string()]
    );
    assert!(core.emit_due_event_slow_warnings().is_empty());
    let patches = core.drain_patches();
    assert!(patches.iter().any(|patch| {
        matches!(
            patch,
            CorePatch::EventSlowWarning { event_id, .. } if event_id == "event-a"
        )
    }));
}

#[test]
fn flat_event_envelope_preserves_payload_and_result_schema_roundtrip() {
    let schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "count": { "type": "integer" }
        },
        "required": ["name", "count"]
    });
    let mut record = event("event-a", "SchemaEvent");
    record.event_result_schema = Some(schema.clone());
    record.payload = json!({ "name": "example", "count": 7 });

    let envelope = EventEnvelope::from_record(&record).unwrap();
    assert_eq!(envelope.event_result_type, Some(schema));
    assert_eq!(envelope.payload.get("name"), Some(&json!("example")));
    assert_eq!(envelope.payload.get("count"), Some(&json!(7)));

    let roundtripped = envelope.into_record().unwrap();
    assert_eq!(roundtripped.payload, record.payload);
    assert_eq!(roundtripped.event_result_schema, record.event_result_schema);

    let mut bad_payload = Map::new();
    bad_payload.insert("event_custom".to_string(), json!(true));
    assert!(EventEnvelope {
        event_type: "BadEvent".to_string(),
        event_version: "0.0.1".to_string(),
        event_timeout: None,
        event_slow_timeout: None,
        event_concurrency: None,
        event_handler_timeout: None,
        event_handler_slow_timeout: None,
        event_handler_concurrency: None,
        event_handler_completion: None,
        event_blocks_parent_completion: false,
        event_result_type: None,
        event_id: "event-b".to_string(),
        event_path: Vec::new(),
        event_parent_id: None,
        event_emitted_by_result_id: None,
        event_emitted_by_handler_id: None,
        event_created_at: "2026-05-08T00:00:00Z".to_string(),
        event_status: EventStatus::Pending,
        event_started_at: None,
        event_completed_at: None,
        payload: bad_payload,
    }
    .into_record()
    .is_err());
}

#[test]
fn protocol_messages_are_versioned_and_patch_stream_is_drainable() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_handler(handler("handler-a", "bus-a", "ProtocolEvent", 0));
    let route_id = core
        .emit_to_bus(event("event-a", "ProtocolEvent"), "bus-a")
        .unwrap();
    let invocation = core.start_eligible_results(&route_id).unwrap().remove(0);

    let patches = core.drain_patches();
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::BusRegistered { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::HandlerRegistered { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::EventEmitted { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::ResultPending { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::ResultStarted { .. })));
    assert!(core.drain_patches().is_empty());

    let message = ProtocolEnvelope::new(
        "session-a",
        HostToCoreMessage::HandlerOutcome {
            result_id: invocation.result_id,
            invocation_id: invocation.invocation_id,
            fence: invocation.fence,
            outcome: HandlerOutcome::Completed {
                value: json!("ok"),
                result_is_event_reference: false,
                result_is_undefined: false,
            },
            process_route_after: true,
            process_available_after: true,
            compact_response: false,
        },
    );
    let encoded = serde_json::to_value(message).unwrap();
    assert_eq!(encoded["protocol_version"], json!(CORE_PROTOCOL_VERSION));
    assert_eq!(encoded["session_id"], json!("session-a"));
    assert_eq!(encoded["message"]["type"], json!("handler_outcome"));
}

#[test]
fn protocol_dispatcher_emits_invocations_and_commits_outcomes_through_core() {
    let mut core = Core::default();
    let bus = bus("bus-a", "BusA#0001", BusDefaults::default());
    let handler = handler("handler-a", "bus-a", "ProtocolEvent", 0);

    let messages = core
        .handle_host_message(HostToCoreMessage::RegisterBus { bus })
        .unwrap();
    assert!(messages.iter().any(|message| matches!(
        message,
        abxbus_core::CoreToHostMessage::Patch {
            patch: CorePatch::BusRegistered { .. },
            ..
        }
    )));

    core.handle_host_message(HostToCoreMessage::RegisterHandler { handler })
        .unwrap();

    let envelope = EventEnvelope::from_record(&event("event-a", "ProtocolEvent")).unwrap();
    let messages = core
        .handle_host_message(HostToCoreMessage::EmitEvent {
            event: envelope,
            bus_id: "bus-a".to_string(),
            defer_start: false,
            compact_response: false,
            parent_invocation_id: None,
            block_parent_completion: false,
            pause_parent_route: false,
        })
        .unwrap();
    let invocation = messages
        .iter()
        .find_map(|message| match message {
            abxbus_core::CoreToHostMessage::InvokeHandler(invocation) => Some(invocation.clone()),
            _ => None,
        })
        .expect("core should request host handler invocation");

    let messages = core
        .handle_host_message(HostToCoreMessage::HandlerOutcome {
            result_id: invocation.result_id.clone(),
            invocation_id: invocation.invocation_id.clone(),
            fence: invocation.fence,
            outcome: HandlerOutcome::Completed {
                value: json!("ok"),
                result_is_event_reference: false,
                result_is_undefined: false,
            },
            process_route_after: true,
            process_available_after: true,
            compact_response: false,
        })
        .unwrap();
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            abxbus_core::CoreToHostMessage::Patch {
                patch: CorePatch::ResultCompleted { .. },
                ..
            }
        )
    }));
    assert!(messages.iter().any(|message| {
        matches!(
            message,
            abxbus_core::CoreToHostMessage::Patch {
                patch: CorePatch::EventCompleted { .. },
                ..
            }
        )
    }));
}

#[test]
fn protocol_routes_invocations_to_handler_host_inboxes() {
    let mut defaults = BusDefaults::default();
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;
    let mut core = Core::default();
    core.handle_host_message(HostToCoreMessage::RegisterBus {
        bus: bus("bus-a", "BusA#0001", defaults),
    })
    .unwrap();

    let mut local = handler("local-handler", "bus-a", "InboxEvent", 0);
    local.host_id = "host-a".to_string();
    let mut remote = handler("remote-handler", "bus-a", "InboxEvent", 1);
    remote.host_id = "host-b".to_string();
    core.handle_host_message_for_host(
        "host-a",
        HostToCoreMessage::RegisterHandler { handler: local },
    )
    .unwrap();
    core.handle_host_message_for_host(
        "host-b",
        HostToCoreMessage::RegisterHandler { handler: remote },
    )
    .unwrap();
    core.handle_host_message(HostToCoreMessage::EmitEvent {
        event: EventEnvelope::from_record(&event("event-a", "InboxEvent")).unwrap(),
        bus_id: "bus-a".to_string(),
        defer_start: false,
        compact_response: false,
        parent_invocation_id: None,
        block_parent_completion: false,
        pause_parent_route: false,
    })
    .unwrap();

    let host_a_messages = core
        .handle_host_message_for_host(
            "host-a",
            HostToCoreMessage::ProcessNextRoute {
                bus_id: "bus-a".to_string(),
                limit: None,
                compact_response: false,
            },
        )
        .unwrap();
    let host_a_invocations = host_a_messages
        .iter()
        .filter_map(|message| match message {
            abxbus_core::CoreToHostMessage::InvokeHandler(invocation) => {
                Some(invocation.handler_id.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(host_a_invocations, vec!["local-handler"]);

    let host_b_messages = core
        .handle_host_message_for_host(
            "host-b",
            HostToCoreMessage::WaitInvocations {
                bus_id: Some("bus-a".to_string()),
                limit: None,
            },
        )
        .unwrap();
    let host_b_invocations = host_b_messages
        .iter()
        .filter_map(|message| match message {
            abxbus_core::CoreToHostMessage::InvokeHandler(invocation) => {
                Some(invocation.handler_id.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(host_b_invocations, vec!["remote-handler"]);
}

#[test]
fn host_disconnect_unregisters_handlers_and_future_routes_do_not_wait_on_dead_hosts() {
    let mut core = Core::default();
    core.connect_host_session("dead-host");
    core.handle_host_message(HostToCoreMessage::RegisterBus {
        bus: bus("bus-a", "BusA#0001", BusDefaults::default()),
    })
    .unwrap();
    let mut remote = handler("remote-handler", "bus-a", "DisconnectEvent", 0);
    remote.host_id = "dead-host".to_string();
    core.handle_host_message_for_host(
        "dead-host",
        HostToCoreMessage::RegisterHandler { handler: remote },
    )
    .unwrap();

    assert!(core.disconnect_host_session("dead-host"));

    core.handle_host_message(HostToCoreMessage::EmitEvent {
        event: EventEnvelope::from_record(&event("event-a", "DisconnectEvent")).unwrap(),
        bus_id: "bus-a".to_string(),
        defer_start: false,
        compact_response: false,
        parent_invocation_id: None,
        block_parent_completion: false,
        pause_parent_route: false,
    })
    .unwrap();
    let route_id = core
        .store
        .routes_by_event
        .get("event-a")
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let RouteStep::RouteCompleted {
        event_completed, ..
    } = core.process_route_step(&route_id, None).unwrap()
    else {
        panic!("route with only disconnected handlers should complete without work");
    };
    assert!(event_completed);
}

#[test]
fn explicit_disconnect_host_message_detaches_host_without_stopping_core() {
    let mut core = Core::default();
    core.connect_host_session("short-lived-host");
    core.handle_host_message(HostToCoreMessage::RegisterBus {
        bus: bus("bus-a", "BusA#0001", BusDefaults::default()),
    })
    .unwrap();
    let mut remote = handler("remote-handler", "bus-a", "DisconnectMessageEvent", 0);
    remote.host_id = "short-lived-host".to_string();
    core.handle_host_message_for_host(
        "short-lived-host",
        HostToCoreMessage::RegisterHandler { handler: remote },
    )
    .unwrap();

    let messages = core
        .handle_host_message_for_host(
            "short-lived-host",
            HostToCoreMessage::DisconnectHost { host_id: None },
        )
        .unwrap();

    assert!(messages.iter().any(|message| {
        matches!(
            message,
            abxbus_core::CoreToHostMessage::Disconnected { host_id }
                if host_id == "short-lived-host"
        )
    }));
    assert!(!core.active_hosts.contains("short-lived-host"));
    assert!(!core.store.handlers_by_bus["bus-a"].contains(&"remote-handler".to_string()));
    assert!(core.store.buses.contains_key("bus-a"));
}

#[test]
fn host_disconnect_cancels_started_results_with_host_disconnected_reason() {
    let mut defaults = BusDefaults::default();
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;
    let mut core = Core::default();
    core.connect_host_session("dead-host");
    core.handle_host_message(HostToCoreMessage::RegisterBus {
        bus: bus("bus-a", "BusA#0001", defaults),
    })
    .unwrap();
    let mut remote = handler("remote-handler", "bus-a", "DisconnectStartedEvent", 0);
    remote.host_id = "dead-host".to_string();
    core.handle_host_message_for_host(
        "dead-host",
        HostToCoreMessage::RegisterHandler { handler: remote },
    )
    .unwrap();
    core.handle_host_message(HostToCoreMessage::EmitEvent {
        event: EventEnvelope::from_record(&event("event-a", "DisconnectStartedEvent")).unwrap(),
        bus_id: "bus-a".to_string(),
        defer_start: false,
        compact_response: false,
        parent_invocation_id: None,
        block_parent_completion: false,
        pause_parent_route: false,
    })
    .unwrap();
    let messages = core
        .handle_host_message_for_host(
            "dead-host",
            HostToCoreMessage::ProcessNextRoute {
                bus_id: "bus-a".to_string(),
                limit: None,
                compact_response: false,
            },
        )
        .unwrap();
    let invocation = messages
        .iter()
        .find_map(|message| match message {
            abxbus_core::CoreToHostMessage::InvokeHandler(invocation) => Some(invocation.clone()),
            _ => None,
        })
        .expect("disconnected-host handler should have started before disconnect");

    assert!(core.disconnect_host_session("dead-host"));
    let result = core.store.results.get(&invocation.result_id).unwrap();
    assert_eq!(result.status, ResultStatus::Cancelled);
    assert_eq!(
        result.error.as_ref().unwrap().kind,
        abxbus_core::CoreErrorKind::HandlerCancelled
    );
    assert!(core.drain_patches().iter().any(|patch| {
        matches!(
            patch,
            CorePatch::ResultCancelled {
                reason: abxbus_core::CancelReason::HostDisconnected,
                ..
            }
        )
    }));
}

#[test]
fn protocol_queries_return_core_owned_event_snapshots_without_host_history_cache() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_handler(handler("handler-a", "bus-a", "QueryEvent", 0));
    let route_id = core
        .emit_to_bus(event("event-a", "QueryEvent"), "bus-a")
        .unwrap();
    core.create_missing_result_rows(&route_id).unwrap();
    let result_id = core
        .store
        .results_by_route
        .get(&route_id)
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let result = core.store.results.get_mut(&result_id).unwrap();
    result.status = ResultStatus::Completed;
    result.result = Some(json!(42));
    result.started_at = Some("2026-05-08T00:00:00Z".to_string());
    result.completed_at = Some("2026-05-08T00:00:01Z".to_string());
    core.emit_to_bus(event("event-b", "OtherEvent"), "bus-a")
        .unwrap();
    core.emit_to_bus(event("event-c", "QueryEvent"), "bus-a")
        .unwrap();
    core.drain_patches();

    let messages = core
        .handle_host_message(HostToCoreMessage::GetEvent {
            event_id: "event-a".to_string(),
        })
        .unwrap();
    let snapshot = messages
        .iter()
        .find_map(|message| match message {
            abxbus_core::CoreToHostMessage::EventSnapshot { event } => Some(event),
            _ => None,
        })
        .expect("get_event should return a snapshot");
    assert_eq!(snapshot.event.event_type, "QueryEvent");
    assert_eq!(snapshot.event.payload.get("field"), Some(&json!("value")));
    let result = snapshot.event_results.get("handler-a").unwrap();
    assert_eq!(result.status, "completed");
    assert_eq!(result.result, Some(json!(42)));
    assert_eq!(result.handler_name, "handler_0");

    let messages = core
        .handle_host_message(HostToCoreMessage::ListEvents {
            event_pattern: "QueryEvent".to_string(),
            limit: Some(1),
            bus_id: None,
        })
        .unwrap();
    let events = messages
        .iter()
        .find_map(|message| match message {
            abxbus_core::CoreToHostMessage::EventList { events } => Some(events),
            _ => None,
        })
        .expect("list_events should return snapshots");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.event_id, "event-c");
}

#[test]
fn tachyon_core_process_exposes_the_same_versioned_protocol() {
    let socket_path = format!(
        "/tmp/abxbus-core-test-{}.sock",
        abxbus_core::uuid_v7_string()
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_abxbus-core-tachyon"))
        .arg(&socket_path)
        .spawn()
        .expect("tachyon core process should start");

    let client = retry_tachyon_connect(&socket_path);
    let bus = bus("bus-a", "BusA#0001", BusDefaults::default());
    let request = ProtocolEnvelope::new("session-a", HostToCoreMessage::RegisterBus { bus });
    let payload = serde_json::to_vec(&request).unwrap();
    let correlation_id = client.call(&payload, 1).unwrap();
    let response = client.wait(correlation_id, 10_000).unwrap();
    let response_bytes = response.data().to_vec();
    response.commit().unwrap();
    drop(client);
    child.kill().unwrap();
    let _ = child.wait();
    let _ = std::fs::remove_file(&socket_path);

    let responses: Vec<ProtocolEnvelope<abxbus_core::CoreToHostMessage>> =
        serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(responses[0].protocol_version, 1);
    assert_eq!(responses[0].session_id, "session-a");
    assert!(matches!(
        responses[0].message,
        abxbus_core::CoreToHostMessage::Patch {
            patch: CorePatch::BusRegistered { .. },
            ..
        }
    ));
}

fn retry_tachyon_connect(socket_path: &str) -> tachyon_ipc::RpcBus {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        match tachyon_ipc::RpcBus::connect(socket_path) {
            Ok(bus) => return bus,
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    panic!("failed to connect to Tachyon core: {last_error:?}");
}

#[test]
fn route_step_owns_start_invoke_and_completion_transitions() {
    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", BusDefaults::default()));
    core.insert_handler(handler("handler-a", "bus-a", "StepEvent", 0));
    let route_id = core
        .emit_to_bus(event("event-a", "StepEvent"), "bus-a")
        .unwrap();
    core.drain_patches();

    let RouteStep::InvokeHandlers { invocations, .. } =
        core.process_route_step(&route_id, None).unwrap()
    else {
        panic!("expected handler invocation step");
    };
    assert_eq!(invocations.len(), 1);
    let patches = core.drain_patches();
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::RouteStarted { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::EventStarted { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::ResultPending { .. })));
    assert!(patches
        .iter()
        .any(|patch| matches!(patch, CorePatch::ResultStarted { .. })));

    let invocation = &invocations[0];
    core.accept_handler_outcome(
        &invocation.result_id,
        &invocation.invocation_id,
        invocation.fence,
        HandlerOutcome::Completed {
            value: json!("ok"),
            result_is_event_reference: false,
            result_is_undefined: false,
        },
    )
    .unwrap();

    let RouteStep::RouteCompleted {
        event_completed, ..
    } = core.process_route_step(&route_id, None).unwrap()
    else {
        panic!("expected route completion step");
    };
    assert!(event_completed);
    assert_eq!(
        core.store.events.get("event-a").unwrap().event_status,
        EventStatus::Completed
    );
}

#[test]
fn process_next_route_for_bus_uses_bus_queue_order_and_skips_parallel_active_routes() {
    let mut defaults = BusDefaults::default();
    defaults.event_concurrency = EventConcurrency::Parallel;
    defaults.event_handler_concurrency = HandlerConcurrency::Serial;

    let mut core = Core::default();
    core.insert_bus(bus("bus-a", "BusA#0001", defaults));
    core.insert_handler(handler("handler-a", "bus-a", "ParallelEvent", 0));

    let first_route = core
        .emit_to_bus(event("event-a", "ParallelEvent"), "bus-a")
        .unwrap();
    let second_route = core
        .emit_to_bus(event("event-b", "ParallelEvent"), "bus-a")
        .unwrap();

    let first_step = core.process_next_route_for_bus("bus-a").unwrap().unwrap();
    let RouteStep::InvokeHandlers {
        route_id: first_step_route,
        invocations: first_invocations,
    } = first_step
    else {
        panic!("expected first route invocation");
    };
    assert_eq!(first_step_route, first_route);
    assert_eq!(first_invocations.len(), 1);

    let second_step = core.process_next_route_for_bus("bus-a").unwrap().unwrap();
    let RouteStep::InvokeHandlers {
        route_id: second_step_route,
        invocations: second_invocations,
    } = second_step
    else {
        panic!("expected second route invocation");
    };
    assert_eq!(second_step_route, second_route);
    assert_eq!(second_invocations.len(), 1);
}

#[test]
fn deferred_parallel_fanout_routes_create_results_before_completion() {
    let mut defaults = BusDefaults::default();
    defaults.event_concurrency = EventConcurrency::BusSerial;
    defaults.event_handler_concurrency = HandlerConcurrency::Parallel;

    let mut core = Core::default();
    core.handle_host_message_for_host(
        "host-a",
        HostToCoreMessage::RegisterBus {
            bus: bus("bus-a", "BusA#0001", defaults),
        },
    )
    .unwrap();
    for idx in 0..30 {
        core.handle_host_message_for_host(
            "host-a",
            HostToCoreMessage::RegisterHandler {
                handler: handler(&format!("handler-{idx}"), "bus-a", "FanoutEvent", idx),
            },
        )
        .unwrap();
    }

    let first_messages = core
        .handle_host_message_for_host(
            "host-a",
            HostToCoreMessage::EmitEvent {
                event: EventEnvelope::from_record(&event("event-0", "FanoutEvent")).unwrap(),
                bus_id: "bus-a".to_string(),
                defer_start: false,
                compact_response: false,
                parent_invocation_id: None,
                block_parent_completion: false,
                pause_parent_route: false,
            },
        )
        .unwrap();
    let first_invocations = first_messages
        .iter()
        .filter_map(|message| match message {
            abxbus_core::CoreToHostMessage::InvokeHandler(invocation) => Some(invocation.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(first_invocations.len(), 30);

    for idx in 1..25 {
        core.handle_host_message_for_host(
            "host-a",
            HostToCoreMessage::EmitEvent {
                event: EventEnvelope::from_record(&event(&format!("event-{idx}"), "FanoutEvent"))
                    .unwrap(),
                bus_id: "bus-a".to_string(),
                defer_start: true,
                compact_response: false,
                parent_invocation_id: None,
                block_parent_completion: false,
                pause_parent_route: false,
            },
        )
        .unwrap();
    }

    let outcomes = first_invocations
        .iter()
        .map(|invocation| HostHandlerOutcome {
            result_id: invocation.result_id.clone(),
            invocation_id: invocation.invocation_id.clone(),
            fence: invocation.fence,
            outcome: HandlerOutcome::Completed {
                value: Value::Null,
                result_is_event_reference: false,
                result_is_undefined: false,
            },
            process_available_after: false,
        })
        .collect::<Vec<_>>();
    core.handle_host_message_for_host(
        "host-a",
        HostToCoreMessage::HandlerOutcomes {
            outcomes,
            compact_response: false,
        },
    )
    .unwrap();

    let messages = core
        .handle_host_message_for_host(
            "host-a",
            HostToCoreMessage::ProcessNextRoute {
                bus_id: "bus-a".to_string(),
                limit: Some(256),
                compact_response: false,
            },
        )
        .unwrap();
    for message in messages {
        let abxbus_core::CoreToHostMessage::Patch { patch, .. } = message else {
            continue;
        };
        let CorePatch::EventCompleted {
            event_id,
            event_results,
            ..
        } = patch
        else {
            continue;
        };
        assert!(
            !event_results.is_empty(),
            "{event_id} completed without result rows"
        );
    }
}

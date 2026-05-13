PASSIVE_ACTIVITY_EXCLUDED_EVENT_NAMES = (
    "send_push",
    "send_email",
)

PASSIVE_ACTIVITY_EXCLUDED_EVENTS_SQL = ", ".join(
    [f"'{event_name}'" for event_name in PASSIVE_ACTIVITY_EXCLUDED_EVENT_NAMES]
)

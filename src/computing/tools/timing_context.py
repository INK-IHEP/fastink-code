from contextvars import ContextVar

submit_id_var: ContextVar[str | None] = ContextVar(
    "submit_id",
    default=None,
)
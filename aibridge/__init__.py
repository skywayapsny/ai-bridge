"""ai-bridge — OpenAI-compatible file-queue bridge.

Any OpenAI-compatible client talks to the bridge; any agent (upstream LLM
worker, external brain via claim/lease, manual CLI) answers from the queue.

Modules:
    queueio — file-queue protocol (atomic writes, claim/lease)
    config  — BridgeConfig / WorkerConfig (env + CLI)
    app     — FastAPI factory (create_app)
    brain   — /brain/* claim/lease router
    openai  — OpenAI wire format helpers
    models  — pydantic schemas
    server  — bridge entry point
    worker  — upstream-LLM queue worker
    agent   — brain client CLI (claim/respond/renew/watch)
    poll    — file-side queue CLI
"""
__version__ = "2.2.0"

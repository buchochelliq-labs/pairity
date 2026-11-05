"""Explicit deployed-App startup: initialize state before serving any requests."""


def main():
    from pairity.web.app import main as serve
    from pairity.web.database import application_documents, database_failure

    stage = "database configuration"
    try:
        documents = application_documents()
        stage = "schema initialization"
        documents.initialize()
    except Exception as exc:
        # Do not log driver/SDK messages, connection strings or credentials.
        raise SystemExit(
            f"Pairity database initialization failed: {database_failure(stage, exc)}"
        ) from None
    print("Pairity database initialized", flush=True)
    serve()

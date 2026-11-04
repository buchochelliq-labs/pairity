"""Explicit deployed-App startup: initialize state before serving any requests."""


def main():
    from pairity.web.app import main as serve
    from pairity.web.database import application_documents

    try:
        application_documents().initialize()
    except Exception as exc:
        # Connection errors can contain sensitive settings; keep startup logs generic.
        raise SystemExit(
            f"Pairity database initialization failed ({type(exc).__name__}); "
            "check Lakebase resource access"
        ) from None
    print("Pairity database initialized")
    serve()

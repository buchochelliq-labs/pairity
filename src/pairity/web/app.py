"""Thin FastAPI endpoints over Pairity's shared services."""

import os
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pairity.config import SuiteConfig
from pairity.config_storage import RevisionConflict
from pairity.schema import configuration_schema
from pairity.serialization import dump_config
from pairity.submission import RunHandle


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Document(Body):
    content: str = Field(max_length=2 * 1024 * 1024)
    format: Literal["json", "yaml"]


class Save(Document):
    expected_revision: int = Field(ge=0)


class Inspect(Document):
    pair_id: str = Field(min_length=1)


class Preview(Save):
    suite: str


class Patch(Body):
    patch: dict
    pair_ids: list[str] | None = None
    tags: list[str] | None = None
    expected_revision: int = Field(ge=0)
    apply: bool = False


class Submit(Body):
    revision: int = Field(ge=1)
    request_id: str = Field(min_length=1, max_length=64)
    pair_ids: list[str] | None = None
    tags: list[str] | None = None


def create_app(service_factory=None, *, mode="databricks", static_dir=None):
    if mode not in {"databricks", "local"}:
        raise ValueError("Unknown application mode")
    app = FastAPI(title="Pairity", docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def mutation_boundary(request, call_next):
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            if request.headers.get("x-pairity-request") != "1":
                return JSONResponse(
                    {"detail": "Missing same-origin request marker"}, status_code=403
                )
            if request.headers.get("content-type", "").split(";")[0] != "application/json":
                return JSONResponse({"detail": "Use application/json"}, status_code=415)
            size = request.headers.get("content-length", "")
            if not size.isdigit() or int(size) > 3 * 1024 * 1024:
                return JSONResponse(
                    {"detail": "Request exceeds 3 MiB or has no length"}, status_code=413
                )
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'self'"
        )
        return response

    @app.exception_handler(RevisionConflict)
    async def conflict(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(FileNotFoundError)
    async def missing(request, exc):
        return JSONResponse({"detail": "Requested resource is not available"}, status_code=404)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse({"detail": "Request validation failed"}, status_code=422)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        detail = "Configuration validation failed" if isinstance(exc, ValidationError) else str(exc)
        return JSONResponse({"detail": detail}, status_code=422)

    @app.exception_handler(Exception)
    async def failed(request, exc):
        return JSONResponse(
            {"detail": "Operation failed; check app logs and resource permissions"}, status_code=500
        )

    def context(request: Request):
        if mode == "databricks" and not request.headers.get("x-forwarded-access-token"):
            raise HTTPException(401, "Databricks user authentication is required")
        factory = service_factory
        if factory is None:
            from pairity.web.runtime import service_for_request

            factory = service_for_request
        return factory(request)

    dependency = Depends(context)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/v1/session")
    def session(ctx=dependency):
        return {"user": ctx[0], "mode": mode}

    @app.get("/api/v1/schema")
    def schema(ctx=dependency):
        return configuration_schema()

    @app.get("/api/v1/discovery/catalogs")
    def catalogs(
        search: str = Query(default="", max_length=256),
        after: str = Query(default="", max_length=256),
        limit: int = Query(default=50, ge=1, le=100),
        ctx=dependency,
    ):
        return ctx[1].catalog_discovery().catalogs(search=search, after=after, limit=limit)

    @app.get("/api/v1/discovery/schemas")
    def schemas(
        catalog: str,
        search: str = Query(default="", max_length=256),
        after: str = Query(default="", max_length=256),
        limit: int = Query(default=50, ge=1, le=100),
        ctx=dependency,
    ):
        return ctx[1].catalog_discovery().schemas(catalog, search=search, after=after, limit=limit)

    @app.get("/api/v1/discovery/tables")
    def tables(
        catalog: str,
        schema: str,
        search: str = Query(default="", max_length=256),
        after: str = Query(default="", max_length=256),
        limit: int = Query(default=50, ge=1, le=100),
        ctx=dependency,
    ):
        return (
            ctx[1]
            .catalog_discovery()
            .tables(catalog, schema, search=search, after=after, limit=limit)
        )

    @app.post("/api/v1/discovery/inspect")
    def inspect(body: Inspect, ctx=dependency):
        return ctx[1].inspect_pair(body.content, body.format, body.pair_id)

    @app.get("/api/v1/suites")
    def suites(ctx=dependency):
        return {"items": ctx[1].list_suites()}

    @app.post("/api/v1/validate")
    def validate(body: Document, ctx=dependency):
        return ctx[1].validate(body.content, body.format)

    @app.post("/api/v1/preview")
    def preview(body: Preview, ctx=dependency):
        return ctx[1].preview(body.suite, body.content, body.format, body.expected_revision)

    @app.get("/api/v1/exports/{suite:path}")
    def export(
        suite: str,
        format: Literal["json", "yaml"] = "yaml",
        revision: int | None = None,
        ctx=dependency,
    ):
        config = SuiteConfig.model_validate(ctx[1].get(suite, revision)["config"])
        return Response(
            dump_config(config, format),
            media_type="application/json" if format == "json" else "application/yaml",
            headers={"Content-Disposition": f'attachment; filename="suite.{format}"'},
        )

    @app.post("/api/v1/suites/{suite:path}/patch")
    def patch(suite: str, body: Patch, ctx=dependency):
        return ctx[1].patch(suite, **body.model_dump())

    @app.post("/api/v1/suites/{suite:path}/runs")
    def submit(suite: str, body: Submit, ctx=dependency):
        return ctx[1].submit(suite, **body.model_dump())

    @app.get("/api/v1/suites/{suite:path}")
    def get_suite(suite: str, ctx=dependency):
        return ctx[1].get(suite)

    @app.put("/api/v1/suites/{suite:path}")
    def save(suite: str, body: Save, ctx=dependency):
        return ctx[1].save(suite, body.content, body.format, body.expected_revision)

    @app.get("/api/v1/jobs/{run_id}")
    def status(run_id: str, ctx=dependency):
        return ctx[1].submitter.status(RunHandle("databricks-jobs", run_id))

    @app.post("/api/v1/jobs/{run_id}/cancel")
    def cancel(run_id: str, ctx=dependency):
        ctx[1].submitter.cancel(RunHandle("databricks-jobs", run_id))
        return {"cancel_requested": run_id}

    @app.get("/api/v1/runs")
    def runs(suite: str | None = None, limit: int = 50, ctx=dependency):
        service = ctx[1]
        if service.results is None:
            raise ValueError("No result store configured")
        reports = service.results.history(suite=suite, limit=limit)
        items, unavailable = [], 0
        for report in reports:
            config = SuiteConfig.model_validate(report["configuration"])
            try:
                service.authorize_sources(config)
            except Exception:
                # A revoked or dropped source must not expose evidence or hide other runs.
                unavailable += 1
                continue
            items.append(report)
        return {"items": items, "unavailable_count": unavailable}

    @app.get("/api/v1/runs/{run_id}/incident")
    def incident(run_id: str, pair_id: str | None = None, ctx=dependency):
        from pairity.incidents import IncidentExporter

        files = ctx[1].incident(run_id, pair_ids=[pair_id] if pair_id else None)
        return Response(
            IncidentExporter.zip_bytes(files),
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="pairity-incident.zip"'},
        )

    @app.get("/api/v1/runs/{run_id}")
    def report(run_id: str, ctx=dependency):
        return ctx[1].report(run_id)

    directory = Path(static_dir) if static_dir else Path(__file__).parent / "static"
    if directory.exists():
        app.mount("/", StaticFiles(directory=directory, html=True), name="ui")
    return app


def main():
    import uvicorn

    mode = os.environ.get("PAIRITY_APP_MODE", "databricks")
    uvicorn.run(
        create_app(mode=mode),
        host="127.0.0.1" if mode == "local" else "0.0.0.0",
        port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
    )

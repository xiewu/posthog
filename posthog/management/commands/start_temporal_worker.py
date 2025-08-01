import asyncio
import datetime as dt
import functools
import signal

import structlog
from temporalio import workflow
from temporalio.worker import Worker

with workflow.unsafe.imports_passed_through():
    from django.conf import settings
    from django.core.management.base import BaseCommand

from posthog.constants import (
    BATCH_EXPORTS_TASK_QUEUE,
    DATA_MODELING_TASK_QUEUE,
    DATA_WAREHOUSE_COMPACTION_TASK_QUEUE,
    DATA_WAREHOUSE_TASK_QUEUE,
    GENERAL_PURPOSE_TASK_QUEUE,
    SYNC_BATCH_EXPORTS_TASK_QUEUE,
    TEST_TASK_QUEUE,
)
from posthog.temporal.ai import ACTIVITIES as AI_ACTIVITIES, WORKFLOWS as AI_WORKFLOWS
from posthog.temporal.batch_exports import (
    ACTIVITIES as BATCH_EXPORTS_ACTIVITIES,
    WORKFLOWS as BATCH_EXPORTS_WORKFLOWS,
)
from posthog.temporal.common.worker import create_worker
from posthog.temporal.data_imports.settings import ACTIVITIES as DATA_SYNC_ACTIVITIES, WORKFLOWS as DATA_SYNC_WORKFLOWS
from posthog.temporal.data_modeling import ACTIVITIES as DATA_MODELING_ACTIVITIES, WORKFLOWS as DATA_MODELING_WORKFLOWS
from posthog.temporal.delete_persons import (
    ACTIVITIES as DELETE_PERSONS_ACTIVITIES,
    WORKFLOWS as DELETE_PERSONS_WORKFLOWS,
)
from posthog.temporal.proxy_service import ACTIVITIES as PROXY_SERVICE_ACTIVITIES, WORKFLOWS as PROXY_SERVICE_WORKFLOWS
from posthog.temporal.quota_limiting import (
    ACTIVITIES as QUOTA_LIMITING_ACTIVITIES,
    WORKFLOWS as QUOTA_LIMITING_WORKFLOWS,
)
from posthog.temporal.session_recordings import (
    ACTIVITIES as SESSION_RECORDINGS_ACTIVITIES,
    WORKFLOWS as SESSION_RECORDINGS_WORKFLOWS,
)
from posthog.temporal.product_analytics import (
    ACTIVITIES as PRODUCT_ANALYTICS_ACTIVITIES,
    WORKFLOWS as PRODUCT_ANALYTICS_WORKFLOWS,
)
from posthog.temporal.subscriptions import (
    ACTIVITIES as SUBSCRIPTION_ACTIVITIES,
    WORKFLOWS as SUBSCRIPTION_WORKFLOWS,
)
from posthog.temporal.tests.utils.workflow import ACTIVITIES as TEST_ACTIVITIES, WORKFLOWS as TEST_WORKFLOWS
from posthog.temporal.usage_reports import ACTIVITIES as USAGE_REPORTS_ACTIVITIES, WORKFLOWS as USAGE_REPORTS_WORKFLOWS

logger = structlog.get_logger(__name__)

# Workflow and activity index
WORKFLOWS_DICT = {
    SYNC_BATCH_EXPORTS_TASK_QUEUE: BATCH_EXPORTS_WORKFLOWS,
    BATCH_EXPORTS_TASK_QUEUE: BATCH_EXPORTS_WORKFLOWS,
    DATA_WAREHOUSE_TASK_QUEUE: DATA_SYNC_WORKFLOWS + DATA_MODELING_WORKFLOWS,
    DATA_WAREHOUSE_COMPACTION_TASK_QUEUE: DATA_SYNC_WORKFLOWS + DATA_MODELING_WORKFLOWS,
    DATA_MODELING_TASK_QUEUE: DATA_MODELING_WORKFLOWS,
    GENERAL_PURPOSE_TASK_QUEUE: PROXY_SERVICE_WORKFLOWS
    + DELETE_PERSONS_WORKFLOWS
    + AI_WORKFLOWS
    + USAGE_REPORTS_WORKFLOWS
    + SESSION_RECORDINGS_WORKFLOWS
    + QUOTA_LIMITING_WORKFLOWS
    + PRODUCT_ANALYTICS_WORKFLOWS
    + SUBSCRIPTION_WORKFLOWS,
    TEST_TASK_QUEUE: TEST_WORKFLOWS,
}
ACTIVITIES_DICT = {
    SYNC_BATCH_EXPORTS_TASK_QUEUE: BATCH_EXPORTS_ACTIVITIES,
    BATCH_EXPORTS_TASK_QUEUE: BATCH_EXPORTS_ACTIVITIES,
    DATA_WAREHOUSE_TASK_QUEUE: DATA_SYNC_ACTIVITIES + DATA_MODELING_ACTIVITIES,
    DATA_WAREHOUSE_COMPACTION_TASK_QUEUE: DATA_SYNC_ACTIVITIES + DATA_MODELING_ACTIVITIES,
    DATA_MODELING_TASK_QUEUE: DATA_MODELING_ACTIVITIES,
    GENERAL_PURPOSE_TASK_QUEUE: PROXY_SERVICE_ACTIVITIES
    + DELETE_PERSONS_ACTIVITIES
    + AI_ACTIVITIES
    + USAGE_REPORTS_ACTIVITIES
    + SESSION_RECORDINGS_ACTIVITIES
    + QUOTA_LIMITING_ACTIVITIES
    + PRODUCT_ANALYTICS_ACTIVITIES
    + SUBSCRIPTION_ACTIVITIES,
    TEST_TASK_QUEUE: TEST_ACTIVITIES,
}


class Command(BaseCommand):
    help = "Start Temporal Python Django-aware Worker"

    def add_arguments(self, parser):
        parser.add_argument(
            "--temporal-host",
            default=settings.TEMPORAL_HOST,
            help="Hostname for Temporal Scheduler",
        )
        parser.add_argument(
            "--temporal-port",
            default=settings.TEMPORAL_PORT,
            help="Port for Temporal Scheduler",
        )
        parser.add_argument(
            "--namespace",
            default=settings.TEMPORAL_NAMESPACE,
            help="Namespace to connect to",
        )
        parser.add_argument(
            "--task-queue",
            default=settings.TEMPORAL_TASK_QUEUE,
            help="Task queue to service",
        )
        parser.add_argument(
            "--server-root-ca-cert",
            default=settings.TEMPORAL_CLIENT_ROOT_CA,
            help="Optional root server CA cert",
        )
        parser.add_argument(
            "--client-cert",
            default=settings.TEMPORAL_CLIENT_CERT,
            help="Optional client cert",
        )
        parser.add_argument(
            "--client-key",
            default=settings.TEMPORAL_CLIENT_KEY,
            help="Optional client key",
        )
        parser.add_argument(
            "--metrics-port",
            default=settings.PROMETHEUS_METRICS_EXPORT_PORT,
            help="Port to export Prometheus metrics on",
        )
        parser.add_argument(
            "--graceful-shutdown-timeout-seconds",
            default=settings.GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
            help="Time that the worker will wait after shutdown before canceling activities, in seconds",
        )
        parser.add_argument(
            "--max-concurrent-workflow-tasks",
            default=settings.MAX_CONCURRENT_WORKFLOW_TASKS,
            help="Maximum number of concurrent workflow tasks for this worker",
        )
        parser.add_argument(
            "--max-concurrent-activities",
            default=settings.MAX_CONCURRENT_ACTIVITIES,
            help="Maximum number of concurrent activity tasks for this worker",
        )

    def handle(self, *args, **options):
        temporal_host = options["temporal_host"]
        temporal_port = options["temporal_port"]
        namespace = options["namespace"]
        task_queue = options["task_queue"]
        server_root_ca_cert = options.get("server_root_ca_cert", None)
        client_cert = options.get("client_cert", None)
        client_key = options.get("client_key", None)
        graceful_shutdown_timeout_seconds = options.get("graceful_shutdown_timeout_seconds", None)
        max_concurrent_workflow_tasks = options.get("max_concurrent_workflow_tasks", None)
        max_concurrent_activities = options.get("max_concurrent_activities", None)

        try:
            workflows = WORKFLOWS_DICT[task_queue]
            activities = ACTIVITIES_DICT[task_queue]
        except KeyError:
            raise ValueError(f'Task queue "{task_queue}" not found in WORKFLOWS_DICT or ACTIVITIES_DICT')

        if options["client_key"]:
            options["client_key"] = "--SECRET--"

        structlog.reset_defaults()

        logger.info(f"Starting Temporal Worker with options: {options}")

        metrics_port = int(options["metrics_port"])

        shutdown_task = None

        def shutdown_worker_on_signal(worker: Worker, sig: signal.Signals, loop: asyncio.events.AbstractEventLoop):
            """Shutdown Temporal worker on receiving signal."""
            nonlocal shutdown_task

            logger.info("Signal %s received", sig)

            if worker.is_shutdown:
                logger.info("Temporal worker already shut down")
                return

            logger.info("Initiating Temporal worker shutdown")
            shutdown_task = loop.create_task(worker.shutdown())
            logger.info("Finished Temporal worker shutdown")

        with asyncio.Runner() as runner:
            worker = runner.run(
                create_worker(
                    temporal_host,
                    temporal_port,
                    metrics_port=metrics_port,
                    namespace=namespace,
                    task_queue=task_queue,
                    server_root_ca_cert=server_root_ca_cert,
                    client_cert=client_cert,
                    client_key=client_key,
                    workflows=workflows,  # type: ignore
                    activities=activities,
                    graceful_shutdown_timeout=dt.timedelta(seconds=graceful_shutdown_timeout_seconds)
                    if graceful_shutdown_timeout_seconds is not None
                    else None,
                    max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
                    max_concurrent_activities=max_concurrent_activities,
                )
            )

            loop = runner.get_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    functools.partial(shutdown_worker_on_signal, worker=worker, sig=sig, loop=loop),
                )
                loop.add_signal_handler(
                    sig,
                    functools.partial(shutdown_worker_on_signal, worker=worker, sig=sig, loop=loop),
                )

            runner.run(worker.run())

            if shutdown_task:
                _ = runner.run(asyncio.wait([shutdown_task]))

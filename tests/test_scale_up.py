import queue
import sys
import tempfile
import time
from pathlib import Path
import threading
import unittest
from concurrent.futures import Future
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from hcloud import APIException
from hcloud.actions.domain import ActionFailedException, ActionTimeoutException
from hcloud.locations.domain import Location
from hcloud.server_types.domain import ServerType
from hcloud.servers.domain import Server

from testflows.github.hetzner.runners import scale_up as scaling
from testflows.github.hetzner.runners import server as server_ops
from testflows.github.hetzner.runners.shell import shell
from testflows.github.hetzner.runners.config import Config

LABELS = ["self-hosted", "setup-none", "type-cx33-cpx32", "in-nbg1-fsn1-hel1"]


def capacity_error():
    return APIException("resource_unavailable", "capacity unavailable", None)


class DeferredFuture(Future):
    def __init__(self, function, kwargs):
        super().__init__()
        self.function = function
        self.kwargs = kwargs

    def result(self, timeout=None):
        if not self.done():
            try:
                self.set_result(self.function(**self.kwargs))
            except Exception as exc:
                self.set_exception(exc)
        return super().result(timeout)


class DeferredPool:
    _max_workers = 30

    def __init__(self):
        self.futures = []

    def submit(self, function, **kwargs):
        future = DeferredFuture(function, kwargs)
        self.futures.append(future)
        return future


def runner_server(name, labels=LABELS, status=Server.STATUS_RUNNING):
    return SimpleNamespace(
        id=123,
        name=name,
        status=status,
        labels={
            f"github-hetzner-runner-label-{i}": label for i, label in enumerate(labels)
        },
        server_type=ServerType(name="cx33"),
        location=Location(name="nbg1"),
        volumes=[],
    )


class PlannerTests(unittest.TestCase):
    def run_cycle(
        self,
        count=30,
        cap=30,
        existing=None,
        runners=None,
        per_label=None,
        per_run=None,
        same_run=False,
        create=None,
        duplicate_runs=False,
        running_jobs=None,
    ):
        jobs = [
            SimpleNamespace(
                id=i + 1,
                run_id=100 if same_run else 100 + i,
                status="queued",
                raw_data={"labels": LABELS},
            )
            for i in range(count)
        ]
        jobs.extend(running_jobs or [])
        grouped = {}
        for job in jobs:
            grouped.setdefault(job.run_id, []).append(job)
        runs = [
            SimpleNamespace(id=run_id, jobs=lambda js=js: js)
            for run_id, js in grouped.items()
        ]
        repo = MagicMock()
        repo.get_workflow_runs.side_effect = lambda status: (
            runs if status == "queued" or duplicate_runs else []
        )
        repo.get_self_hosted_runners.return_value = runners or []
        repo.get_self_hosted_runner.side_effect = lambda runner_id: next(
            runner
            for runner in (runners or [])
            if getattr(runner, "id", None) == runner_id
        )
        client = MagicMock()
        client.servers.get_all.return_value = existing or []
        client.volumes.get_all.return_value = []
        github = MagicMock()
        github.get_repo.return_value = repo
        stop = threading.Event()
        pool = DeferredPool()
        mailbox = queue.Queue()
        cfg = Config(
            github_token="test",
            hetzner_token="test",
            github_repository="test/repo",
            max_runners=cap,
            max_runners_for_label=per_label or [],
            max_runners_in_workflow_run=per_run,
            recycle=False,
        )
        calls = []

        def successful_create(**kwargs):
            calls.append(kwargs)
            return runner_server(kwargs["name"])

        with ExitStack() as stack:
            stack.enter_context(patch.object(scaling, "Client", return_value=client))
            stack.enter_context(patch.object(scaling, "Github", return_value=github))
            stack.enter_context(patch.object(scaling, "metrics", MagicMock()))
            stack.enter_context(
                patch.object(scaling, "get_server_image", return_value=MagicMock())
            )
            stack.enter_context(
                patch.object(scaling, "get_setup_script", return_value="none.sh")
            )
            stack.enter_context(
                patch.object(scaling, "get_startup_script", return_value="startup.sh")
            )
            stack.enter_context(
                patch.object(
                    scaling, "create_server", side_effect=create or successful_create
                )
            )
            stack.enter_context(
                patch.object(scaling.time, "sleep", side_effect=lambda _: stop.set())
            )
            scaling.scale_up(
                stop, mailbox, pool, [SimpleNamespace(name="test-key")], cfg
            )
        return pool, calls, mailbox

    def test_thirty_jobs_reserve_thirty_runners_with_six_alternatives(self):
        pool, calls, _ = self.run_cycle()
        self.assertEqual(len(pool.futures), 30)
        self.assertEqual(len(calls), 30)
        self.assertTrue(all(len(f.kwargs["candidates"]) == 6 for f in pool.futures))
        self.assertEqual(len({c["name"] for c in calls}), 30)

    def test_existing_and_pending_servers_enforce_global_cap(self):
        existing = [
            runner_server(f"github-hetzner-runner-existing-{i}") for i in range(4)
        ]
        pool, calls, mailbox = self.run_cycle(count=25, cap=10, existing=existing)
        self.assertEqual(len(pool.futures), 6)
        self.assertEqual(len(calls), 6)
        self.assertIsInstance(
            mailbox.get_nowait().exception, scaling.MaxNumberOfServersReached
        )

    def test_cap_one_still_tries_sixth_capacity_alternative(self):
        attempts = []

        def create(**kwargs):
            attempts.append(
                (kwargs["server_type"].name, kwargs["server_location"].name)
            )
            if len(attempts) < 6:
                raise capacity_error()
            return runner_server(kwargs["name"])

        pool, _, _ = self.run_cycle(count=25, cap=1, create=create)
        self.assertEqual(len(pool.futures), 1)
        self.assertEqual(
            attempts,
            [
                ("cx33", "nbg1"),
                ("cx33", "fsn1"),
                ("cx33", "hel1"),
                ("cpx32", "nbg1"),
                ("cpx32", "fsn1"),
                ("cpx32", "hel1"),
            ],
        )
        self.assertIsNone(pool.futures[0].exception())

    def test_idle_runner_is_reserved_for_only_one_queued_job(self):
        name = "github-hetzner-runner-idle"
        idle = SimpleNamespace(
            name=name + "-cx33-nbg1",
            status="online",
            busy=False,
            labels=[{"name": label} for label in LABELS],
        )
        pool, calls, _ = self.run_cycle(existing=[runner_server(name)], runners=[idle])
        self.assertEqual(len(pool.futures), 29)
        self.assertEqual(len(calls), 29)

    def test_named_idle_runner_is_reserved_for_its_job_regardless_of_order(self):
        for run_id, job_id in [(100, 1), (120, 21)]:
            with self.subTest(job_id=job_id):
                name = f"github-hetzner-runner-{run_id}-{job_id}"
                idle = SimpleNamespace(
                    name=name + "-cx33-nbg1",
                    status="online",
                    busy=False,
                    labels=[{"name": label} for label in LABELS],
                )
                pool, calls, _ = self.run_cycle(
                    existing=[runner_server(name)], runners=[idle]
                )
                self.assertEqual(len(pool.futures), 29)
                self.assertEqual(len(calls), 29)

    def test_duplicate_job_does_not_consume_idle_then_create_replacement(self):
        name = "github-hetzner-runner-99-9"
        idle = SimpleNamespace(
            name=name + "-cx33-nbg1",
            status="online",
            busy=False,
            labels=[{"name": label} for label in LABELS],
        )
        pool, calls, _ = self.run_cycle(
            count=1,
            existing=[runner_server(name)],
            runners=[idle],
            duplicate_runs=True,
        )
        self.assertEqual(len(pool.futures), 0)
        self.assertEqual(len(calls), 0)

    def test_label_limit_counts_logical_pending_runners(self):
        pool, calls, _ = self.run_cycle(per_label=[({"type-cx33"}, 4)])
        self.assertEqual(len(pool.futures), 4)
        self.assertEqual(len(calls), 4)

    def test_workflow_limit_counts_logical_pending_runners(self):
        pool, calls, _ = self.run_cycle(per_run=3, same_run=True)
        self.assertEqual(len(pool.futures), 3)
        self.assertEqual(len(calls), 3)

    def test_duplicate_workflow_snapshot_does_not_reserve_twice(self):
        pool, calls, _ = self.run_cycle(count=5, duplicate_runs=True)
        self.assertEqual(len(pool.futures), 5)
        self.assertEqual(len(calls), 5)

    def test_global_provider_error_does_not_try_every_location(self):
        attempts = []

        def create(**kwargs):
            attempts.append(kwargs)
            raise APIException("resource_limit_exceeded", "quota", None)

        pool, _, mailbox = self.run_cycle(count=1, create=create)
        self.assertEqual(len(attempts), 1)
        self.assertIsInstance(pool.futures[0].exception(), APIException)
        self.assertEqual(mailbox.get_nowait().exception.code, "resource_limit_exceeded")

    def busy_runner_and_job(self, server_name, run_id=200, runner_id=42):
        worker = SimpleNamespace(
            id=runner_id,
            name=server_name + "-cx33-nbg1",
            status="online",
            busy=True,
            labels=[{"name": label} for label in LABELS],
        )
        job = SimpleNamespace(
            id=1,
            run_id=run_id,
            status="in_progress",
            raw_data={
                "labels": LABELS,
                "runner_id": runner_id,
                "runner_name": worker.name,
            },
        )
        return worker, job

    def test_running_stolen_job_with_no_backlog_creates_no_vm(self):
        name = "github-hetzner-runner-99-1"
        worker, job = self.busy_runner_and_job(name)
        pool, calls, _ = self.run_cycle(
            count=0,
            existing=[runner_server(name)],
            runners=[worker],
            running_jobs=[job],
        )
        self.assertEqual(len(pool.futures), 0)
        self.assertEqual(calls, [])

    def test_queued_job_whose_named_vm_is_busy_gets_one_replacement(self):
        name = "github-hetzner-runner-100-1"
        worker, job = self.busy_runner_and_job(name)
        pool, calls, _ = self.run_cycle(
            count=1,
            existing=[runner_server(name)],
            runners=[worker],
            running_jobs=[job],
        )
        self.assertEqual(len(pool.futures), 1)
        self.assertEqual(calls[0]["name"], "github-hetzner-runner-200-1")

    def test_several_running_jobs_cannot_reserve_for_one_queued_job(self):
        names = [
            "github-hetzner-runner-100-1",
            "github-hetzner-runner-90-1",
            "github-hetzner-runner-91-1",
        ]
        pairs = [
            self.busy_runner_and_job(name, run_id=200 + i, runner_id=42 + i)
            for i, name in enumerate(names)
        ]
        pool, calls, _ = self.run_cycle(
            count=1,
            existing=[runner_server(name) for name in names],
            runners=[worker for worker, _ in pairs],
            running_jobs=[job for _, job in pairs],
        )
        self.assertEqual(len(pool.futures), 1)
        self.assertEqual(len(calls), 1)

    def test_pending_reservation_covers_queue_before_stolen_runner_processing(self):
        name = "github-hetzner-runner-99-1"
        worker, job = self.busy_runner_and_job(name)
        pool, calls, _ = self.run_cycle(
            count=1,
            existing=[runner_server(name)],
            runners=[worker],
            running_jobs=[job],
        )
        self.assertEqual(len(pool.futures), 1)
        self.assertEqual(calls[0]["name"], "github-hetzner-runner-100-1")

    def test_idle_spare_covers_queue_on_busy_named_vm(self):
        name = "github-hetzner-runner-100-1"
        spare = "github-hetzner-runner-98-1"
        worker, job = self.busy_runner_and_job(name)
        idle = SimpleNamespace(
            id=43,
            name=spare + "-cx33-nbg1",
            status="online",
            busy=False,
            labels=[{"name": label} for label in LABELS],
        )
        pool, calls, _ = self.run_cycle(
            count=1,
            existing=[runner_server(name), runner_server(spare)],
            runners=[worker, idle],
            running_jobs=[job],
        )
        self.assertEqual(len(pool.futures), 0)
        self.assertEqual(calls, [])

    def test_spare_capacity_named_for_running_job_covers_queued_demand(self):
        for spare_state in ("idle", "initializing"):
            with self.subTest(spare_state=spare_state):
                a = "github-hetzner-runner-100-1"
                b = "github-hetzner-runner-200-1"
                c = "github-hetzner-runner-90-1"
                worker_b, job_b = self.busy_runner_and_job(a, run_id=200, runner_id=42)
                worker_c, job_c = self.busy_runner_and_job(c, run_id=201, runner_id=43)
                runners = [worker_b, worker_c]
                if spare_state == "idle":
                    runners.append(
                        SimpleNamespace(
                            id=44,
                            name=b + "-cx33-nbg1",
                            status="online",
                            busy=False,
                            labels=[{"name": label} for label in LABELS],
                        )
                    )
                pool, calls, _ = self.run_cycle(
                    count=1,
                    existing=[runner_server(a), runner_server(b), runner_server(c)],
                    runners=runners,
                    running_jobs=[job_b, job_c],
                )
                self.assertEqual(len(pool.futures), 0)
                self.assertEqual(calls, [])

    def test_spare_initializing_server_covers_only_one_job(self):
        pool, calls, _ = self.run_cycle(
            count=3,
            existing=[runner_server("github-hetzner-runner-99-1")],
        )
        self.assertEqual(len(pool.futures), 2)
        self.assertEqual(len(calls), 2)

    def test_spare_initializing_server_must_match_queued_labels(self):
        pool, calls, _ = self.run_cycle(
            count=3,
            existing=[
                runner_server(
                    "github-hetzner-runner-99-1", labels=["self-hosted", "other-pool"]
                )
            ],
        )
        self.assertEqual(len(pool.futures), 3)
        self.assertEqual(len(calls), 3)

    def test_one_capacity_failure_does_not_block_other_jobs(self):
        attempts = []

        def create(**kwargs):
            attempts.append(kwargs["name"])
            if kwargs["name"].endswith("100-1"):
                raise capacity_error()
            return runner_server(kwargs["name"])

        pool, _, _ = self.run_cycle(count=3, create=create)
        self.assertEqual(len(pool.futures), 3)
        self.assertIsInstance(pool.futures[0].exception(), APIException)
        self.assertIsNone(pool.futures[1].exception())
        self.assertIsNone(pool.futures[2].exception())
        self.assertEqual(len(attempts), 8)


class ProvisioningTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.servers.get_by_name.return_value = None
        self.server = MagicMock()
        self.server.status = Server.STATUS_RUNNING
        self.server.name = "github-hetzner-runner-100-1"
        self.response = SimpleNamespace(
            server=self.server,
            action=MagicMock(),
            next_actions=[MagicMock()],
        )
        self.client.servers.create.return_value = self.response
        self.setup = MagicMock()
        completed = Future()
        completed.set_result(None)
        self.setup.submit.return_value = completed
        self.kwargs = dict(
            hetzner_token="test",
            setup_worker_pool=self.setup,
            labels=LABELS,
            name=self.server.name,
            server_type=ServerType(name="cx33"),
            server_location=Location(name="nbg1"),
            server_volumes=[],
            server_image=MagicMock(),
            server_net_config=MagicMock(),
            startup_script="startup.sh",
            setup_script="none.sh",
            github_token="test",
            github_repository="test/repo",
            ssh_keys=[SimpleNamespace(name="test-key")],
            volumes=[],
            timeout=60,
        )
        self.addCleanup(patch.stopall)
        patch.object(scaling, "Client", return_value=self.client).start()
        patch.object(scaling, "metrics", MagicMock()).start()

    def action_error(self, code="resource_unavailable"):
        action = SimpleNamespace(
            id=1, command="start_server", error={"code": code, "message": "failed"}
        )
        return ActionFailedException(action)

    def test_waits_for_create_and_start_before_registering(self):
        order = []
        self.response.action.wait_until_finished.side_effect = lambda **_: order.append(
            "create"
        )
        self.response.next_actions[0].wait_until_finished.side_effect = (
            lambda **_: order.append("start")
        )
        self.setup.submit.side_effect = (
            lambda *a, **k: order.append("setup") or FutureWithResult()
        )
        self.assertIs(scaling.create_server(**self.kwargs), self.server)
        self.assertEqual(order, ["create", "start", "setup"])

    def test_failed_start_is_deleted_before_capacity_fallback_is_allowed(self):
        self.server.status = Server.STATUS_OFF
        self.response.next_actions[0].wait_until_finished.side_effect = (
            self.action_error()
        )
        with self.assertRaises(APIException) as caught:
            scaling.create_server(**self.kwargs)
        self.assertEqual(caught.exception.code, "resource_unavailable")
        self.server.delete.return_value.wait_until_finished.assert_called_once_with(
            max_retries=60
        )
        self.setup.submit.assert_not_called()

    def test_failed_start_with_unknown_server_state_does_not_delete_or_fallback(self):
        self.server.status = Server.STATUS_STARTING
        self.response.action.wait_until_finished.side_effect = self.action_error()
        with self.assertRaises(RuntimeError):
            scaling.create_server(**self.kwargs)
        self.server.delete.assert_not_called()
        self.setup.submit.assert_not_called()

    def test_action_timeout_is_retained_for_reconciliation(self):
        action = SimpleNamespace(id=1, command="create_server", error=None)
        self.response.action.wait_until_finished.side_effect = ActionTimeoutException(
            action
        )
        with self.assertRaises(ActionTimeoutException):
            scaling.create_server(**self.kwargs)
        self.server.delete.assert_not_called()
        self.setup.submit.assert_not_called()

    def test_lost_create_response_with_allocated_server_does_not_fallback(self):
        self.client.servers.create.side_effect = capacity_error()
        self.client.servers.get_by_name.return_value = self.server
        with self.assertRaisesRegex(RuntimeError, "uncertain"):
            scaling.create_server(**self.kwargs)
        self.server.delete.assert_not_called()
        self.setup.submit.assert_not_called()

    def test_asynchronous_capacity_failure_retries_after_confirmed_deletion(self):
        order = []
        self.server.status = Server.STATUS_OFF
        self.response.next_actions[0].wait_until_finished.side_effect = (
            self.action_error()
        )
        self.server.delete.return_value.wait_until_finished.side_effect = (
            lambda **_: order.append("deleted")
        )
        second_server = MagicMock()
        second_response = SimpleNamespace(
            server=second_server, action=MagicMock(), next_actions=[]
        )
        responses = iter([self.response, second_response])

        def create(**kwargs):
            order.append(kwargs["location"].name)
            return next(responses)

        self.client.servers.create.side_effect = create
        kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k not in {"server_type", "server_location", "startup_script"}
        }
        result = scaling.create_server_with_fallback(
            candidates=[
                (ServerType(name="cx33"), Location(name=region), "startup.sh")
                for region in ("nbg1", "fsn1")
            ],
            **kwargs,
        )
        self.assertIs(result, second_server)
        self.assertEqual(order, ["nbg1", "deleted", "fsn1"])
        self.setup.submit.assert_called_once()

    def run_fallback(self):
        kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k not in {"server_type", "server_location", "startup_script"}
        }
        return scaling.create_server_with_fallback(
            candidates=[
                (ServerType(name="cx33"), Location(name=region), "startup.sh")
                for region in ("nbg1", "fsn1")
            ],
            **kwargs,
        )

    def assert_provider_removed_server_retries(self):
        self.server.reload.side_effect = APIException("not_found", "server gone", None)
        second_server = MagicMock()
        self.client.servers.create.side_effect = [
            self.response,
            SimpleNamespace(server=second_server, action=MagicMock(), next_actions=[]),
        ]
        self.assertIs(self.run_fallback(), second_server)
        self.assertEqual(
            [
                call.kwargs["location"].name
                for call in self.client.servers.create.call_args_list
            ],
            ["nbg1", "fsn1"],
        )
        self.server.delete.assert_not_called()
        self.setup.submit.assert_called_once()

    def test_provider_removed_failed_create_retries_next_region(self):
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.assert_provider_removed_server_retries()

    def test_provider_removed_failed_start_retries_next_region(self):
        self.response.next_actions[0].wait_until_finished.side_effect = (
            self.action_error()
        )
        self.assert_provider_removed_server_retries()

    def test_confirmed_absence_releases_volumes_before_fallback(self):
        volume = MagicMock()
        self.kwargs["server_volumes"] = [MagicMock()]
        patch.object(scaling, "get_server_bound_volumes", return_value=[volume]).start()
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.server.reload.side_effect = APIException("not_found", "server gone", None)
        with self.assertRaises(APIException) as caught:
            scaling.create_server(**self.kwargs)
        self.assertEqual(caught.exception.code, "resource_unavailable")
        self.assertIsNone(volume.server)
        self.server.delete.assert_not_called()

    def test_unexpected_reload_error_prevents_fallback(self):
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.server.reload.side_effect = APIException("forbidden", "denied", None)
        with self.assertRaises(APIException) as caught:
            self.run_fallback()
        self.assertEqual(caught.exception.code, "forbidden")
        self.client.servers.create.assert_called_once()
        self.server.delete.assert_not_called()

    def test_delete_race_retries_only_after_server_absence_confirmed(self):
        self.server.status = Server.STATUS_OFF
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.server.reload.side_effect = [None, APIException("not_found", "gone", None)]
        self.server.delete.side_effect = APIException("not_found", "gone", None)
        second_server = MagicMock()
        self.client.servers.create.side_effect = [
            self.response,
            SimpleNamespace(server=second_server, action=MagicMock(), next_actions=[]),
        ]
        self.assertIs(self.run_fallback(), second_server)
        self.assertEqual(self.server.reload.call_count, 2)
        self.assertEqual(self.client.servers.create.call_count, 2)

    def test_missing_delete_action_with_existing_server_prevents_fallback(self):
        self.server.status = Server.STATUS_OFF
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.server.delete.return_value.wait_until_finished.side_effect = APIException(
            "not_found", "action gone", None
        )
        with self.assertRaises(APIException) as caught:
            self.run_fallback()
        self.assertEqual(caught.exception.code, "not_found")
        self.assertEqual(self.server.reload.call_count, 2)
        self.client.servers.create.assert_called_once()

    def test_missing_action_without_capacity_failure_prevents_fallback(self):
        self.response.action.wait_until_finished.side_effect = APIException(
            "not_found", "action gone", None
        )
        with self.assertRaises(APIException) as caught:
            self.run_fallback()
        self.assertEqual(caught.exception.code, "not_found")
        self.client.servers.create.assert_called_once()
        self.server.reload.assert_not_called()
        self.server.delete.assert_not_called()

    def test_failed_deletion_prevents_fallback(self):
        self.server.status = Server.STATUS_OFF
        self.response.action.wait_until_finished.side_effect = self.action_error()
        self.server.delete.side_effect = APIException(
            "locked", "operation still active", None
        )
        kwargs = {
            k: v
            for k, v in self.kwargs.items()
            if k not in {"server_type", "server_location", "startup_script"}
        }
        with self.assertRaises(APIException) as caught:
            scaling.create_server_with_fallback(
                candidates=[
                    (ServerType(name="cx33"), Location(name=region), "startup.sh")
                    for region in ("nbg1", "fsn1")
                ],
                **kwargs,
            )
        self.assertEqual(caught.exception.code, "locked")
        self.client.servers.create.assert_called_once()

    def test_future_wait_timeout_cancels_queued_setup(self):
        from concurrent.futures import TimeoutError as FutureTimeoutError

        future = MagicMock()
        future.result.side_effect = FutureTimeoutError()
        self.setup.submit.return_value = future
        with self.assertRaises(FutureTimeoutError):
            scaling.create_server(**self.kwargs)
        future.result.assert_called_once_with(timeout=65)
        future.cancel.assert_called_once()

    def test_setup_failure_is_observed(self):
        failed = Future()
        failed.set_exception(RuntimeError("registration failed"))
        self.setup.submit.return_value = failed
        with self.assertRaisesRegex(RuntimeError, "registration failed"):
            scaling.create_server(**self.kwargs)
        self.server.delete.assert_not_called()


class BoundedSetupTests(unittest.TestCase):
    def test_silent_command_and_descendant_are_terminated(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = str(Path(directory) / "child-survived")
            child = (
                "import time,pathlib; time.sleep(0.4); pathlib.Path("
                + repr(marker)
                + ").touch()"
            )
            parent = (
                "import subprocess,sys; subprocess.Popen([sys.executable,'-c',"
                + repr(child)
                + "])"
            )
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                shell([sys.executable, "-c", parent], shell=False, timeout=0.1)
            self.assertLess(time.monotonic() - started, 2)
            time.sleep(0.5)
            self.assertFalse(Path(marker).exists())

    def test_failed_command_does_not_expose_command_secrets(self):
        with self.assertRaises(RuntimeError) as caught:
            shell(
                [sys.executable, "-c", "raise SystemExit(1)", "test-secret"],
                shell=False,
                timeout=1,
            )
        self.assertNotIn("test-secret", str(caught.exception))

    def test_ssh_readiness_stops_at_deadline(self):
        attempts = []

        def failed(*args, timeout, **kwargs):
            attempts.append(timeout)
            time.sleep(timeout)
            raise TimeoutError("unreachable")

        with patch.object(
            server_ops, "ip_address", return_value="127.0.0.1"
        ), patch.object(server_ops, "ssh", side_effect=failed):
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                server_ops.wait_ssh(SimpleNamespace(name="test"), timeout=0.02)
            self.assertLess(time.monotonic() - started, 1)
            self.assertEqual(len(attempts), 1)
            self.assertLessEqual(attempts[0], 0.02)

    def test_ssh_readiness_retries_a_timed_out_attempt(self):
        clock = SimpleNamespace(now=0)
        attempts = []

        def attempt(*args, timeout, **kwargs):
            attempts.append(timeout)
            if len(attempts) == 1:
                clock.now += timeout
                raise TimeoutError("first connection attempt timed out")
            return 0

        def sleep(seconds):
            clock.now += seconds

        with patch.object(
            server_ops, "ip_address", return_value="127.0.0.1"
        ), patch.object(server_ops, "ssh", side_effect=attempt), patch.object(
            server_ops,
            "time",
            SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep),
        ):
            server_ops.wait_ssh(SimpleNamespace(name="test"), timeout=30)
        self.assertEqual(attempts, [10, 10])
        self.assertEqual(clock.now, 15)

    def test_expired_queued_setup_does_not_start_network_operations(self):
        with patch.object(scaling, "wait_ssh") as readiness, patch.object(
            scaling, "request"
        ) as request:
            with self.assertRaises(TimeoutError):
                scaling.server_setup(
                    runner_server("test"),
                    "none.sh",
                    "startup.sh",
                    "test",
                    "test/repo",
                    "self-hosted",
                    timeout=1,
                    deadline=time.monotonic() - 1,
                )
            readiness.assert_not_called()
            request.assert_not_called()

    def test_setup_propagates_remaining_budget_to_token_and_ssh_commands(self):
        clock = SimpleNamespace(now=0)
        budgets = []

        def ready(**kwargs):
            clock.now += 2

        def request(*args, **kwargs):
            self.assertEqual(kwargs["timeout"], 8)
            self.assertEqual(kwargs["max_retries"], 0)
            clock.now += 3
            return {"token": "test"}, None

        def ssh(*args, **kwargs):
            budgets.append(kwargs["timeout"])
            clock.now += 2
            return 0

        with patch.object(
            scaling.time, "monotonic", side_effect=lambda: clock.now
        ), patch.object(scaling, "wait_ssh", side_effect=ready), patch.object(
            scaling, "request", side_effect=request
        ), patch.object(
            scaling, "ssh", side_effect=ssh
        ):
            scaling.server_setup(
                runner_server("test"),
                "none.sh",
                "startup.sh",
                "test",
                "test/repo",
                "self-hosted",
                timeout=10,
            )
        self.assertEqual(budgets, [5, 3])


class FutureWithResult(Future):
    def __init__(self):
        super().__init__()
        self.set_result(None)


if __name__ == "__main__":
    unittest.main()

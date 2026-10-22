import queue
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from github.GithubException import GithubException, UnknownObjectException

from testflows.github.hetzner.runners import scale_down as sd
from testflows.github.hetzner.runners.config import Config

LABELS = ["self-hosted", "juicemind-autoscale-v2"]
SSH_KEY = SimpleNamespace(name="autoscaler-v2")


def server(name="github-hetzner-runner-1-2", status="running", labels=LABELS):
    return SimpleNamespace(
        id=123,
        name=name,
        status=status,
        STATUS_OFF="off",
        STATUS_RUNNING="running",
        labels={
            sd.github_runner_label: "active",
            sd.server_ssh_key_label: SSH_KEY.name,
            **{
                f"github-hetzner-runner-label-{i}": label
                for i, label in enumerate(labels)
            },
        },
        server_type=SimpleNamespace(name="cx33"),
        location=SimpleNamespace(name="fsn1"),
        delete=Mock(return_value=Mock()),
        power_off=Mock(),
        update=Mock(),
    )


def runner(
    name="github-hetzner-runner-1-2-worker", busy=False, status="online", labels=LABELS
):
    return SimpleNamespace(
        id=456,
        name=name,
        busy=busy,
        status=status,
        labels=[{"name": label} for label in labels],
    )


class CleanupTest(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.repo = Mock()
        self.server = server()
        self.runner = runner()
        self.runners = [self.runner]
        self.client.servers.get_by_id.return_value = self.server
        self.client.servers.get_by_name.return_value = self.server
        self.client.servers.get_all.return_value = [self.server]
        self.repo.get_self_hosted_runners.side_effect = lambda: list(self.runners)
        self.repo.get_self_hosted_runner.return_value = self.runner

        def remove(current):
            self.runners = [r for r in self.runners if r.id != current.id]
            return True

        self.repo.remove_self_hosted_runner.side_effect = remove
        self.metrics = patch.object(sd, "metrics").start()
        self.log = patch.object(sd.Action, "__enter__", lambda action: action).start()
        self.addCleanup(patch.stopall)

    def prepare(self, **kwargs):
        return sd.prepare_server_cleanup(
            self.client,
            self.repo,
            self.server,
            SSH_KEY,
            LABELS,
            kwargs.pop("expected_status", self.server.status),
            **kwargs,
        )

    def cycles(self, count=2, **options):
        config = Config(
            github_token="test",
            github_repository="example/repo",
            hetzner_token="test",
            recycle=False,
            with_label=LABELS,
            scale_down_interval=60,
            max_powered_off_time=30,
            max_unused_runner_time=30,
            max_runner_registration_time=300,
        )
        for name, value in options.items():
            setattr(config, name, value)
        terminate = threading.Event()
        clock = SimpleNamespace(now=100, cycles=0)

        def sleep(seconds):
            clock.now += seconds
            clock.cycles += 1
            if clock.cycles == count:
                terminate.set()

        with (
            patch.object(sd, "Client", return_value=self.client),
            patch.object(sd, "Github") as github,
            patch.object(
                sd, "time", SimpleNamespace(time=lambda: clock.now, sleep=sleep)
            ),
        ):
            github.return_value.get_repo.return_value = self.repo
            sd.scale_down(terminate, queue.Queue(), SSH_KEY, config)

    def test_busy_runner_from_fresh_lookup_is_not_removed(self):
        self.repo.get_self_hosted_runner.return_value = runner(busy=True)
        self.assertIsNone(self.prepare())
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_job_starts_during_deregistration(self):
        self.repo.remove_self_hosted_runner.side_effect = GithubException(
            422, "runner is busy"
        )
        self.cycles()
        self.server.delete.assert_not_called()

    def test_github_lookup_failure_leaves_vm_untouched(self):
        self.repo.get_self_hosted_runner.side_effect = GithubException(
            503, "unavailable"
        )
        self.cycles()
        self.server.delete.assert_not_called()
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_github_list_failure_leaves_vm_untouched(self):
        self.repo.get_self_hosted_runners.side_effect = GithubException(
            503, "unavailable"
        )
        self.cycles()
        self.server.delete.assert_not_called()

    def test_404_is_confirmed_against_runner_list(self):
        self.repo.get_self_hosted_runner.side_effect = UnknownObjectException(
            404, "not found"
        )
        self.assertIsNone(self.prepare())
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_runner_removed_after_list_is_safe(self):
        def gone(_):
            self.runners = []
            raise UnknownObjectException(404, "not found")

        self.repo.get_self_hosted_runner.side_effect = gone
        self.assertIs(self.prepare(), self.server)

    def test_deregistration_failure_result_prevents_cleanup(self):
        self.repo.remove_self_hosted_runner.side_effect = None
        self.repo.remove_self_hosted_runner.return_value = False
        self.assertIsNone(self.prepare())

    def test_idle_runner_deregistered_before_vm_deletion(self):
        calls = []
        original = self.repo.remove_self_hosted_runner.side_effect

        def remove(current):
            calls.append("deregister")
            return original(current)

        self.repo.remove_self_hosted_runner.side_effect = remove
        self.server.delete.side_effect = lambda: calls.append("delete") or Mock()
        self.cycles()
        self.assertEqual(calls, ["deregister", "delete"])

    def test_powered_off_completed_worker_is_deleted(self):
        self.server.status = "off"
        self.runners = []
        self.cycles()
        self.server.delete.assert_called_once()
        self.server.delete.return_value.wait_until_finished.assert_called_once_with(
            max_retries=60
        )

    def test_failed_vm_deletion_is_retried_without_another_idle_timeout(self):
        self.server.delete.side_effect = [RuntimeError("temporary failure"), Mock()]
        self.cycles(count=3)
        self.assertEqual(self.server.delete.call_count, 2)
        self.repo.remove_self_hosted_runner.assert_called_once()

    def test_failed_delete_retry_never_targets_replacement_with_same_name(self):
        replacement = server()
        replacement.id = 789

        def lost_response():
            self.client.servers.get_all.return_value = [replacement]
            self.client.servers.get_by_name.return_value = replacement
            self.client.servers.get_by_id.return_value = replacement
            raise RuntimeError("delete succeeded but response was lost")

        self.server.delete.side_effect = lost_response
        self.cycles(count=3)
        self.server.delete.assert_called_once()
        replacement.delete.assert_not_called()

    def test_replacement_powered_off_vm_gets_its_own_grace_period(self):
        self.server.status = "off"
        replacement = server(status="off")
        replacement.id = 789
        self.runners = []
        self.client.servers.get_all.side_effect = [[self.server], [replacement]]
        self.client.servers.get_by_id.return_value = replacement
        self.cycles()
        replacement.delete.assert_not_called()

    def test_replacement_zombie_gets_its_own_registration_timeout(self):
        replacement = server()
        replacement.id = 789
        self.runners = []
        self.client.servers.get_all.side_effect = [[self.server], [replacement]]
        self.client.servers.get_by_id.return_value = replacement
        self.cycles(max_runner_registration_time=30)
        replacement.delete.assert_not_called()

    def test_replacement_runner_gets_its_own_idle_timeout(self):
        replacement = runner()
        replacement.id = 789
        self.repo.get_self_hosted_runners.side_effect = [[self.runner], [replacement]]
        self.repo.get_self_hosted_runner.return_value = replacement
        self.cycles()
        self.server.delete.assert_not_called()
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_failed_powered_off_deletion_is_retried_next_cycle(self):
        self.server.status = "off"
        self.runners = []
        self.server.delete.side_effect = [RuntimeError("temporary failure"), Mock()]
        self.cycles(count=3)
        self.assertEqual(self.server.delete.call_count, 2)

    def test_failed_delete_action_is_retried(self):
        self.server.status = "off"
        self.runners = []
        action = Mock()
        action.wait_until_finished.side_effect = RuntimeError("delete action failed")
        self.server.delete.side_effect = [action, Mock()]
        self.cycles(count=3)
        self.assertEqual(self.server.delete.call_count, 2)

    def test_powered_off_worker_that_restarted_is_preserved(self):
        self.server.status = "off"
        self.runners = []
        self.client.servers.get_by_id.return_value = server(status="running")
        self.cycles()
        self.server.delete.assert_not_called()

    def test_state_change_while_deregistering_prevents_deletion(self):
        self.server.status = "off"
        self.client.servers.get_by_id.side_effect = [
            self.server,
            server(status="running"),
        ]
        self.assertIsNone(self.prepare())

    def test_renamed_recycled_worker_is_preserved(self):
        self.client.servers.get_by_id.return_value = server(
            name="github-hetzner-runner-3-4"
        )
        self.assertIsNone(self.prepare())
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_busy_offline_runner_is_preserved(self):
        self.runner.status = "offline"
        self.runner.busy = True
        self.cycles()
        self.server.delete.assert_not_called()

    def test_recycled_worker_is_deleted_when_recycling_disabled(self):
        self.server.name = "github-hetzner-runner-recycle-123"
        self.server.status = "off"
        self.runners = []
        self.cycles()
        self.server.delete.assert_called_once()
        self.server.power_off.assert_not_called()

    def test_unregistered_cancelled_job_worker_is_deleted_after_timeout(self):
        self.runners = []
        self.cycles(max_runner_registration_time=30)
        self.server.delete.assert_called_once()

    def test_newly_registered_zombie_is_preserved(self):
        self.assertIsNone(self.prepare(require_unregistered=True))
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_running_job_never_enters_cleanup(self):
        self.runner.busy = True
        self.cycles(count=4)
        self.server.delete.assert_not_called()
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_legacy_pool_is_not_touched(self):
        self.server.labels.pop("github-hetzner-runner-label-1")
        self.runner.labels = [{"name": "self-hosted"}]
        self.cycles(count=4, max_runner_registration_time=30)
        self.server.delete.assert_not_called()
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_server_with_other_owner_is_not_deleted(self):
        self.server.labels[sd.server_ssh_key_label] = "other-controller"
        self.cycles(count=4, max_runner_registration_time=30)
        self.server.delete.assert_not_called()
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_server_with_other_name_is_not_deleted(self):
        self.server.name = "production-database"
        self.runners = []
        self.cycles(count=4, max_runner_registration_time=30)
        self.server.delete.assert_not_called()

    def test_ownership_change_before_cleanup_is_preserved(self):
        replacement = server()
        replacement.labels[sd.server_ssh_key_label] = "other-controller"
        self.client.servers.get_by_id.return_value = replacement
        self.assertIsNone(self.prepare())
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_label_change_before_cleanup_is_preserved(self):
        self.client.servers.get_by_id.return_value = server(labels=["self-hosted"])
        self.assertIsNone(self.prepare())
        self.repo.remove_self_hosted_runner.assert_not_called()

    def test_orphaned_idle_runner_is_removed(self):
        self.client.servers.get_all.return_value = []
        self.client.servers.get_by_name.return_value = None
        self.cycles()
        self.repo.remove_self_hosted_runner.assert_called_once()
        self.server.delete.assert_not_called()

    def test_hetzner_lookup_failure_preserves_runner_and_vm(self):
        self.client.servers.get_by_name.side_effect = RuntimeError("unavailable")
        self.cycles()
        self.repo.remove_self_hosted_runner.assert_not_called()
        self.server.delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()

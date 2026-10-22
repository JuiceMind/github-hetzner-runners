# JuiceMind runner autoscaler

This fork provisions one ephemeral Hetzner worker per eligible GitHub Actions job.
Machine type and region alternatives share one capacity reservation. Each idle
runner satisfies one queued job, and the configured fleet and label limits also
include workers being provisioned.

Provider capacity failures try the next alternative after the failed allocation
has been deleted. Ambiguous creation responses are reconciled by server name;
quota and authentication errors do not trigger repeated regional attempts.

Cleanup checks the worker's pool, SSH-key owner, identity, and current status.
Idle GitHub runners are deregistered before VM deletion. Cleanup retries failed
deletions and preserves busy workers and uncertain API results.

Deployment configuration and the immutable source revision are maintained in
[`JuiceMind/juicemind-iac`, `ops/hetzner-runners`](https://github.com/JuiceMind/juicemind-iac/tree/main/ops/hetzner-runners).
The deployed pool uses the `juicemind-autoscale-v2` label, a maximum of 30 workers,
no standby workers, and recycling disabled. The controller remains running after
the worker fleet returns to zero.

Run the regression suite with the dependencies declared by `setup.py` installed:

```sh
python -m unittest discover -s tests -v
```

The manual **Hetzner runner canary** workflow exercises real provisioning with
1, 5, 25, or 30 workers. Its success and intentional-failure modes, plus cancelling
a running canary, verify the lifecycle before application deployments use the
pool. Run this only while a controller targets this repository; live canaries
create billable Hetzner VMs. Confirm GitHub registrations and owned worker VMs
return to zero after each run.

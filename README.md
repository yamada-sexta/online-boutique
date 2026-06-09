CloudLab Online Boutique profile
================================

This repository contains a repo-based CloudLab geni-lib profile for deploying
Google's Online Boutique / microservices-demo application on a multi-node K3s
cluster and running an always-on benchmark from a dedicated client node.

CloudLab repo-based profiles require a top-level `profile.py`. When an
experiment starts, CloudLab clones this profile repository to
`/local/repository` on every experiment node, then the profile's `Execute`
services run the setup scripts in `scripts/`.

Important: `profile.py` is executed by CloudLab/geni-lib with Python 2
semantics. Keep `profile.py` Python-2-compatible. The uv-managed Python 3.14
environment is only for the benchmark runner and local editor tooling.

What it does
------------

* Requests one Ubuntu 22.04 raw PC named `control`.
* Requests `worker_count` Ubuntu 22.04 raw PCs named `worker0`, `worker1`, ...
* Requests one Ubuntu 22.04 raw PC named `benchmark`.
* Connects all nodes on a private experiment LAN at `192.168.10.0/24`.
* Installs K3s server on `control`.
* Installs K3s agents on every worker and joins them to the control plane.
* Installs Istio and enables sidecar injection for Online Boutique pods.
* Deploys Prometheus, node-exporter, and kube-state-metrics.
* Clones an Online Boutique git repository on `control`.
* Applies `release/kubernetes-manifests.yaml`.
* Deploys Jaeger 2.19 all-in-one for local in-memory tracing.
* Enables OpenTelemetry tracing on instrumented Online Boutique services.
* Exposes frontend, Jaeger, and Prometheus through Kubernetes NodePorts.
* Runs the benchmark from the dedicated `benchmark` node with `uv run`.
* Pushes benchmark results to a git repository when result pushing is enabled.
* Adds the public SSH keys from `github_key_user` to `ssh_key_login` on every
  experiment node.

Profile parameters
------------------

* `worker_count`: number of Kubernetes worker nodes. The default is `2`.
* `repo_url`: public git URL for the Online Boutique repository. The default is
  `https://github.com/GoogleCloudPlatform/microservices-demo.git`.
* `repo_ref`: branch, tag, or commit to deploy. The default is `main`.
* `node_port`: frontend NodePort. The default is `30080`.
* `jaeger_ui_port`: Jaeger UI NodePort. The default is `30686`.
* `prometheus_port`: Prometheus NodePort. The default is `30090`.
* `k3s_token`: shared K3s join token.
* `benchmark_target_rps`: target frontend requests per second. The default is
  `5`.
* `benchmark_duration_seconds`: benchmark measurement duration. The default is
  `60`.
* `benchmark_warmup_seconds`: warmup duration before measured requests. The
  default is `10`.
* `benchmark_concurrency`: maximum concurrent benchmark requests. The default
  is `8`.
* `benchmark_request_timeout_seconds`: per-request timeout. The default is
  `30`.
* `benchmark_request_paths`: comma-separated frontend paths to request. The
  default is `/`.
* `benchmark_rtt_samples`: number of ping RTT samples to collect. The default
  is `10`.
* `benchmark_trace_limit`: Jaeger trace query limit per service. The default is
  `500`.
* `benchmark_lookback`: Jaeger trace lookback window. The default is `1h`.
* `resource_window`: Prometheus resource query window. The default is `5m`.
* `results_push_enabled`: push benchmark results to git. The default is `true`.
* `results_repo`: git URL for the private benchmark results repository. The
  default is `git@github.com:yamada-sexta/online-boutique-bench-res.git`.
* `results_branch`: branch to push results to. The default is `main`.
* `github_key_user`: GitHub user whose public SSH keys are added to each node.
  The default is `yamada-sexta`.
* `ssh_key_login`: CloudLab login that receives those SSH keys. The default is
  `angl5`.

Benchmark runtime
-----------------

The `benchmark` node installs `curl`, `git`, SSH tooling, uv, and the Python
version from `.python-version`. It then runs:

```sh
cd /local/repository
uv run scripts/run-benchmark.py ...
```

Benchmark defaults live in `profile.py` and `scripts/run-benchmark.py`; there
is no benchmark JSON config file.

Benchmark result deploy key
---------------------------

This profile includes an Ed25519 deploy key pair:

* `keys/submission-key.b64`
* `keys/submission-key.pub`

Before enabling result pushes, add `keys/submission-key.pub` as a write-enabled
deploy key on `yamada-sexta/online-boutique-bench-res`. During result pushing,
the benchmark node decodes the base64 private key into
`/root/.ssh/submission-key`.

Benchmark outputs
-----------------

Automatic output is written to `/local/benchmark-results/<timestamp>/` on the
benchmark node and then pushed to:

```text
runs/<benchmark-hostname>/<timestamp>/
```

The benchmark writes normalized CSV/text/TOML files:

* `http-samples.csv`: per-request curl timing data.
* `warmup-http-samples.csv`: warmup request timing data when warmup is enabled.
* `rtt-samples.csv`: ping RTT samples from the benchmark node to the control
  node.
* `jaeger-spans.csv`: normalized Jaeger spans.
* `service-latency.csv`: per-service span-duration summaries.
* `operation-latency.csv`: per-service/operation span-duration summaries.
* `container-metrics.csv`: selected cAdvisor container counter rates.
* `node-metrics.csv`: selected node-exporter counter rates.
* `container-resource-limits.csv`: CPU and memory limits from kube-state-metrics.
* `deployment-replicas.csv`: deployment replica counts from kube-state-metrics.
* `service-traffic-bytes.csv`: Istio request/response byte totals by service
  pair and direction.
* `kubectl-*.txt`: Kubernetes metadata captured on the control node and copied
  into the benchmark result bundle.
* `summary.toml`: machine-readable run metadata, aggregates, output paths, and
  collection errors.
* `summary.md`: human-readable benchmark summary.

Istio sidecars are enabled because `service-traffic-bytes.csv` depends on
Istio metrics such as `istio_request_bytes_sum` and
`istio_response_bytes_sum`. These are aggregate byte counters, not raw request
payloads.

After instantiation
-------------------

On the `control` node, watch setup progress:

```sh
sudo tail -f /local/logs/online-boutique-setup.log
```

On the `benchmark` node, watch benchmark progress:

```sh
sudo tail -f /local/logs/benchmark-runner-setup.log
```

Open the app:

```text
http://<control-hostname>:30080
```

Open Jaeger:

```text
http://<control-hostname>:30686
```

Open Prometheus:

```text
http://<control-hostname>:30090
```

Inspect Kubernetes on the control node:

```sh
sudo kubectl get nodes -o wide
sudo kubectl get pods -A -o wide
sudo kubectl get service frontend-external
sudo kubectl get service jaeger jaeger-ui
sudo kubectl get service prometheus -n istio-system
```

Worker setup logs are on each worker:

```sh
sudo tail -f /local/logs/k3s-agent-setup.log
```

Deploying local Online Boutique changes
---------------------------------------

The local path `/home/yamada/Repos/microservices-demo` cannot be cloned by a
CloudLab node. To deploy local changes from that repo, push them to a public or
otherwise CloudLab-readable git remote, then instantiate this profile with that
remote as `repo_url` and the desired branch/tag/commit as `repo_ref`.

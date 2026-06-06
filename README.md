CloudLab Online Boutique profile
================================

This repository contains a repo-based CloudLab geni-lib profile for deploying
Google's Online Boutique / microservices-demo application on a multi-node K3s
cluster.

CloudLab repo-based profiles require a top-level `profile.py`. When an
experiment starts, CloudLab clones this profile repository to
`/local/repository` on every experiment node, then the profile's `Execute`
services run the setup scripts in `scripts/`.

What it does
------------

* Requests one Ubuntu 22.04 raw PC named `control`.
* Requests `worker_count` Ubuntu 22.04 raw PCs named `worker0`, `worker1`, ...
* Connects all nodes on a private experiment LAN at `192.168.10.0/24`.
* Installs K3s server on `control`.
* Installs K3s agents on every worker and joins them to the control plane.
* Clones an Online Boutique git repository on `control`.
* Applies `release/kubernetes-manifests.yaml`.
* Deploys Jaeger 2.19 all-in-one for local in-memory tracing.
* Enables OpenTelemetry tracing on instrumented Online Boutique services.
* Exposes the frontend as a Kubernetes NodePort, defaulting to port `30080`.
* Exposes the Jaeger UI as a Kubernetes NodePort, defaulting to port `30686`.
* Runs a configured benchmark after deployment.
* Pushes benchmark results to a git repository using the committed base64
  encoded submission deploy key.
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
* `k3s_token`: shared K3s join token. The default is fine for a class/demo
  experiment; change it if you need a less guessable token.
* `benchmark_enabled`: run the benchmark after deployment. The default is
  `true`.
* `results_push_enabled`: push benchmark results to git. The default is `true`.
* `results_repo`: git URL for the private benchmark results repository. The
  default is `git@github.com:yamada-sexta/online-boutique-bench-res.git`.
* `results_branch`: branch to push results to. The default is `main`.
* `github_key_user`: GitHub user whose public SSH keys are added to each node.
  The default is `yamada-sexta`.
* `ssh_key_login`: CloudLab login that receives those SSH keys. The default is
  `angl5`.

Benchmark result deploy key
---------------------------

This profile includes an Ed25519 deploy key pair:

* `keys/submission-key.b64`
* `keys/submission-key.pub`

Before enabling result pushes, add `keys/submission-key.pub` as a write-enabled
deploy key on `yamada-sexta/online-boutique-bench-res`. During setup, only the
control node decodes the base64 private key into `/root/.ssh/submission-key`
for the git push.

Benchmark configuration
-----------------------

The automatic benchmark reads `benchmark/config.json`. By default, it sends a
small, steady load to the frontend, records HTTP timing fields with `curl`,
collects RTT samples with `ping`, queries Jaeger for service/span latency, and
captures Kubernetes metadata.

Automatic output is written to `/local/benchmark-results/<timestamp>/` on the
control node and then pushed to:

```text
runs/<control-hostname>/<timestamp>/
```

The local path `/home/yamada/Repos/microservices-demo` cannot be cloned by a
CloudLab node. To deploy local changes from that repo, push them to a public or
otherwise CloudLab-readable git remote, then instantiate this profile with that
remote as `repo_url` and the desired branch/tag/commit as `repo_ref`.

After instantiation
-------------------

On the `control` node, watch setup progress:

```sh
sudo tail -f /local/logs/online-boutique-setup.log
```

Open the app:

```text
http://<control-hostname>:30080
```

Open Jaeger:

```text
http://<control-hostname>:30686
```

Inspect Kubernetes:

```sh
sudo kubectl get nodes -o wide
sudo kubectl get pods -o wide
sudo kubectl get service frontend-external
sudo kubectl get service jaeger jaeger-ui
```

Worker setup logs are on each worker:

```sh
sudo tail -f /local/logs/k3s-agent-setup.log
```

Collect latency data
--------------------

Run this on the `control` node after the app is ready:

```sh
/local/repository/scripts/collect-latency.py \
  --endpoint http://$(hostname -f):30080/ \
  --samples 100
```

The collector writes:

* `http-samples.csv`: per-request RTT, TCP connect time, request time,
  time-to-first-byte, and response time.
* `jaeger-spans.csv`: raw trace spans from Jaeger.
* `service-latency.csv`: per-service span-duration summaries.
* `summary.json`: aggregate HTTP and trace statistics.

Outputs are placed under `/local/latency/<timestamp>/`.

Run the configured benchmark manually
-------------------------------------

The setup script runs this automatically when `benchmark_enabled=true`, but it
can also be run manually on the control node:

```sh
sudo /local/repository/scripts/run-benchmark.py \
  --config /local/repository/benchmark/config.json \
  --endpoint http://$(hostname -f):30080/
```

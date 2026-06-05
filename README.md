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

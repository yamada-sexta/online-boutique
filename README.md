CloudLab Online Boutique profile
================================

This repository contains a repo-based CloudLab geni-lib profile for deploying
Google's Online Boutique / microservices-demo application on a single raw PC.

CloudLab repo-based profiles require a top-level `profile.py`. When an
experiment starts, CloudLab clones this profile repository to
`/local/repository` on the experiment node, then the profile's `Execute`
service runs `scripts/setup-online-boutique.sh`.

What it does
------------

* Requests one Ubuntu 22.04 raw PC.
* Installs K3s.
* Clones an Online Boutique git repository.
* Applies `release/kubernetes-manifests.yaml`.
* Exposes the frontend as a Kubernetes NodePort, defaulting to port `30080`.

Profile parameters
------------------

* `repo_url`: public git URL for the Online Boutique repository. The default is
  `https://github.com/GoogleCloudPlatform/microservices-demo.git`.
* `repo_ref`: branch, tag, or commit to deploy. The default is `main`.
* `node_port`: frontend NodePort. The default is `30080`.

The local path `/home/yamada/Repos/microservices-demo` cannot be cloned by a
CloudLab node. To deploy local changes from that repo, push them to a public or
otherwise CloudLab-readable git remote, then instantiate this profile with that
remote as `repo_url` and the desired branch/tag/commit as `repo_ref`.

After instantiation
-------------------

Watch setup progress:

```sh
sudo tail -f /local/logs/online-boutique-setup.log
```

Open the app:

```text
http://<node-hostname>:30080
```

Inspect Kubernetes:

```sh
sudo kubectl get pods
sudo kubectl get service frontend-external
```

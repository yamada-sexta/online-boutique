"""Deploy Online Boutique on a multi-node K3s cluster.

This repository-based CloudLab profile creates one Kubernetes control-plane
node and a configurable number of worker nodes. CloudLab clones this profile
repository to /local/repository on every experiment node before running the
startup commands.

Instructions:
Wait for the control node to finish setup. Then open:

    http://<control-hostname>:30080

You can watch progress with:

    sudo tail -f /local/logs/online-boutique-setup.log

Kubernetes access is available on the control node with:

    sudo kubectl get nodes -o wide
    sudo kubectl get pods -o wide
"""

try:
    from shlex import quote as shell_quote
except ImportError:
    from pipes import quote as shell_quote

import geni.portal as portal
import geni.rspec.pg as rspec


DEFAULT_REPO = "https://github.com/GoogleCloudPlatform/microservices-demo.git"
DEFAULT_REF = "main"
DEFAULT_NODE_PORT = 30080
DEFAULT_WORKER_COUNT = 2
DEFAULT_K3S_TOKEN = "cloudlab-online-boutique-k3s"
CONTROL_IP = "192.168.10.10"
NETMASK = "255.255.255.0"


portal.context.defineParameter(
    "worker_count",
    "Number of Kubernetes worker nodes",
    portal.ParameterType.INTEGER,
    DEFAULT_WORKER_COUNT,
)
portal.context.defineParameter(
    "repo_url",
    "Online Boutique git repository URL",
    portal.ParameterType.STRING,
    DEFAULT_REPO,
)
portal.context.defineParameter(
    "repo_ref",
    "Git branch, tag, or commit to deploy",
    portal.ParameterType.STRING,
    DEFAULT_REF,
)
portal.context.defineParameter(
    "node_port",
    "Frontend NodePort",
    portal.ParameterType.INTEGER,
    DEFAULT_NODE_PORT,
)
portal.context.defineParameter(
    "k3s_token",
    "K3s cluster join token",
    portal.ParameterType.STRING,
    DEFAULT_K3S_TOKEN,
)

params = portal.context.bindParameters()

if params.worker_count < 1 or params.worker_count > 8:
    portal.context.reportError(
        portal.ParameterError(
            "worker_count must be between 1 and 8.",
            ["worker_count"],
        )
    )

if not (
    params.repo_url.startswith("https://")
    or params.repo_url.startswith("http://")
    or params.repo_url.startswith("git://")
):
    portal.context.reportError(
        portal.ParameterError(
            "repo_url must be a public http, https, or git URL that the nodes can clone.",
            ["repo_url"],
        )
    )

if params.node_port < 30000 or params.node_port > 32767:
    portal.context.reportError(
        portal.ParameterError(
            "node_port must be in Kubernetes' default NodePort range, 30000-32767.",
            ["node_port"],
        )
    )

if len(params.k3s_token) < 8 or " " in params.k3s_token:
    portal.context.reportError(
        portal.ParameterError(
            "k3s_token must be at least 8 characters and must not contain spaces.",
            ["k3s_token"],
        )
    )

portal.context.verifyParameters()

request = portal.context.makeRequestRSpec()
lan = request.LAN("k3s-lan")

control = request.RawPC("control")
control.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"
control_iface = control.addInterface("if0")
control_iface.addAddress(rspec.IPv4Address(CONTROL_IP, NETMASK))
lan.addInterface(control_iface)

expected_nodes = params.worker_count + 1
control_command = " ".join(
    [
        "/local/repository/scripts/setup-k3s-server.sh",
        shell_quote(CONTROL_IP),
        shell_quote(params.k3s_token),
        str(expected_nodes),
        shell_quote(params.repo_url),
        shell_quote(params.repo_ref),
        str(params.node_port),
    ]
)
control.addService(rspec.Execute(shell="bash", command=control_command))

for i in range(params.worker_count):
    worker = request.RawPC("worker" + str(i))
    worker.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"
    worker_ip = "192.168.10." + str(11 + i)
    worker_iface = worker.addInterface("if0")
    worker_iface.addAddress(rspec.IPv4Address(worker_ip, NETMASK))
    lan.addInterface(worker_iface)

    worker_command = " ".join(
        [
            "/local/repository/scripts/setup-k3s-agent.sh",
            shell_quote(CONTROL_IP),
            shell_quote(worker_ip),
            shell_quote(params.k3s_token),
        ]
    )
    worker.addService(rspec.Execute(shell="bash", command=worker_command))

portal.context.printRequestRSpec()

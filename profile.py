"""Deploy Online Boutique on a single-node K3s cluster.

This is a repository-based CloudLab profile. CloudLab clones this profile
repository to /local/repository on the experiment node, then runs the setup
script from that clone.

Instructions:
Wait for the node to finish setup. Then open:

    http://<node-hostname>:30080

You can watch progress with:

    sudo tail -f /local/logs/online-boutique-setup.log

Kubernetes access is available on the node with:

    sudo kubectl get pods
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

params = portal.context.bindParameters()

if not (
    params.repo_url.startswith("https://")
    or params.repo_url.startswith("http://")
    or params.repo_url.startswith("git://")
):
    portal.context.reportError(
        portal.ParameterError(
            "repo_url must be a public http, https, or git URL that the node can clone.",
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

portal.context.verifyParameters()

request = portal.context.makeRequestRSpec()

node = request.RawPC("k3s")
node.disk_image = "urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU22-64-STD"

setup_command = " ".join(
    [
        "/local/repository/scripts/setup-online-boutique.sh",
        shell_quote(params.repo_url),
        shell_quote(params.repo_ref),
        str(params.node_port),
    ]
)

node.addService(rspec.Execute(shell="bash", command=setup_command))

portal.context.printRequestRSpec()

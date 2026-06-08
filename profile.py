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
DEFAULT_JAEGER_UI_PORT = 30686
DEFAULT_WORKER_COUNT = 2
DEFAULT_K3S_TOKEN = "cloudlab-online-boutique-k3s"
DEFAULT_RESULTS_REPO = "git@github.com:yamada-sexta/online-boutique-bench-res.git"
DEFAULT_RESULTS_BRANCH = "main"
DEFAULT_GITHUB_KEY_USER = "yamada-sexta"
DEFAULT_SSH_KEY_LOGIN = "angl5"
DEFAULT_BENCHMARK_TARGET_RPS = 5
DEFAULT_BENCHMARK_DURATION_SECONDS = 60
DEFAULT_BENCHMARK_WARMUP_SECONDS = 10
DEFAULT_BENCHMARK_CONCURRENCY = 8
DEFAULT_BENCHMARK_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_BENCHMARK_REQUEST_PATHS = "/"
DEFAULT_BENCHMARK_RTT_SAMPLES = 10
DEFAULT_BENCHMARK_TRACE_LIMIT = 500
DEFAULT_BENCHMARK_LOOKBACK = "1h"
CONTROL_IP = "192.168.10.10"
NETMASK = "255.255.255.0"


def bool_arg(value):
    return "true" if value else "false"


def safe_name(value, allowed_extra):
    if not value:
        return False
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + allowed_extra
    return all(char in allowed for char in value)


def valid_lookback(value):
    if len(value) < 2:
        return False
    return value[:-1].isdigit() and value[-1] in "smhdw"


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
    "jaeger_ui_port",
    "Jaeger UI NodePort",
    portal.ParameterType.INTEGER,
    DEFAULT_JAEGER_UI_PORT,
)
portal.context.defineParameter(
    "k3s_token",
    "K3s cluster join token",
    portal.ParameterType.STRING,
    DEFAULT_K3S_TOKEN,
)
portal.context.defineParameter(
    "benchmark_enabled",
    "Run benchmark after deployment",
    portal.ParameterType.BOOLEAN,
    True,
)
portal.context.defineParameter(
    "benchmark_target_rps",
    "Benchmark target requests per second",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_TARGET_RPS,
)
portal.context.defineParameter(
    "benchmark_duration_seconds",
    "Benchmark duration in seconds",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_DURATION_SECONDS,
)
portal.context.defineParameter(
    "benchmark_warmup_seconds",
    "Benchmark warmup duration in seconds",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_WARMUP_SECONDS,
)
portal.context.defineParameter(
    "benchmark_concurrency",
    "Benchmark request concurrency",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_CONCURRENCY,
)
portal.context.defineParameter(
    "benchmark_request_timeout_seconds",
    "Benchmark per-request timeout in seconds",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_REQUEST_TIMEOUT_SECONDS,
)
portal.context.defineParameter(
    "benchmark_request_paths",
    "Comma-separated frontend request paths",
    portal.ParameterType.STRING,
    DEFAULT_BENCHMARK_REQUEST_PATHS,
)
portal.context.defineParameter(
    "benchmark_rtt_samples",
    "Benchmark ping RTT sample count",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_RTT_SAMPLES,
)
portal.context.defineParameter(
    "benchmark_trace_limit",
    "Jaeger trace query limit per service",
    portal.ParameterType.INTEGER,
    DEFAULT_BENCHMARK_TRACE_LIMIT,
)
portal.context.defineParameter(
    "benchmark_lookback",
    "Jaeger trace lookback window",
    portal.ParameterType.STRING,
    DEFAULT_BENCHMARK_LOOKBACK,
)
portal.context.defineParameter(
    "results_push_enabled",
    "Push benchmark results to git",
    portal.ParameterType.BOOLEAN,
    True,
)
portal.context.defineParameter(
    "results_repo",
    "Benchmark results repository",
    portal.ParameterType.STRING,
    DEFAULT_RESULTS_REPO,
)
portal.context.defineParameter(
    "results_branch",
    "Benchmark results branch",
    portal.ParameterType.STRING,
    DEFAULT_RESULTS_BRANCH,
)
portal.context.defineParameter(
    "github_key_user",
    "GitHub user whose public SSH keys are installed on each node",
    portal.ParameterType.STRING,
    DEFAULT_GITHUB_KEY_USER,
)
portal.context.defineParameter(
    "ssh_key_login",
    "CloudLab login that receives the GitHub public SSH keys",
    portal.ParameterType.STRING,
    DEFAULT_SSH_KEY_LOGIN,
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

if params.jaeger_ui_port < 30000 or params.jaeger_ui_port > 32767:
    portal.context.reportError(
        portal.ParameterError(
            "jaeger_ui_port must be in Kubernetes' default NodePort range, 30000-32767.",
            ["jaeger_ui_port"],
        )
    )

if params.jaeger_ui_port == params.node_port:
    portal.context.reportError(
        portal.ParameterError(
            "jaeger_ui_port must be different from node_port.",
            ["jaeger_ui_port", "node_port"],
        )
    )

if len(params.k3s_token) < 8 or " " in params.k3s_token:
    portal.context.reportError(
        portal.ParameterError(
            "k3s_token must be at least 8 characters and must not contain spaces.",
            ["k3s_token"],
        )
    )

if params.results_push_enabled and not params.results_repo:
    portal.context.reportError(
        portal.ParameterError(
            "results_repo is required when results_push_enabled is true.",
            ["results_repo", "results_push_enabled"],
        )
    )

if params.benchmark_target_rps < 1 or params.benchmark_target_rps > 10000:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_target_rps must be between 1 and 10000.",
            ["benchmark_target_rps"],
        )
    )

if params.benchmark_duration_seconds < 1 or params.benchmark_duration_seconds > 86400:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_duration_seconds must be between 1 and 86400.",
            ["benchmark_duration_seconds"],
        )
    )

if params.benchmark_warmup_seconds < 0 or params.benchmark_warmup_seconds > 3600:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_warmup_seconds must be between 0 and 3600.",
            ["benchmark_warmup_seconds"],
        )
    )

if params.benchmark_concurrency < 1 or params.benchmark_concurrency > 4096:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_concurrency must be between 1 and 4096.",
            ["benchmark_concurrency"],
        )
    )

if (
    params.benchmark_request_timeout_seconds < 1
    or params.benchmark_request_timeout_seconds > 300
):
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_request_timeout_seconds must be between 1 and 300.",
            ["benchmark_request_timeout_seconds"],
        )
    )

if not params.benchmark_request_paths:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_request_paths must contain at least one path.",
            ["benchmark_request_paths"],
        )
    )

for path in params.benchmark_request_paths.split(","):
    path = path.strip()
    if not path:
        continue
    if not (path.startswith("/") or path.startswith("http://") or path.startswith("https://")):
        portal.context.reportError(
            portal.ParameterError(
                "benchmark_request_paths entries must start with '/', 'http://', or 'https://'.",
                ["benchmark_request_paths"],
            )
        )

if params.benchmark_rtt_samples < 0 or params.benchmark_rtt_samples > 1000:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_rtt_samples must be between 0 and 1000.",
            ["benchmark_rtt_samples"],
        )
    )

if params.benchmark_trace_limit < 1 or params.benchmark_trace_limit > 10000:
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_trace_limit must be between 1 and 10000.",
            ["benchmark_trace_limit"],
        )
    )

if not valid_lookback(params.benchmark_lookback):
    portal.context.reportError(
        portal.ParameterError(
            "benchmark_lookback must use digits and a Jaeger time unit like 30m, 1h, or 2d.",
            ["benchmark_lookback"],
        )
    )

if params.results_branch and " " in params.results_branch:
    portal.context.reportError(
        portal.ParameterError(
            "results_branch must not contain spaces.",
            ["results_branch"],
        )
    )

if not safe_name(params.github_key_user, "-"):
    portal.context.reportError(
        portal.ParameterError(
            "github_key_user may contain only letters, numbers, and hyphens.",
            ["github_key_user"],
        )
    )

if not safe_name(params.ssh_key_login, "-_"):
    portal.context.reportError(
        portal.ParameterError(
            "ssh_key_login may contain only letters, numbers, hyphens, and underscores.",
            ["ssh_key_login"],
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
        "sudo",
        "/local/repository/scripts/setup-k3s-server.sh",
        shell_quote(CONTROL_IP),
        shell_quote(params.k3s_token),
        str(expected_nodes),
        shell_quote(params.repo_url),
        shell_quote(params.repo_ref),
        str(params.node_port),
        str(params.jaeger_ui_port),
        shell_quote(bool_arg(params.benchmark_enabled)),
        shell_quote(bool_arg(params.results_push_enabled)),
        shell_quote(params.results_repo),
        shell_quote(params.results_branch),
        shell_quote(params.github_key_user),
        shell_quote(params.ssh_key_login),
        str(params.benchmark_target_rps),
        str(params.benchmark_duration_seconds),
        str(params.benchmark_warmup_seconds),
        str(params.benchmark_concurrency),
        str(params.benchmark_request_timeout_seconds),
        shell_quote(params.benchmark_request_paths),
        str(params.benchmark_rtt_samples),
        str(params.benchmark_trace_limit),
        shell_quote(params.benchmark_lookback),
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
            "sudo",
            "/local/repository/scripts/setup-k3s-agent.sh",
            shell_quote(CONTROL_IP),
            shell_quote(worker_ip),
            shell_quote(params.k3s_token),
            shell_quote(params.github_key_user),
            shell_quote(params.ssh_key_login),
        ]
    )
    worker.addService(rspec.Execute(shell="bash", command=worker_command))

portal.context.printRequestRSpec()

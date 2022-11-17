import subprocess
import json

from ansible.plugins.inventory import BaseInventoryPlugin, Constructable, Cacheable
from ansible.errors import AnsibleRuntimeError

DOCUMENTATION = """
    description:
        - Generate ansible inventory from Kubernetes nodes
    options:
        plugin:
            choices: ['kubernetes_nodes', 'oddbit.kubernetes_nodes.kubernetes_nodes']
            description: token that ensures this is a source file for the 'constructed' plugin.
            required: True
        vars:
            default: {}
            description: list of variables that will be applied to all nodes.
            required: False
            type: dict
        address_from:
            choices: ['InternalIP', 'Hostname']
            default: 'InternalIP'
            description: which node address to use for ansible_host
            required: False
        node_selectors:
            default: {}
            description: label selectors to limit the node selection.
            required: False
            type: dict
    extends_documentation_fragment:
      - constructed
"""


def get_nodes(selectors: dict | None = None):
    cmd = ["kubectl", "get", "nodes", "-o", "json"]

    if selectors:
        for k, v in selectors.items():
            if v is None:
                cmd.extend(("-l", k))
            else:
                cmd.extend(("-l", f"{k}={v}"))

    try:
        return json.loads(subprocess.check_output(cmd)).get("items", [])
    except subprocess.CalledProcessError as err:
        raise AnsibleRuntimeError(f"failed to get nodes: {err}")


class InventoryModule(BaseInventoryPlugin, Constructable, Cacheable):

    NAME = "kubernetes_nodes"

    def verify_file(self, path):
        valid = False
        if super(InventoryModule, self).verify_file(path):
            if path.endswith(("kubernetes_nodes.yaml", "kubernetes_nodes.yml")):
                valid = True
        return valid

    def parse(self, inventory, loader, path, cache=True):
        # call base method to ensure properties are available for use with other helper methods
        super(InventoryModule, self).parse(inventory, loader, path, cache)

        # this method will parse 'common format' inventory sources and
        # update any options declared in DOCUMENTATION as needed
        self.config = self._read_config_data(path)
        self.add_nodes()

    def add_nodes(self):
        for node in get_nodes(selectors=self.get_option('node_selectors')):
            self.add_node(node)

    def add_node(self, node):
        hostname = node["metadata"]["name"]
        addr = next(
            addr
            for addr in node["status"]["addresses"]
            if addr["type"] == self.get_option("address_from")
        )["address"]
        self.inventory.add_host(hostname)
        self.inventory.set_variable(hostname, "ansible_host", addr)
        self.inventory.set_variable(hostname, "node_labels", node["metadata"]["labels"])
        self.inventory.set_variable(
            hostname, "node_annotations", node["metadata"]["annotations"]
        )
        self.inventory.set_variable(hostname, "node_info", node["status"]["nodeInfo"])

        for varname, varval in self.get_option("vars").items():
            self.inventory.set_variable(hostname, varname, varval)

        host_vars = self.inventory.hosts[hostname].vars
        strict = self.get_option("strict", True)

        # Add variables created by the user's Jinja2 expressions to the host
        self._set_composite_vars(
            self.get_option("compose"), host_vars, hostname, strict=True
        )

        # The following two methods combine the provided variables dictionary
        # with the latest host variables Using these methods after
        # _set_composite_vars() allows groups to be created with the composed
        # variables
        self._add_host_to_composed_groups(
            self.get_option("groups"), host_vars, hostname, strict=strict
        )
        self._add_host_to_keyed_groups(
            self.get_option("keyed_groups"), host_vars, hostname, strict=strict
        )

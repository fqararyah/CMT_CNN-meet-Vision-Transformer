import json
from collections import deque

# === Load JSON ===
with open("model_dag_raw.json") as f:
    nodes = json.load(f)

# === Step 1: Build ID maps ===
id_to_node = {node["id"]: node for node in nodes}
id_to_parents = {node["id"]: node.get("parents", []) for node in nodes}
id_to_children = {node["id"]: node.get("children", []) for node in nodes}

# === Step 2: Identify valid nodes ===
valid_types = {"s", "dw", "pw"}
valid_ids = {node["id"] for node in nodes if node["type"] in valid_types}

# === Step 3: Topological sort ===
def topological_sort():
    in_degree = {node["id"]: 0 for node in nodes}
    for node in nodes:
        for child in node.get("children", []):
            in_degree[child] += 1

    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    sorted_ids = []

    while queue:
        nid = queue.popleft()
        sorted_ids.append(nid)
        for child in id_to_children[nid]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    return sorted_ids

topo_order = topological_sort()

# === Helper functions ===
def get_last_valid_ancestors(nid):
    visited = set()
    result = set()
    def dfs(current):
        if current in visited:
            return
        visited.add(current)
        if current in valid_ids:
            result.add(current)
        else:
            for p in id_to_parents.get(current, []):
                dfs(p)
    for pid in id_to_parents.get(nid, []):
        dfs(pid)
    return list(result)

def get_first_valid_descendants(nid):
    visited = set()
    result = set()
    def dfs(current):
        if current in visited:
            return
        visited.add(current)
        if current in valid_ids:
            result.add(current)
        else:
            for c in id_to_children.get(current, []):
                dfs(c)
    for cid in id_to_children.get(nid, []):
        dfs(cid)
    return list(result)

# === Step 4: Build valid + relinked graph ===
valid_nodes = []
for nid in topo_order:
    if nid not in valid_ids:
        continue
    node = id_to_node[nid].copy()
    node["parents"] = get_last_valid_ancestors(nid)
    node["children"] = get_first_valid_descendants(nid)
    valid_nodes.append(node)

# === Step 5: Rebuild bidirectional connections ===
valid_id_to_node = {node["id"]: node for node in valid_nodes}
for node in valid_nodes:
    nid = node["id"]
    for child_id in node["children"]:
        child = valid_id_to_node.get(child_id)
        if child and nid not in child["parents"]:
            child["parents"].append(nid)
    for parent_id in node["parents"]:
        parent = valid_id_to_node.get(parent_id)
        if parent and nid not in parent["children"]:
            parent["children"].append(nid)

# === Step 6: Reassign IDs sequentially ===
old_to_new_id = {}
for new_id, node in enumerate(valid_nodes):
    old_to_new_id[node["id"]] = new_id

for node in valid_nodes:
    node["id"] = old_to_new_id[node["id"]]
    node["parents"] = [old_to_new_id[pid] for pid in node["parents"]]
    node["children"] = [old_to_new_id[cid] for cid in node["children"]]

# === Save final result ===
with open("model_dag.json", "w") as f:
    json.dump(valid_nodes, f, indent=4)

print(f"✅ Final node count: {len(valid_nodes)}. IDs reassigned from 0 to {len(valid_nodes)-1}.")

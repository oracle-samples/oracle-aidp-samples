"""Topological layering by Kahn's algorithm. Pure functions, zero I/O.

Edge {"from": dependent, "to": dependency} means `from` needs `to` to exist
first, so dependencies land in earlier waves.

Cycles are REPORTED, never broken by picking an arbitrary edge to drop. Objects
in a cycle are excluded from the waves and surfaced for a human decision.

A cycle is a strongly connected component of the nodes Kahn's algorithm could
not place, not the whole leftover set: that set once came back as one "cycle",
so a view that merely reads a cycle member was called a member, and two
unrelated cycles merged into one whose reasons named each other's objects.
Stuck nodes outside every cycle are reported as blocked behind the cycle(s)
they depend on.
"""
from __future__ import annotations

import collections
from typing import Callable

__all__ = ["compute_waves"]


def compute_waves(nodes: list[str], edges: list[dict],
                  sort_key: Callable[[str], tuple] | None = None) -> dict:
    node_set = set(nodes)
    key = sort_key or (lambda n: (n,))

    dependents: dict[str, set[str]] = collections.defaultdict(set)
    indegree: dict[str, int] = {n: 0 for n in node_set}
    seen: set[tuple[str, str]] = set()

    for edge in edges:
        dependent, dependency = edge["from"], edge["to"]
        if dependent not in node_set or dependency not in node_set:
            continue
        if dependent == dependency or (dependent, dependency) in seen:
            continue
        seen.add((dependent, dependency))
        dependents[dependency].add(dependent)
        indegree[dependent] += 1

    waves: list[list[str]] = []
    ready = sorted((n for n in node_set if indegree[n] == 0), key=key)
    placed: set[str] = set()

    while ready:
        waves.append(ready)
        placed.update(ready)
        nxt: list[str] = []
        for node in ready:
            for dependent in dependents[node]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    nxt.append(dependent)
        ready = sorted(nxt, key=key)

    stuck = node_set - placed
    needs = {n: {d for d, ds in dependents.items() if n in ds and d in stuck}
             for n in stuck}
    cycles = sorted(sorted(c) for c in _components(needs) if len(c) > 1)
    member = {n: i for i, c in enumerate(cycles) for n in c}
    behind: dict[str, list[str]] = {}
    for node in sorted(stuck - set(member)):
        # Every stuck node outside a cycle reaches one through what it needs;
        # that is the only reason Kahn's algorithm left it unplaced.
        reached, visited, todo = set(), {node}, [node]
        while todo:
            for dep in needs[todo.pop()]:
                if dep in member:
                    reached.add(member[dep])
                elif dep not in visited:
                    visited.add(dep)
                    todo.append(dep)
        behind[node] = sorted(n for i in reached for n in cycles[i])
    return {"waves": waves, "cycles": cycles, "blocked_behind_cycle": behind}


def _components(graph: dict[str, set[str]]) -> list[set[str]]:
    """Strongly connected components (Tarjan), iteratively: a long chain of
    views must not hit the recursion limit."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    out: list[set[str]] = []
    for root in sorted(graph):
        if root in index:
            continue
        index[root] = low[root] = len(index)
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(sorted(graph[root])))]
        while work:
            node, children = work[-1]
            child = next(children, None)
            if child is not None:
                if child not in index:
                    index[child] = low[child] = len(index)
                    stack.append(child)
                    on_stack.add(child)
                    work.append((child, iter(sorted(graph[child]))))
                elif child in on_stack:
                    low[node] = min(low[node], index[child])
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index[node]:
                comp = set()
                while True:
                    top = stack.pop()
                    on_stack.discard(top)
                    comp.add(top)
                    if top == node:
                        break
                out.append(comp)
    return out

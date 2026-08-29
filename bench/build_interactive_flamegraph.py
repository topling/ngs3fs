#!/usr/bin/env python3

import argparse
import html
import json
from pathlib import Path


def add_stack(root: dict, frames: list[str], samples: int) -> None:
    root["value"] += samples
    node = root
    for frame in frames:
        children = node["children"]
        child = children.get(frame)
        if child is None:
            child = {"name": frame, "value": 0, "children": {}}
            children[frame] = child
        child["value"] += samples
        node = child


def pack_tree(root: dict) -> dict:
    names: list[str] = []
    name_ids: dict[str, int] = {}

    def name_id(name: str) -> int:
        result = name_ids.get(name)
        if result is None:
            result = len(names)
            name_ids[name] = result
            names.append(name)
        return result

    def pack(node: dict) -> list:
        children = sorted(
            node["children"].values(),
            key=lambda child: child["value"],
            reverse=True,
        )
        child_samples = sum(child["value"] for child in children)
        return [
            name_id(node["name"]),
            node["value"],
            node["value"] - child_samples,
            [pack(child) for child in children],
        ]

    return {"names": names, "tree": pack(root)}


def load_folded(path: Path) -> dict:
    root = {"name": "all samples", "value": 0, "children": {}}
    with path.open("r", encoding="utf-8") as source:
        for line in source:
            line = line.rstrip()
            if not line:
                continue
            stack, count = line.rsplit(" ", 1)
            add_stack(root, stack.split(";"), int(count))
    return pack_tree(root)


FRAGMENT = """<div id="ngs3fs-flamegraph-root">
  <style>
    #ngs3fs-flamegraph-root {
      position: relative;
      color: var(--foreground);
    }

    #ngs3fs-flamegraph-root .flame-heading {
      margin-bottom: 10px;
    }

    #ngs3fs-flamegraph-root .flame-status {
      min-height: 22px;
      margin: 8px 0;
      overflow-wrap: anywhere;
    }

    #ngs3fs-flamegraph-root .flame-plot {
      width: 100%;
      min-width: 0;
    }

    #ngs3fs-flamegraph-root svg {
      display: block;
      width: 100%;
    }

    #ngs3fs-flamegraph-root .flame-frame rect {
      fill: var(--viz-series-1);
      fill-opacity: 0.72;
      stroke: var(--background);
      stroke-width: 1;
      cursor: pointer;
    }

    #ngs3fs-flamegraph-root .flame-frame:hover rect {
      fill-opacity: 1;
    }

    #ngs3fs-flamegraph-root .flame-frame.is-match rect {
      fill: var(--viz-series-2);
      fill-opacity: 1;
    }

    #ngs3fs-flamegraph-root .flame-frame text {
      fill: var(--foreground);
      font-size: 12px;
      pointer-events: none;
    }

    #ngs3fs-flamegraph-root .flame-tooltip {
      position: absolute;
      z-index: 2;
      display: none;
      max-width: min(560px, calc(100% - 24px));
      padding: 8px 10px;
      background: var(--popover);
      color: var(--popover-foreground);
      border: 1px solid var(--border);
      overflow-wrap: anywhere;
      pointer-events: none;
    }
  </style>

  <h2 class="flame-heading">__TITLE__</h2>
  <div class="viz-controls">
    <label class="form-label" for="ngs3fs-flame-search">
      搜索函数
      <input id="ngs3fs-flame-search" class="form-control" type="search"
             placeholder="例如 sign_v4 或 llhttp">
    </label>
    <button id="ngs3fs-flame-reset" class="btn" type="button">返回根节点</button>
  </div>
  <div id="ngs3fs-flame-status" class="flame-status" aria-live="polite"></div>
  <div id="ngs3fs-flame-plot" class="flame-plot"></div>
  <div id="ngs3fs-flame-tooltip" class="flame-tooltip" role="tooltip"></div>

  <script>
    (() => {
      const root = document.getElementById("ngs3fs-flamegraph-root");
      const plot = document.getElementById("ngs3fs-flame-plot");
      const status = document.getElementById("ngs3fs-flame-status");
      const search = document.getElementById("ngs3fs-flame-search");
      const reset = document.getElementById("ngs3fs-flame-reset");
      const tooltip = document.getElementById("ngs3fs-flame-tooltip");
      const profile = __PROFILE__;
      const names = profile.names;
      const profileRoot = profile.tree;
      let selected = profileRoot;
      let query = "";

      function maxDepth(node) {
        let depth = 0;
        for (const child of node[3]) {
          depth = Math.max(depth, 1 + maxDepth(child));
        }
        return depth;
      }

      function percentage(value, base) {
        return `${(100 * value / base).toFixed(2)}%`;
      }

      function frameName(node) {
        return names[node[0]];
      }

      function showTooltip(event, node) {
        const name = frameName(node);
        tooltip.textContent = `${name} — inclusive ${percentage(node[1], profileRoot[1])}, self ${percentage(node[2], profileRoot[1])}`;
        tooltip.style.display = "block";
        const bounds = root.getBoundingClientRect();
        const left = Math.min(event.clientX - bounds.left + 12,
          Math.max(8, bounds.width - tooltip.offsetWidth - 8));
        tooltip.style.left = `${Math.max(8, left)}px`;
        tooltip.style.top = `${event.clientY - bounds.top + 12}px`;
      }

      function render() {
        const width = Math.max(320, Math.floor(plot.getBoundingClientRect().width));
        const barHeight = 22;
        const depth = maxDepth(selected);
        const height = Math.max(110, (depth + 1) * barHeight + 2);
        const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
        svg.setAttribute("height", String(height));
        svg.setAttribute("role", "img");
        svg.setAttribute("aria-label", "可点击下钻的 ngs3fs CPU 火焰图");
        let matches = 0;

        function draw(node, x, frameWidth, level) {
          if (frameWidth < 0.25) return;
          const name = frameName(node);
          const y = height - (level + 1) * barHeight;
          const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
          group.setAttribute("class", "flame-frame");
          if (query && name.toLocaleLowerCase().includes(query)) {
            group.classList.add("is-match");
            matches += 1;
          }

          const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
          rect.setAttribute("x", String(x));
          rect.setAttribute("y", String(y));
          rect.setAttribute("width", String(frameWidth));
          rect.setAttribute("height", String(barHeight - 1));
          group.appendChild(rect);

          if (frameWidth >= 30) {
            const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
            const maxCharacters = Math.max(1, Math.floor((frameWidth - 8) / 7));
            label.setAttribute("x", String(x + 4));
            label.setAttribute("y", String(y + 15));
            label.textContent = name.length > maxCharacters
              ? `${name.slice(0, Math.max(1, maxCharacters - 1))}…`
              : name;
            group.appendChild(label);
          }

          group.addEventListener("click", () => {
            selected = node;
            render();
          });
          group.addEventListener("pointermove", event => showTooltip(event, node));
          group.addEventListener("pointerleave", () => {
            tooltip.style.display = "none";
          });
          svg.appendChild(group);

          let childX = x;
          for (const child of node[3]) {
            const childWidth = frameWidth * child[1] / node[1];
            draw(child, childX, childWidth, level + 1);
            childX += childWidth;
          }
        }

        draw(selected, 0, width, 0);
        plot.replaceChildren(svg);
        const selectedName = frameName(selected);
        const matchText = query ? `；匹配 ${matches} 个栈帧` : "";
        status.textContent = `${selectedName}：inclusive ${percentage(selected[1], profileRoot[1])}，self ${percentage(selected[2], profileRoot[1])}${matchText}`;
      }

      search.addEventListener("input", () => {
        query = search.value.trim().toLocaleLowerCase();
        render();
      });
      reset.addEventListener("click", () => {
        selected = profileRoot;
        render();
      });
      new ResizeObserver(render).observe(plot);
      render();
    })();
  </script>
</div>
"""

DOCUMENT = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  <style>
    :root {
      --background: #f6f7f9;
      --foreground: #202124;
      --border: #c8ccd3;
      --popover: #ffffff;
      --popover-foreground: #202124;
      --viz-series-1: #e85d3f;
      --viz-series-2: #ffd447;
    }

    body {
      margin: 0;
      padding: 16px;
      background: var(--background);
      color: var(--foreground);
      font: 14px/1.45 system-ui, sans-serif;
    }

    .viz-controls {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: end;
    }

    .form-label {
      display: grid;
      gap: 4px;
    }

    .form-control,
    .btn {
      min-height: 34px;
      box-sizing: border-box;
      border: 1px solid var(--border);
      border-radius: 4px;
      background: var(--popover);
      color: var(--popover-foreground);
    }

    .form-control {
      width: min(420px, 80vw);
      padding: 6px 8px;
    }

    .btn {
      padding: 6px 12px;
      cursor: pointer;
    }
  </style>
</head>
<body>
__CONTENT__
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("folded", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--title", default="ngs3fs interactive flame graph")
    arguments = parser.parse_args()

    profile = json.dumps(load_folded(arguments.folded), separators=(",", ":"))
    title = html.escape(arguments.title)
    fragment = FRAGMENT.replace("__PROFILE__", profile).replace(
        "__TITLE__", title
    )
    document = DOCUMENT.replace("__TITLE__", title).replace(
        "__CONTENT__", fragment
    )
    arguments.output.write_text(document, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

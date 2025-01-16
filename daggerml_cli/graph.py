import json

from daggerml_cli import repo


def write_dag_to_html(nodes, output_file):
    """
    Write a DAG represented as a list of nodes to an HTML file using D3.js.

    Parameters:
        nodes (list): List of nodes representing the DAG.
        output_file (str): Path to the output HTML file.
    """
    def convert_nodes_to_d3(nodes):
        """
        Convert a list of nodes into D3-compatible JSON format.

        Returns:
            dict: Nodes and links for the D3 graph.
        """
        d3_nodes = []
        d3_links = []
        node_id_map = {node["id"]: idx for idx, node in enumerate(nodes)}

        for node in nodes:
            d3_nodes.append({
                "id": node_id_map[node["id"]],
                "name": node["id"],
                "hover": node["hover"],
                "color": node["color"],
                "style": node["style"]
            })
            for parent in node["parents"]:
                d3_links.append({"source": node_id_map[parent], "target": node_id_map[node["id"]]})

        return {"nodes": d3_nodes, "links": d3_links}

    # Convert nodes to D3 format
    d3_data = convert_nodes_to_d3(nodes)
    # D3 visualization HTML template
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>DAG Visualization</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        .node {{
            stroke: #000;
            stroke-width: 1.5px;
        }}
        .link {{
            fill: none;
            stroke-width: 1.5px;
        }}
        .tooltip {{
            position: absolute;
            text-align: center;
            width: 100px;
            height: 28px;
            padding: 2px;
            font: 12px sans-serif;
            background: lightsteelblue;
            border: 0px;
            border-radius: 8px;
            pointer-events: none;
        }}
    </style>
</head>
<body>
    <h1>DAG Visualization</h1>
    <svg width="960" height="600">
        <defs>
            <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" style="stop-color:rgb(255,0,0);stop-opacity:1" />
                <stop offset="100%" style="stop-color:rgb(0,0,255);stop-opacity:1" />
            </linearGradient>
        </defs>
    </svg>
    <script>
        const data = {json.dumps(d3_data)};
        const width = 960;
        const height = 600;
        const svg = d3.select("svg"),
            g = svg.append("g").attr("transform", "translate(40, 40)");
        const simulation = d3.forceSimulation(data.nodes)
            .force("link", d3.forceLink(data.links).id(d => d.id).distance(50))
            .force("charge", d3.forceManyBody().strength(-200))
            .force("center", d3.forceCenter(width / 2, height / 2));
        const link = g.append("g")
            .attr("class", "links")
            .selectAll("path")
            .data(data.links)
            .enter().append("path")
            .attr("class", "link")
            .attr("stroke", "url(#gradient)");
        const node = g.append("g")
            .attr("class", "nodes")
            .selectAll("path")
            .data(data.nodes)
            .enter().append("path")
            .attr("class", "node")
            .attr("d", d => d.style === "circle" ? d3.symbol().type(d3.symbolCircle).size(200)() :
                            d.style === "square" ? d3.symbol().type(d3.symbolSquare).size(200)() :
                            d3.symbol().type(d3.symbolCircle).size(200)())
            .attr("fill", d => d.color)
            .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended))
            .on("mouseover", function(event, d) {{
                tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                tooltip.html(d.name + "<br>" + d.hover)
                    .style("left", (event.pageX + 5) + "px")
                    .style("top", (event.pageY - 28) + "px");
            }})
            .on("mouseout", function(d) {{
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            }});
        const tooltip = d3.select("body").append("div")
            .attr("class", "tooltip")
            .style("opacity", 0);
        simulation.on("tick", () => {{
            link.attr("d", d => `M${{d.source.x}},${{d.source.y}}C${{(d.source.x + d.target.x) / 2}},${{d.source.y}} ${{(d.source.x + d.target.x) / 2}},${{d.target.y}} ${{d.target.x}},${{d.target.y}}`);
            node.attr("transform", d => `translate(${{d.x}},${{d.y}})`);
        }});

        function dragstarted(event, d) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }}

        function dragged(event, d) {{
            d.fx = event.x;
            d.fy = event.y;
        }}

        function dragended(event, d) {{
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }}
    </script>
</body>
</html>
"""
    with open(output_file, "w") as f:
        f.write(html_template)


if __name__ == "__main__":
    from pathlib import Path
    from tempfile import TemporaryDirectory

    from daggerml_cli import api, repo
    from tests.util import SimpleApi

    # Example usage
    FN = repo.Resource(str(Path(__file__).parent.parent / "tests/fn.py"),
                       adapter='./tests/python-local-adapter')
    with TemporaryDirectory() as config_dir:
        with SimpleApi.begin('d0', config_dir=config_dir) as d0:
            d0.commit(d0.put_literal(23))
        with SimpleApi.begin('d0.1', config_dir=config_dir) as d0:
            d0.commit(d0.put_literal(24))
        with SimpleApi.begin('d1', config_dir=config_dir) as d1:
            nodes = [
                d1.put_literal(FN),
                d1.put_load('d0'),
                d1.put_load('d0.1'),
                d1.put_literal(1),
                d1.put_literal(23),
            ]
            # result = d1.start_fn(*nodes)
            result = d1.put_literal(nodes)
            ref = d1.commit(result)
        print(f"{ref.name = }")
        from pprint import pprint
        desc = api.describe_dag(d1.ctx, ref.to)
        pprint(desc)
        js = []
        edges = {k: [] for k in desc["nodes"]}
        edges = {**edges, **desc["edges"]}
        for k, v in list(edges.items()):
            tmp = api.describe_ref(d1.ctx, "node", k.split("/")[1])
            if isinstance(v, str):
                if v not in edges:
                    edges[v] = []
                edges[k] = [v]
                print(tmp)
                # print(f"{tmp = } -- {tmp.value = }")
            elif len(v) == 0:
                print(tmp)
        pprint(edges)
        for k, v in edges.items():
            if k == desc["result"]:
                color = "purple"
            elif k.startswith("dag/"):
                color = "red"
            elif len(v) == 0:
                color = "green"
            elif v[0].startswith("dag/"):
                color = "yellow"
            else:
                color = "blue"
            js.append({"id": k, "parents": v, "hover": k, "color": color, "style": "square"})
        write_dag_to_html(js, "dag_vis.html")

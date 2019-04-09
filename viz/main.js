(function($){

    var Client = function(nodes, edges) {
      var nodes = nodes;
      var edges = edges;
      var client = new SwaggerClient("/log.swagger.json");
      var addNodeIfNotExist = function(n) {
          if (nodes.get(n) == null) {
            nodes.add({id: n, label: n});
          }
          //if (sys.getNode(n) == null) {
          //    sys.addNode(n, {name: n});
          //    console.log(sys.getNode(n));
          //}
      }
      var addEdgeIfNotExist = function(from, to, label) {
          if (edges.get(from+to) == null) {
            edges.add({id: from+to, from: from, to: to, label: label, length: 200, arrows: 'to'});
          }
      }

      var that = {
          query: function(q) {
              client.then( (res) => {
                  return res.apis.HodDB.Select({body: JSON.stringify(q)})
              }).then( (res) => {
                  res.body.rows.forEach(function(row) {
                      console.log(JSON.stringify(row.values));
                  });
              }, (reason) => {
                  console.error(reason);
              });
          },
          getNodes: function(n) {
              if (n == null) { return }
              console.log(n);
              client.then( (res) => {
                  return res.apis.HodDB.Select({body: JSON.stringify({
                      vars: ['?tstat', '?tstatclass','?pred','?object','?objectclass'],
                      graphs: ['movie'],
                      filter: "Before",
                      timestamp: luxon.DateTime.local().toMillis()*1000000,
                      where: [
                          {
                              subject: {value: "?tstat"},
                              predicate: [{namespace: "rdf", value:"type"}],
                              object: {namespace: "brick", value: n},
                          },
                          {
                              subject: {value: "?tstat"},
                              predicate: [{namespace: "rdf", value:"type"}],
                              object: {value: "?tstatclass"},
                          },
                          {
                              subject: {value: "?tstat"},
                              predicate: [{value: "?pred"}],
                              object: {value: "?object"},
                          },
                          {
                              subject: {value: "?object"},
                              predicate: [{namespace: "rdf", value: "type"}],
                              object: {value: "?objectclass"},
                          },
                      ],
                  })});
              }).then( (res) => {
                  res.body.rows.forEach(function(row) {
                      if (row.values[1].value == "Class") { return }
                      if (row.values[4].value == "Class") { return }
                      if (row.values[1].value == "Site") { return }
                      if (row.values[4].value == "Site") { return }
                      addNodeIfNotExist(row.values[1].value);
                      addNodeIfNotExist(row.values[4].value);
                      addEdgeIfNotExist(row.values[1].value, row.values[4].value, row.values[2].value);
                  });
              }, (reason) => {
                  console.error(reason);
              });

          }
      }

      return that
    }

    var QueryBuilder = function(nodes, edges, network) {
        nodes = nodes;
        edges = edges;
        network = network;

        // nodes and edges in our query
        query = {
            nodes: [],
            edges: [],
        };
        // Maintain the invariant that the edges + nodes must always be connected

        var nodeInQuery = function(node) {
            // check if we've already selected the node; in this case we deselect it
            for (i=0;i<query.nodes.length;i++) {
                if (query.nodes[i] == node) {
                    return i;
                }
            }
            return -1;
        }

        var checkNodeConnected = function(node) {
            // can start with any node
            if (query.nodes.length == 0) {
                return true;
            }
            // check nodes connected to nodes we've selected
            for (i=0;i<query.nodes.length;i++) {
                connected = network.getConnectedNodes(query.nodes[i]);
                for (j=0;j<connected.length;j++) {
                    if (connected[j] == node) {
                        return true;
                    }
                }
            }
            return false;
        }

        var getEdgeFromNodeToNode = function(src, others) {
            connected = network.getConnectedNodes(src);
            for (j=0;j<connected.length;j++) {
                neighbor = connected[j];
                for (o=0;o<others.length;o++) {
                    dest = others[o];
                    if (o != src && neighbor == dest) {
                        // edge exists
                        console.log("found neighbor to use")
                        nedges = network.getConnectedEdges(src);
                        for (e=0;e<nedges.length;e++) {
                            edge = edges.get(nedges[e]);
                            if (edge.to == dest) {
                                return {dest: dest, edge: edge, idx: o}
                            }
                        }

                        return;
                    }
                }
            }
        }

        var that = {
            generateClassTriple: function(varname, classname) {
                return {
                    subject: {value: varname}, // TODO: generate a var name
                    predicate: [{namespace: "rdf", value: "type"}],
                    object: {namespace: "brick", value: classname}
                }
            },
            processClickedNode: function(n) {
                idx = nodeInQuery(n);
                if (idx >= 0) {
                    //console.log("process clicked node", n);
                    // TODO: if node is selected, deselect it if it wouldn't orphan another node
                    query.nodes.splice(idx, 1);
                    nodes.update([{id:n, color:{background: "#97C2FC"}}]);
                } else if (checkNodeConnected(n)) {
                    //console.log("Node is clicked and can be added");
                    query.nodes.push(n);
                    nodes.update([{id:n, color:{background: "#44AD44"}}]);
                } else {
                    nodes.update([{id:n, color:{background: "#97C2FC"}}]);
                }
            },
            build: function() {
                terms = [];
                variables = [];
                mynodelist = query.nodes.slice();
                if (mynodelist.length == 0) {
                    return;
                }
                var root = mynodelist[0];
                varname = "?"+root;
                terms.push({
                    subject: {value: varname},
                    predicate: [{namespace: "rdf", value: "type"}],
                    object: {namespace: "brick", value: root}
                });
                variables.push(varname);
                console.log("nodes", mynodelist.length);
                mynodelist.splice(0, 1);
                while (mynodelist.length > 0) {
                    e = getEdgeFromNodeToNode(root, mynodelist);
                    console.log(">>>", e, mynodelist.length);
                    if (e==null) { break }
                    mynodelist.splice(e.idx, 1);
                    rootname = "?"+root;
                    destname = "?"+e.dest;
                    variables.push(destname);
                    terms.push({
                        subject: {value: destname},
                        predicate: [{namespace: "rdf", value: "type"}],
                        object: {namespace: "brick", value: e.dest}
                    });
                    terms.push({
                        subject: {value: rootname},
                        predicate: [{namespace: "bf", value:e.edge.label}],
                        object: {value: destname}
                    });
                    root = e.dest;
                }

                //query.nodes.forEach(function(classname) {
                //    terms.push({
                //        varname = "?"+classname
                //        subject: {value: varname},
                //        predicate: [{namespace: "rdf", value: "type"}],
                //        object: {namespace: "brick", value: classname}
                //    });
                //});
                console.log(terms);
                return {
                    vars: variables,
                    graphs: ['movie'],
                    filter: "Before",
                    timestamp: luxon.DateTime.local().toMillis()*1000000,
                    where: terms,
                }
            },
        }
        return that
    }

	var nodes = new vis.DataSet();
	var edges = new vis.DataSet();

	// create a network
	var container = document.getElementById('viewport');
	var data = {
		nodes: nodes,
		edges: edges
	};
	var options = {};
	network = new vis.Network(container, data, options);

    var Query = QueryBuilder(nodes, edges, network);

	var client = new Client(nodes, edges);

    network.on("click", function (params) {
        console.log('click event, getNodeAt returns: ' + this.getNodeAt(params.pointer.DOM));
        // TODO: track nodes already expanded
        var clickednode = this.getNodeAt(params.pointer.DOM);
        if (clickednode != null) {
            client.getNodes(clickednode);
            Query.processClickedNode(clickednode);
            network.unselectAll();
        }
        q = Query.build();
        client.query(q);
    });


    $(document).ready(function(){
		nodes.add({id:"Room", label:"Room"});
    })

})(this.jQuery)

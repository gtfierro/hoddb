(function($){

    // init tabs
    //$('.tabular.menu .item').tab();
    var _queryres = [];
    var _querycols = [];
    var _MAXCOLS = 16;

    var uriToString = function(u) {
        if (u.namespace != null) {
            return u.namespace + ":" + u.value;
        }
        return u.value;
    }
    var queryToText = function(q) {
        var qstr = "SELECT ";
        q.vars.forEach(function(varname) {
            qstr += varname + " ";
        });
        qstr += " WHERE {\n";
        q.where.forEach(function(term) {
            qstr += "\t";
            qstr += uriToString(term.subject);
            qstr += " ";
            qstr += uriToString(term.predicate[0]);
            qstr += " ";
            qstr += uriToString(term.object);
            qstr += " .\n";
        });
        qstr += "};";
        console.log(qstr)
        return qstr;
    }

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
                  sites = new Set();
                  res.body.rows.forEach(function(row) {
                      row.values.forEach(function(v) {
                        sites.add(v.namespace);
                      })
                  });
                  document.getElementById("numsites").textContent = sites.size
                  document.getElementById("numresults").textContent = res.body.rows.length
                  console.log("# results:", res.body.rows.length);
                  console.log("# sites:", sites.size);
                  _queryres.length = 0;
                  _querycols.length = 0;
                  console.log(q);
                  q.vars.forEach( (v) => { _querycols.push({title: v, name: v, visible: true})});
                  for (i=_querycols.length;i<_MAXCOLS;i++) {
                      _querycols.push({title: '', name: '', visible: false});
                  }
                  console.log(_querycols);
                  res.body.rows.forEach(function(row) {
                      var toadd = row.values.map( (r) => r.value )
                      for (i=toadd.length;i<_MAXCOLS;i++) {
                          toadd.push(null);
                      }
                      _queryres.push( toadd );
                  });
              }, (reason) => {
                  console.error(reason);
              });
          },
          getNodes: function(n) {
              if (n == null) { return }
              console.log("N",n);
              client.then( (res) => {
                  return res.apis.HodDB.Select({body: JSON.stringify({
                      vars: ['?tstat', '?tstatclass','?pred','?object','?objectclass'],
                      graphs: ['*'],
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
              }, (reason, x) => {
                  console.error(reason, x);
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

        var getEdgeFromNodeToNode = function(srcs, others) {
            for (i=0;i<srcs.length;i++) {
                src = srcs[i];
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
                                    return {src: src, dest: dest, edge: edge, idx: o}
                                }
                            }
                        }
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
                nodesinquery = [];
                variables = [];
                console.log(query.nodes.length);
                mynodelist = query.nodes.slice();
                if (mynodelist.length == 0) {
                    return;
                }
                var root = mynodelist[0];
                varname = "?"+root.replace(/([A-Z])[a-z]*([a-z])(_|$)/gi,"$1$2");
                terms.push({
                    subject: {value: varname},
                    predicate: [{namespace: "rdf", value: "type"}],
                    object: {namespace: "brick", value: root}
                });
                variables.push(varname);
                nodesinquery.push(root);
                console.log("nodes", mynodelist.length);
                mynodelist.splice(0, 1);
                while (mynodelist.length > 0) {
                    e = getEdgeFromNodeToNode(nodesinquery, mynodelist);
                    if (e==null) { break }
                    mynodelist.splice(e.idx, 1);
					console.log(e);
                    rootname = "?"+e.src.replace(/([A-Z])[a-z]*([a-z])(_|$)/gi,"$1$2")
                    destname = "?"+e.dest.replace(/([A-Z])[a-z]*([a-z])(_|$)/gi,"$1$2")
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
                    nodesinquery.push(e.dest);
                }

                generated_query = {
                    vars: variables,
                    graphs: ['*'],
                    filter: "Before",
                    timestamp: luxon.DateTime.local().toMillis()*1000000,
                    where: terms,
                };

                return generated_query;
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
    var network = null;

    //$('.tabular.menu .item').tab({'onVisible':function(){console.log('net visible'); network.redraw()}});

    var yasqe = YASQE(document.getElementById("query"));
    yasqe.options.syntaxErrorCheck = false;
    yasqe.options.showQueryButton = false;
    yasqe.options.readOnly = true;
    yasqe.options.createShareLink = null;
    //yasqe.options.lineNumbers = false;
    yasqe.setSize("100%")
    yasqe.setValue("");


    $(document).ready(function(){
        network = new vis.Network(container, data, options);

        var dt = null;

        $('.tabular.menu .item').tab(
            {
                'onVisible':function(e){
                    console.log('net visible', e);
                    if (e == 'builder' && network != null) {
                        network.fit();
                    } else if (e == 'results') {
                        ////$("#resultstable").DataTable().destroy();
                        //console.log(_querycols);
                        //console.log(_queryres);
                    }
                },
                'onLoad': function(e) {
                    if (e == 'results') {
                        if (dt != null && $.fn.DataTable.isDataTable("#resultstable")) {
                            dt.clear();
                            dt.destroy();
                            dt = $("#resultstable").DataTable({
                                data: _queryres,
                                columns: _querycols,
                                autoWidth: false,
                            }).draw();
                            console.log(dt);
                            //dt.columns.adjust().draw();
                        }
                    }
                },
                'onFirstLoad': function(e) {
                    if (e == 'results') {
                        dt = $("#resultstable").DataTable({
                            destroy: true,
                            autoWidth: false,
                            data: _queryres,
                            columns: _querycols,
                        }).draw();
                    }
                },
            }
        );

        $.tab('change tab','builder');

        var Query = QueryBuilder(nodes, edges, network);

        var client = new Client(nodes, edges);

        $(".button").click(function(e) {
            startclass = e.currentTarget.dataset.brickclass;
            console.log(nodes);
            nodes.clear();
            edges.clear()
            client = new Client(nodes, edges);
            Query = QueryBuilder(nodes, edges, network);
            network.unselectAll();
            nodes.add({id: startclass, label:startclass});
        });

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
            yasqe.setValue(queryToText(q));
            client.query(q);
        });

        nodes.add({id:"Room", label:"Room"});
        network.redraw();
        network.fit();
    })

})(this.jQuery)

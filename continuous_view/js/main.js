(function($){

    // init tabs
    //$('.tabular.menu .item').tab();
    var _queryres = [];
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
          querystr: function(qstr) {
              var t = this;
              client.then( (res) => {
                  return res.apis.HodDB.Parse({body: JSON.stringify({query: qstr})})
              }).then( (res) => {
                let q = res.obj;
                q.graphs = ['*'];
                console.log(q);
                t.query(q);
              });
          },
          query: function(q) {
              client.then( (res) => {
                  return res.apis.HodDB.Select({body: JSON.stringify(q)})
              }).then( (res) => {
                  sites = new Set();

                  if (res.body.rows == null) {
                      $("#errdiv").show();
                      $("#errmessage").text("No results");
                      return;
                  } else {
                      $("#errdiv").hide();
                  }
                  //nodes.clear();
                  //edges.clear()

                  console.log(q);
                  let varmap = {};
                  q.vars.forEach(function(v, i) {
                    varmap[v] = i;
                  });

                  console.log("# results:", res.body.rows.length);
                  _queryres.length = 0;
                  res.body.rows.forEach(function(row) {
                      q.where.forEach(function(term) {
                          let newtriple = {};
                          let sv = term.subject.value;
                          let pv = term.predicate[0].value;
                          let ov = term.object.value;
                          if (varmap[sv] !== undefined) {
                            newtriple.subject = row.values[varmap[sv]];
                          } else if (sv.startsWith("?")) {
                            return
                          } else {
                            newtriple.subject = term.subject;
                          }
                          if (varmap[pv] !== undefined) {
                            newtriple.predicate = row.values[varmap[pv]];
                          } else if (pv.startsWith("?")) {
                            return
                          } else {
                            newtriple.predicate = term.predicate[0];
                          }
                          if (varmap[ov] !== undefined) {
                            newtriple.object = row.values[varmap[ov]];
                          } else if (ov.startsWith("?")) {
                            return
                          } else {
                            newtriple.object = term.object;
                          }
                          console.log(newtriple);
                          addNodeIfNotExist(newtriple.subject.value);
                          addNodeIfNotExist(newtriple.object.value);
                          addEdgeIfNotExist(newtriple.subject.value, newtriple.object.value, newtriple.predicate.value);
                      });
                      row.values.forEach(function(v) {
                        //addNodeIfNotExist(v.value);
                        sites.add(v.namespace);
                      })
                      var toadd = row.values.map( (r) => r.value )
                      for (i=toadd.length;i<_MAXCOLS;i++) {
                          toadd.push(null);
                      }
                      _queryres.push( toadd );
                  });
                  console.log("# sites:", sites.size);
              }, (reason) => {
                  console.error(reason);
                  $("#errdiv").show();
                  $("#errmessage").text(reason);
              });
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
    //yasqe.options.readOnly = true;
    yasqe.options.createShareLink = null;
    yasqe.options.autocompleters = [];
    //yasqe.options.lineNumbers = false;
    yasqe.setSize("100%")

    $(document).ready(function(){
        network = new vis.Network(container, data, options);

        var dt = null;

        $("#errdiv").hide();

        $.tab('change tab','builder');

        $(".viewbutton").on('click', function(e) {
            console.log('clicked', $(this).data('query'));
            yasqe.setValue($(this).data('query'));
        });

        var querystore = {query:"SELECT ?r WHERE {\n\t?r rdf:type brick:Room\n};"};
        var client = new Client(nodes, edges);
        yasqe.on("change", function() {
            querystore.query = yasqe.getValue();
            nodes.clear();
            edges.clear()
            client.querystr(yasqe.getValue());
            network.redraw();
            network.fit();
        });

        setInterval(function() {
            console.log("should run q", querystore.query);
            client.querystr(yasqe.getValue());
            network.redraw();
            //network.fit();
        }, 5000);

        yasqe.setValue("SELECT ?r WHERE {\n\t?r rdf:type brick:Room\n};");

    })

})(this.jQuery)

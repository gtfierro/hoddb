import requests

query_string = "SELECT ?x WHERE { ?x rdf:type brick:VAV }"

resp = requests.post("http://localhost:47809/v1/hoddb/parse", json={"query": query_string})
query = resp.json()

resp = requests.post("http://localhost:47809/v1/hoddb/select", json=query)
results = resp.json()
for row in results['rows']:
    print(row)

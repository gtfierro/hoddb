# Cloudless Brick Queries

Super quick lambda implementation for HodDB.

We store HodDB backups of individual buildings in S3.
When the lambda executes, it fetches the required Brick model from S3 using the name of the building, rebuilds a HodDB instance from the backup, and serves the query.

## How to Query

Send a POST request to  [https://0zd2do1tq9.execute-api.us-west-1.amazonaws.com/default/hoddb_test](https://0zd2do1tq9.execute-api.us-west-1.amazonaws.com/default/hoddb_test).
The payload is JSON:

```json
{
	"query": "<SPARQL QUERY HERE>",
	"graph": "<NAME OF BUILDING>"
}
```

## Making Backup Files

The `make_backup` script creates the backup files from which queries are served

```
Usage of ./make_backup:
  -building string
        Name of building (default "ciee")
  -config string
        Path to hodconfig.yml file (default "hodconfig.yml")
  -ttl string
        Path to building.ttl file (default "ciee.ttl")
```

The resulting file is called `<building name>.badger` and can be uploaded to S3

## Performance

Initial loading of the database from the backup usually takes a little over 1 second.
Subsequent queries are usually in the 300-500 ms range

### Query 1

```json
{
    "query": "SELECT ?temp ?room ?zone WHERE { ?temp rdf:type/rdfs:subClassOf* brick:Temperature_Sensor . ?temp bf:hasLocation ?room . ?room rdf:type brick:Room . ?room bf:isPartOf ?zone . ?zone rdf:type brick:HVAC_Zone };",
    "graph": "ciee"
}
```

Results:

```
-----------------------------------
|             Summary             |
-----------------------------------
|        observations: 100        |
|      min value: 323.761230      |
|        mean : 399.756550        |
|      max value: 1707.591309     |
-----------------------------------
```

### Query 2

```json
{
    "query": "SELECT ?room WHERE { ?room rdf:type brick:Room };",
    "graph": "csu-dominguez-hills"
}
```

Results:

```
-----------------------------------
|             Summary             |
-----------------------------------
|        observations: 100        |
|      min value: 327.301514      |
|        mean : 414.922444        |
|      max value: 1646.214844     |
-----------------------------------
```

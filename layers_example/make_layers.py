from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, RDFS, OWL
G = Graph()
BRICK_VERSION = '1.0.3'

BRICK = Namespace(f"https://brickschema.org/schema/{BRICK_VERSION}/Brick#")
BF = Namespace(f"https://brickschema.org/schema/{BRICK_VERSION}/BrickFrame#")
TAG = Namespace(f"https://brickschema.org/schema/{BRICK_VERSION}/BrickTag#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
DCTERMS = Namespace("http://purl.org/dc/terms#")
SDO = Namespace("http://schema.org#")
SOSA = Namespace("http://www.w3.org/ns/sosa#")
NET = Namespace("http://xbos.io/ontologies/network#")

MYBLDG = Namespace(f"http://xbos.io/example_building#")
MYNET = Namespace(f"http://xbos.io/example_network#")

A = RDF.type

G.bind('rdf', RDF)
G.bind('owl', OWL)
G.bind('dcterms', DCTERMS)
G.bind('sdo', SDO)
G.bind('rdfs', RDFS)
G.bind('skos', SKOS)
G.bind('sosa', SOSA)
G.bind('brick', BRICK)
G.bind('bf', BF)
G.bind('net', NET)

G.bind("mybldg", MYBLDG)
G.bind("mynet", MYNET)

# make network

hosts = [
    {
        'name': 'router',
        'address': '192.168.1.1',
        'manufacturer': 'US Robotics',
        #'mac': 'TODO',
    },
    {
        'name': 'laptop1',
        'address': '192.168.1.86',
        'manufacturer': 'Apple Inc',
    },
    {
        'name': 'pixel3',
        'address': '192.168.1.88',
        'manufacturer': 'Google Inc',
    },
    {
        'name': 'huebridge',
        'address': '192.168.1.91',
        'manufacturer': 'Philips Hue BV',
    },
    {
        'name': 'nfs-server',
        'address': '192.168.1.104',
        'manufacturer': 'Intel Corporation',
    },
    {
        'name': 'philips-hue-cloud',
        'address': '54.3.17.89',
        'manufacturer': 'Amazon Inc',
    },
]

for host in hosts:
    G.add((MYNET[host['name']], A, NET.Host))
    G.add((MYNET[host['name']], NET.hasHostname, Literal(host['name'])))
    G.add((MYNET[host['name']], NET.hasAddress, Literal(host['address'])))
    G.add((MYNET[host['name']], NET.hasManufacturer, Literal(host['manufacturer'])))


flows = [
    {
        'srcaddr': '192.168.1.1',
        'dstaddr': '192.168.1.86',
        'protocol': 'ICMP',
    },
    {
        'srcaddr': '192.168.1.1',
        'dstaddr': '192.168.1.91',
        'protocol': 'ICMP',
    },
    {
        'srcaddr': '192.168.1.1',
        'dstaddr': '192.168.1.104',
        'protocol': 'ICMP',
    },
    {
        'srcaddr': '192.168.1.86',
        'dstaddr': '192.168.1.104',
        'protocol': 'NFS',
    },
    {
        'srcaddr': '192.168.1.88',
        'dstaddr': '192.168.1.91',
        'protocol': 'HTTPS',
    },
    {
        'srcaddr': '192.168.1.91',
        'dstaddr': '54.3.17.89',
        'protocol': 'HTTPS',
    },
]

for flow in flows:
    srchost = list(G.query(f"SELECT ?host WHERE {{ ?host net:hasAddress \"{flow['srcaddr']}\" }}"))[0]
    dsthost = list(G.query(f"SELECT ?host WHERE {{ ?host net:hasAddress \"{flow['dstaddr']}\" }}"))[0]
    G.add((srchost[0], NET.talksTo, dsthost[0]))
    G.add((srchost[0], NET.speaks, NET[flow['protocol']]))
    G.add((dsthost[0], NET.speaks, NET[flow['protocol']]))
    G.add((NET[flow['protocol']], A, NET.Protocol))

## make building

building_stuff = {
        MYBLDG['gabe_apartment']: {
            A: BRICK.Site,
            BF.hasPart: [MYBLDG['floor1'], MYBLDG['zone1']],
        },
        MYBLDG['floor1']: {
            BF.hasPart: [MYBLDG['living_room'], MYBLDG['bedroom'], MYBLDG['bathroom'], MYBLDG['kitchen']],
        },
        MYBLDG['zone1']: {
            BF.hasPart: [MYBLDG['bedroom']],
        },
        MYBLDG['living_room']: {
            A: BRICK.Room,
            BF.isLocationOf: [MYBLDG['bulb_lamp']],
        },
        MYBLDG['bedroom']: {
            A: BRICK.Room,
            BF.isLocationOf: [MYBLDG['hue_bridge'], MYBLDG['desk_lamp'], MYBLDG['corner_lamp']],
        },
        MYBLDG['bathroom']: {
            A: BRICK.Room,
        },
        MYBLDG['kitchen']: {
            A: BRICK.Room,
        },
        MYBLDG['hue_bridge']: {
            A: BRICK.Philips_Hue_Bridge,
            BF.controls: [MYBLDG['desk_lamp'], MYBLDG['corner_lamp'], MYBLDG['bulb_lamp']],
            # link to the network presence
            OWL.sameAs: MYNET['huebridge'],
        },
        MYBLDG['desk_lamp']: {
            A: BRICK.Luminaire
        },
        MYBLDG['bulb_lamp']: {
            A: BRICK.Luminaire
        },
        MYBLDG['corner_lamp']: {
            A: BRICK.Luminaire
        },
    }

def evaluate_rdf_dict(d):
    for subject, restof in d.items():
        for pred, objects in restof.items():
            if not isinstance(objects, list):
                objects = [objects]
            for obj in objects:
                G.add((subject, pred, obj))
evaluate_rdf_dict(building_stuff)

with open('out.ttl', 'wb') as f:
    f.write(G.serialize(format='ttl'))

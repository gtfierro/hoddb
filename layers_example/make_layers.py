from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef, RDFS, OWL

def evaluate_rdf_dict(d):
    for subject, restof in d.items():
        for pred, objects in restof.items():
            if not isinstance(objects, list):
                objects = [objects]
            for obj in objects:
                G.add((subject, pred, obj))

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
XBOS = Namespace("https://xbos.io/ontologies/xbos#")

MYBLDG = Namespace("http://xbos.io/example_building#")
MYNET = Namespace("http://xbos.io/example_network#")
MYDEP = Namespace("http://xbos.io/example_deployment#")

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
G.bind('xbos', XBOS)

G.bind("mybldg", MYBLDG)
G.bind("mynet", MYNET)
G.bind("mydep", MYDEP)

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
            XBOS.hasHost: MYNET['huebridge'],
            XBOS.hasProcess: MYDEP['philipshue_driver'],
        },
        MYBLDG['desk_lamp']: {
            A: BRICK.Luminaire,
            BF.hasPoint: [MYBLDG['desk_lamp_state'], MYBLDG['desk_lamp_brightness']],
        },
        MYBLDG['bulb_lamp']: {
            A: BRICK.Luminaire,
            BF.hasPoint: [MYBLDG['bulb_lamp_state'], MYBLDG['bulb_lamp_brightness']],
        },
        MYBLDG['corner_lamp']: {
            A: BRICK.Luminaire,
            BF.hasPoint: [MYBLDG['corner_lamp_state'], MYBLDG['corner_lamp_brightness']],
        },
        MYBLDG['desk_lamp_state']: {
            A: BRICK.Luminance_Command,
        },
        MYBLDG['bulb_lamp_state']: {
            A: BRICK.Luminance_Command,
        },
        MYBLDG['corner_lamp_state']: {
            A: BRICK.Luminance_Command,
        },
        MYBLDG['desk_lamp_brightness']: {
            A: BRICK.Luminance_Setpoint,
        },
        MYBLDG['bulb_lamp_brightness']: {
            A: BRICK.Luminance_Setpoint,
        },
        MYBLDG['corner_lamp_brightness']: {
            A: BRICK.Luminance_Setpoint,
        },

        BRICK.Luminaire: {
            A: OWL.Class,
            RDFS.subClassOf: BRICK.Equipment
        }
    }

evaluate_rdf_dict(building_stuff)

## process view
processes = {
    XBOS.Entity: {A: OWL.Class},
    XBOS.Process: {A: OWL.Class},
    XBOS.Namespace: {A: OWL.Class},
    XBOS.Resource: {A: OWL.Class},

    MYDEP['philipshue_driver']: {
        A: XBOS.Process,
        XBOS.hasEntity: MYDEP['philipshue_entity'],
        XBOS.hasResource: [MYDEP['desk_lamp_resource'], MYDEP['corner_lamp_resource'], MYDEP['bulb_lamp_resource']]
    },
    MYDEP['desk_lamp_resource']: {
        A: XBOS.Resource,
        XBOS.hasURI: Literal("lights/desk_lamp"),
        XBOS.hasNamespace: Literal("gabehome"),
    },
    MYDEP['corner_lamp_resource']: {
        A: XBOS.Resource,
        XBOS.hasURI: Literal("lights/corner_lamp"),
        XBOS.hasNamespace: Literal("gabehome"),
    },
    MYDEP['bulb_lamp_resource']: {
        A: XBOS.Resource,
        XBOS.hasURI: Literal("lights/bulb_lamp"),
        XBOS.hasNamespace: Literal("gabehome"),
    }
}
evaluate_rdf_dict(processes)

with open('out.ttl', 'wb') as f:
    f.write(G.serialize(format='ttl'))

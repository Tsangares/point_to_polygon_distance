import os
from dotenv import load_dotenv
load_dotenv()

from neo4j import GraphDatabase
from neo4j.spatial import WGS84Point, CartesianPoint

from pymongo import MongoClient

### NEO4J FUNCTIONS ###
#Create a neo4j session
def get_neo4j_session():
    driver = GraphDatabase.driver(os.getenv("NEO4J_URI"), auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),database=os.getenv("NEO4J_DATABASE"))
    session = driver.session()
    return session

#Upload the traffic stops to neo4j
def upload_traffic_stops_neo4j(session,df):
    for node in df.to_dict('records'):
        node['coord'] = WGS84Point((node['lng'], node['lat']))
        session.run("CREATE (n:TrafficStop {SrcId: $SourceReferenceId, Profile: $ProfileNo, coord: $coord})", node)
    print("Uploaded traffic stops")

#Test the neo4j session 
def test_neo4j():
    print("Testing neo4j session")
    session = get_neo4j_session()
    result = session.run("MATCH (n) RETURN COUNT(n)")
    for record in result:
        print(record)

### MONGODB FUNCTIONS ###
def get_mongo_client():
    client = MongoClient(os.getenv("MONGODB_URI"))
    return client

# Test the mongo client if its live
def test_mongo(client):
    print("Testing mongo client")
    print(client.list_database_names())


# I used an aggregation to seperate the geojson
def unwind_polygon(db,shape_collection,output_collection: str):
    aggregration = [
        {
            '$unwind': '$features'
        }, {
            '$replaceRoot': {
                'newRoot': '$features'
            }
        }, {
            '$out': f'{output_collection}'
        }
    ]
    db.shape_collection.aggregate(aggregration)
    db[output_collection].create_index([("geometry", "2dsphere")])


def set_mongodb_indexes(collection):
    collection.create_index([("coord", "2dsphere")])
    print("Created index for traffic stops")


#Upload the traffic stops to mongodb
def upload_traffic_stops_mongodb(collection,df):
    set_mongodb_indexes(collection)
    df = df.assign(coord=df.apply(lambda x: [x['lng'], x['lat']], axis=1))[['SourceReferenceId', 'ProfileNo', 'coord']]
    collection.insert_many(df.to_dict('records'))
    print('Uploaded traffic stops')


if __name__=="__main__":
    pass
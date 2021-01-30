from pymongo import MongoClient, GEOSPHERE
import json

client = MongoClient('mongodb://localhost:27017')
db = client.gmDB

########################################################################################################################
# Ingest States
########################################################################################################################

states_meta = [{
    'nameProperty': 'name',
    'geojson': '../data/simplified/us-states.geojson',
}]

states = {}

for state_meta in states_meta:
    with open(state_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        state = {
            'name': feature['properties'][state_meta['nameProperty']],
            'geometry': feature['geometry'],
        }
        result = db['states'].insert_one(state)
        states[state['name']] = result.inserted_id

db['states'].create_index('name')
db['states'].create_index([('geometry', GEOSPHERE)])

########################################################################################################################
# Ingest Counties
########################################################################################################################

counties_meta = [{
    'stateName': 'Wisconsin',
    'nameProperty': 'COUNTY_NAME',
    'geojson': '../data/simplified/County_Boundaries_24K.geojson',
}]

counties = []

for county_meta in counties_meta:
    counties.append({})
    with open(county_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        county = {
            'state': states[county_meta['stateName']],
            'name': feature['properties'][county_meta['nameProperty']],
            'geometry': feature['geometry'],
        }
        result = db['counties'].insert_one(county)
        counties[-1][county['name']] = result.inserted_id

db['counties'].create_index('state')
db['counties'].create_index('name')
db['counties'].create_index([('geometry', GEOSPHERE)])

########################################################################################################################
# Ingest Assembly Districts
########################################################################################################################

assembly_districts_meta = [{
    'stateName': 'Wisconsin',
    'year': '2011',
    'nameProperty': 'District_S',
    'geojson': '../data/simplified/Wisconsin_Assembly_Districts_2012.geojson',
}]

assembly_districts = []

for assembly_district_meta in assembly_districts_meta:
    assembly_districts.append({})
    with open(assembly_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        assembly_district = {
            'state': states[assembly_district_meta['stateName']],
            'year': assembly_district_meta['year'],
            'name': feature['properties'][assembly_district_meta['nameProperty']],
            'geometry': feature['geometry'],
        }
        result = db['assemblyDistricts'].insert_one(assembly_district)
        assembly_districts[-1][assembly_district['name']] = result.inserted_id

db['assemblyDistricts'].create_index('state')
db['assemblyDistricts'].create_index('year')
db['assemblyDistricts'].create_index('name')
db['assemblyDistricts'].create_index([('geometry', GEOSPHERE)])

########################################################################################################################
# Ingest Senate Districts
########################################################################################################################

senate_districts_meta = [{
    'stateName': 'Wisconsin',
    'year': '2011',
    'nameProperty': 'SEN_NUM',
    'geojson': '../data/simplified/Wisconsin_Senate_Districts.geojson',
}]

senate_districts = []

for senate_district_meta in senate_districts_meta:
    senate_districts.append({})
    with open(senate_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        senate_district = {
            'state': states[senate_district_meta['stateName']],
            'year': senate_district_meta['year'],
            'name': feature['properties'][senate_district_meta['nameProperty']],
            'geometry': feature['geometry'],
        }
        result = db['senateDistricts'].insert_one(senate_district)
        senate_districts[-1][senate_district['name']] = result.inserted_id

db['senateDistricts'].create_index('state')
db['senateDistricts'].create_index('year')
db['senateDistricts'].create_index('name')
db['senateDistricts'].create_index([('geometry', GEOSPHERE)])

########################################################################################################################
# Ingest Congressional Districts
########################################################################################################################

congressional_districts_meta = [{
    'stateName': 'Wisconsin',
    'year': '2011',
    'nameProperty': 'District_N',
    'geojson': '../data/simplified/Wisconsin_Congressional_Districts_2011.geojson',
}]

congressional_districts = []

for congressional_district_meta in congressional_districts_meta:
    congressional_districts.append({})
    with open(congressional_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        congressional_district = {
            'state': states[congressional_district_meta['stateName']],
            'year': congressional_district_meta['year'],
            'name': feature['properties'][congressional_district_meta['nameProperty']],
            'geometry': feature['geometry'],
        }
        result = db['congressionalDistricts'].insert_one(congressional_district)
        congressional_districts[-1][congressional_district['name']] = result.inserted_id

db['congressionalDistricts'].create_index('state')
db['congressionalDistricts'].create_index('year')
db['congressionalDistricts'].create_index('name')
db['congressionalDistricts'].create_index([('geometry', GEOSPHERE)])

########################################################################################################################
# Ingest Wards
########################################################################################################################

wards_meta = [{
    'stateName': 'Wisconsin',
    'year': '2011',
    'countyProperty': 'CNTY_NAME',
    'assemblyDistrictProperty': 'ASM',
    'senateDistrictProperty': 'SEN',
    'congressionalDistrictProperty': 'CON',
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

for i, ward_meta in enumerate(wards_meta):
    with open(ward_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        ward = {
            'state': states[ward_meta['stateName']],
            'year': ward_meta['year'],
            'county': counties[i][feature['properties'][ward_meta['countyProperty']]],
            'assemblyDistrict': assembly_districts[i][feature['properties'][ward_meta['assemblyDistrictProperty']]],
            'senateDistrict': senate_districts[i][feature['properties'][ward_meta['senateDistrictProperty']]],
            'congressionalDistrict': congressional_districts[i][feature['properties'][ward_meta['congressionalDistrictProperty']]],
            'geometry': feature['geometry'],
        }
        db['wards'].insert_one(ward)

db['wards'].create_index('state')
db['wards'].create_index('year')
db['wards'].create_index('county')
db['wards'].create_index('assemblyDistrict')
db['wards'].create_index('senateDistrict')
db['wards'].create_index('congressionalDistrict')
db['wards'].create_index([('geometry', GEOSPHERE)])

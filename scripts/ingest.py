import psycopg2
import json

conn = psycopg2.connect(user='gm_ingest', host='127.0.0.1', port='5432', database='gm')
cur = conn.cursor()

########################################################################################################################
# Ingest States
########################################################################################################################

cur.execute('''
CREATE TABLE gm.states (
    id SERIAL,
    name VARCHAR NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (name)
);
''')

cur.execute('CREATE INDEX states_geometry_idx ON gm.states USING GIST (geometry);')

states_meta = [{
    'name_property': 'name',
    'geojson': '../data/simplified/us-states.geojson',
}]

state_ids = {}

query = 'INSERT INTO gm.states (name, geometry) VALUES (%s, ST_GeomFromGeoJSON(%s)) RETURNING id;'

for state_meta in states_meta:
    with open(state_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        state = (
            feature['properties'][state_meta['name_property']],
            json.dumps(feature['geometry'])
        )
        cur.execute(query, state)
        state_ids[state[0]] = cur.fetchone()[0]

conn.commit()

#########################################################################################################################
# Ingest Counties
########################################################################################################################

cur.execute('''
CREATE TABLE gm.counties (
    id SERIAL,
    state_id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    UNIQUE (state_id, name)
);
''')

cur.execute('CREATE INDEX counties_state_id_fkey ON gm.counties(state_id);')
cur.execute('CREATE INDEX counties_geometry_idx ON gm.counties USING GIST (geometry);')

counties_meta = [{
    'state': 'Wisconsin',
    'name_property': 'COUNTY_NAME',
    'geojson': '../data/simplified/County_Boundaries_24K.geojson',
}]

county_ids = []

query = 'INSERT INTO gm.counties (state_id, name, geometry) VALUES (%s, %s, ST_GeomFromGeoJSON(%s)) RETURNING id;'

for county_meta in counties_meta:
    county_ids.append({})
    with open(county_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        county = (
            state_ids[county_meta['state']],
            feature['properties'][county_meta['name_property']],
            json.dumps(feature['geometry']),
        )
        cur.execute(query, county)
        county_ids[-1][county[1]] = cur.fetchone()[0]

conn.commit()

########################################################################################################################
# Ingest Assembly Districts
########################################################################################################################

cur.execute('''
CREATE TABLE gm.assembly_districts (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    name VARCHAR NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    UNIQUE (state_id, year, name)
);
''')

cur.execute('CREATE INDEX assembly_districts_state_id_fkey ON gm.assembly_districts(state_id);')
cur.execute('CREATE INDEX assembly_districts_geometry_idx ON gm.assembly_districts USING GIST (geometry);')

assembly_districts_meta = [{
    'state': 'Wisconsin',
    'year': '2011',
    'name_property': 'District_S',
    'geojson': '../data/simplified/Wisconsin_Assembly_Districts_2012.geojson',
}]

assembly_district_ids = []

query = '''
INSERT INTO gm.assembly_districts (
    state_id,
    year,
    name,
    geometry
) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNING id;
'''

for assembly_district_meta in assembly_districts_meta:
    assembly_district_ids.append({})
    with open(assembly_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        assembly_district = (
            state_ids[assembly_district_meta['state']],
            assembly_district_meta['year'],
            feature['properties'][assembly_district_meta['name_property']],
            json.dumps(feature['geometry']),
        )
        cur.execute(query, assembly_district)
        assembly_district_ids[-1][assembly_district[2]] = cur.fetchone()[0]

conn.commit()

########################################################################################################################
# Ingest Senate Districts
########################################################################################################################

cur.execute('''
CREATE TABLE gm.senate_districts (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    name VARCHAR NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    UNIQUE (state_id, year, name)
);
''')

cur.execute('CREATE INDEX senate_districts_state_id_fkey ON gm.senate_districts(state_id);')
cur.execute('CREATE INDEX senate_districts_geometry_idx ON gm.senate_districts USING GIST (geometry);')

senate_districts_meta = [{
    'state': 'Wisconsin',
    'year': '2011',
    'name_property': 'SEN_NUM',
    'geojson': '../data/simplified/Wisconsin_Senate_Districts.geojson',
}]

senate_district_ids = []

query = '''
INSERT INTO gm.senate_districts (
    state_id,
    year,
    name,
    geometry
) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNING id;
'''

for senate_district_meta in senate_districts_meta:
    senate_district_ids.append({})
    with open(senate_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        senate_district = (
            state_ids[senate_district_meta['state']],
            senate_district_meta['year'],
            feature['properties'][senate_district_meta['name_property']],
            json.dumps(feature['geometry']),
        )
        cur.execute(query, senate_district)
        senate_district_ids[-1][senate_district[2]] = cur.fetchone()[0]

conn.commit()

########################################################################################################################
# Ingest Congressional Districts
########################################################################################################################

cur.execute('''
CREATE TABLE gm.congressional_districts (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    name VARCHAR NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    UNIQUE (state_id, year, name)
);
''')

cur.execute('CREATE INDEX congressional_districts_state_id_fkey ON gm.congressional_districts(state_id);')
cur.execute('CREATE INDEX congressional_districts_geometry_idx ON gm.congressional_districts USING GIST (geometry);')

congressional_districts_meta = [{
    'state': 'Wisconsin',
    'year': '2011',
    'name_property': 'District_N',
    'geojson': '../data/simplified/Wisconsin_Congressional_Districts_2011.geojson',
}]

congressional_district_ids = []

query = '''
INSERT INTO gm.congressional_districts (
    state_id,
    year,
    name,
    geometry
) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNING id;
'''

for congressional_district_meta in congressional_districts_meta:
    congressional_district_ids.append({})
    with open(congressional_district_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        congressional_district = (
            state_ids[congressional_district_meta['state']],
            congressional_district_meta['year'],
            feature['properties'][congressional_district_meta['name_property']],
            json.dumps(feature['geometry']),
        )
        cur.execute(query, congressional_district)
        congressional_district_ids[-1][congressional_district[2]] = cur.fetchone()[0]

conn.commit()

########################################################################################################################
# Ingest Wards
########################################################################################################################

cur.execute('''
CREATE TABLE gm.wards (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    county_id INTEGER NOT NULL,
    assembly_district_id INTEGER NOT NULL,
    senate_district_id INTEGER NOT NULL,
    congressional_district_id INTEGER NOT NULL,
    geometry GEOMETRY NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    FOREIGN KEY (county_id) REFERENCES gm.counties(id),
    FOREIGN KEY (assembly_district_id) REFERENCES gm.assembly_districts(id),
    FOREIGN KEY (senate_district_id) REFERENCES gm.senate_districts(id),
    FOREIGN KEY (congressional_district_id) REFERENCES gm.congressional_districts(id)
);
''')

cur.execute('CREATE INDEX wards_state_id_fkey ON gm.wards(state_id);')
cur.execute('CREATE INDEX wards_county_id_fkey ON gm.wards(county_id);')
cur.execute('CREATE INDEX wards_assembly_district_id_fkey ON gm.wards(assembly_district_id);')
cur.execute('CREATE INDEX wards_senate_district_id_fkey ON gm.wards(senate_district_id);')
cur.execute('CREATE INDEX wards_congressional_district_id_fkey ON gm.wards(congressional_district_id);')
cur.execute('CREATE INDEX wards_geometry_idx ON gm.wards USING GIST (geometry);')

wards_meta = [{
    'state': 'Wisconsin',
    'year': '2011',
    'county_property': 'CNTY_NAME',
    'assembly_district_property': 'ASM',
    'senate_district_property': 'SEN',
    'congressional_district_property': 'CON',
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

ward_ids = []

query = '''
INSERT INTO gm.wards (
    state_id,
    year,
    county_id,
    assembly_district_id,
    senate_district_id,
    congressional_district_id,
    geometry
) VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s)) RETURNING id;
'''

for i, ward_meta in enumerate(wards_meta):
    ward_ids.append([])
    with open(ward_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        ward = (
            state_ids[ward_meta['state']],
            ward_meta['year'],
            county_ids[i][feature['properties'][ward_meta['county_property']]],
            assembly_district_ids[i][feature['properties'][ward_meta['assembly_district_property']]],
            senate_district_ids[i][feature['properties'][ward_meta['senate_district_property']]],
            congressional_district_ids[i][feature['properties'][ward_meta['congressional_district_property']]],
            json.dumps(feature['geometry']),
        )
        cur.execute(query, ward)
        ward_ids[-1].append(cur.fetchone()[0])

conn.commit()

########################################################################################################################
# Ingest Elections
########################################################################################################################

cur.execute('''
CREATE TABLE gm.elections (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    race VARCHAR NOT NULL,
    ward_id INTEGER NOT NULL,
    total INTEGER NOT NULL,
    democrat INTEGER NOT NULL,
    republican INTEGER NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    FOREIGN KEY (ward_id) REFERENCES gm.wards(id),
    UNIQUE (state_id, year, race, ward_id)
);
''')

cur.execute('CREATE INDEX elections_state_id_fkey ON gm.elections(state_id);')
cur.execute('CREATE INDEX elections_ward_id_fkey ON gm.elections(ward_id);')

elections_meta = [{
    'state': 'Wisconsin',
    'races': {
        'President': [{
            'year': '2012',
            'total_property': 'PRETOT12',
            'democrat_property': 'PREDEM12',
            'republican_property': 'PREREP12',
        }, {
            'year': '2016',
            'total_property': 'PRETOT16',
            'democrat_property': 'PREDEM16',
            'republican_property': 'PREREP16',
        }],
        'Senate': [{
            'year': '2012',
            'total_property': 'USSTOT12',
            'democrat_property': 'USSDEM12',
            'republican_property': 'USSREP12',
        }, {
            'year': '2016',
            'total_property': 'USSTOT16',
            'democrat_property': 'USSDEM16',
            'republican_property': 'USSREP16',
        }, {
            'year': '2018',
            'total_property': 'USSTOT18',
            'democrat_property': 'USSDEM18',
            'republican_property': 'USSREP18',
        }],
        'House': [{
            'year': '2012',
            'total_property': 'USHTOT12',
            'democrat_property': 'USHDEM12',
            'republican_property': 'USHREP12',
        }, {
            'year': '2014',
            'total_property': 'USHTOT14',
            'democrat_property': 'USHDEM14',
            'republican_property': 'USHREP14',
        }, {
            'year': '2016',
            'total_property': 'USHTOT16',
            'democrat_property': 'USHDEM16',
            'republican_property': 'USHREP16',
        }, {
            'year': '2018',
            'total_property': 'USHTOT18',
            'democrat_property': 'USHDEM18',
            'republican_property': 'USHREP18',
        }],
        'Governor': [{
            'year': '2012',
            'total_property': 'GOVTOT12',
            'democrat_property': 'GOVDEM12',
            'republican_property': 'GOVREP12',
        }, {
            'year': '2014',
            'total_property': 'GOVTOT14',
            'democrat_property': 'GOVDEM14',
            'republican_property': 'GOVREP14',
        }, {
            'year': '2018',
            'total_property': 'GOVTOT18',
            'democrat_property': 'GOVDEM18',
            'republican_property': 'GOVREP18',
        }],
        'State Senate': [{
            'year': '2012',
            'total_property': 'WSSTOT12',
            'democrat_property': 'WSSDEM12',
            'republican_property': 'WSSREP12',
        }, {
            'year': '2014',
            'total_property': 'WSSTOT14',
            'democrat_property': 'WSSDEM14',
            'republican_property': 'WSSREP14',
        }, {
            'year': '2016',
            'total_property': 'WSSTOT16',
            'democrat_property': 'WSSDEM16',
            'republican_property': 'WSSREP16',
        }, {
            'year': '2018',
            'total_property': 'WSSTOT18',
            'democrat_property': 'WSSDEM18',
            'republican_property': 'WSSREP18',
        }],
        'State Assembly': [{
            'year': '2012',
            'total_property': 'WSATOT12',
            'democrat_property': 'WSADEM12',
            'republican_property': 'WSAREP12',
        }, {
            'year': '2014',
            'total_property': 'WSATOT14',
            'democrat_property': 'WSADEM14',
            'republican_property': 'WSAREP14',
        }, {
            'year': '2016',
            'total_property': 'WSATOT16',
            'democrat_property': 'WSADEM16',
            'republican_property': 'WSAREP16',
        }, {
            'year': '2018',
            'total_property': 'WSATOT18',
            'democrat_property': 'WSADEM18',
            'republican_property': 'WSAREP18',
        }],
        'County District Attorney': [{
            'year': '2012',
            'total_property': 'CDATOT12',
            'democrat_property': 'CDADEM12',
            'republican_property': 'CDAREP12',
        }, {
            'year': '2016',
            'total_property': 'CDATOT16',
            'democrat_property': 'CDADEM16',
            'republican_property': 'CDAREP16',
        }],
        'Attorney General': [{
            'year': '2014',
            'total_property': 'WAGTOT14',
            'democrat_property': 'WAGDEM14',
            'republican_property': 'WAGREP14',
        }, {
            'year': '2018',
            'total_property': 'WAGTOT18',
            'democrat_property': 'WAGDEM18',
            'republican_property': 'WAGREP18',
        }],
        'Treasurer': [{
            'year': '2014',
            'total_property': 'TRSTOT14',
            'democrat_property': 'TRSDEM14',
            'republican_property': 'TRSREP14',
        }, {
            'year': '2018',
            'total_property': 'TRSTOT18',
            'democrat_property': 'TRSDEM18',
            'republican_property': 'TRSREP18',
        }],
        'Secretary of State': [{
            'year': '2014',
            'total_property': 'SOSTOT14',
            'democrat_property': 'SOSDEM14',
            'republican_property': 'SOSREP14',
        }, {
            'year': '2018',
            'total_property': 'SOSTOT18',
            'democrat_property': 'SOSDEM18',
            'republican_property': 'SOSREP18',
        }],
    },
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

query = '''
INSERT INTO gm.elections (
    state_id,
    year,
    race,
    ward_id,
    total,
    democrat,
    republican
) VALUES (%s, %s, %s, %s, %s, %s, %s);
'''

for i, election_meta in enumerate(elections_meta):
    with open(election_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for j, feature in enumerate(geojson['features']):
        for race, years in election_meta['races'].items():
            for year in years:
                election = (
                    state_ids[election_meta['state']],
                    year['year'],
                    race,
                    ward_ids[i][j],
                    feature['properties'][year['total_property']],
                    feature['properties'][year['democrat_property']],
                    feature['properties'][year['republican_property']],
                )
                cur.execute(query, election)

conn.commit()

########################################################################################################################
# Ingest Demographics
########################################################################################################################

cur.execute('''
CREATE TABLE gm.demographics (
    id SERIAL,
    state_id INTEGER NOT NULL,
    year CHAR(4) NOT NULL,
    ward_id INTEGER NOT NULL,
    total INTEGER NOT NULL,
    white INTEGER NOT NULL,
    black INTEGER NOT NULL,
    american_indian INTEGER NOT NULL,
    asian INTEGER NOT NULL,
    pacific_islander INTEGER NOT NULL,
    hispanic INTEGER NOT NULL,
    other INTEGER NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (state_id) REFERENCES gm.states(id),
    FOREIGN KEY (ward_id) REFERENCES gm.wards(id),
    UNIQUE (state_id, year, ward_id)
);
''')

cur.execute('CREATE INDEX demographics_state_id_fkey ON gm.demographics(state_id);')
cur.execute('CREATE INDEX demographics_ward_id_fkey ON gm.demographics(ward_id);')

demographics_meta = [{
    'state': 'Wisconsin',
    'year': '2011',
    'total_property': 'PERSONS18',
    'white_property': 'WHITE18',
    'black_property': 'BLACK18',
    'american_indian_property': 'AMINDIAN18',
    'asian_property': 'ASIAN18',
    'pacific_islander_property': 'PISLAND18',
    'hispanic_property': 'HISPANIC18',
    'other_property': 'OTHER18',
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

query = '''
INSERT INTO gm.demographics (
    state_id,
    year,
    ward_id,
    total,
    white,
    black,
    american_indian,
    asian,
    pacific_islander,
    hispanic,
    other
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''

for i, demographic_meta in enumerate(demographics_meta):
    with open(demographic_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for j, feature in enumerate(geojson['features']):
        demographic = (
            state_ids[demographic_meta['state']],
            demographic_meta['year'],
            ward_ids[i][j],
            feature['properties'][demographic_meta['total_property']],
            feature['properties'][demographic_meta['white_property']],
            feature['properties'][demographic_meta['black_property']],
            feature['properties'][demographic_meta['american_indian_property']],
            feature['properties'][demographic_meta['asian_property']],
            feature['properties'][demographic_meta['pacific_islander_property']],
            feature['properties'][demographic_meta['hispanic_property']],
            feature['properties'][demographic_meta['other_property']],
        )
        cur.execute(query, demographic)

conn.commit()

cur.close()
conn.close()

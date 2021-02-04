import psycopg2
import re
import json


def sanitize(name):
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    name = re.sub(r'(^_+|_+$)', '', name)
    return name.lower()


conn = psycopg2.connect(user='gm_ingest', host='127.0.0.1', port='5432', database='gm')
cur = conn.cursor()

########################################################################################################################
# Ingest States
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.states CASCADE;')

cur.execute('''
CREATE TABLE gm.states (
    name     VARCHAR  NOT NULL,

             UNIQUE (name),

    geometry GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX states_geometry_idx ON gm.states USING GIST (geometry);')

states_meta = [{
    'name_property': 'name',
    'geojson': '../data/simplified/us-states.geojson',
}]

query = 'INSERT INTO gm.states (name, geometry) VALUES (%s, ST_GeomFromGeoJSON(%s));'

for state_meta in states_meta:
    with open(state_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        state = (
            sanitize(feature['properties'][state_meta['name_property']]),
            json.dumps(feature['geometry'])
        )
        cur.execute(query, state)

conn.commit()

#########################################################################################################################
# Ingest Counties
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.counties CASCADE;')

cur.execute('''
CREATE TABLE gm.counties (
    state    VARCHAR  NOT NULL,

             FOREIGN KEY (state)
              REFERENCES gm.states(name),

    name     VARCHAR  NOT NULL,

             UNIQUE (state, name),

    geometry GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX counties_state_fkey ON gm.counties(state);')
cur.execute('CREATE INDEX counties_geometry_idx ON gm.counties USING GIST (geometry);')

counties_meta = [{
    'state': 'wisconsin',
    'name_property': 'COUNTY_NAME',
    'geojson': '../data/simplified/County_Boundaries_24K.geojson',
}]

query = 'INSERT INTO gm.counties (state, name, geometry) VALUES (%s, %s, ST_GeomFromGeoJSON(%s));'

for county_meta in counties_meta:
    with open(county_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        county = (
            county_meta['state'],
            sanitize(feature['properties'][county_meta['name_property']]),
            json.dumps(feature['geometry']),
        )
        cur.execute(query, county)

conn.commit()

########################################################################################################################
# Ingest Assembly Districts
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.assemblies CASCADE;')

cur.execute('''
CREATE TABLE gm.assemblies (
    state    VARCHAR  NOT NULL,

             FOREIGN KEY (state)
              REFERENCES gm.states(name),

    year     CHAR(4)  NOT NULL,
    name     VARCHAR  NOT NULL,

             UNIQUE (state, year, name),

    geometry GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX assemblies_state_fkey ON gm.assemblies(state);')
cur.execute('CREATE INDEX assemblies_geometry_idx ON gm.assemblies USING GIST (geometry);')

assemblies_meta = [{
    'state': 'wisconsin',
    'year': '2011',
    'name_property': 'District_S',
    'geojson': '../data/simplified/Wisconsin_Assembly_Districts_2012.geojson',
}]

query = 'INSERT INTO gm.assemblies (state, year, name, geometry) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s));'

for assembly_meta in assemblies_meta:
    with open(assembly_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        assembly = (
            assembly_meta['state'],
            assembly_meta['year'],
            sanitize(feature['properties'][assembly_meta['name_property']]),
            json.dumps(feature['geometry']),
        )
        cur.execute(query, assembly)

conn.commit()

########################################################################################################################
# Ingest Senate Districts
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.senates CASCADE;')

cur.execute('''
CREATE TABLE gm.senates (
    state    VARCHAR  NOT NULL,

             FOREIGN KEY (state)
              REFERENCES gm.states(name),

    year     CHAR(4)  NOT NULL,
    name     VARCHAR  NOT NULL,

             UNIQUE (state, year, name),

    geometry GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX senates_state_fkey ON gm.senates(state);')
cur.execute('CREATE INDEX senates_geometry_idx ON gm.senates USING GIST (geometry);')

senates_meta = [{
    'state': 'wisconsin',
    'year': '2011',
    'name_property': 'SEN_NUM',
    'geojson': '../data/simplified/Wisconsin_Senate_Districts.geojson',
}]

query = 'INSERT INTO gm.senates (state, year, name, geometry) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s));'

for senate_meta in senates_meta:
    with open(senate_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        senate = (
            senate_meta['state'],
            senate_meta['year'],
            sanitize(feature['properties'][senate_meta['name_property']]),
            json.dumps(feature['geometry']),
        )
        cur.execute(query, senate)

conn.commit()

########################################################################################################################
# Ingest Congressional Districts
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.congressionals CASCADE;')

cur.execute('''
CREATE TABLE gm.congressionals (
    state    VARCHAR  NOT NULL,

             FOREIGN KEY (state)
              REFERENCES gm.states(name),

    year     CHAR(4)  NOT NULL,
    name     VARCHAR  NOT NULL,

             UNIQUE (state, year, name),

    geometry GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX congressionals_state_fkey ON gm.congressionals(state);')
cur.execute('CREATE INDEX congressionals_geometry_idx ON gm.congressionals USING GIST (geometry);')

congressionals_meta = [{
    'state': 'wisconsin',
    'year': '2011',
    'name_property': 'District_N',
    'geojson': '../data/simplified/Wisconsin_Congressional_Districts_2011.geojson',
}]

query = 'INSERT INTO gm.congressionals (state, year, name, geometry) VALUES (%s, %s, %s, ST_GeomFromGeoJSON(%s));'

for congressional_meta in congressionals_meta:
    with open(congressional_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        congressional = (
            congressional_meta['state'],
            congressional_meta['year'],
            sanitize(feature['properties'][congressional_meta['name_property']]),
            json.dumps(feature['geometry']),
        )
        cur.execute(query, congressional)

conn.commit()

########################################################################################################################
# Ingest Wards
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.wards CASCADE;')

cur.execute('''
CREATE TABLE gm.wards (
    state         VARCHAR  NOT NULL,

                  FOREIGN KEY (state)
                   REFERENCES gm.states(name),

    year          CHAR(4)  NOT NULL,
    name          VARCHAR  NOT NULL,

                  UNIQUE (state, year, name),

    county        VARCHAR  NOT NULL,

                  FOREIGN KEY (state, county)
                   REFERENCES gm.counties(state, name),

    assembly      VARCHAR  NOT NULL,

                  FOREIGN KEY (state, year, assembly)
                   REFERENCES gm.assemblies(state, year, name),

    senate        VARCHAR  NOT NULL,

                  FOREIGN KEY (state, year, senate)
                   REFERENCES gm.senates(state, year, name),

    congressional VARCHAR  NOT NULL,

                  FOREIGN KEY (state, year, congressional)
                   REFERENCES gm.congressionals(state, year, name),

    geometry      GEOMETRY NOT NULL
);
''')

cur.execute('CREATE INDEX wards_state_fkey ON gm.wards(state);')
cur.execute('CREATE INDEX wards_state_county_fkey ON gm.wards(state, county);')
cur.execute('CREATE INDEX wards_state_year_assembly_fkey ON gm.wards(state, year, assembly);')
cur.execute('CREATE INDEX wards_state_year_senate_fkey ON gm.wards(state, year, senate);')
cur.execute('CREATE INDEX wards_state_year_congressional_fkey ON gm.wards(state, year, congressional);')
cur.execute('CREATE INDEX wards_geometry_idx ON gm.wards USING GIST (geometry);')

wards_meta = [{
    'state': 'wisconsin',
    'county_property': 'CNTY_NAME',
    'year': '2011',
    'name_property': 'LABEL',
    'assembly_property': 'ASM',
    'senate_property': 'SEN',
    'congressional_property': 'CON',
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

query = '''
INSERT INTO gm.wards (
    state,
    year,
    name,
    county,
    assembly,
    senate,
    congressional,
    geometry
) VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromGeoJSON(%s));
'''

for ward_meta in wards_meta:
    with open(ward_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        ward = (
            ward_meta['state'],
            ward_meta['year'],
            sanitize(feature['properties'][ward_meta['county_property']]) + '_' + sanitize(feature['properties'][ward_meta['name_property']]),
            sanitize(feature['properties'][ward_meta['county_property']]),
            sanitize(feature['properties'][ward_meta['assembly_property']]),
            sanitize(feature['properties'][ward_meta['senate_property']]),
            sanitize(feature['properties'][ward_meta['congressional_property']]),
            json.dumps(feature['geometry']),
        )
        cur.execute(query, ward)

conn.commit()

########################################################################################################################
# Ingest Votes
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.votes CASCADE;')

cur.execute('''
CREATE TABLE gm.votes (
    state      VARCHAR NOT NULL,

               FOREIGN KEY (state)
                REFERENCES gm.states(name),

    race       VARCHAR NOT NULL,
    year       CHAR(4) NOT NULL,
    ward_year  CHAR(4) NOT NULL,
    ward       VARCHAR NOT NULL,

               UNIQUE (state, race, year, ward_year, ward),

               FOREIGN KEY (state, ward_year, ward)
                REFERENCES gm.wards(state, year, name),

    total      INTEGER NOT NULL,
    democrat   INTEGER NOT NULL,
    republican INTEGER NOT NULL
);
''')

cur.execute('CREATE INDEX votes_state_fkey ON gm.votes(state);')
cur.execute('CREATE INDEX votes_state_ward_year_ward_fkey ON gm.votes(state, ward_year, ward);')

votes_meta = [{
    'state': 'wisconsin',
    'county_property': 'CNTY_NAME',
    'ward_year': '2011',
    'ward_property': 'LABEL',
    'races': {
        'president': [{
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
        'senate': [{
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
        'house': [{
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
        'governor': [{
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
        'state_senate': [{
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
        'state_assembly': [{
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
        'county_district_attorney': [{
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
        'state_attorney_general': [{
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
        'state_treasurer': [{
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
        'state_secretary_of_state': [{
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
INSERT INTO gm.votes (
    state,
    race,
    year,
    ward_year,
    ward,
    total,
    democrat,
    republican
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
'''

for vote_meta in votes_meta:
    with open(vote_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        for race, years in vote_meta['races'].items():
            for year in years:
                vote = (
                    vote_meta['state'],
                    race,
                    year['year'],
                    vote_meta['ward_year'],
                    sanitize(feature['properties'][vote_meta['county_property']]) + '_' + sanitize(feature['properties'][vote_meta['ward_property']]),
                    feature['properties'][year['total_property']],
                    feature['properties'][year['democrat_property']],
                    feature['properties'][year['republican_property']],
                )
                cur.execute(query, vote)

conn.commit()

########################################################################################################################
# Ingest Populations
########################################################################################################################

cur.execute('DROP TABLE IF EXISTS gm.populations CASCADE;')

cur.execute('''
CREATE TABLE gm.populations (
    state            VARCHAR NOT NULL,

                     FOREIGN KEY (state)
                      REFERENCES gm.states(name),

    year             CHAR(4) NOT NULL,
    ward_year        CHAR(4) NOT NULL,
    ward             VARCHAR NOT NULL,

                     UNIQUE (state, year, ward_year, ward),

                     FOREIGN KEY (state, ward_year, ward)
                      REFERENCES gm.wards(state, year, name),

    total            INTEGER NOT NULL,
    white            INTEGER NOT NULL,
    black            INTEGER NOT NULL,
    american_indian  INTEGER NOT NULL,
    asian            INTEGER NOT NULL,
    pacific_islander INTEGER NOT NULL,
    hispanic         INTEGER NOT NULL
);
''')

cur.execute('CREATE INDEX populations_state_fkey ON gm.populations(state);')
cur.execute('CREATE INDEX populations_state_ward_year_ward_fkey ON gm.populations(state, ward_year, ward);')

populations_meta = [{
    'state': 'wisconsin',
    'year': '2010',
    'county_property': 'CNTY_NAME',
    'ward_year': '2011',
    'ward_property': 'LABEL',
    'total_property': 'PERSONS18',
    'white_property': 'WHITE18',
    'black_property': 'BLACK18',
    'american_indian_property': 'AMINDIAN18',
    'asian_property': 'ASIAN18',
    'pacific_islander_property': 'PISLAND18',
    'hispanic_property': 'HISPANIC18',
    'geojson': '../data/simplified/2018-2012_Election_Data_with_2011_Wards.geojson',
}]

query = '''
INSERT INTO gm.populations (
    state,
    year,
    ward_year,
    ward,
    total,
    white,
    black,
    american_indian,
    asian,
    pacific_islander,
    hispanic
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''

for population_meta in populations_meta:
    with open(population_meta['geojson'], 'r') as geojson_file:
        geojson = json.load(geojson_file)
    for feature in geojson['features']:
        population = (
            population_meta['state'],
            population_meta['year'],
            population_meta['ward_year'],
            sanitize(feature['properties'][population_meta['county_property']]) + '_' + sanitize(feature['properties'][population_meta['ward_property']]),
            feature['properties'][population_meta['total_property']],
            feature['properties'][population_meta['white_property']],
            feature['properties'][population_meta['black_property']],
            feature['properties'][population_meta['american_indian_property']],
            feature['properties'][population_meta['asian_property']],
            feature['properties'][population_meta['pacific_islander_property']],
            feature['properties'][population_meta['hispanic_property']],
        )
        cur.execute(query, population)

conn.commit()

cur.close()
conn.close()

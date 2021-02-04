const express = require('express')
const http = require('http')
const pg = require('pg')
const uuid = require('uuid').v4

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Global Constants
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const POOL = new pg.Pool({
  host: 'localhost',
  port: 5432,
  user: 'gm_api',
  password: null,
  database: 'gm',
})
const STATUS_CODE_OK = 200
const STATUS_CODE_BAD_REQUEST = 400
const STATUS_CODE_NOT_FOUND = 404
const STATUS_CODE_INTERNAL_SERVER_ERROR = 500
const STATUS_BAD_REQUEST = 'BAD_REQUEST'
const STATUS_NOT_FOUND = 'NOT_FOUND'
const STATUS_INTERNAL_SERVER_ERROR = 'INTERNAL_SERVER_ERROR'
const LOG_OPTIONS = {
  'color': true,
  'depth': null,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const parseCountyURI = county => {
  if (county) {
    const match = county.match(/^\/states\/(?<state>[^\/]*)\/counties\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    } else {
      throw new Error(`County '${county}' does not match the pattern '/states/{state}/counties/{county}'`)
    }
  }
}

const parseAssemblyURI = assembly => {
  if (assembly) {
    const match = assembly.match(/^\/states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/assemblies\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    } else {
      throw new Error(`Assembly '${assembly}' does not match the pattern '/states/{state}/years/{year}/assemblies/{assembly}'`)
    }
  }
}

const parseSenateURI = senate => {
  if (senate) {
    const match = senate.match(/^\/states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/senates\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    } else {
      throw new Error(`Senate '${senate}' does not match the pattern '/states/{state}/years/{year}/senates/{senate}'`)
    }
  }
}

const parseCongressionalURI = congressional => {
  if (congressional) {
    const match = congressional.match(/^\/states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/congressionals\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    } else {
      throw new Error(`Congressional '${congressional}' does not match the pattern '/states/{state}/years/{year}/congressionals/{congressional}'`)
    }
  }
}

const parseWardURI = ward => {
  if (ward) {
    const match = ward.match(/^\/states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/wards\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    } else {
      throw new Error(`Ward '${ward}' does not match the pattern '/states/{state}/years/{year}/wards/{ward}'`)
    }
  }
}

const prettifySQL = sql => {
  sql = sql.replace(/(\s*\n){2,}/g, '\n\n')
  sql = sql.replace(/(^(\s*\n)+|(\n\s*)+$)/g, '')
  const indent = sql.match(/^ */)[0]
  sql = sql.replace(new RegExp(`^${indent}`, 'gm'), '')
  return `\n${sql}`
}

const extractProperties = result => {
  return Object.entries(result).filter(([k, _]) => {
    return k !== 'geometry'
  }).reduce((kvs, [k, v]) => {
    return { [k]: v, ...kvs }
  }, {})
}

const extractGeometry = result => {
  return JSON.parse(result['geometry'])
}

const extractFeature = result => {
  return {
    'type': 'Feature',
    'properties': extractProperties(result),
    'geometry': extractGeometry(result),
  }
}

const extractFeatures = results => {
  return {
    'type': 'FeatureCollection',
    'features': results.map(extractFeature),
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Logging Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const logRequest = (id, req) => {
  console.dir({
    'id': id,
    'request': {
      'method': req['method'],
      'url': req['url'],
      'params': req['params'],
      'query': req['query'],
      'body': req['body']
    }
  }, LOG_OPTIONS)
}

const logResponse = (id, sql, res, body) => {
  console.dir({
    'id': id,
    'response': {
      'statusCode': res['statusCode'],
      'statusMessage': res['statusMessage'],
      'sql': sql,
      'body': body
    }
  }, LOG_OPTIONS)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Queries
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const statesListSQL = params => {
  return `
    SELECT st.name AS name,
           ST_AsGeoJSON(st.geometry) AS geometry

      FROM states AS st

         ${(params['county']) ? `
           JOIN counties AS cty
           ON st.name = cty.state
         ` : ''}

         ${(params['assembly']) ? `
           JOIN assemblies AS asm
           ON st.name = asm.state
         ` : ''}

         ${(params['senate']) ? `
           JOIN senates AS sen
           ON st.name = sen.state
         ` : ''}

         ${(params['congressional']) ? `
           JOIN congressionals AS con
           ON st.name = con.state
         ` : ''}

         ${(params['ward']) ? `
           JOIN wards AS wrd
           ON st.name = wrd.state
         ` : ''}

     WHERE TRUE
     ${(params['within']) ? `
       AND ST_Within(st.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(st.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(st.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['county']) ? `
       AND cty.state = '${params['county']['state']}'
       AND cty.name = '${params['county']['name']}'
     ` : ''}
     ${(params['assembly']) ? `
       AND asm.state = '${params['assembly']['state']}'
       AND asm.year = '${params['assembly']['year']}'
       AND asm.name = '${params['assembly']['name']}'
     ` : ''}
     ${(params['senate']) ? `
       AND sen.state = '${params['senate']['state']}'
       AND sen.year = '${params['senate']['year']}'
       AND sen.name = '${params['senate']['name']}'
     ` : ''}
     ${(params['congressional']) ? `
       AND con.state = '${params['congressional']['state']}'
       AND con.year = '${params['congressional']['year']}'
       AND con.name = '${params['congressional']['name']}'
     ` : ''}
     ${(params['ward']) ? `
       AND wrd.state = '${params['ward']['state']}'
       AND wrd.year = '${params['ward']['year']}'
       AND wrd.name = '${params['ward']['name']}'
     ` : ''}
  `
}

const statesGetSQL = params => {
  return `
    SELECT '${params['state']}' AS name,
           ST_AsGeoJSON(st.geometry) AS geometry

      FROM states AS st

     WHERE st.name = '${params['state']}'
  `
}

const countiesListSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           cty.name AS name,
           ST_AsGeoJSON(cty.geometry) AS geometry

      FROM counties AS cty

         ${(params['ward']) ? `
           JOIN wards AS wrd
           ON cty.name = wrd.county
         ` : ''}

     WHERE cty.state = '${params['state']}'
     ${(params['within']) ? `
       AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['ward']) ? `
       AND wrd.state = '${params['ward']['state']}'
       AND wrd.year = '${params['ward']['year']}'
       AND wrd.name = '${params['ward']['name']}'
     ` : ''}
  `
}

const countiesGetSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['county']}' AS name,
           ST_AsGeoJSON(cty.geometry) AS geometry

      FROM counties AS cty

     WHERE cty.state = '${params['state']}'
       AND cty.name = '${params['county']}'
  `
}

const assembliesListSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           asm.name AS name,
           ST_AsGeoJSON(asm.geometry) AS geometry

      FROM assemblies AS asm

         ${(params['ward']) ? `
           JOIN wards AS wrd
           ON asm.name = wrd.assembly
         ` : ''}

     WHERE asm.state = '${params['state']}'
       AND asm.year = '${params['year']}'
     ${(params['within']) ? `
       AND ST_Within(asm.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(asm.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(asm.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['ward']) ? `
       AND wrd.state = '${params['ward']['state']}'
       AND wrd.year = '${params['ward']['year']}'
       AND wrd.name = '${params['ward']['name']}'
     ` : ''}
  `
}

const assembliesGetSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           '${params['assembly']}' AS name,
           ST_AsGeoJSON(asm.geometry) AS geometry

      FROM assemblies AS asm

     WHERE asm.state = '${params['state']}'
       AND asm.year = '${params['year']}'
       AND asm.name = '${params['assembly']}'
  `
}

const senatesListSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           sen.name AS name,
           ST_AsGeoJSON(sen.geometry) AS geometry

      FROM senates AS sen

         ${(params['ward']) ? `
           JOIN wards AS wrd
           ON sen.name = wrd.senate
         ` : ''}

     WHERE sen.state = '${params['state']}'
       AND sen.year = '${params['year']}'
     ${(params['within']) ? `
       AND ST_Within(sen.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(sen.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(sen.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['ward']) ? `
       AND wrd.state = '${params['ward']['state']}'
       AND wrd.year = '${params['ward']['year']}'
       AND wrd.name = '${params['ward']['name']}'
     ` : ''}
  `
}

const senatesGetSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           '${params['senate']}' AS name,
           ST_AsGeoJSON(sen.geometry) AS geometry

      FROM senates AS sen

     WHERE sen.state = '${params['state']}'
       AND sen.year = '${params['year']}'
       AND sen.name = '${params['senate']}'
  `
}

const congressionalsListSQL = params => {
  return `
    SELECT 'states/${params['state']}' AS state,
           '${params['year']}' AS year,
           con.name AS name,
           ST_AsGeoJSON(con.geometry) AS geometry

      FROM congressionals AS con

         ${(params['ward']) ? `
           JOIN wards AS wrd
           ON con.name = wrd.congressional
         ` : ''}

     WHERE con.state = '${params['state']}'
       AND con.year = '${params['year']}'
     ${(params['within']) ? `
       AND ST_Within(con.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(con.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(con.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['ward']) ? `
       AND wrd.state = '${params['ward']['state']}'
       AND wrd.year = '${params['ward']['year']}'
       AND wrd.name = '${params['ward']['name']}'
     ` : ''}
  `
}

const congressionalsGetSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           '${params['congressional']}' AS name,
           ST_AsGeoJSON(con.geometry) AS geometry

      FROM congressionals AS con

     WHERE con.state = '${params['state']}'
       AND con.year = '${params['year']}'
       AND con.name = '${params['congressional']}'
  `
}

const wardsListSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           wrd.name AS name,
           CONCAT('/states/${params['state']}/counties/', wrd.county) AS county,
           CONCAT('/states/${params['state']}/years/${params['year']}/assemblies/', wrd.assembly) AS assembly,
           CONCAT('/states/${params['state']}/years/${params['year']}/senates/', wrd.senate) AS senate,
           CONCAT('/states/${params['state']}/years/${params['year']}/congressionals/', wrd.congressional) AS congressional,
           ST_AsGeoJSON(wrd.geometry) AS geometry

      FROM wards AS wrd

     WHERE wrd.state = '${params['state']}'
       AND wrd.year = '${params['year']}'
     ${(params['within']) ? `
       AND ST_Within(wrd.geometry, ST_GeomFromGeoJSON('${params['within']}'))
     ` : ''}
     ${(params['intersects']) ? `
       AND ST_Intersects(wrd.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
     ` : ''}
     ${(params['contains']) ? `
       AND ST_Contains(wrd.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
     ` : ''}
     ${(params['county']) ? `
       AND wrd.state = '${params['county']['state']}'
       AND wrd.county = '${params['county']['name']}'
     ` : ''}
     ${(params['assembly']) ? `
       AND wrd.state = '${params['assembly']['state']}'
       AND wrd.year = '${params['assembly']['year']}'
       AND wrd.assembly = '${params['assembly']['name']}'
     ` : ''}
     ${(params['senate']) ? `
       AND wrd.state = '${params['senate']['state']}'
       AND wrd.year = '${params['senate']['year']}'
       AND wrd.senate = '${params['senate']['name']}'
     ` : ''}
     ${(params['congressional']) ? `
       AND wrd.state = '${params['congressional']['state']}'
       AND wrd.year = '${params['congressional']['year']}'
       AND wrd.congressional = '${params['congressional']['name']}'
     ` : ''}
  `
}

const wardsGetSQL = params => {
  return `
    SELECT '/states/${params['state']}' AS state,
           '${params['year']}' AS year,
           '${params['ward']}' AS name,
           CONCAT('/states/${params['state']}/counties/', wrd.county) AS county,
           CONCAT('/states/${params['state']}/years/${params['year']}/assemblies/', wrd.assembly) AS assembly,
           CONCAT('/states/${params['state']}/years/${params['year']}/senates/', wrd.senate) AS senate,
           CONCAT('/states/${params['state']}/years/${params['year']}/congressionals/', wrd.congressional) AS congressional,
           ST_AsGeoJSON(wrd.geometry) AS geometry

      FROM wards AS wrd

     WHERE wrd.state = '${params['state']}'
       AND wrd.year = '${params['year']}'
       AND wrd.name = '${params['ward']}'
  `
}

const votesListSQL = params => {
  switch (params['group']) {
    case 'state':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               '/states/${params['state']}' AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(st.geometry) AS geometry

          FROM (SELECT vt.state AS state,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                 WHERE vt.state = '${params['state']}'
                   AND vt.race = '${params['race']}'
                   AND vt.year = '${params['year']}'

                 GROUP BY vt.state) AS vt
              
               JOIN states AS st
               ON vt.state = st.name

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(st.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(st.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(st.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
      `
    case 'county':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               CONCAT('/states/', cty.state, '/counties/', cty.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(cty.geometry) AS geometry

          FROM (SELECT cty.state AS state,
                       cty.name AS name,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                       JOIN wards AS wrd
                       ON vt.state = wrd.state
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN counties AS cty
                       ON wrd.state = cty.state
                          AND wrd.county = cty.name

                 WHERE vt.state = '${params['state']}'
                   AND vt.race = '${params['race']}'
                   AND vt.year = '${params['year']}'

                 GROUP BY cty.state,
                          cty.name) AS vt
              
               JOIN counties AS cty
               ON vt.state = cty.state
                  AND vt.name = cty.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON cty.name = wrd.county
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['county']) ? `
           AND cty.state = '${params['county']['state']}'
           AND cty.name = '${params['county']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'assembly':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               CONCAT('/states/', asm.state, '/years/', asm.year, '/assemblies/', asm.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(asm.geometry) AS geometry

          FROM (SELECT asm.state AS state,
                       asm.year AS year,
                       asm.name AS name,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                       JOIN wards AS wrd
                       ON vt.state = wrd.state
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN assemblies AS asm
                       ON wrd.state = asm.state
                          AND wrd.year = asm.year
                          AND wrd.assembly = asm.name

                 WHERE vt.state = '${params['state']}'
                   AND vt.race = '${params['race']}'
                   AND vt.year = '${params['year']}'

                 GROUP BY asm.state,
                          asm.year,
                          asm.name) AS vt
              
               JOIN assemblies AS asm
               ON vt.state = asm.state
                  AND vt.year = asm.year
                  AND vt.name = asm.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON asm.name = wrd.assembly
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(asm.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(asm.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(asm.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['assembly']) ? `
           AND asm.state = '${params['assembly']['state']}'
           AND asm.year = '${params['assembly']['year']}'
           AND asm.name = '${params['assembly']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'senate':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               CONCAT('/states/', sen.state, '/years/', sen.year, '/senates/', sen.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(sen.geometry) AS geometry

          FROM (SELECT sen.state AS state,
                       sen.year AS year,
                       sen.name AS name,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                       JOIN wards AS wrd
                       ON vt.state = wrd.state
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN senates AS sen
                       ON wrd.state = sen.state
                          AND wrd.year = sen.year
                          AND wrd.senate = sen.name

                 WHERE vt.state = '${params['state']}'
                   AND vt.race = '${params['race']}'
                   AND vt.year = '${params['year']}'

                 GROUP BY sen.state,
                          sen.year,
                          sen.name) AS vt
              
               JOIN senates AS sen
               ON vt.state = sen.state
                  AND vt.year = sen.year
                  AND vt.name = sen.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON sen.name = wrd.senate
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(sen.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(sen.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(sen.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['senate']) ? `
           AND sen.state = '${params['senate']['state']}'
           AND sen.year = '${params['senate']['year']}'
           AND sen.name = '${params['senate']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'congressional':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               CONCAT('/states/', con.state, '/years/', con.year, '/congressionals/', con.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(con.geometry) AS geometry

          FROM (SELECT con.state AS state,
                       con.year AS year,
                       con.name AS name,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                       JOIN wards AS wrd
                       ON vt.state = wrd.state
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN congressionals AS con
                       ON wrd.state = con.state
                          AND wrd.year = con.year
                          AND wrd.congressional = con.name

                 WHERE vt.state = '${params['state']}'
                   AND vt.race = '${params['race']}'
                   AND vt.year = '${params['year']}'

                 GROUP BY con.state,
                          con.year,
                          con.name) AS vt
              
               JOIN congressionals AS con
               ON vt.state = con.state
                  AND vt.year = con.year
                  AND vt.name = con.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON con.name = wrd.congressional
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(con.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(con.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(con.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['congressional']) ? `
           AND con.state = '${params['congressional']['state']}'
           AND con.year = '${params['congressional']['year']}'
           AND con.name = '${params['congressional']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'ward':
    case null:
    case undefined:
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['race']}' AS race,
               '${params['year']}' AS year,
               CONCAT('/states/', wrd.state, '/years/', wrd.year, '/wards/', wrd.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(wrd.geometry) AS geometry

          FROM votes AS vt
              
               JOIN wards AS wrd
               ON vt.state = wrd.state
                  AND vt.ward_year = wrd.year
                  AND vt.ward = wrd.name

         WHERE vt.state = '${params['state']}'
           AND vt.race = '${params['race']}'
           AND vt.year = '${params['year']}'
         ${(params['within']) ? `
           AND ST_Within(wrd.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(wrd.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(wrd.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['county']) ? `
           AND wrd.state = '${params['county']['state']}'
           AND wrd.county = '${params['county']['name']}'
         ` : ''}
         ${(params['assembly']) ? `
           AND wrd.state = '${params['assembly']['state']}'
           AND wrd.year = '${params['assembly']['year']}'
           AND wrd.assembly = '${params['assembly']['name']}'
         ` : ''}
         ${(params['senate']) ? `
           AND wrd.state = '${params['senate']['state']}'
           AND wrd.year = '${params['senate']['year']}'
           AND wrd.senate = '${params['senate']['name']}'
         ` : ''}
         ${(params['congressional']) ? `
           AND wrd.state = '${params['congressional']['state']}'
           AND wrd.year = '${params['congressional']['year']}'
           AND wrd.congressional = '${params['congressional']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    default:
      throw new Error(`group '${params['group']}' is invalid`)
  }
}

const populationsListSQL = params => {
  switch (params['group']) {
    case 'state':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               '/states/${params['state']}' AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(st.geometry) AS geometry

          FROM (SELECT pop.state AS state,
                       SUM(pop.total) AS total,
                       SUM(pop.white) AS white,
                       SUM(pop.black) AS black,
                       SUM(pop.american_indian) AS american_indian,
                       SUM(pop.asian) AS asian,
                       SUM(pop.pacific_islander) AS pacific_islander,
                       SUM(pop.hispanic) AS hispanic

                  FROM populations AS pop

                 WHERE pop.state = '${params['state']}'
                   AND pop.year = '${params['year']}'

                 GROUP BY pop.state) AS pop
              
               JOIN states AS st
               ON pop.state = st.name

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(st.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(st.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(st.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
      `
    case 'county':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               CONCAT('/states/', cty.state, '/counties/', cty.name) AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(cty.geometry) AS geometry

          FROM (SELECT cty.state AS state,
                       cty.name AS name,
                       SUM(pop.total) AS total,
                       SUM(pop.white) AS white,
                       SUM(pop.black) AS black,
                       SUM(pop.american_indian) AS american_indian,
                       SUM(pop.asian) AS asian,
                       SUM(pop.pacific_islander) AS pacific_islander,
                       SUM(pop.hispanic) AS hispanic

                  FROM populations AS pop

                       JOIN wards AS wrd
                       ON pop.state = wrd.state
                          AND pop.ward_year = wrd.year
                          AND pop.ward = wrd.name

                       JOIN counties AS cty
                       ON wrd.state = cty.state
                          AND wrd.county = cty.name

                 WHERE pop.state = '${params['state']}'
                   AND pop.year = '${params['year']}'

                 GROUP BY cty.state,
                          cty.name) AS pop
              
               JOIN counties AS cty
               ON pop.state = cty.state
                  AND pop.name = cty.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON cty.name = wrd.county
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['county']) ? `
           AND cty.state = '${params['county']['state']}'
           AND cty.name = '${params['county']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'assembly':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               CONCAT('/states/', asm.state, '/years/', asm.year, '/assemblies/', asm.name) AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(asm.geometry) AS geometry

          FROM (SELECT asm.state AS state,
                       asm.year AS year,
                       asm.name AS name,
                       SUM(pop.total) AS total,
                       SUM(pop.white) AS white,
                       SUM(pop.black) AS black,
                       SUM(pop.american_indian) AS american_indian,
                       SUM(pop.asian) AS asian,
                       SUM(pop.pacific_islander) AS pacific_islander,
                       SUM(pop.hispanic) AS hispanic

                  FROM populations AS pop

                       JOIN wards AS wrd
                       ON pop.state = wrd.state
                          AND pop.ward_year = wrd.year
                          AND pop.ward = wrd.name

                       JOIN assemblies AS asm
                       ON wrd.state = asm.state
                          AND wrd.year = asm.year
                          AND wrd.assembly = asm.name

                 WHERE pop.state = '${params['state']}'
                   AND pop.year = '${params['year']}'

                 GROUP BY asm.state,
                          asm.year,
                          asm.name) AS pop
              
               JOIN assemblies AS asm
               ON pop.state = asm.state
                  AND pop.year = asm.year
                  AND pop.name = asm.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON asm.name = wrd.assembly
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(asm.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(asm.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(asm.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['assembly']) ? `
           AND asm.state = '${params['assembly']['state']}'
           AND asm.year = '${params['assembly']['year']}'
           AND asm.name = '${params['assembly']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'senate':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               CONCAT('/states/', sen.state, '/years/', sen.year, '/senates/', sen.name) AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(sen.geometry) AS geometry

          FROM (SELECT sen.state AS state,
                       sen.year AS year,
                       sen.name AS name,
                       SUM(pop.total) AS total,
                       SUM(pop.white) AS white,
                       SUM(pop.black) AS black,
                       SUM(pop.american_indian) AS american_indian,
                       SUM(pop.asian) AS asian,
                       SUM(pop.pacific_islander) AS pacific_islander,
                       SUM(pop.hispanic) AS hispanic

                  FROM populations AS pop

                       JOIN wards AS wrd
                       ON pop.state = wrd.state
                          AND pop.ward_year = wrd.year
                          AND pop.ward = wrd.name

                       JOIN senates AS sen
                       ON wrd.state = sen.state
                          AND wrd.year = sen.year
                          AND wrd.senate = sen.name

                 WHERE pop.state = '${params['state']}'
                   AND pop.year = '${params['year']}'

                 GROUP BY sen.state,
                          sen.year,
                          sen.name) AS pop
              
               JOIN senates AS sen
               ON pop.state = sen.state
                  AND pop.year = sen.year
                  AND pop.name = sen.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON sen.name = wrd.senate
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(sen.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(sen.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(sen.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['senate']) ? `
           AND sen.state = '${params['senate']['state']}'
           AND sen.year = '${params['senate']['year']}'
           AND sen.name = '${params['senate']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'congressional':
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               CONCAT('/states/', con.state, '/years/', con.year, '/congressionals/', con.name) AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(con.geometry) AS geometry

          FROM (SELECT con.state AS state,
                       con.year AS year,
                       con.name AS name,
                       SUM(pop.total) AS total,
                       SUM(pop.white) AS white,
                       SUM(pop.black) AS black,
                       SUM(pop.american_indian) AS american_indian,
                       SUM(pop.asian) AS asian,
                       SUM(pop.pacific_islander) AS pacific_islander,
                       SUM(pop.hispanic) AS hispanic

                  FROM populations AS pop

                       JOIN wards AS wrd
                       ON pop.state = wrd.state
                          AND pop.ward_year = wrd.year
                          AND pop.ward = wrd.name

                       JOIN congressionals AS con
                       ON wrd.state = con.state
                          AND wrd.year = con.year
                          AND wrd.congressional = con.name

                 WHERE pop.state = '${params['state']}'
                   AND pop.year = '${params['year']}'

                 GROUP BY con.state,
                          con.year,
                          con.name) AS pop
              
               JOIN congressionals AS con
               ON pop.state = con.state
                  AND pop.year = con.year
                  AND pop.name = con.name

             ${(params['ward']) ? `
               JOIN wards AS wrd
               ON con.name = wrd.congressional
             ` : ''}

         WHERE TRUE
         ${(params['within']) ? `
           AND ST_Within(con.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(con.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(con.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['congressional']) ? `
           AND con.state = '${params['congressional']['state']}'
           AND con.year = '${params['congressional']['year']}'
           AND con.name = '${params['congressional']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    case 'ward':
    case null:
    case undefined:
      return `
        SELECT '/states/${params['state']}' AS state,
               '${params['year']}' AS year,
               CONCAT('/states/', wrd.state, '/years/', wrd.year, '/wards/', wrd.name) AS group,
               pop.total AS total,
               pop.white AS white,
               pop.black AS black,
               pop.american_indian AS american_indian,
               pop.asian AS asian,
               pop.pacific_islander AS pacific_islander,
               pop.hispanic AS hispanic,
               ST_AsGeoJSON(wrd.geometry) AS geometry

          FROM populations AS pop
              
               JOIN wards AS wrd
               ON pop.state = wrd.state
                  AND pop.ward_year = wrd.year
                  AND pop.ward = wrd.name

         WHERE pop.state = '${params['state']}'
           AND pop.year = '${params['year']}'
         ${(params['within']) ? `
           AND ST_Within(wrd.geometry, ST_GeomFromGeoJSON('${params['within']}'))
         ` : ''}
         ${(params['intersects']) ? `
           AND ST_Intersects(wrd.geometry, ST_GeomFromGeoJSON('${params['intersects']}'))
         ` : ''}
         ${(params['contains']) ? `
           AND ST_Contains(wrd.geometry, ST_GeomFromGeoJSON('${params['contains']}'))
         ` : ''}
         ${(params['county']) ? `
           AND wrd.state = '${params['county']['state']}'
           AND wrd.county = '${params['county']['name']}'
         ` : ''}
         ${(params['assembly']) ? `
           AND wrd.state = '${params['assembly']['state']}'
           AND wrd.year = '${params['assembly']['year']}'
           AND wrd.assembly = '${params['assembly']['name']}'
         ` : ''}
         ${(params['senate']) ? `
           AND wrd.state = '${params['senate']['state']}'
           AND wrd.year = '${params['senate']['year']}'
           AND wrd.senate = '${params['senate']['name']}'
         ` : ''}
         ${(params['congressional']) ? `
           AND wrd.state = '${params['congressional']['state']}'
           AND wrd.year = '${params['congressional']['year']}'
           AND wrd.congressional = '${params['congressional']['name']}'
         ` : ''}
         ${(params['ward']) ? `
           AND wrd.state = '${params['ward']['state']}'
           AND wrd.year = '${params['ward']['year']}'
           AND wrd.name = '${params['ward']['name']}'
         ` : ''}
      `
    default:
      throw new Error(`group '${params['group']}' is invalid`)
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resource Methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const listMethod = (buildSQL, req, res) => {
  const id = uuid()
  logRequest(id, req)
  var sql
  try {
    sql = prettifySQL(buildSQL())
  } catch (error) {
    const body = {
      'error': {
        'code': STATUS_CODE_BAD_REQUEST,
        'message': `Request is malformed: ${error.message}`,
        'status': STATUS_BAD_REQUEST,
      }
    }
    res.status(STATUS_CODE_BAD_REQUEST).json(body)
    logResponse(id, '', res, body)
    return
  }
  POOL.query(sql).then(results => {
    const body = extractFeatures(results.rows)
    res.status(STATUS_CODE_OK).json(body)
    logResponse(id, sql, res, body)
  }).catch(error => {
    const body = {
      'error': {
        'code': STATUS_CODE_INTERNAL_SERVER_ERROR,
        'message': `Unexpected error occurred when listing resources: ${error.message}`,
        'status': STATUS_INTERNAL_SERVER_ERROR,
      }
    }
    res.status(STATUS_CODE_INTERNAL_SERVER_ERROR).json(body)
    logResponse(id, sql, res, body)
  })
}

const getMethod = (buildSQL, req, res) => {
  const id = uuid()
  logRequest(id, req)
  var sql
  try {
    sql = prettifySQL(buildSQL())
  } catch (error) {
    const body = {
      'error': {
        'code': STATUS_CODE_BAD_REQUEST,
        'message': `Request is malformed: ${error.message}`,
        'status': STATUS_BAD_REQUEST,
      }
    }
    res.status(STATUS_CODE_BAD_REQUEST).json(body)
    logResponse(id, '', res, body)
    return
  }
  POOL.query(sql).then(results => {
    if (results.rows.length > 0) {
      const body = extractFeature(results.rows[0])
      res.status(STATUS_CODE_OK).json(body)
      logResponse(id, sql, res, body)
    } else {
      const body = {
        'error': {
          'code': STATUS_CODE_NOT_FOUND,
          'message': `Resource '${req.url}' could not be found`,
          'status': STATUS_NOT_FOUND,
        }
      }
      res.status(STATUS_CODE_NOT_FOUND).json(body)
      logResponse(id, sql, res, body)
    }
  }).catch(error => {
    const body = {
      'error': {
        'code': STATUS_CODE_INTERNAL_SERVER_ERROR,
        'message': `Unexpected error occurred when getting resource '${req.url}': ${error.message}`,
        'status': STATUS_INTERNAL_SERVER_ERROR,
      }
    }
    res.status(STATUS_CODE_INTERNAL_SERVER_ERROR).json(body)
    logResponse(id, sql, res, body)
  })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// App Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const app = express()

app.use(express.json())
app.set('json spaces', 2)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// REST Methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Method: states.list
app.get('/states', (req, res) => {
  listMethod(() => {
    return statesListSQL({
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      county: parseCountyURI(req.query['county']),
      assembly: parseAssemblyURI(req.query['assembly']),
      senate: parseSenateURI(req.query['senate']),
      congressional: parseCongressionalURI(req.query['congressional']),
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: states.get
app.get('/states/:state', (req, res) => {
  getMethod(() => {
    return statesGetSQL({
      state: req.params['state'],
    })
  }, req, res)
})

// Method: counties.list
app.get('/states/:state/counties', (req, res) => {
  listMethod(() => {
    return countiesListSQL({
      state: req.params['state'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: counties.get
app.get('/states/:state/counties/:county', (req, res) => {
  getMethod(() => {
    return countiesGetSQL({
      state: req.params['state'],
      county: req.params['county'],
    })
  }, req, res)
})

// Method: assemblies.list
app.get('/states/:state/years/:year/assemblies', (req, res) => {
  listMethod(() => {
    return assembliesListSQL({
      state: req.params['state'],
      year: req.params['year'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: assemblies.get
app.get('/states/:state/years/:year/assemblies/:assembly', (req, res) => {
  getMethod(() => {
    return assembliesGetSQL({
      state: req.params['state'],
      year: req.params['year'],
      assembly: req.params['assembly'],
    })
  }, req, res)
})

// Method: senates.list
app.get('/states/:state/years/:year/senates', (req, res) => {
  listMethod(() => {
    return senatesListSQL({
      state: req.params['state'],
      year: req.params['year'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: senates.get
app.get('/states/:state/years/:year/senates/:senate', (req, res) => {
  getMethod(() => {
    return senatesGetSQL({
      state: req.params['state'],
      year: req.params['year'],
      senate: req.params['senate'],
    })
  }, req, res)
})

// Method: congressionals.list
app.get('/states/:state/years/:year/congressionals', (req, res) => {
  listMethod(() => {
    return congressionalsListSQL({
      state: req.params['state'],
      year: req.params['year'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: congressionals.get
app.get('/states/:state/years/:year/congressionals/:congressional', (req, res) => {
  getMethod(() => {
    return congressionalsGetSQL({
      state: req.params['state'],
      year: req.params['year'],
      congressional: req.params['congressional'],
    })
  }, req, res)
})

// Method: wards.list
app.get('/states/:state/years/:year/wards', (req, res) => {
  listMethod(() => {
    return wardsListSQL({
      state: req.params['state'],
      year: req.params['year'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      county: parseCountyURI(req.query['county']),
      assembly: parseAssemblyURI(req.query['assembly']),
      senate: parseSenateURI(req.query['senate']),
      congressional: parseCongressionalURI(req.query['congressional']),
    })
  }, req, res)
})

// Method: wards.get
app.get('/states/:state/years/:year/wards/:ward', (req, res) => {
  getMethod(() => {
    return wardsGetSQL({
      state: req.params['state'],
      year: req.params['year'],
      ward: req.params['ward'],
    })
  }, req, res)
})

// Method: votes.list
app.get('/states/:state/races/:race/years/:year/votes', (req, res) => {
  listMethod(() => {
    return votesListSQL({
      state: req.params['state'],
      race: req.params['race'],
      year: req.params['year'],
      group: req.query['group'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      county: parseCountyURI(req.query['county']),
      assembly: parseAssemblyURI(req.query['assembly']),
      senate: parseSenateURI(req.query['senate']),
      congressional: parseCongressionalURI(req.query['congressional']),
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

// Method: populations.list
app.get('/states/:state/years/:year/populations', (req, res) => {
  listMethod(() => {
    return populationsListSQL({
      state: req.params['state'],
      year: req.params['year'],
      group: req.query['group'],
      within: req.query['within'],
      intersects: req.query['intersects'],
      contains: req.query['contains'],
      county: parseCountyURI(req.query['county']),
      assembly: parseAssemblyURI(req.query['assembly']),
      senate: parseSenateURI(req.query['senate']),
      congressional: parseCongressionalURI(req.query['congressional']),
      ward: parseWardURI(req.query['ward']),
    })
  }, req, res)
})

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Connect
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

http.createServer(app).listen(8008)

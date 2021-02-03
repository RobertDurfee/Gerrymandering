const express = require('express')
const http = require('http')
const pg = require('pg')

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// App Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const app = express()

app.use(express.json())
app.set('json spaces', 2)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// REST Methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Method: states.get
app.get('/states/:state', (req, res) => {
  const state = req.params['state']
  POOL.query(`
    SELECT '${state}' AS name,
           ST_AsGeoJSON(st.geometry) AS geometry

      FROM states AS st

     WHERE st.name = '${state}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: counties.get
app.get('/states/:state/counties/:county', (req, res) => {
  const state = req.params['state']
  const county = req.params['county']
  POOL.query(`
    SELECT 'states/${state}' AS state,
           '${county}' AS name,
           ST_AsGeoJSON(cty.geometry) AS geometry

      FROM counties AS cty

     WHERE cty.state = '${state}'
       AND cty.name = '${county}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: assemblies.get
app.get('/states/:state/years/:year/assemblies/:assembly', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const assembly = req.params['assembly']
  POOL.query(`
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${assembly}' AS name,
           ST_AsGeoJSON(asm.geometry) AS geometry

      FROM assemblies AS asm

     WHERE asm.state = '${state}'
       AND asm.year = '${year}'
       AND asm.name = '${assembly}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: senates.get
app.get('/states/:state/years/:year/senates/:senate', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const senate = req.params['senate']
  POOL.query(`
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${senate}' AS name,
           ST_AsGeoJSON(sen.geometry) AS geometry

      FROM senates AS sen

     WHERE sen.state = '${state}'
       AND sen.year = '${year}'
       AND sen.name = '${senate}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: congressionals.get
app.get('/states/:state/years/:year/congressionals/:congressional', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const congressional = req.params['congressional']
  POOL.query(`
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${congressional}' AS name,
           ST_AsGeoJSON(con.geometry) AS geometry

      FROM congressionals AS con

     WHERE con.state = '${state}'
       AND con.year = '${year}'
       AND con.name = '${congressional}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: wards.get
app.get('/states/:state/counties/:county/years/:year/wards/:ward', (req, res) => {
  const state = req.params['state']
  const county = req.params['county']
  const year = req.params['year']
  const ward = req.params['ward']
  POOL.query(`
    SELECT 'states/${state}' AS state,
           'states/${state}/counties/${county}' AS county,
           '${year}' AS year,
           '${ward}' AS name,
           CONCAT('states/${state}/years/${year}/assemblies/', wrd.assembly) AS assembly,
           CONCAT('states/${state}/years/${year}/senates/', wrd.senate) AS senate,
           CONCAT('states/${state}/years/${year}/congressionals/', wrd.congressional) AS congressional,
           ST_AsGeoJSON(wrd.geometry) AS geometry

      FROM wards AS wrd

     WHERE wrd.state = '${state}'
       AND wrd.county = '${county}'
       AND wrd.year = '${year}'
       AND wrd.name = '${ward}';
  `).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: votes.list
app.get('/states/:state/races/:race/years/:year/votes', (req, res) => {
  const state = req.params['state']
  const race = req.params['race']
  const year = req.params['year']
  switch (req.query['group']) {
    case 'state':
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(st.geometry) AS geometry

          FROM (SELECT vt.state AS state,
                       SUM(vt.total) AS total,
                       SUM(vt.democrat) AS democrat,
                       SUM(vt.republican) AS republican

                  FROM votes AS vt

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY vt.state) AS vt
              
               JOIN states AS st
               ON vt.state = st.name;
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    case 'county':
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', cty.state, '/counties/', cty.name) AS county,
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
                          AND vt.county = wrd.county
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN counties AS cty
                       ON wrd.state = cty.state
                          AND wrd.county = cty.name

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY cty.state,
                          cty.name) AS vt
              
               JOIN counties AS cty
               ON vt.state = cty.state
                  AND vt.name = cty.name;
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    case 'assembly':
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', asm.state, '/years/', asm.year, '/assemblies/', asm.name) AS assembly,
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
                          AND vt.county = wrd.county
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN assemblies AS asm
                       ON wrd.state = asm.state
                          AND wrd.year = asm.year
                          AND wrd.assembly = asm.name

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY asm.state,
                          asm.year,
                          asm.name) AS vt
              
               JOIN assemblies AS asm
               ON vt.state = asm.state
                  AND vt.year = asm.year
                  AND vt.name = asm.name;
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    case 'senate':
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', sen.state, '/years/', sen.year, '/senates/', sen.name) AS senate,
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
                          AND vt.county = wrd.county
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN senates AS sen
                       ON wrd.state = sen.state
                          AND wrd.year = sen.year
                          AND wrd.senate = sen.name

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY sen.state,
                          sen.year,
                          sen.name) AS vt
              
               JOIN senates AS sen
               ON vt.state = sen.state
                  AND vt.year = sen.year
                  AND vt.name = sen.name;
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    case 'congressional':
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', con.state, '/years/', con.year, '/congressionals/', con.name) AS congressional,
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
                          AND vt.county = wrd.county
                          AND vt.ward_year = wrd.year
                          AND vt.ward = wrd.name

                       JOIN congressionals AS con
                       ON wrd.state = con.state
                          AND wrd.year = con.year
                          AND wrd.congressional = con.name

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY con.state,
                          con.year,
                          con.name) AS vt
              
               JOIN congressionals AS con
               ON vt.state = con.state
                  AND vt.year = con.year
                  AND vt.name = con.name;
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    case 'ward':
    default:
      POOL.query(`
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', wrd.state, '/counties/', wrd.county, '/years/', wrd.year, '/wards/', wrd.name) AS ward,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(wrd.geometry) AS geometry

          FROM votes AS vt
              
               JOIN wards AS wrd
               ON vt.state = wrd.state
                  AND vt.county = wrd.county
                  AND vt.ward_year = wrd.year
                  AND vt.ward = wrd.name

         WHERE vt.state = '${state}'
           AND vt.race = '${race}'
           AND vt.year = '${year}';
      `).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
  }
})

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Connect
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

http.createServer(app).listen(8008)

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
// Helper Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const parseCountyURI = ward => {
  if (ward) {
    const match = ward.match(/^states\/(?<state>[^\/]*)\/counties\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    }
  }
}

const parseAssemblyURI = ward => {
  if (ward) {
    const match = ward.match(/^states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/assemblies\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    }
  }
}

const parseSenateURI = ward => {
  if (ward) {
    const match = ward.match(/^states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/senates\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    }
  }
}

const parseCongressionalURI = ward => {
  if (ward) {
    const match = ward.match(/^states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/congressionals\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    }
  }
}

const parseWardURI = ward => {
  if (ward) {
    const match = ward.match(/^states\/(?<state>[^\/]*)\/years\/(?<year>[^\/]*)\/wards\/(?<name>[^\/]*)$/)
    if (match) {
      return match.groups
    }
  }
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
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const county = parseCountyURI(req.query['county'])
  const assembly = parseAssemblyURI(req.query['assembly'])
  const senate = parseSenateURI(req.query['senate'])
  const congressional = parseCongressionalURI(req.query['congressional'])
  const ward = parseWardURI(req.query['ward'])
  const query = `
    SELECT st.name AS name,
           ST_AsGeoJSON(st.geometry) AS geometry

      FROM states AS st

         ${(county) ? `
           JOIN counties AS cty
           ON st.name = cty.state
         ` : ''}

         ${(assembly) ? `
           JOIN assemblies AS asm
           ON st.name = asm.state
         ` : ''}

         ${(senate) ? `
           JOIN senates AS sen
           ON st.name = sen.state
         ` : ''}

         ${(congressional) ? `
           JOIN congressionals AS con
           ON st.name = con.state
         ` : ''}

         ${(ward) ? `
           JOIN wards AS wrd
           ON st.name = wrd.state
         ` : ''}

     WHERE TRUE
     ${(within) ? `
       AND ST_Within(st.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(st.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(st.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(county) ? `
       AND cty.state = '${county['state']}'
       AND cty.name = '${county['name']}'
     ` : ''}
     ${(assembly) ? `
       AND asm.state = '${assembly['state']}'
       AND asm.year = '${assembly['year']}'
       AND asm.name = '${assembly['name']}'
     ` : ''}
     ${(senate) ? `
       AND sen.state = '${senate['state']}'
       AND sen.year = '${senate['year']}'
       AND sen.name = '${senate['name']}'
     ` : ''}
     ${(congressional) ? `
       AND con.state = '${congressional['state']}'
       AND con.year = '${congressional['year']}'
       AND con.name = '${congressional['name']}'
     ` : ''}
     ${(ward) ? `
       AND wrd.state = '${ward['state']}'
       AND wrd.year = '${ward['year']}'
       AND wrd.name = '${ward['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: states.get
app.get('/states/:state', (req, res) => {
  const state = req.params['state']
  const query = `
    SELECT '${state}' AS name,
           ST_AsGeoJSON(st.geometry) AS geometry

      FROM states AS st

     WHERE st.name = '${state}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: counties.list
app.get('/states/:state/counties', (req, res) => {
  const state = req.params['state']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const ward = parseWardURI(req.query['ward'])
  const query = `
    SELECT 'states/${state}' AS state,
           cty.name AS name,
           ST_AsGeoJSON(cty.geometry) AS geometry

      FROM counties AS cty

         ${(ward) ? `
           JOIN wards AS wrd
           ON cty.name = wrd.county
         ` : ''}

     WHERE cty.state = '${state}'
     ${(within) ? `
       AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(ward) ? `
       AND wrd.state = '${ward['state']}'
       AND wrd.year = '${ward['year']}'
       AND wrd.name = '${ward['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: counties.get
app.get('/states/:state/counties/:county', (req, res) => {
  const state = req.params['state']
  const county = req.params['county']
  const query = `
    SELECT 'states/${state}' AS state,
           '${county}' AS name,
           ST_AsGeoJSON(cty.geometry) AS geometry

      FROM counties AS cty

     WHERE cty.state = '${state}'
       AND cty.name = '${county}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: assemblies.list
app.get('/states/:state/years/:year/assemblies', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const ward = parseWardURI(req.query['ward'])
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           asm.name AS name,
           ST_AsGeoJSON(asm.geometry) AS geometry

      FROM assemblies AS asm

         ${(ward) ? `
           JOIN wards AS wrd
           ON asm.name = wrd.assembly
         ` : ''}

     WHERE asm.state = '${state}'
       AND asm.year = '${year}'
     ${(within) ? `
       AND ST_Within(asm.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(asm.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(asm.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(ward) ? `
       AND wrd.state = '${ward['state']}'
       AND wrd.year = '${ward['year']}'
       AND wrd.name = '${ward['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
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
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${assembly}' AS name,
           ST_AsGeoJSON(asm.geometry) AS geometry

      FROM assemblies AS asm

     WHERE asm.state = '${state}'
       AND asm.year = '${year}'
       AND asm.name = '${assembly}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: senates.list
app.get('/states/:state/years/:year/senates', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const ward = parseWardURI(req.query['ward'])
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           sen.name AS name,
           ST_AsGeoJSON(sen.geometry) AS geometry

      FROM senates AS sen

         ${(ward) ? `
           JOIN wards AS wrd
           ON sen.name = wrd.senate
         ` : ''}

     WHERE sen.state = '${state}'
       AND sen.year = '${year}'
     ${(within) ? `
       AND ST_Within(sen.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(sen.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(sen.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(ward) ? `
       AND wrd.state = '${ward['state']}'
       AND wrd.year = '${ward['year']}'
       AND wrd.name = '${ward['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
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
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${senate}' AS name,
           ST_AsGeoJSON(sen.geometry) AS geometry

      FROM senates AS sen

     WHERE sen.state = '${state}'
       AND sen.year = '${year}'
       AND sen.name = '${senate}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: congressionals.list
app.get('/states/:state/years/:year/congressionals', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const ward = parseWardURI(req.query['ward'])
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           con.name AS name,
           ST_AsGeoJSON(con.geometry) AS geometry

      FROM congressionals AS con

         ${(ward) ? `
           JOIN wards AS wrd
           ON con.name = wrd.congressional
         ` : ''}

     WHERE con.state = '${state}'
       AND con.year = '${year}'
     ${(within) ? `
       AND ST_Within(con.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(con.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(con.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(ward) ? `
       AND wrd.state = '${ward['state']}'
       AND wrd.year = '${ward['year']}'
       AND wrd.name = '${ward['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
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
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${congressional}' AS name,
           ST_AsGeoJSON(con.geometry) AS geometry

      FROM congressionals AS con

     WHERE con.state = '${state}'
       AND con.year = '${year}'
       AND con.name = '${congressional}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: wards.list
app.get('/states/:state/years/:year/wards', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const county = parseCountyURI(req.query['county'])
  const assembly = parseAssemblyURI(req.query['assembly'])
  const senate = parseSenateURI(req.query['senate'])
  const congressional = parseCongressionalURI(req.query['congressional'])
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           wrd.name AS name,
           CONCAT('states/${state}/counties/', wrd.county) AS county,
           CONCAT('states/${state}/years/${year}/assemblies/', wrd.assembly) AS assembly,
           CONCAT('states/${state}/years/${year}/senates/', wrd.senate) AS senate,
           CONCAT('states/${state}/years/${year}/congressionals/', wrd.congressional) AS congressional,
           ST_AsGeoJSON(wrd.geometry) AS geometry

      FROM wards AS wrd

     WHERE wrd.state = '${state}'
       AND wrd.year = '${year}'
     ${(within) ? `
       AND ST_Within(wrd.geometry, ST_GeomFromGeoJSON('${within}'))
     ` : ''}
     ${(intersects) ? `
       AND ST_Intersects(wrd.geometry, ST_GeomFromGeoJSON('${intersects}'))
     ` : ''}
     ${(contains) ? `
       AND ST_Contains(wrd.geometry, ST_GeomFromGeoJSON('${contains}'))
     ` : ''}
     ${(county) ? `
       AND wrd.state = '${county['state']}'
       AND wrd.county = '${county['name']}'
     ` : ''}
     ${(assembly) ? `
       AND wrd.state = '${assembly['state']}'
       AND wrd.year = '${assembly['year']}'
       AND wrd.assembly = '${assembly['name']}'
     ` : ''}
     ${(senate) ? `
       AND wrd.state = '${senate['state']}'
       AND wrd.year = '${senate['year']}'
       AND wrd.senate = '${senate['name']}'
     ` : ''}
     ${(congressional) ? `
       AND wrd.state = '${congressional['state']}'
       AND wrd.year = '${congressional['year']}'
       AND wrd.congressional = '${congressional['name']}'
     ` : ''}
  `
  // console.log(query)
  POOL.query(query).then(results => {
    res.status(200).json(results.rows)
  }).catch(error => {
    res.status(500).json({ "error": error })
  })
})

// Method: wards.get
app.get('/states/:state/years/:year/wards/:ward', (req, res) => {
  const state = req.params['state']
  const year = req.params['year']
  const ward = req.params['ward']
  const query = `
    SELECT 'states/${state}' AS state,
           '${year}' AS year,
           '${ward}' AS name,
           CONCAT('states/${state}/counties/', wrd.county) AS county,
           CONCAT('states/${state}/years/${year}/assemblies/', wrd.assembly) AS assembly,
           CONCAT('states/${state}/years/${year}/senates/', wrd.senate) AS senate,
           CONCAT('states/${state}/years/${year}/congressionals/', wrd.congressional) AS congressional,
           ST_AsGeoJSON(wrd.geometry) AS geometry

      FROM wards AS wrd

     WHERE wrd.state = '${state}'
       AND wrd.year = '${year}'
       AND wrd.name = '${ward}'
  `
  // console.log(query)
  POOL.query(query).then(results => {
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
  const group = req.query['group']
  const within = req.query['within']
  const intersects = req.query['intersects']
  const contains = req.query['contains']
  const county = parseCountyURI(req.query['county'])
  const assembly = parseAssemblyURI(req.query['assembly'])
  const senate = parseSenateURI(req.query['senate'])
  const congressional = parseCongressionalURI(req.query['congressional'])
  const ward = parseWardURI(req.query['ward'])
  switch (group) {
    case 'state': {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               'states/${state}' AS group,
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
               ON vt.state = st.name

         WHERE TRUE
         ${(within) ? `
           AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    } case 'county': {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', cty.state, '/counties/', cty.name) AS group,
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

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY cty.state,
                          cty.name) AS vt
              
               JOIN counties AS cty
               ON vt.state = cty.state
                  AND vt.name = cty.name

             ${(ward) ? `
               JOIN wards AS wrd
               ON cty.name = wrd.county
             ` : ''}

         WHERE TRUE
         ${(within) ? `
           AND ST_Within(cty.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(cty.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(cty.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
         ${(county) ? `
           AND cty.state = '${county['state']}'
           AND cty.name = '${county['name']}'
         ` : ''}
         ${(ward) ? `
           AND wrd.state = '${ward['state']}'
           AND wrd.year = '${ward['year']}'
           AND wrd.name = '${ward['name']}'
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    } case 'assembly': {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', asm.state, '/years/', asm.year, '/assemblies/', asm.name) AS group,
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

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY asm.state,
                          asm.year,
                          asm.name) AS vt
              
               JOIN assemblies AS asm
               ON vt.state = asm.state
                  AND vt.year = asm.year
                  AND vt.name = asm.name

             ${(ward) ? `
               JOIN wards AS wrd
               ON asm.name = wrd.assembly
             ` : ''}

         WHERE TRUE
         ${(within) ? `
           AND ST_Within(asm.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(asm.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(asm.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
         ${(assembly) ? `
           AND asm.state = '${assembly['state']}'
           AND asm.year = '${assembly['year']}'
           AND asm.name = '${assembly['name']}'
         ` : ''}
         ${(ward) ? `
           AND wrd.state = '${ward['state']}'
           AND wrd.year = '${ward['year']}'
           AND wrd.name = '${ward['name']}'
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    } case 'senate': {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', sen.state, '/years/', sen.year, '/senates/', sen.name) AS group,
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

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY sen.state,
                          sen.year,
                          sen.name) AS vt
              
               JOIN senates AS sen
               ON vt.state = sen.state
                  AND vt.year = sen.year
                  AND vt.name = sen.name

             ${(ward) ? `
               JOIN wards AS wrd
               ON sen.name = wrd.senate
             ` : ''}

         WHERE TRUE
         ${(within) ? `
           AND ST_Within(sen.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(sen.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(sen.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
         ${(senate) ? `
           AND sen.state = '${senate['state']}'
           AND sen.year = '${senate['year']}'
           AND sen.name = '${senate['name']}'
         ` : ''}
         ${(ward) ? `
           AND wrd.state = '${ward['state']}'
           AND wrd.year = '${ward['year']}'
           AND wrd.name = '${ward['name']}'
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    } case 'congressional': {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', con.state, '/years/', con.year, '/congressionals/', con.name) AS group,
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

                 WHERE vt.state = '${state}'
                   AND vt.race = '${race}'
                   AND vt.year = '${year}'

                 GROUP BY con.state,
                          con.year,
                          con.name) AS vt
              
               JOIN congressionals AS con
               ON vt.state = con.state
                  AND vt.year = con.year
                  AND vt.name = con.name

             ${(ward) ? `
               JOIN wards AS wrd
               ON con.name = wrd.congressional
             ` : ''}

         WHERE TRUE
         ${(within) ? `
           AND ST_Within(con.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(con.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(con.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
         ${(congressional) ? `
           AND con.state = '${congressional['state']}'
           AND con.year = '${congressional['year']}'
           AND con.name = '${congressional['name']}'
         ` : ''}
         ${(ward) ? `
           AND wrd.state = '${ward['state']}'
           AND wrd.year = '${ward['year']}'
           AND wrd.name = '${ward['name']}'
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    } case 'ward':
      default: {
      const query = `
        SELECT 'states/${state}' AS state,
               '${race}' AS race,
               '${year}' AS year,
               CONCAT('states/', wrd.state, '/years/', wrd.year, '/wards/', wrd.name) AS group,
               vt.total AS total,
               vt.democrat AS democrat,
               vt.republican AS republican,
               ST_AsGeoJSON(wrd.geometry) AS geometry

          FROM votes AS vt
              
               JOIN wards AS wrd
               ON vt.state = wrd.state
                  AND vt.ward_year = wrd.year
                  AND vt.ward = wrd.name

         WHERE vt.state = '${state}'
           AND vt.race = '${race}'
           AND vt.year = '${year}'
         ${(within) ? `
           AND ST_Within(wrd.geometry, ST_GeomFromGeoJSON('${within}'))
         ` : ''}
         ${(intersects) ? `
           AND ST_Intersects(wrd.geometry, ST_GeomFromGeoJSON('${intersects}'))
         ` : ''}
         ${(contains) ? `
           AND ST_Contains(wrd.geometry, ST_GeomFromGeoJSON('${contains}'))
         ` : ''}
         ${(county) ? `
           AND wrd.state = '${county['state']}'
           AND wrd.county = '${county['name']}'
         ` : ''}
         ${(assembly) ? `
           AND wrd.state = '${assembly['state']}'
           AND wrd.year = '${assembly['year']}'
           AND wrd.assembly = '${assembly['name']}'
         ` : ''}
         ${(senate) ? `
           AND wrd.state = '${senate['state']}'
           AND wrd.year = '${senate['year']}'
           AND wrd.senate = '${senate['name']}'
         ` : ''}
         ${(congressional) ? `
           AND wrd.state = '${congressional['state']}'
           AND wrd.year = '${congressional['year']}'
           AND wrd.congressional = '${congressional['name']}'
         ` : ''}
         ${(ward) ? `
           AND wrd.state = '${ward['state']}'
           AND wrd.year = '${ward['year']}'
           AND wrd.name = '${ward['name']}'
         ` : ''}
      `
      // console.log(query)
      POOL.query(query).then(results => {
        res.status(200).json(results.rows)
      }).catch(error => {
        res.status(500).json({ "error": error })
      })
      break
    }
  }
})

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Connect
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

http.createServer(app).listen(8008)

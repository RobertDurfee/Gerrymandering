const express = require('express')
const http = require('http')
const mongo = require('mongodb')
const MongoClient = mongo.MongoClient
const uuid = require('uuid').v4

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Global Constants
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MONGO_URL = 'mongodb://localhost:27017'
const GM_DB = 'gmDB'
const STATES = 'states'
const COUNTIES = 'counties'
const ASSEMBLY_DISTRICTS = 'assemblyDistricts'
const SENATE_DISTRICTS = 'senateDistricts'
const CONGRESSIONAL_DISTRICTS = 'congressionalDistricts'
const WARDS = 'wards'
const ELECTIONS = 'elections'
const DEMOGRAPHICS = 'demographics'
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
// Global Variables
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var gmDB

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Logging Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const parseQuery = queryParameters => {
  var query = {}
  for (const [key, value] of Object.entries(queryParameters)) {
    switch (key) {
      case 'within': 
      case 'intersects':
        try {
          query = { [key]: JSON.parse(value), ...query }
        } catch (error) {
          query = { [key]: value, ...query }
        }
        break
      default:
        query = { [key]: value, ...query }
        break
    }
  }
  return query
}

const logRequest = (id, req) => {
  console.dir({
    'id': id,
    'request': {
      'method': req['method'],
      'url': req['url'],
      'params': req['params'],
      'query': parseQuery(req['query']),
      'body': req['body']
    }
  }, LOG_OPTIONS)
}

const logResponse = (id, res, body) => {
  console.dir({
    'id': id,
    'response': {
      'statusCode': res['statusCode'],
      'statusMessage': res['statusMessage'],
      'body': body
    }
  }, LOG_OPTIONS)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const buildQuery = queryParameters => {
  var query = {}
  for (const [key, value] of Object.entries(queryParameters)) {
    switch (key) {
      case 'within':
        query = { 'geometry': { '$geoWithin': { '$geometry': JSON.parse(value) } }, ...query }
        break
      case 'intersects':
        query = { 'geometry': { '$geoIntersects': { '$geometry': JSON.parse(value) } }, ...query }
        break
      default:
        query = { [key]: value, ...query }
        break
    }
  }
  return query
}

const getMethod = (collection, req, res, schema) => {
  const id = uuid()
  logRequest(id, req)
  var resourceId
  try {
    resourceId = mongo.ObjectId(req.params['resourceId'])
  } catch (error) {
    const body = {
      'error': {
        'code': STATUS_CODE_BAD_REQUEST,
        'message': `Resource ID ${req.params['resourceId']} is malformed: ${error.message}`,
        'status': STATUS_BAD_REQUEST,
      }
    }
    res.status(STATUS_CODE_BAD_REQUEST).json(body)
    logResponse(id, res, body)
    return
  }
  gmDB.collection(collection).findOne({
    '_id': resourceId,
  }).then(result => {
    if (result) {
      const body = schema(result)
      res.status(STATUS_CODE_OK).json(body)
      logResponse(id, res, body)
    } else {
      const body = {
        'error': {
          'code': STATUS_CODE_NOT_FOUND,
          'message': `Resource ${req.params['resourceId']} was not found.`,
          'status': STATUS_NOT_FOUND,
        }
      }
      res.status(STATUS_CODE_NOT_FOUND).json(body)
      logResponse(id, res, body)
    }
  }).catch(error => {
    const body = {
      'error': {
        'code': STATUS_CODE_INTERNAL_SERVER_ERROR,
        'message': `Unexpected error occured when getting resource ${req.params['resourceId']}: ${error.message}`,
        'status': STATUS_INTERNAL_SERVER_ERROR,
      }
    }
    res.status(STATUS_CODE_SERVER_ERROR).json(body)
    logResponse(id, res, body)
  })
}

const listMethod = (collection, req, res, schema) => {
  const id = uuid()
  logRequest(id, req)
  var query
  try {
    query = buildQuery(req.query)
  } catch (error) {
    const body = {
      'error': {
        'code': STATUS_CODE_BAD_REQUEST,
        'message': `Query is malformed: ${error.message}`,
        'status': STATUS_BAD_REQUEST,
      }
    }
    res.status(STATUS_CODE_BAD_REQUEST).json(body)
    logResponse(id, res, body)
    return
  }
  gmDB.collection(collection).find(query).toArray().then(results => {
    const body = {
      'items': results.map(schema)
    }
    res.status(STATUS_CODE_OK).json(body)
    logResponse(id, res, body)
  }).catch(error => {
    const body = {
      'error': {
        'code': STATUS_CODE_INTERNAL_SERVER_ERROR,
        'message': `Unexpected error occurred when listing resources: ${error.message}`,
        'status': STATUS_INTERNAL_SERVER_ERROR,
      }
    }
    res.status(STATUS_CODE_INTERNAL_SERVER_ERROR).json(body)
    logResponse(id, res, body)
  })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// App Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const app = express()

app.use(express.json())
app.set('json spaces', 2)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// REST Resource Schemas
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const stateSchema = result => {
  return {
    'id': `${result['_id']}`,
    'name': `${result['name']}`,
    'geometry': `${result['geometry']}`,
  }
}

const countySchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'name': `${result['name']}`,
    'geometry': `${result['geometry']}`,
  }
}

const assemblyDistrictSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'name': `${result['name']}`,
    'geometry': `${result['geometry']}`,
  }
}

const senateDistrictSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'name': `${result['name']}`,
    'geometry': `${result['geometry']}`,
  }
}

const congressionalDistrictSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'name': `${result['name']}`,
    'geometry': `${result['geometry']}`,
  }
}

const wardSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'assemblyDistrict': `${result['assemblyDistrict']}`,
    'senateDistrict': `${result['senateDistrict']}`,
    'congressionalDistrict': `${result['congressionalDistrict']}`,
    'geometry': `${result['geometry']}`,
  }
}

const electionSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'race': `${result['race']}`,
    'ward': `${result['ward']}`,
    'total': `${result['total']}`,
    'democrat': `${result['democrat']}`,
    'republican': `${result['republican']}`,
    'green': `${result['green']}`,
    'libertarian': `${result['libertarian']}`,
    'constitution': `${result['constitution']}`,
    'independent': `${result['independent']}`,
    'scatter': `${result['scatter']}`,
  }
}

const demographicSchema = result => {
  return {
    'id': `${result['_id']}`,
    'state': `${result['state']}`,
    'year': `${result['year']}`,
    'ward': `${result['ward']}`,
    'total': {
      'total': `${result['total']['total']}`,
      'adult': `${result['total']['adult']}`,
    },
    'white': {
      'total': `${result['white']['total']}`,
      'adult': `${result['white']['adult']}`,
    },
    'black': {
      'total': `${result['black']['total']}`,
      'adult': `${result['black']['adult']}`,
    },
    'americanIndian': {
      'total': `${result['americanIndian']['total']}`,
      'adult': `${result['americanIndian']['adult']}`,
    },
    'asian': {
      'total': `${result['asian']['total']}`,
      'adult': `${result['asian']['adult']}`,
    },
    'pacificIslander': {
      'total': `${result['pacificIslander']['total']}`,
      'adult': `${result['pacificIslander']['adult']}`,
    },
    'hispanic': {
      'total': `${result['hispanic']['total']}`,
      'adult': `${result['hispanic']['adult']}`,
    },
    'other': {
      'total': `${result['other']['total']}`,
      'adult': `${result['other']['adult']}`,
    },
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// REST Methods
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Method: states.get
app.get('/states/:resourceId', (req, res) => {
  getMethod(STATES, req, res, stateSchema)
})

// Method: states.list
app.get('/states', (req, res) => {
  listMethod(STATES, req, res, stateSchema)
})

// Method: counties.get
app.get('/counties/:resourceId', (req, res) => {
  getMethod(COUNTIES, req, res, countySchema)
})

// Method: counties.list
app.get('/counties', (req, res) => {
  listMethod(COUNTIES, req, res, countySchema)
})

// Method: assemblyDistricts.get
app.get('/assemblyDistricts/:resourceId', (req, res) => {
  getMethod(ASSEMBLY_DISTRICTS, req, res, assemblyDistrictSchema)
})

// Method: assemblyDistricts.list
app.get('/assemblyDistricts', (req, res) => {
  listMethod(ASSEMBLY_DISTRICTS, req, res, assemblyDistrictSchema)
})

// Method: senateDistricts.get
app.get('/senateDistricts/:resourceId', (req, res) => {
  getMethod(SENATE_DISTRICTS, req, res, senateDistrictSchema)
})

// Method: senateDistricts.list
app.get('/senateDistricts', (req, res) => {
  listMethod(SENATE_DISTRICTS, req, res, senateDistrictSchema)
})

// Method: congressionalDistricts.get
app.get('/congressionalDistricts/:resourceId', (req, res) => {
  getMethod(CONGRESSIONAL_DISTRICTS, req, res, congressionalDistrictSchema)
})

// Method: congressionalDistricts.list
app.get('/congressionalDistricts', (req, res) => {
  listMethod(CONGRESSIONAL_DISTRICTS, req, res, congressionalDistrictSchema)
})

// Method: wards.get
app.get('/wards/:resourceId', (req, res) => {
  getMethod(WARDS, req, res, wardSchema)
})

// Method: wards.list
app.get('/wards', (req, res) => {
  listMethod(WARDS, req, res, wardSchema)
})

// Method: elections.get
app.get('/elections/:resourceId', (req, res) => {
  getMethod(ELECTIONS, req, res, electionSchema)
})

// Method: elections.list
app.get('/elections', (req, res) => {
  listMethod(ELECTIONS, req, res, electionSchema)
})

// Method: demographics.get
app.get('/demographics/:resourceId', (req, res) => {
  getMethod(DEMOGRAPHICS, req, res, demographicSchema)
})

// Method: demographics.list
app.get('/demographics', (req, res) => {
  listMethod(DEMOGRAPHICS, req, res, demographicSchema)
})

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Connect
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

MongoClient.connect(MONGO_URL, { useUnifiedTopology: true }).then(client => {
    gmDB = client.db(GM_DB)
    http.createServer(app).listen(8008)
}).catch(error => {
    console.error(error)
    process.exit(1)
})

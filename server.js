// server.js
// where your node app starts

// init project
const express = require('express');
const process = require('process');
const app = express();

// http://expressjs.com/en/starter/static-files.html
app.use(express.static('public'));

// reference files needed for our Cassandra database
const database = require('./cassandra');
const connectDatabase = require('./connect-database');

const dateOptions = { 
  weekday: 'long', 
  year: 'numeric', 
  month: 'long', 
  day: 'numeric', 
  hour: 'numeric', 
  minute: 'numeric', 
  second: 'numeric', 
  timeZoneName: 'short' 
};

// Simple in-memory store to hold on to example
// table ids for reference later
const dataStore = [{
    type: 'string', 
    example: 'Initializing database'
  }];

/** 
 * Initialize the connection to our Apollo or DataStax Enterprise cluster
 * and display the elapsed time of initialization
 */
const start = process.hrtime();
connectDatabase.initCaasConnection()
//connectDatabase.initDseConnection()
  .then(result => {
    const hrtime = process.hrtime(start);
    const elapsed = (hrtime[0] + (hrtime[1] / 1e9)).toFixed(3);
    console.info(`Completed in ${elapsed}s.`);
  
    message = result + ' in ' + elapsed + 's';
    dataStore.push({type: 'string', example:message});
  })
  /**.then(() => {
    console.log('Creating example keyspace');
    database.createKeyspace();
  })*/
  .then(() => {
    console.log('Creating example table');
    database.createTable();
  })
  .catch(err => console.error('Error at table initialization', err));


/**
 * Setup some basic routes
 * http://expressjs.com/en/starter/basic-routing.html
 */
app.get('/', function(request, response) {  
  response.sendFile(__dirname + '/views/index.html')
});

app.get('/dreams', function(request, response) {
  const dataStoreSize = dataStore.length;
  const last = dataStoreSize - 1;
  console.debug('GET dreams are: %d', dataStoreSize);
  
  if (dataStoreSize > 0) {
    if (dataStore[last].type == 'data') {
      database.getExampleDataById1AndId2(dataStore[last].id1, dataStore[last].id2)
      .then(example => {
        const row = example.first();
        const id1 = row['id1']; // Uuid
        const id2 = row['id2'].getDate(); // pulls date from TimeUUID
        const txt = row['txt']; // Text/String value
        const val = row['val']; // integer
        console.log('Retrieved row: %s, %s, %s, %d', id1, id2, txt, val);

        const exampleRow = [txt + " @ " + id2];
        console.log('Example is %s', exampleRow);

      })
      .catch(err => console.error('Error at /dreams GET', err));
      
    }
    response.send(dataStore).reverse;

  } else {
    response.sendStatus(200);
  }
  
});

// could also use the POST body instead of query string: http://expressjs.com/en/api.html#req.body
app.post("/dreams", function (request, response) {
  console.log('POST request is: %s', request.query.dream);
  
  let dreamRequest = request.query.dream;
  let id1; let id2;
  
  // Do insert here
  database.insertData(dreamRequest)
  .then(resultIds => {
    id1 = resultIds[0];
    id2 = resultIds[1];
    
    const dateString = id2.getDate().toLocaleDateString("en-US", dateOptions);
    const exampleRow = [dreamRequest + " @ " + dateString];
    dataStore.push({type: 'data', id1: id1, id2: id2, txt: dreamRequest, example: exampleRow});
    let dataStoreSize = dataStore.length;
    
    console.debug('dataStore push is: ' + dataStore[dataStoreSize - 1].id1)
    response.send(dataStore[dataStoreSize - 1]);
    //response.sendStatus(200);
  })
  .catch(err => console.error('Error at /dreams POST', err));
});


// listen for requests :)
const listener = app.listen(process.env.PORT, function() {
  console.log('Your app is listening on port ' + listener.address().port);
});
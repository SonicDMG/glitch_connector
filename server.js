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


/** 
 * Initialize the connection to our Apollo or DataStax Enterprise cluster
 * and display the elapsed time of initialization
 */
const start = process.hrtime();
connectDatabase.initCaasConnection()
//connectDatabase.initDseConnection()
  .then(() => {
      const hrtime = process.hrtime(start);
      const elapsed = (hrtime[0] + (hrtime[1] / 1e9)).toFixed(3);
      console.info(`Completed in ${elapsed}s.`);
  });


/**
 * Setup some basic routes
 * http://expressjs.com/en/starter/basic-routing.html
 */
app.get('/', async function(request, response) { 
  await database.createTable()
    .then(result => {
      console.log('createTable is %s', result)})
    .then(() => {
      console.log('Inserting data')
      database.insertData()
      response.sendFile(__dirname + '/views/index.html')})
    .catch(err => console.error('Error at /', err));
  
});

app.get('/example', function(request, response) {
  database.getExampleData()
  .then(example => {
    console.log('Example is %j', example.rows)
    response.send(example.rows)})
  .catch(err => console.log('Error at /example', err));
  
});


// listen for requests :)
const listener = app.listen(process.env.PORT, function() {
  console.log('Your app is listening on port ' + listener.address().port);
});
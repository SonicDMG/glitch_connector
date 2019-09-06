// server.js
// where your node app starts

/**
 * I decided to use babel because who doesn't like using "import" statements???
 * Check out this blog post for a quick tutorial on how to get this working.
 * https://hackernoon.com/using-babel-7-with-node-7e401bc28b04
 */

// init project
import express from 'express';
import process from 'process';

import database from './cassandra';
import { init_caas_connection, init_dse_connection } from './connect-database';

const app = express();

// http://expressjs.com/en/starter/static-files.html
app.use(express.static('public'));

// http://expressjs.com/en/starter/basic-routing.html
app.get('/', function(request, response) {
  database.getMetadata()
    .then(function(result) {
      console.log('Metadata is %s', result);
    
    }, function(err) {
      console.log('Error at /', err);
    })
  
  response.sendFile(__dirname + '/views/index.html');
});

app.get('/video', function(request, response) {
  let video;
  
  database.getVideo()
    .then(function(result) {
      video = result;
      console.log('Video is %s', video.name);
      response.send(video.name);
    
    }, function(err) {
      console.log('Error at /video', err);
    })
});

// listen for requests :)
const listener = app.listen(process.env.PORT, function() {
  console.log('Your app is listening on port ' + listener.address().port);
});

/** 
 * Initialize the connection to our Apollo or DataStax Enterprise cluster
 * and display the elapsed time of initialization
 */
const start = process.hrtime();
init_caas_connection()
//init_dse_connection()
  .then(() => {
      const hrtime = process.hrtime(start);
      const elapsed = (hrtime[0] + (hrtime[1] / 1e9)).toFixed(3);
      console.info(`Completed in ${elapsed}s.`);
  });
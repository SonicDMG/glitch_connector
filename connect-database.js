'use strict';

// Provide rudimentary connection to DSE cluster 
// and default to localQuorum for all queries.
// Using documentation from: 
// https://docs.datastax.com/en/developer/nodejs-driver/4.1/getting-started/
const cassandra = require('cassandra-driver');
//const cassandra = require('dse-driver');

// A singleton client instance to be reused throughout the application
let clientInstance = null;

/**
 * Gets a Cassandra client instance.
 */
module.exports.getCassandraClient = function () {
  if (clientInstance === null) {
    throw new Error('No client instance found. Did you forget to call initCaasConnection OR initDseConnection?');
  }

  return clientInstance;
};

/** 
 * An optimization function that cuts down
 * on the amount of code duplication and focuses
 * connection handling in a single place.
 */
async function establishConnection (client, type) {
  try {
    await client.connect();
    clientInstance = client;
    const numHosts = client.hosts.length;
    const hostKeys = client.hosts.keys();
    
    console.log('Connected to cluster with %d host(s): %j', numHosts, hostKeys);
    return 'Connected to ' + type + ' successfully with ' + numHosts + ' host(s) [' + hostKeys + ']';

    } catch (err) {
      console.error('There was an error when connecting to %s', type, err);
      await client.shutdown().then(() => { throw err; });
    }
}

/**
 * Create a connection to a DSE cluster
 */
module.exports.initDseConnection = async function () {
  
  const client = new cassandra.Client({ 
    contactPoints: [
      process.env.CONTACT_POINT1,
      process.env.CONTACT_POINT2,
      process.env.CONTACT_POINT3
    ], 
    localDataCenter: process.env.DATACENTER_NAME, 
    queryOptions: { consistency: cassandra.types.consistencies.local_quorum }
  });
  
  return await establishConnection(client, 'DataStax Enterprise')
};

/**
 * Create a connection to Apollo
 */
module.exports.initCaasConnection = async function () { 
  // Notice secureConnectBundle below with a parameter of apollo.zip.
  // The zip file is generated from "Connection Details" section of your
  // Apollo cluster and contains all of the artifacts you need to create
  // a secure connection to your cluster. Just download it, add it with 
  // New File -> Upload a File from the left hand menu in Glitch, and then 
  // provide the Glitch generated CDN URL to the file as seen in package.json
  // using wget -O apollo.zip <file URL here>. I renamed it to apollo.zip 
  // with wget for easier reading and access.
  const client = new cassandra.Client({
    cloud: { secureConnectBundle: 'apollo.zip' },
    authProvider: new cassandra.auth.PlainTextAuthProvider(
      process.env.DATABASE_USER, 
      process.env.DATABASE_PASSWORD
    ),
    localDataCenter: process.env.DATACENTER_NAME,
    queryOptions: { consistency: cassandra.types.consistencies.local_quorum },
    keyspace: process.env.KEYSPACE_NAME
  });

  return await establishConnection(client, 'Apollo')
};
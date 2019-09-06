'use strict';

import { Client, types as CassandraTypes } from 'cassandra-driver';
//import { Client, types as CassandraTypes } from 'dse-driver';

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
export function getCassandraClient() {
  if (clientInstance === null) {
    throw new Error('No client instance found. Did you forget to call init_caas_connection OR init_connection?');
  }

  return clientInstance;
};

/** 
 * An optimization function that cuts down
 * on the amount of code duplication and focuses
 * connection handling in a single place.
 */
async function establishConnection (connection, type) {
  try {
    await connection.client.connect();
    console.info('Connected to %s successfully', type);
    clientInstance = connection;
    
    console.log('Connected to cluster with %d host(s): %j', connection.client.hosts.length, connection.client.hosts.keys());

    } catch (err) {
      console.error('There was an error when connecting to %s', type, err);
      await connection.client.shutdown().then(() => { throw err; });

    } finally {
      //await connection.client.shutdown();
    }
}

/**
 * Create a connection to a DSE cluster
 */
module.exports.init_dse_connection = async function () {
  let connection = {};
  
  connection.client = new Client({ 
    contactPoints: [
      process.env.CONTACT_POINT1,
      process.env.CONTACT_POINT2,
      process.env.CONTACT_POINT3
    ], 
    localDataCenter: process.env.DATACENTER_NAME, 
    queryOptions: { consistency: CassandraTypes.consistencies.quorum }
  });
  
  establishConnection(connection, 'DataStax Enterprise');
};

/**
 * Create a connection to Apollo
 */
module.exports.init_caas_connection = async function () {
  let connection = {};
  
  // Notice secureConnectBundle below with a parameter of apollo.zip.
  // The zip file is generated from "Connection Details" section of your
  // Apollo cluster and contains all of the artifacts you need to create
  // a secure connection to your cluster. Just download it, add it with 
  // New File -> Upload a File from the left hand menu in Glitch, and then 
  // provide the Glitch generated CDN URL to the file as seen in package.json
  // using wget -O apollo.zip <file URL here>. I renamed it to apollo.zip 
  // with wget for easier reading and access.
  connection.client = new Client({
    cloud: { secureConnectBundle: 'apollo.zip' },
    authProvider: new cassandra.auth.PlainTextAuthProvider(
      process.env.DATABASE_USER, 
      process.env.DATABASE_PASSWORD
    ),
    queryOptions: { consistency: CassandraTypes.consistencies.quorum },
    keyspace: process.env.KEYSPACE_NAME
  });

  establishConnection(connection, 'Apollo');
};
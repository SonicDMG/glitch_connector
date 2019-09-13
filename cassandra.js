const connectDatabase = require('./connect-database');
const cassandra = require('cassandra-driver');

const Uuid = cassandra.types.Uuid;
const TimeUuid = cassandra.types.TimeUuid;

/** Example query methods **/
const keyspace = process.env.KEYSPACE;
const exampleTable = "meta_tbl1";

// A quick test method to pull keyspace metadata
// We are not using an async function here because
// metadata info is already handled in the background
// via the driver, no need for an async query
module.exports.getMetadata = async function () {
  let tableMetadataResponse;
  let client = connectDatabase.getCassandraClient();
  
  try {
    tableMetadataResponse = client.metadata.keyspaces;
    return Object.keys(tableMetadataResponse);
    
  } catch (err) {
    console.error('Http error in module.exports.getMetadata', err)
  }
};

// Create keyspace defined in .env file via process.env.KEYSPACE
// Apollo automatically creates the keyspace you specifiy
// when creating the cluster. This is really here for
// cases when connecting to an exsiting DSE cluster.
// ** Based off of code found from the DataStax nodejs
// ** driver example github https://github.com/datastax/nodejs-driver/tree/master/examples
module.exports.createKeyspace = async function () {
  let client = connectDatabase.getCassandraClient();
  
  // The replication factor here is only used for development purposes.
  // In any production environment start with a class of 
  // NetworkTopologyStrategy and a replication_factor of 3
  const query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication =" +
  " {'class': 'SimpleStrategy', 'replication_factor': '1' }";
  
  return await client.execute(query)
  .catch(function (err) {
    console.error('There was an error in module.exports.createKeyspace', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Create an example table in the keyspace defined in
// process.env.KEYSPACE variable located in your .env file.
// NOTE: For Apollo users the is the keyspace you were 
// required to create on cluster creation within the console.
// ** Based off of code found from the DataStax nodejs
// ** driver example github https://github.com/datastax/nodejs-driver/tree/master/examples
module.exports.createTable = async function () {
  let client = connectDatabase.getCassandraClient();
  
  const query = "CREATE TABLE IF NOT EXISTS " + keyspace + "." + exampleTable +
    " (id1 uuid, id2 timeuuid, txt text, val int, PRIMARY KEY(id1, id2))";
  
  return await client.execute(query)
  .then(function () {
    return client.metadata.getTable(keyspace, exampleTable);
  })
  .then(function (table) {
    console.log('Table information');
    console.log('- Name: %s', table.name);
    console.log('- Columns:', table.columns);
    console.log('- Partition keys:', table.partitionKeys);
    console.log('- Clustering keys:', table.clusteringKeys);
  })
  .catch(function (err) {
    console.error('There was an error in module.exports.createTable', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Insert some basic data into our example table
// ** Based off of code found from the DataStax nodejs
// ** driver example github https://github.com/datastax/nodejs-driver/tree/master/examples
module.exports.insertData = async function (textValue) {
  let client = connectDatabase.getCassandraClient();
  const id1 = Uuid.random();
  const id2 = TimeUuid.now();
  
  const query = "INSERT INTO " + keyspace + "." + exampleTable + 
    " (id1, id2, txt, val) VALUES (?, ?, ?, ?)";
  
  return await client.execute(query, [id1, id2, textValue, 42], { prepare: true })
  .then(() => { return [id1, id2]; })
  .catch(function (err) {
    console.error('There was an error in module.exports.insertData', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Perform a simple query to pull a row
// from the keyspace and example table
// defined at the top of this file
module.exports.getExampleData = async function () {
  let result;
  let client = connectDatabase.getCassandraClient();
  
  result = await client.execute("select * from " + keyspace + "." + exampleTable + " LIMIT 1")
  const row = result.first();

  return result;
};

// Perform a simple query to pull a partition
// using both id1 and id2 of our composite key
// from the table we created in createTable()
module.exports.getExampleDataById1AndId2 = async function (id1, id2) {
  let result;
  let client = connectDatabase.getCassandraClient();
  
  const query = "SELECT * FROM " + keyspace + "." + exampleTable + " WHERE id1 = ? AND id2 = ?";
  
  result = await client.execute(query, [id1, id2], { prepare: true });
  const row = result.first();

  return result;
};
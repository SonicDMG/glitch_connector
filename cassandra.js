const connectDatabase = require('./connect-database');
const cassandra = require('cassandra-driver');

const Uuid = cassandra.types.Uuid;
const TimeUuid = cassandra.types.TimeUuid;

/** Example query methods **/

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
module.exports.createKeyspace = async function () {
  let client = connectDatabase.getCassandraClient();
  
  // The replication factor here is only used for development purposes.
  // In any production environment start with a class of 
  // NetworkTopologyStrategy and a replication_factor of 3
  const query = "CREATE KEYSPACE IF NOT EXISTS " + process.env.KEYSPACE + " WITH replication =" +
  "{'class': 'SimpleStrategy', 'replication_factor': '1' }";
  
  return await client.execute(query)
  .catch(function (err) {
    console.error('There was an error in module.exports.createSchema', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Create an example table in the keyspace defined in
// process.env.KEYSPACE variable located in your .env file.
// NOTE: For Apollo users the is the keyspace you were 
// required to create on cluster creation within the console.
module.exports.createTable = async function () {
  let client = connectDatabase.getCassandraClient();
  
  const query = "CREATE TABLE IF NOT EXISTS " + process.env.KEYSPACE + ".meta_tbl1 " +
    "(id1 uuid, id2 timeuuid, txt text, val int, PRIMARY KEY(id1, id2))";
  
  return await client.execute(query)
  .then(function () {
    return client.metadata.getTable(process.env.KEYSPACE, 'meta_tbl1');
  })
  .then(function (table) {
    console.log('Table information');
    console.log('- Name: %s', table.name);
    console.log('- Columns:', table.columns);
    console.log('- Partition keys:', table.partitionKeys);
    console.log('- Clustering keys:', table.clusteringKeys);
  })
  .catch(function (err) {
    console.error('There was an error in module.exports.createSchema', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Insert some basic data into our example table
module.exports.insertData = async function () {
  let client = connectDatabase.getCassandraClient();
  const id1 = Uuid.random();
  const id2 = TimeUuid.now();
  
  const query = "INSERT INTO " + process.env.KEYSPACE + ".meta_tbl1 " + 
    "(id1, id2, txt, val) VALUES (?, ?, ?, ?)";
  
  return await client.execute(query, [id1, id2, 'Strata NYC', 42], { prepare: true})
  .then(function () {
    const query = "SELECT * FROM " + process.env.KEYSPACE + ".meta_tbl1 " + "WHERE id1 = ? AND id2 = ?";
    return client.execute(query, [id1, id2], { prepare: true });
  })
  .then(function (result) {
    const test = result.rows[0].id2.getDate();
    const row = result.first();
    console.log('Retrieved row: %s, %s, %s, %d', row['id1'], row['id2'].getDate(), row['txt'], row['val']);
  })
  .catch(function (err) {
    console.error('There was an error in module.exports.insertData', err);
    return client.shutdown().then(() => { throw err; });
  });
};

// Perform a simple query to pull a video
// from the killrvideo.videos table
module.exports.getExampleData = async function () {
  let result;
  let client = connectDatabase.getCassandraClient();
  
  result = await client.execute("select * from " + process.env.KEYSPACE + ".meta_tbl1 LIMIT 5")
  //const video = result.first();

  return result;
};
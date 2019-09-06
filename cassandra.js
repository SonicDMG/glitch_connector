import { getCassandraClient } from './connect-database';

/** Example query methods **/

// A quick test method to pull keyspace metadata
module.exports.getMetadata = async function () {
  let tableMetadataResponse;
  let client = getCassandraClient();
  
  try {
    tableMetadataResponse = await client.client.metadata.keyspaces;
    return Object.keys(tableMetadataResponse);
    
  } catch (err) {
    console.error('Http error in module.exports.getMetadata:', err)
  }
};

// Perform a simple query to pull a video
// from the killrvideo.videos table
module.exports.getVideo = async function () {
  let result;
  let client = getCassandraClient();
  
  try {
    result = await client.client.execute("select * from killrvideo.videos limit 1")
    //Which one?
    let video = result.first();
    //let video = result.rows[0];
    return video;
    
  } catch (err) {
    console.error('Http error in module.exports.getVideo:', err);
  }
};
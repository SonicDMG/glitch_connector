"use strict";

var _cassandraDriver = require("cassandra-driver");

//const cassandra = require('cassandra-driver');
// Provide rudimentary connection to DSE cluster
var client = new _cassandraDriver.Client({
  contactPoints: [process.env.CONTACT_POINTS],
  localDataCenter: process.env.DATACENTER_NAME,
  queryOptions: {
    consistency: types.consistencies.quorum
  }
});
client.connect().then(function () {
  console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
  console.log('Keyspaces: %j', Object.keys(client.metadata.keyspaces));
})["catch"](function (err) {
  console.error('There was an error when connecting', err);
  return client.shutdown().then(function () {
    throw err;
  });
});

module.exports.performQuery = function (call, callback) {
  return new Promise(function (resolve, reject) {
    return client.execute("select * from killrvideo.videos limit 1").then(function (result) {
      var video = result.first();
      resolve(video);
    });
  });
};
/**module.exports = {
  performQuery: performQuery
}*/
//# sourceMappingURL=cassandra.js.map
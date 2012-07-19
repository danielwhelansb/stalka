var batcho = require('batcho');
var nano = require('nano')('http://localhost:5984');

var dummyStore = function(docs) {
  console.log("Storing ", data);
};

var couchStore = function(docs) {
  var alice = nano.use('alice');
  docs.forEach(function(doc) {
    alice.insert(doc, function(err, body, header) {
      if (err) {
        console.log("error", err);
        return;
      } 
      console.log("yay");
    });
  });
};

batcho.start({
    feedDb: 'http://localhost/abcd',
    sequenceDb: 'http://localhost/abcd',
    batchReadSize: 50,
    batchWriteSize: 10,
    store: dummyStore
});

// batcho.resume(); 
// batcho.pause();
// batcho.stop();
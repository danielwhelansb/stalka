module.exports = stalka();

/*!
 * Stalk couch changes and do stuff with them :)
*/

/**
 * Module dependencies.
 */
 var request = require('request');
 var querystring = require('querystring');
 var async = require('async');

/**
 * Constructor
 */
function stalka() {
  return new Stalka();
};

function Stalka() {
};

Stalka.prototype.stop = function() {
  this.running = false;
}

Stalka.prototype.start = function (dbUri, changesWriter, options, mainCallback) {
  this.running = true;

  var db = require('nano')(dbUri);
  var self = this;
  options = options || {};
  var limit = options.limit || 10;

  this.readSequence(db, function(err, sequenceDoc) {
    
    var startSequence = options.since || (sequenceDoc && sequenceDoc.lastSequence) || 0;

    if (err) {
      console.error("Unable to read sequence.", err);
      mainCallback(err);
      return;
    }

    async.whilst(
      function () { return self.running; },
      function (callback) {
        self.readChanges(dbUri, {limit: limit, since: startSequence}, function(err, body) {
          if (err) {
            console.error("Unable to read changes.", err);
            callback(err);
            return;
          }

          // Write a chunk of changes
          var changes = JSON.parse(body);
          changesWriter(changes, function(err) {
            if (err) {
              console.error("Unable to write changes.", err);
              callback(err);
              return;
            }

            // Record the latest sequence
            startSequence = changes.last_seq;
            if (sequenceDoc) {
              sequenceDoc.lastSequence = changes.last_seq;  
            } else {
              sequenceDoc = {lastSequence: changes.last_seq};
            }

            self.updateSequence(db, sequenceDoc, function(err, newSeqDoc) {
              if (err) {
                console.error("Unable to update sequence.", err);
                callback(err);
                return;
              } else {
                sequenceDoc._rev = newSeqDoc.rev;
                callback();
              }
            });
          });
        });
      },
      function (err) {
        if (err) {
          console.error("Failed to stalk ", dbUri, err);
        }
        mainCallback(err);
      }
    );

  });
};

Stalka.prototype.readSequence = function(db, callback) {
  db.get('_local/feed', function(err, body) {
    if (body) {
      callback(null, body);
    } else if (err && err.status_code == 404) {
      callback(null, {lastSequence: 0});
    } else {
      callback(err);
    }
  });
}

Stalka.prototype.updateSequence = function(db, sequenceDoc, callback) {
  db.insert(sequenceDoc, '_local/feed', function(err, body, header) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, body);
    }
  });
}

Stalka.prototype.readChanges = function(dbUri, options, callback) {
  options = options || {};
  if (!options.feed) {
    options.feed = 'longpoll'; 
  }

  request(dbUri + '/_changes?' + querystring.stringify(options), function (error, response, body) {
    if (error) {
      callback(error, null);
    } else {
      callback(null, body);
    }
  });
}
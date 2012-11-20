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
}

function Stalka() {
  this.running = false;
}

Stalka.prototype.stop = function() {
  this.running = false;
};

Stalka.prototype.resume = function() {
  this.start(this.dbUri, this.changesWriter, this.options, this.mainCallback);
};

Stalka.prototype.isRunning = function() {
  return this.running;
};

Stalka.prototype.getLastSeq = function(callback) {
  var db = require('nano')(this.dbUri);
  this.readSequence(db, function(err, sequenceDoc) {
    callback(err, sequenceDoc);
  });
};

Stalka.prototype.registerStatsWriter = function(statsWriter) {
  this.statsWriter = statsWriter;
};

Stalka.prototype.start = function (dbUri, changesWriter, options, mainCallback) {
  var self = this;
  options = options || {};
  var retryCount = options.retryCount || 0;
  var maxRetryCount = options.maxRetryCount || 3;
  var retrySleepMs = options.retrySleepMs || 1000;

  this._start(dbUri, changesWriter, options, function(err) {
    if (err) {
      console.error('An error has occurred. Code: %s. Message: %s', err.code, err.message);
    } else if (retryCount >= 1) {
      console.log('Successful retry - Retry count reset from %d to 0', options.retryCount);
      options.retryCount = 0;
    }
    if (err && (err.code === 'ECONNRESET' || err.message === 'Document update conflict.')) {
      if (retryCount > maxRetryCount) {
        console.log('Stop retrying - Max retry count %d', maxRetryCount);
        mainCallback(err);
      } else {
        options.retryCount = retryCount + 1;
        console.log('Sleep for %dms before retry #%d', retrySleepMs, options.retryCount);
        setTimeout(function() {
          self.start(dbUri, changesWriter, options, mainCallback);
        }, retrySleepMs);
      }
    } else {
      mainCallback(err);
    }
  });
};

Stalka.prototype._start = function(dbUri, changesWriter, options, mainCallback) {
 // Store so that we can use them to resume
  this.dbUri = dbUri;
  this.changesWriter = changesWriter;
  this.options = options;
  this.mainCallback = mainCallback;

  this.running = true;

  var db = require('nano')(dbUri);
  var self = this;
  options = options || {};
  var limit = options.limit || 10;

  this.readSequence(db, function(err, sequenceDoc) {
    
    var startSequence = options.since || (sequenceDoc && sequenceDoc.lastSequence) || 0;

    if (err) {
      console.error("Unable to read sequence.", err.message);
      mainCallback(err);
      return;
    }

    async.whilst(
      function () { return self.running; },
      function (callback) {
        self.readChanges(dbUri, {limit: limit, since: startSequence}, function(err, body) {
          if (err) {
            console.error("Unable to read changes.", err.message);
            callback(err);
            return;
          }

          // Parse Body
          var changes = null;
          try {
            changes = JSON.parse(body);
          } catch (e) {
            console.error("Changes body is invalid.", e);
            callback(e);
            return;
          }

          // Write a chunk of changes
          changesWriter(changes, function(err) {
            if (err) {
              console.error("Unable to write changes.", err.message);
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
                console.error("Unable to update sequence.", err.message);
                callback(err);
                return;
              } else {
                this.lastSequence = sequenceDoc.lastSequence;
                sequenceDoc._rev = newSeqDoc.rev;

                if (self.statsWriter) {
                  self.statsWriter(this.lastSequence);
                }
                
                callback();
              }
            });
          });
        });
      },
      function (err) {
        if (err) {
          console.error("Failed to stalk ", dbUri, err.message);
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
    } else if (err && err.status_code === 404) {
      callback(null, {lastSequence: 0});
    } else {
      callback(err);
    }
  });
};

Stalka.prototype.updateSequence = function(db, sequenceDoc, callback) {
  var self = this;
  db.insert(sequenceDoc, '_local/feed', function(err, body, header) {
    if (err && err.status_code === 404) {
      delete sequenceDoc._rev;
      self.updateSequence(db, sequenceDoc, callback); 
    } else if (err) {
      callback(err, null);
    } else {
      callback(null, body);
    }
  });
};

Stalka.prototype.readChanges = function(dbUri, options, callback) {
  var headers;
  options = options || {};
  
  if (options.headers) {
    headers = options.headers;
    delete options.headers;
  }
  headers = headers || {};

  if(!options.heartbeat) {
    options.heartbeat = '3000';
  }
  if (!options.since) {
    delete options.since; 
  }
  if (!options.feed) {
    options.feed = 'longpoll'; 
  }
  request({url: dbUri + '/_changes?' + querystring.stringify(options), headers: headers}, function (error, response, body) {
    if (error) {
      callback(error, null);
    } else {
      callback(null, body);
    }
  });
};
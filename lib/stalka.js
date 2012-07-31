module.exports = stalka;

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
function stalka(changeWriter, options) {
  return new Stalka(changeWriter, options || {});
};

function Stalka(changeWriter, options) {
  this.changeWriter = changeWriter;
  this.sequenceWriter = options.sequenceWriter || _couchSequenceWriter;
  this.sequenceReader = options.sequenceReader || _couchSequenceReader;
  this.running = true;
};


/**
 * Couch based sequence reader
 */
function _couchSequenceReader(baseDbUri, callback) {
  this.sequenceDb = this.sequenceDb || require('nano')(baseDbUri);
  
  var self = this;
  this.sequenceDb.get('_local/feed', function(err, body) {
    if (body) {
      callback(body.lastSequence);
    } else {
      callback(0);
    }
  });
}

/**
 * Couch based sequence writer
 */
function _couchSequenceWriter(baseDbUri, sequence, callback) {
  this.sequenceDb = this.sequenceDb || require('nano')(baseDbUri);

  var self = this;
  this.sequenceDb.get('_local/feed', function(err, body) {
    if ((err && err.status_code == 404) || body) {
      var doc = {lastSequence: sequence};
      if (body) {
        doc = body;
        doc.lastSequence = sequence;
      }
      self.sequenceDb.insert(doc, '_local/feed', function(err, body, header) {
        if (err) {
          console.error("Failed to insert _local/feed document.", err);
          callback(err);
        } else {
          callback();
        }
      });
    } else {
      console.error("Failed to retrieve the _local/feed document.", err);
      callback(err);
    }
  });
}

/**
 * Wrapper to get sequence from the sequence reader if the sequence is not specified.
 */
function _getSequence(baseDbUri, sequence, sequenceReader, callback) {
  if (sequence && sequence >= 0) {
    callback(sequence);
  } else {
    sequenceReader(baseDbUri, function(startSequence) {
      callback(startSequence);
    });
  }
}

/**
 * Reads the changes from the change feed for the given couch db url.
 */
function _readChanges(baseDbUri, options, callback) {
  if (!options.feed) {
    options.feed = 'longpoll';
  }

  request(baseDbUri + '/_changes?' + querystring.stringify(options), function (error, response, body) {
    if (!error && response.statusCode == 200) {
      callback(null, body);
    } else {
      callback(error, body);
    }
  });
}

/**
 * Main loop that will read changes, block, then write them, then update the sequence etc..
 */
Stalka.prototype.start = function(baseDbUri, startSequence, limit) {
  var self = this;
  var limit = limit || 10;
  _getSequence(baseDbUri, startSequence, this.sequenceReader, function(since) {
    async.whilst(
      function () { return self.running; },
      function (callback) {
        _readChanges(baseDbUri, {since: since, limit: limit}, function (error, body) {
          if (error) {
            console.error("Unable to read changes", error);
            callback();
          } else {
            var changes = JSON.parse(body);
            self.changeWriter(changes, function() {
              since = changes.last_seq;
              self.sequenceWriter(baseDbUri, changes.last_seq, function() {
                callback();  
              });
            });
          }
        });
      },
      function (err) {
        if (err) {
          console.error("Error processing read changes", err);
        }
      }
    );
  });
}

Stalka.prototype.stop = function() {
  this.running = false;
}
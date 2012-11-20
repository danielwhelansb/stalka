var should = require("should");
var sandbox = require('sandboxed-module');

var libpath = process.env.STALKA_COV ? '../lib-cov' : '../lib';

// jscoverage support. This allows the global to be copied across to the new sandboxed context.
if (typeof _$jscoverage === 'undefined') _$jscoverage = {};

var fakenano = function(dbUri) {
  return  {
    get: function(id, callback) {
    },
    insert: function(doc, id, callback) {
    }
  };
};

var fakerequest = function(url, callback) {};

var stalka = sandbox.require(libpath + '/stalka', {
  requires: {'nano': fakenano, 'request': fakerequest}
});

describe('Stalka', function() {
  describe('#readSequence', function() {
    it("should use _local/feed as the sequence document id", function(done) {
      var db = { 
        get: function(id, callback) {
          id.should.equal("_local/feed");
          callback();
        }
      };
      stalka.readSequence(db, function(err, body) {
        done();
      });
    }),
    it("should return sequence document when its found", function(done) {
      var db = { 
        get: function(id, callback) {
          id.should.equal("_local/feed");
          callback(null, {lastSequence: 13}); 
        }
      };
      stalka.readSequence(db, function(err, body) {
        should.deepEqual(body, {lastSequence: 13});
        done();
      });
    }),
    it("should return a lastSequence of 0 when its not found", function(done) {
      var db = { 
        get: function(id, callback) { callback({status_code: 404}, null); }
      };

      stalka.readSequence(db, function(err, body) {
        should.deepEqual(body, {lastSequence: 0});
        done();
      });
    }),
    it("should return an error when an error occurs", function(done) {
      var db = { 
        get: function(id, callback) { callback("Failed to read sequence.", null); }
      };

      stalka.readSequence(db, function(err, body) {
        should.not.exist(body);
        err.should.equal("Failed to read sequence.");
        done();
      });
    });
  }),
  describe('#updateSequence', function() {
    it("should use _local/feed as the sequence document id", function(done) {
      var db = { 
        insert: function(document, id, callback) {
          id.should.equal("_local/feed");
          callback();
        }
      };
      stalka.updateSequence(db, {}, function(err, body) {
        done();
      });
    }),
    it("should insert the given sequence document", function(done) {
      var db = { 
        insert: function(document, id, callback) {
          document.should.eql({something: 123});
          callback();
        }
      };
      stalka.updateSequence(db, {something: 123}, function(err, body) {
        done();
      });
    }),
    it("should return the result from a successful insert", function(done) {
      var db = { 
        insert: function(document, id, callback) {
          callback(null, document, null);
        }
      };
      stalka.updateSequence(db, {something: 123}, function(err, body) {
        body.should.eql({something: 123});
        done();
      });
    }),
    it("should return the error from an unsuccessful insert", function(done) {
      var db = { 
        insert: function(document, id, callback) {
          callback("Failed to insert", null, null);
        }
      };
      stalka.updateSequence(db, {something: 123}, function(err, body) {
        should.not.exist(body);
        err.should.equal("Failed to insert");
        done();
      });
    });
  }),
  describe('#readChanges', function() {
    it("should default the feed to 'longpoll'", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.include("feed=longpoll");
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", null, function(err, changes) {
        done();
      });
    }),
    it("should not include since parameter when options is null", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.not.include("since");
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", null, function(err, changes) {
        done();
      });
    }),
    it("should not include since parameter when since property exists but its value is null", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.not.include("since");
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", { since: null }, function(err, changes) {
        done();
      });
    }),
    it("should not include since parameter when since property exists but its value is undefined", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.not.include("since");
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", { since: undefined }, function(err, changes) {
        done();
      });
    }),
    it("should query with any extra options", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.include("pizza=yummy");
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", { pizza: 'yummy'}, function(err, changes) {
        done();
      });
    }),
    it("should query with the correct base url", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.url.should.match(/^http:\/\/somehost:1234\/somedb\/_changes?.*/);
        callback();
      });

      stalka.readChanges("http://somehost:1234/somedb", null, function(err, changes) {
        done();
      });
    }),
    it("should return the changes on a successful read", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        callback(null, null, {changes: 'yay'});
      });
      stalka.readChanges("http://somehost:1234/somedb", null, function(err, changes) {
        changes.should.eql({changes: 'yay'});
        done();
      });
    }),
    it("should return the error on an unsuccessful read", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        callback("Error reading changes", null, null);
      });
      stalka.readChanges("http://somehost:1234/somedb", null, function(err, changes) {
        err.should.eql("Error reading changes");
        done();
      });
    }),
    it("should pass through headers option and remove it from options", function(done) {
      var stalka = fakeRequestStalka(function(options, callback) {
        options.headers.should.eql({'xyz':1});
        options.url.should.not.include("xyz");
        callback();
      });
      stalka.readChanges("http://somehost:1234/somedb", { headers: { 'xyz':1 }}, function(err, changes) {
        done();
      });
    });
  }),
  describe('#start', function() {
    it("should exit when error reading sequence", function(done) {
      stalka.readSequence = function(db, callback) {
        callback("Error reading sequence", null);
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        err.should.equal("Error reading sequence");
        done();
      });
    }),
    it("should exit main loop when stop called", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, {lastSequence: 10});
      };
      stalka.readChanges = function(db, options, callback) {
        callback(null, JSON.stringify({last_seq: 15}));
      };
      stalka.updateSequence = function(db, sequenceDoc, callback) {
        stalka.stop();
        callback(null, {rev: '123'});
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        should.not.exist(err);
        done();
      });
    }),
    it("should read changes from the stored local sequence", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, {lastSequence: 11});
      };
      stalka.readChanges = function(db, options, callback) {
        options.since.should.equal(11);
        stalka.stop();
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        done();
      });
    }),
    it("should reset retry count to 0 when read succeeds after previously failing due to ECONNRESET error", function(done) {
      var options = {},
        readCount = 0;
      stalka.readSequence = function(db, callback) {
        if (readCount === 0) {
          var error = new Error('Socket hang up');
          error.code = 'ECONNRESET';
          callback(error);
          readCount += 1;
        } else {
          callback(null, {lastSequence: 11});
        }
      };
      stalka.start("http://randomhost:2423/somedb", function(changes, callback) {
        callback();
      }, options, function(err) {
        options.retryCount.should.equal(0);
        done();
      });
    }),
    it("should set retry count to max retry count + 1 when read always fails due to ECONNRESET error", function(done) {
      this.timeout(5000); // larger timeout due to the need to simulate failure twice
      var options = { maxRetryCount: 1 },
        readCount = 0;
      stalka.readSequence = function(db, callback) {
        var error = new Error('Socket hang up');
        error.code = 'ECONNRESET';
        callback(error);
        readCount += 1;
      };
      stalka.start("http://randomhost:2423/somedb", function(changes, callback) {
        callback();
      }, options, function(err) {
        options.retryCount.should.equal(2);
        done();
      });
    }),
    it("should not set retry count opt and should pass error to main callback when read always fails due to non ECONNRESET error", function(done) {
      this.timeout(5000); // larger timeout due to the need to simulate failure twice
      var options = { maxRetryCount: 1 },
        readCount = 0;
      stalka.readSequence = function(db, callback) {
        var error = new Error('No space left');
        error.code = 'ENOSPC';
        callback(error);
        readCount += 1;
      };
      stalka.start("http://randomhost:2423/somedb", function(changes, callback) {
        callback();
      }, options, function(err) {
        should.not.exist(options.retryCount);
        err.message.should.equal('No space left');
        err.code.should.equal('ENOSPC');
        done();
      });
    }),
    it("should reset retry count to 0 when read succeeds after previously failing due to document update conflict error", function(done) {
      var options = {},
        readCount = 0;
      stalka.readSequence = function(db, callback) {
        if (readCount === 0) {
          callback(new Error('Document update conflict.'));
          readCount += 1;
        } else {
          callback(null, {lastSequence: 11});
        }
      };
      stalka.start("http://randomhost:2423/somedb", function(changes, callback) {
        callback();
      }, options, function(err) {
        options.retryCount.should.equal(0);
        done();
      });
    }),
    it("should read changes from the specified start sequence", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, {lastSequence: 11});
      };
      stalka.readChanges = function(db, options, callback) {
        options.since.should.equal(133);
        stalka.stop();
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, {since: 133}, function(err) {
        done();
      });
    }),
    it("should read changes from 0 if no local or specified sequence", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        options.since.should.equal(0);
        stalka.stop();
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        done();
      });
    }),
    it("should read changes with a default limit of 10", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        options.limit.should.equal(10);
        stalka.stop();
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        done();
      });
    }),
    it("should read changes with the specified limit", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        options.limit.should.equal(246);
        stalka.stop();
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, {limit: 246}, function(err) {
        done();
      });
    }),
    it("should return when read changes fails", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        stalka.stop();
        callback("Error", null);
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        err.should.equal("Error");
        done();
      });
    }),
    it("should return when writing changes fails", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        callback(null, JSON.stringify({}));
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback("Error");
        stalka.stop();
      }, null, function(err) {
        err.should.equal("Error");
        done();
      });
    }),
    it("should return when writing the sequence fails", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, null);
      };
      stalka.readChanges = function(db, options, callback) {
        callback(null, JSON.stringify({}));
      };
      stalka.updateSequence = function(db, sequenceDoc, callback) {
        callback("Error", null);
        stalka.stop();
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        callback();
      }, null, function(err) {
        err.should.equal("Error");
        done();
      });
    }),
    it("should read and write changes", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, {lastSequence: 123});
      };
      stalka.readChanges = function(db, options, callback) {
        callback(null, JSON.stringify({last_seq: 200}));
      };
      stalka.updateSequence = function(db, sequenceDoc, callback) {
        sequenceDoc.lastSequence.should.equal(200);
        stalka.stop();
        callback(null, {rev: '1234'});
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        var expected = {last_seq: 200};
        expected.should.eql(changes);
        callback();
      }, null, function(err) {
        should.not.exist(err);
        done();
      });
    }),
    it("should continue processing when theres an invalid changes body", function(done) {
      stalka.readSequence = function(db, callback) {
        callback(null, {lastSequence: 123});
      };
      stalka.readChanges = function(db, options, callback) {
        stalka.stop();
        callback(null, " xyz ");
      };
      stalka.start("http://randomhost:2422/somedb", function(changes, callback) {
        should.fail("Changes should not be written when empty body");
      }, null, function(err) {
        done();
      });
    });
  });
});

function fakeRequestStalka(fakeRequestFunction) {
  return sandbox.require(libpath + '/stalka', {
    requires: {'nano': fakenano, 'request': fakeRequestFunction}
  });
}
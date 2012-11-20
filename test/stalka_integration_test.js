var stalka = require('../lib/stalka');
var express = require('express');
var should = require('should');
var app = express();
var async = require('async');
var requestCount = 0;

describe('Stalka Integration', function() {
  describe('socket errors', function() {

    before(function(done) {
      app.listen(1337);
      done();
    });

    after(function(done){
      done();
    });

    describe('reading changes', function() {
      describe('on socket hang up', function() {
        it('retries the request at most 3 times', function(done) {
          app.get('/xyz/_changes', function(req, res) {
            if (requestCount < 3) {
              req.socket.destroy();
              requestCount++;
            } else {
              res.json({results: [], last_seq: 1000});
            }
          });

          stalka.start('http://localhost:1337/xyz', function(changes, cb) {
            should.exist(changes.last_seq);
            done();
          }, { since: 999, retrySleepMs: 100 }, function(err) {
            should.not.exist(err);
          });
        }),
        it('retries the request 2 times then succeed', function(done) {
          app.get('/def/_changes', function(req, res) {
            if (requestCount < 1) {
              req.socket.destroy();
              requestCount++;
            } else {
              res.json({results: [], last_seq: 1000});
            }
          });

          stalka.start('http://localhost:1337/def', function(changes, cb) {
            should.exist(changes.last_seq);
            done();
          }, { since: 999, retrySleepMs: 100 }, function(err) {
            should.not.exist(err);
          });
        }),
        it('fails when it retries more than 3 times', function(done) {
          app.get('/abc/_changes', function(req, res) {
            req.socket.destroy();
          });

          stalka.start('http://localhost:1337/abc', function(changes, cb) {
            should.not.exist(changes);
          }, { since: 999, retrySleepMs: 100 }, function(err) {
            should.exist(err);
            done();
          });
        });
      });
    });
  });
});


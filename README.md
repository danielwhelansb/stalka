Stalka
======

## Simple - Defaults to reading/writing sequence from/to <baseDbUri>/_local/feed (lastSequence)
	var stalka = require('./stalka')(changeWriter);
	function changeWriter(changes, callback) {
	  console.log("Writing changes...");
	  callback();
	}

## Custom Sequence Management
	var stalka = require('./stalka')(changeWriter, {
	  sequenceWriter: sequenceWriter,
	  sequenceReader: sequenceReader
	});

	function sequenceReader(baseDbUri, callback) {
	  console.log("Reading sequence from ", baseDbUri);
	  callback(1);
	}

	function sequenceWriter(baseDbUri, sequence, callback) {
	  console.log("Writing sequence %s to %s", sequence, baseDbUri);
	  callback();
	}

## Simple - Starts at either at 0 or from the _local/feed -> lastSequence, Limit defaults to 10
	stalka.start('http://localhost:5984/about');

## Specific Start Sequence - Starts at 25, Limit: defaults to 10
	stalka.start('http://localhost:5984/about', {since: 10});

## Specific Start Sequence and limit, Starts at 25, Limit to 50
	stalka.start('http://localhost:5984/about', {since: 10, limit: 25});
Stalka
======

## Getting Started
	var stalka = require('stalka');
	function changeWriter(changes, callback) {
	  console.log("Writing changes...");
	  callback();
	}
	stalka.start("http://somehost:5984/somedb", changeWriter);
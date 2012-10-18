var http = require('http');
var fs   = require('fs');

// Download statistics
var stats = {
  errors: 0,
  bytesReceived: 0,
  duplicates: 0,
  parts: 0
}


var fragments = [];


function isReceived(chunk) {
  var data = String(chunk);
  var name = (data.match(/"imageName"\s*:\s*"(\w+\.\w+)"\s?,/) || []).pop();
  var totalSize = (data.match(/"sizeOfImageInBytes"\s*:\s*(\d+)\s*[,}]/) || []).pop();
  if (name == undefined || totalSize == undefined) 
    return true;
  if (!fragments[name]) {
    console.log("create image with name = " + name);
    var image = {
      totalSize: totalSize,
      currentSize: 0,
      parts: 0,
      data: Array.apply(null, Array(100)).map(function() { return null; })
    }
    fragments[name] = image;
    return false;
  }
  var number = (data.match(/"imagePartNumber"\s*:\s*(\d+)\s*[,}]/) || []).pop();  
  return fragments[name][number];
}

function markAsReceived(fragment) {
  var name = fragment.imageName;
  fragments[name].currentSize += fragment.sizeOfPartInBytes;
  fragments[name].parts++;
  fragments[name].data[fragment.imagePartNumber - 1] = fragment;
  stats.parts++;
  console.log("part no:" + fragment.imagePartNumber + "\t-->>\t" + fragment.imageName +
    "\t| total size:" + fragments[name].totalSize + "\t| current size:" + fragments[name].currentSize + 
    "\t| total parts:" + stats.parts);
}

function gotAllImageFragments(imageName) {
  for (var i = 0; i < fragments[imageName].length; i++) {
    if (!fragments[imageName][i])
      return false;
  }
  return true;
}

function gotAllFragments() {
  if (fragments.length == 0) return false;
  
  for (var i in fragments) {
    if (!gotAllImageFragments(i)) 
      return false;
  }
  return true;
};

//
// Repeatedly issues requests to a server until we have all fragments of all
// images, then fires a `done` callback.
//
function getAllFragments(done) {
  var endpoint = 0;

  function _getAllFragments() {
    getFragment(endpoint + 1, function (fragment) {
      if (gotAllFragments()) {
        done();
      } else {
        // Round-robin poll endpoints.
        endpoint = (endpoint + 1) % 5;
        _getAllFragments(done);
      }
    });
  }

  // Start the polling cycle.
  _getAllFragments();
}

//
// Issues a single request to a server and calls `done` when a previously
// unseen fragment is encountered.
//
function getFragment(endpoint, done) {
  var request = http.get('http://89.253.235.155:8080/endpoint' + endpoint);
  request.on('response', function (response) {

    if (response.statusCode == 200) {    
      // Collect all incoming data into a single big string.
      var body = '';
      response.on('data', function (chunk) {
        if (body == '') {
          if (isReceived(chunk)) {
            response.destroy();
            stats.duplicates++;
            done(null);
          } else {
            body += chunk;
            response.on('end', function() {
            done(endReceiving(body));
          });
        }
        } else { 
          body += chunk;
          stats.bytesReceived += chunk.length;
        }
      });
    } else {
      console.log("Server error ...");
      stats.errors++;
      done(null);
    }
  });
}

function endReceiving(body) {
  try {
    var fragment = JSON.parse(body);
  } catch (e) {
    console.log("parser error. Data = " + body);
    stats.errors++;
    return null;
  }  
  markAsReceived(fragment);
  return fragment;
}

function writeFragments(done) {
  for (var i in fragments) {
    // Fragments of i-th image.
    var imageFragments = fragments[i];

    var fd = fs.openSync(i, 'w');

    for (var j = 0; j < imageFragments.data.length; j++) {
      var fragment = imageFragments.data[j];
      var buffer = new Buffer(fragment.base64Data, 'base64');

      var toWrite = buffer.length;
      var offset = 0;
      while (toWrite > 0) {
        var written = fs.writeSync(fd, buffer, offset, toWrite);
        if (written < toWrite) process.stdout.write('!');
        toWrite -= written;
        offset += written;
      }
    }

    fs.closeSync(fd);
  }

  done();
}

// Start the stopwatch and go get 'em all
var timeStart = Date.now();
getAllFragments(function () {
  console.log('Received all image fragments, writing files...');
  stats.timeTaken = Date.now() - timeStart;

  writeFragments(function () {
    console.log('All done.');
    console.log(require('util').inspect(stats));
  });
});

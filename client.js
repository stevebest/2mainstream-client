var http = require('http');
var fs   = require('fs');

// Download statistics
var stats = {
  errors: 0,
  bytesReceived: 0,
  duplicates: 0
}

// Fragment availability map, which is just an `array[image_id][fragment_id]`
// of fragment objects.
var fragments = Array.apply(null, Array(5)).map(function () {
  return Array.apply(null, Array(100)).map(function () { return null; });
});

function isReceived(fragment) {
  return !!fragments[fragment.image_id - 1][fragment.fragment_id - 1];
}

function markAsReceived(fragment) {
  var isNew = !isReceived(fragment);
  fragments[fragment.image_id - 1][fragment.fragment_id - 1] = fragment;
  return isNew;
}

function gotAllImageFragments(imageId) {
  imageId = imageId - 1; // argument is 1-based, index is 0-based
  for (var i = 0; i < fragments[imageId].length; i++) {
    if (!fragments[imageId][i]) return false;
  }
  return true;
}

function gotAllFragments() {
  for (var i = 1; i <= fragments.length; i++) { // 1-based `image_id`s
    if (!gotAllImageFragments(i)) return false;
  }
  return true;
};

//
// Repeatedly issues requests to a server until we have all fragments of all
// images, then fires a `done` callback.
//
function getAllFragments(done) {
  var endpoint = 0;
  var queue = [];

  function _getAllFragments() {
    if (gotAllFragments()) {
      done();
    } else {
      if (queue.length < 5) {
        queue.push(endpoint);
        getFragment(endpoint + 1, function (fragment) {
            if (fragment !== null && gotAllImageFragments(fragment.image_id)) {
                writeImage(fragment.image_id);
            }
            queue.pop();
        });
        endpoint = (endpoint + 1) % 5;
        _getAllFragments();
      } else {
        setTimeout(function(){ _getAllFragments(); }, 5);
      }
    }
  }

  // Start the polling cycle.
  _getAllFragments();
}

//
// Issues a single request to a server and calls `done` when a previously
// unseen fragment is encountered.
//
function getFragment(endpoint, done) {
  var request = http.get('http://localhost:8080/endpoint' + endpoint);

  request.on('socket', function (socket) {
    socket.setTimeout(500);
    socket.on('timeout', function() {
      request.abort();
    });
  });

  request.on('error', function(e) {
    stats.errors++;
    done(null);
  });

  request.on('response', function (response) {
    // Collect all incoming data into a single big string.
    var body = '';
    var abort = false;
    var image_id, fragment_id;

    if (response.statusCode !== 200) {
      stats.errors++;
      abort = true;
      request.abort();
    }

    response.on('data', function (chunk) {
      body += chunk;
      stats.bytesReceived += chunk.length;

      image_id    = image_id    || (body.match(/"image_id"\s*:\s*(\d+)\s*[,}]/)    || []).pop();
      fragment_id = fragment_id || (body.match(/"fragment_id"\s*:\s*(\d+)\s*[,}]/) || []).pop();

      if (fragment_id && image_id) {
        if (isReceived({ image_id: image_id, fragment_id: fragment_id})) {
          stats.duplicates++;
          abort = true;
          request.abort();
        }
      }
    });

    // Process the received data when the response is complete.
    response.on('end', function () {
      if (abort) { return done(null); }

      try {
        var fragment = JSON.parse(body);
        // If it'a new fragment, mark it as received and return it.
        if (markAsReceived(fragment)) {
          return done(fragment);
        }

        // Otherwise, indicate that no new fragment was received.
        stats.duplicates++;

      } catch (e) {
        stats.errors++;
      }
      done(null);
    });
  });
}

function writeImage(image_id) {
    // Fragments of i-th image.
    var imageFragments = fragments[image_id - 1];

    var fd = fs.openSync(imageFragments[0].image_name, 'w');

    for (var j = 0; j < imageFragments.length; j++) {
      var fragment = imageFragments[j];
      var buffer = new Buffer(fragment.content, 'base64');

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

// Start the stopwatch and go get 'em all
var timeStart = Date.now();
getAllFragments(function () {
  stats.timeTaken = Date.now() - timeStart;
  console.log('All done.');
  console.log(require('util').inspect(stats));
});

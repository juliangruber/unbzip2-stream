var through = require('through2');
var bz2 = require('./lib/bzip2');
var bitIterator = require('./lib/bit_iterator');

module.exports = unbzip2Stream;

function unbzip2Stream() {
    var bufferQueue = [];
    var hasBytes = 0;
    var blockSize = 0;
    var done = false;
    var bitReader = null;

    function decompressBlock(push){
        if(!blockSize){
            blockSize = bz2.header(bitReader);
            //console.error("got header of", blockSize);
            return true;
        }else{
            var bufsize = 100000 * blockSize;
            var buf = new Int32Array(bufsize);
            
            var chunk = [];
            var f = function(b) {
                chunk.push(b);
            };

            var done = bz2.decompress(bitReader, f, buf, bufsize);        
            if (done) {
                push(null);
                //console.error('done');
                return false;
            }else{
                //console.error('decompressed', chunk.length,'bytes');
                push(new Buffer(chunk));
                return true;
            }
        }
    }

    var outlength = 0;
    function decompressAndQueue(stream) {
        try {
            return decompressBlock(function(d) {
                stream.push(d);
                if (d !== null) {
                    //console.error('write at', outlength.toString(16));
                    outlength += d.length;
                } else {
                    //console.error('written EOS');
                }
            });
        } catch(e) {
            //console.error(e);
            stream.emit('error', e);
            return false;
        }
    }

    return through(
        function write(data, _, cb) {
            //console.error('received', data.length,'bytes in', typeof data);
            bufferQueue.push(data);
            hasBytes += data.length;
            if (bitReader === null) {
                bitReader = bitIterator(function() {
                    return bufferQueue.shift();
                });
            }
            while (hasBytes - bitReader.bytesRead + 1 >= (100000 * blockSize || 4)){
                //console.error('decompressing with', hasBytes - bitReader.bytesRead + 1, 'bytes in buffer');
                if (!done) done = !decompressAndQueue(this);
                if (done) break;
            }
            cb()
        },
        function end(cb) {
            //console.error(x,'last compressing with', hasBytes, 'bytes in buffer');
            if (!done) {
                while(decompressAndQueue(this));
            }
            cb()
        }
    );
}


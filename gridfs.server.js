var path = Npm.require('path');
var mongodb = Npm.require('mongodb');
var ObjectID = Npm.require('mongodb').ObjectID;
var Grid = Npm.require('gridfs-stream');
//var Grid = Npm.require('gridfs-locking-stream');

var chunkSize = 1024*1024*2; // 256k is default GridFS chunk size, but performs terribly for largish files

/**
 * @public
 * @constructor
 * @param {String} name - The store name
 * @param {Object} options
 * @param {Function} [options.beforeSave] - Function to run before saving a file from the server. The context of the function will be the `FS.File` instance we're saving. The function may alter its properties.
 * @param {Number} [options.maxTries=5] - Max times to attempt saving a file
 * @returns {FS.StorageAdapter} An instance of FS.StorageAdapter.
 *
 * Creates a GridFS store instance on the server. Inherits from FS.StorageAdapter
 * type.
 */


// XXX: Would be nice if we could just use the meteor id directly in the mongodb
// It should be possible?
var UNMISTAKABLE_CHARS = "23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz";

var UNMISTAKABLE_CHARS_LOOKUP = {};
for (var a = 0; a < UNMISTAKABLE_CHARS.length; a++) {
  UNMISTAKABLE_CHARS_LOOKUP[UNMISTAKABLE_CHARS[a]] = a;
}

var meteorToMongoId = function(meteorId) {
  var unit = UNMISTAKABLE_CHARS.length;
  var index = 1;
  result = 0;
  for (var i = 0; i < meteorId.length; i++) {
    if (typeof UNMISTAKABLE_CHARS_LOOKUP[meteorId[i]] !== 'undefined') {
      // Add the number at index
      result += UNMISTAKABLE_CHARS_LOOKUP[meteorId[i]] * index;
      // Inc index by ^unit
      index *= unit;
    } else {
      throw new Error('Not a meteor ID');
    }
  }

  // convert to hex
  result = result.toString(16).substring(0, 24);

  return result;
};

FS.Store.GridFS = function(name, options) {
  var self = this;
  options = options || {};

  var gridfsName = name;
  var mongoOptions = options.mongoOptions || {};

  if (!(self instanceof FS.Store.GridFS))
    throw new Error('FS.Store.GridFS missing keyword "new"');

  if (!options.mongoUrl) {
    options.mongoUrl = process.env.MONGO_URL;
    // When using a Meteor MongoDB instance, preface name with "cfs_gridfs."
    gridfsName = "cfs_gridfs." + name;
  }

  if (!options.mongoOptions) {
    options.mongoOptions = { db: { native_parser: true }, server: { auto_reconnect: true }};
  }

  if (options.chunkSize) {
    chunkSize = options.chunkSize;
  }

  return new FS.StorageAdapter(name, options, {

    typeName: 'storage.gridfs',
    fileKey: function(fileObj) {
      // We could have this return an object with _id and name
      // since the stream-lock only allows createStream from id
      // The TempStore should track uploads by id too - at the moment
      // TempStore only sets name, _id, collectionName for us to generate the
      // id from.
      // Create a backup filename in case the file is missing a name
      var filename = fileObj.collectionName + '-' + fileObj._id;

      return {
        // Convert the
        _id: meteorToMongoId(fileObj._id),
        // suffix root this allows the namespacing needed to make the meteor
        // id work here
        root: gridfsName + '.' + fileObj.collectionName,
        // We also want to store the filename
        filename: fileObj.name || filename
      };
    },
    createReadStream: function(fileKey, options) {
      // Init GridFS
      var gfs = new Grid(self.db, mongodb);

      return gfs.createReadStream({
        _id: fileKey._id,
        root: fileKey.root,
      });

    },
    createWriteStream: function(fileKey, options) {
      options = options || {};

      // Init GridFS
      var gfs = new Grid(self.db, mongodb);

      var writeStream = gfs.createWriteStream({
        _id: fileKey._id,
        filename: fileKey.filename,
        mode: 'w',
        root: fileKey.root,
        chunk_size: options.chunk_size || chunkSize,
        // We allow aliases, metadata and contentType to be passed in via
        // options
        aliases: options.aliases || [],
        metadata: options.metadata || null,
        content_type: options.contentType || 'application/octet-stream'
      });

      writeStream.on('close', function(file) {
        if (FS.debug) console.log('SA GridFS - DONE!');
        // Emit end and return the fileKey, size, and updated date
        writeStream.emit('stored', {
          fileKey: file._id,
          size: file.length,
          storedAt: file.uploadDate || new Date()
        });
      });

      return writeStream;

    },
    remove: function(fileKey, callback) {
      // Init GridFS
      var gfs = new Grid(self.db, mongodb);

      try {
        gfs.remove({ _id: fileKey._id, root: fileKey.root }, callback);
      } catch(err) {
        callback(err);
      }
    },

    // Not implemented
    watch: function() {
      throw new Error("GridFS storage adapter does not support the sync option");
    },

    init: function(callback) {
      mongodb.MongoClient.connect(options.mongoUrl, mongoOptions, function (err, db) {
        if (err) { return callback(err); }
        self.db = db;

        console.log('GridFS init ' + name + ' on ' + options.mongoUrl);

        callback(null);
      });
    }
  });
};

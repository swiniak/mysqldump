var async = require('async');
var _ = require('lodash');
var fs = require('fs');
var mysql2 = require('mysql2');
var logger = require('winston');
var sync = require('synchronize');

var anonymizer = undefined;


var extend = function(obj) {
	for (var i = 1; i < arguments.length; i++) for (var key in arguments[i]) obj[key] = arguments[i][key];
	return obj;
}

var typeCastOptions = { typeCast: function (field, next) {
	if (field.type === "GEOMETRY") {
		var offset = field.parser._offset;
		var buffer = field.buffer();
		field.parser._offset = offset;
		var result = field.geometry();
		annotateWkbTypes(result, buffer, 4);
		return result;
	}
	return next();
}}

var annotateWkbTypes = function(geometry, buffer, offset) {

	if (!buffer) return offset;

	var byteOrder = buffer.readUInt8(offset); offset += 1;
	var ignorePoints = function(count) { offset += count * 16; }
	var readInt = function() {
		var result = byteOrder ? buffer.readUInt32LE(offset) : buffer.readUInt32BE(offset);
		offset += 4;
		return result;
	}

	geometry._wkbType = readInt();

	if (geometry._wkbType === 1) {
		ignorePoints(1);
	} else if (geometry._wkbType === 2) {
		ignorePoints(readInt());
	} else if (geometry._wkbType === 3) {
		var rings = readInt();
		for (var i=0; i<rings; i++) {
			ignorePoints(readInt());
		}
	} else if (geometry._wkbType === 7) {
		var elements = readInt();
		for (var i=0; i<elements; i++) {
			offset = annotateWkbTypes(geometry[i], buffer, offset);
		}
	}
	return offset
}

var escapeGeometryType = function(val) {

	var constructors = {1: "POINT", 2: "LINESTRING", 3: "POLYGON", 4: "MULTIPOINT", 5: "MULTILINESTRING", 6: "MULTIPOLYGON", 7: "GEOMETRYCOLLECTION" };

	var isPointType = function(val) { return val && typeof val.x === 'number' && typeof val.y === 'number'; }
	var close = function(str) { return str.length && str[0] === '(' ? str : '(' + str + ')'; }

	function escape(val) {

		var result = isPointType(val) ? (val.x + " " + val.y) :
			"(" + val.map(escape).join(',') + ")";
		if (val._wkbType) {
			result = constructors[val._wkbType] + close(result);
		}
		return result;
	}

	return "GeomFromText('" + escape(val) + "')";
}

var isset = function(){
	var a = arguments;
	var l = a.length;
	var i = 0;
	var undef;

	if (l === 0) throw new Error('Empty isset');

	while (i!==l) {
		if(a[i]===undef || a[i]===null) return false;
		++i;
	}
	return true;
}

var anonymize = function(table, column, value, callback) {
	if(!anonymizer)
		return value;
	anonymizer.check(value, column, function (err, results) {
		if(results && results.length > 0) {
			console.log(results);
		}
		callback(null, value);
	});
}

var buildInsert = function(rows,table,cols){
	sync.fiber(function () {
		var cols = _.keys(rows[0]);
		var sql = [];
		for (var i in rows) {
			var values = [];
			for (var k in rows[i]) {
				if (typeof rows[i][k] === 'function') continue;
				if (!isset(rows[i][k])) {
					if (rows[i][k] == null) {
						values.push("NULL");
					} else {
						values.push(" ");
					}
				} else if (rows[i][k] !== '') {
					rows[i][k] = sync.await(anonymize(table, k, rows[i][k], sync.defer()));
					if (rows[i][k]._wkbType) {
						var geometry = escapeGeometryType(rows[i][k]);
						values.push(geometry);
					} else if (typeof rows[i][k] === 'number') {
						values.push(rows[i][k]);
					} else {
						values.push(mysql2.escape(rows[i][k]));
					}
				} else {
					values.push("''");
				}
			}
			insertSql = "INSERT INTO `" + table + "` (`" + cols.join("`,`") + "`) VALUES (" + values.join() + ");";
			logger.debug(insertSql);
			sql.push(insertSql);
		}
		return sql.join('\n');
	}, function (err) {
		return null;
	});
}

module.exports = function(options,done){
	if(done === undefined)
		done = function () {};

	var defaultConnection = {
		host: 'localhost',
		user: 'root',
		password: '',
		database: 'test',
		charset: 'UTF8_GENERAL_CI',
	};

	var defaultOptions = {
		tables:null,
		schema:true,
		data:true,
		ifNotExist:true,
		autoIncrement:true,
		dropTable:false,
		getDump:false,
		dest:'./data.sql',
		where: null
	}

	options = extend({},defaultConnection,defaultOptions,options);

	if(!options.database) throw new Error('Database not specified');

	if(options.anonymizer) {
		anonymizer = options.anonymizer;
	}

	mysql = mysql2.createConnection({
		host: options.host,
		user: options.user,
		password: options.password,
		database: options.database,
		port:options.port,
		charset:options.charset,
		socketPath:options.socketPath
	});

	async.auto({
		getTables:function(callback){
			if(!options.tables || !options.tables.length){ // if not especifed, get all
				mysql.query("SHOW TABLES FROM `"+[options.database]+"`",function(err,data){
					if(err) return callback(err);
					var resp = [];
					for(var i=0;i<data.length;i++) resp.push(data[i]['Tables_in_'+options.database]);
					logger.debug(resp);
					callback(err,resp);
				});
			} else {
				callback(null, options.tables);
			}
		},
		createSchemaDump:['getTables',function(results,callback){
			if(!options.schema) {
				callback();
				return;
			}
			var run = [];
			results.getTables.forEach(function(table){
				run.push(function(callback){
					mysql.query("SHOW CREATE TABLE `"+table+"`", callback);
				})
			})
			async.parallel(run,function(err,data,fields){
				if (err) return callback(err);
				var resp = [];
				for(var i in data){
					var r = data[i][0]['Create Table']+";";

					if(options.dropTable) r = r.replace(/CREATE TABLE `/, 'DROP TABLE IF EXISTS `' + data[i][0]['Table'] + '`;\nCREATE TABLE `');
					if(options.ifNotExist) r = r.replace(/CREATE TABLE `/,'CREATE TABLE IF NOT EXISTS `');
					if(!options.autoIncrement) r = r.replace(/AUTO_INCREMENT=\d+ /g,'');
					logger.debug(r);
					resp.push(r)
				}
				callback(err,resp);
			});
		}],
		createDataDump:['createSchemaDump',function(results,callback){
			var tbls = [];
			if (options.data) {
				tbls = results.getTables; // get data for all tables
			} else if (options.where) {
				tbls = Object.keys(options.where); // get data for tables with a where specified
			} else {
				callback();
				return;
			}
			var run = [];
			_.each(tbls,function(table){
				run.push(function(callback){
					var opts = {cols:'*', from:"`"+table+"`"};
					let selectSql = 'SELECT * FROM `'+table+'`';
					if ((options.where != null) && (typeof options.where[table] != 'undefined')) {
						opts.where = options.where[table];
						selectSql += ` WHERE ${options.where[table]}`;
					}
					if (options.orderBy) {
						opts.orderBy = options.orderBy;
						if(options.orderBy['*']) {
							opts.orderBy = options.orderBy['*'];
						}
						if (options.orderBy[table]) {
							opts.orderBy = options.orderBy[table];
						}
						if(!opts.orderBy.trim()) {
							selectSql += ` ORDER BY ${opts.orderBy}`;
						}
					}
					if (options.limit) {
						opts.limit = options.limit;
						if(options.limit['*']) {
							opts.limit = options.limit['*'];
						}
						if (options.limit[table]) {
							opts.limit = options.limit[table];
						}
						if(!opts.limit.toString().trim()) {
							selectSql += ` LIMIT ${opts.limit}`;
						}
					}
					logger.debug(selectSql);
					mysql.execute(selectSql, function(err,data){
						if (err) {
							logger.error(selectSql + ' => ' + err);
							return callback(err);
						}
						callback(err,buildInsert(data,table));
					});
				});
			});
			async.parallel(run,callback)
		}],
		getDataDump:['createSchemaDump','createDataDump',function(results, callback){
			if(!results.createSchemaDump || !results.createSchemaDump.length) results.createSchemaDump=[];
			if(!results.createDataDump || !results.createDataDump.length) results.createDataDump=[];
			callback(null,results.createSchemaDump.concat(results.createDataDump).join("\n\n"));
		}]
	},function(err,results){
		if(err) return done(err);
		if(options.getDump) return done(err, results.getDataDump);
		fs.writeFile(options.dest, results.getDataDump, done);
	});
}

var async = require('async');
var _ = require('lodash');
var fs = require('fs');
var mysql2 = require('mysql2');
var logger = require('winston');
var sync = require('synchronize');
const validator = require('validator');
const voca = require('voca');

var anonymizer = undefined;
var constraints = [];
var constraintsFlat = [];

var extend = function (obj) {
	for (var i = 1; i < arguments.length; i++) for (var key in arguments[i]) obj[key] = arguments[i][key];
	return obj;
}

var typeCastOptions = {
	typeCast: function (field, next) {
		if (field.type === "GEOMETRY") {
			var offset = field.parser._offset;
			var buffer = field.buffer();
			field.parser._offset = offset;
			var result = field.geometry();
			annotateWkbTypes(result, buffer, 4);
			return result;
		}
		return next();
	}
}

var annotateWkbTypes = function (geometry, buffer, offset) {

	if (!buffer) return offset;

	var byteOrder = buffer.readUInt8(offset);
	offset += 1;
	var ignorePoints = function (count) {
		offset += count * 16;
	}
	var readInt = function () {
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
		for (var i = 0; i < rings; i++) {
			ignorePoints(readInt());
		}
	} else if (geometry._wkbType === 7) {
		var elements = readInt();
		for (var i = 0; i < elements; i++) {
			offset = annotateWkbTypes(geometry[i], buffer, offset);
		}
	}
	return offset
}

var escapeGeometryType = function (val) {

	var constructors = {
		1: "POINT",
		2: "LINESTRING",
		3: "POLYGON",
		4: "MULTIPOINT",
		5: "MULTILINESTRING",
		6: "MULTIPOLYGON",
		7: "GEOMETRYCOLLECTION"
	};

	var isPointType = function (val) {
		return val && typeof val.x === 'number' && typeof val.y === 'number';
	}
	var close = function (str) {
		return str.length && str[0] === '(' ? str : '(' + str + ')';
	}

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

var isset = function () {
	var a = arguments;
	var l = a.length;
	var i = 0;
	var undef;

	if (l === 0) throw new Error('Empty isset');

	while (i !== l) {
		if (a[i] === undef || a[i] === null) return false;
		++i;
	}
	return true;
}

var setMatcherHints = function (hints, callback) {
	if(!hints) {
		return process.nextTick(callback);
	}
	async.eachOf(hints, (tableHints, table, callback) => {
		if (typeof tableHints === 'string') {
			anonymizer.setMatcherHint(table, null, null, null, hints);
			process.nextTick(callback);
		} else {
			async.eachOf(tableHints, (columnHints, column, callback) => {
				if (typeof columnHints === 'string') {
					anonymizer.setMatcherHint(table, null, null, null, tableHints);
					process.nextTick(callback);
				} else {
					async.eachOf(columnHints, (jsonParentHints, jsonParent, callback) => {
						if (typeof jsonParentHints === 'string' || Array.isArray(jsonParentHints)) {
							anonymizer.setMatcherHint(table, column, null, null, columnHints);
							process.nextTick(callback);
						} else {
							async.eachOf(jsonParentHints, (hint, jsonElement, callback) => {
								if (typeof hint === 'string' || Array.isArray(hint)) {
									anonymizer.setMatcherHint(table, column, jsonParent, null, jsonParentHints);
								} else {
									anonymizer.setMatcherHint(table, column, jsonParent, jsonElement, hint);
								}
								process.nextTick(callback);
							}, (err) => {
								process.nextTick(callback);
							});
						}
					}, (err) => {
						process.nextTick(callback);
					});
				}
			}, (err) => {
				process.nextTick(callback);
			});
		}
	}, (err) => {
		process.nextTick(callback);
	});
}


var anonymize = function (table, column, additionalKey, value, callback) {
	if (!anonymizer || constraintsFlat.findIndex(item => `[${table}].[${column}]`.toLowerCase() === item.toLowerCase()) >= 0)
		return process.nextTick(callback, null, value);
	if (typeof value === 'string' && validator.isJSON(value)) {
		return anonymizer.processJson(JSON.parse(value), table, column, column, '', function (err, json) {
			return process.nextTick(callback, null, JSON.stringify(json));
		});
	}
	let hint = anonymizer.getMatcherHint(table, column, null, null);
	if(hint) {
		hint.keyColumnValue = (additionalKey) ? `${table}.${additionalKey}` : undefined;
	}
	anonymizer.check(value, `${table}.${column}`.toLowerCase(), hint, function (err, results) {
		if (results && results.length > 0) {
			logger.debug(`[${results[0].type}] ${table}.${column}: ${results[0].original} -> ${results[0].anonymized}`);
			let subTypes = (Array.isArray(results[0].subTypes)) ? _.map(results[0].subTypes, 'name').join() : '';
			let cacheKey = `${results[0].type}|${subTypes}|${voca.latinise(value).trim()}`.toLowerCase();
			anonymizer.addToStats(`${table}.${column}`, value, results[0].type);
			if (anonymizer.similarMemoryCache[cacheKey]) {
				logger.silly(`Cache hit: ${cacheKey} -> ${anonymizer.similarMemoryCache[cacheKey]}`)
				return process.nextTick(callback, null, anonymizer.similarMemoryCache[cacheKey]);
			}
			let alternativeCacheKey = `${results[0].type}||${voca.latinise(value).trim()}`.toLowerCase();
			if (anonymizer.similarMemoryCache[alternativeCacheKey]) {
				logger.silly(`Cache hit: ${alternativeCacheKey} -> ${anonymizer.similarMemoryCache[alternativeCacheKey]}`)
				return process.nextTick(callback, null, anonymizer.similarMemoryCache[alternativeCacheKey]);
			}
			if (String(value) === results[0].anonymized) {
				anonymizer.similarMemoryCache[cacheKey] = value;
				return process.nextTick(callback, null, value);
			}
			anonymizer.similarMemoryCache[cacheKey] = results[0].anonymized;
			return process.nextTick(callback, null, results[0].anonymized);
		}
		else {
			anonymizer.addToStats(`${table}.${column}`, value, 'NOT_MATCHED');
		}
		return process.nextTick(callback, null, value);
	});
}

var buildInsert = function (rows, table, callback) {
	var sql = [];
	async.eachLimit(rows, 100, function (row, callback) {
		async.mapValues(row, function (value, key, callback) {
			if (typeof value === 'function') return process.nextTick(callback);
			if (!isset(value)) {
				if (value == null) {
					return process.nextTick(callback, null, "NULL");
				} else {
					return process.nextTick(callback, null, " ");
				}
			} else if (value !== '') {
				let hint = anonymizer.getMatcherHint(table, key);
				let additionalKey = null;
				if(hint && hint.keyColumn) {
					additionalKey = row[hint.keyColumn];
				}
				anonymize(table, key, additionalKey, value, function (err, anonymizedValue) {
					if (err) {
						return process.nextTick(callback, err);
					}
					if (anonymizedValue._wkbType) {
						var geometry = escapeGeometryType(anonymizedValue);
						return process.nextTick(callback, null, geometry);
					} else if (typeof anonymizedValue === 'number') {
						return process.nextTick(callback, null, anonymizedValue);
					} else {
						return process.nextTick(callback, null, mysql2.escape(anonymizedValue));
					}
				});
			} else {
				return process.nextTick(callback, null, "''");
			}
		}, function (err, results) {
			if (err) {
				logger.error(err);
				return process.nextTick(callback, err);
			}
			let cols = [], values = [];
			_.forEach(results, function (value, key) {
				cols.push(key);
				values.push(value);
			});
			insertSql = "INSERT INTO `" + table + "` (`" + cols.join("`,`") + "`) VALUES (" + values.join() + ");";
			logger.debug(insertSql);
			sql.push(insertSql);
			return process.nextTick(callback);
		});
	}, function (err) {
		if (err) {
			logger.error(err);
			return process.nextTick(callback, err);
		}
		return process.nextTick(callback, err, sql.join('\n'));
	});
}

module.exports = function (options, done) {
	if (done === undefined)
		done = function () {
		};

	var defaultConnection = {
		host: 'localhost',
		user: 'root',
		password: '',
		database: 'test',
		charset: 'UTF8_GENERAL_CI',
	};

	var defaultOptions = {
		tables: null,
		schema: true,
		data: true,
		ifNotExist: true,
		autoIncrement: true,
		dropTable: false,
		getDump: false,
		dest: './data.sql',
		where: null
	}

	options = extend({}, defaultConnection, defaultOptions, options);

	if (!options.database) throw new Error('Database not specified');

	if (options.anonymizer) {
		anonymizer = options.anonymizer;
		anonymizer.setMatcherHint('*', '*', '*', '*', {});
	}

	// mysql = mysql2.createConnection({
	//     host: options.host,
	//     user: options.user,
	//     password: options.password,
	//     database: options.database,
	//     port: options.port,
	//     charset: options.charset,
	//     socketPath: options.socketPath
	// });
	let mysql = mysql2.createPool({
		connectionLimit : 10,
		host: options.host,
		user: options.user,
		password: options.password,
		database: options.database,
		port: options.port,
		charset: options.charset,
		socketPath: options.socketPath
	});


	async.auto({
		setMatcherHints: (callback) => { setMatcherHints(options.matcherHints, callback) },
		loadConstraints: function (callback) {
			mysql.query(`SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA = '${options.database}' AND (constraint_name = 'PRIMARY' OR REFERENCED_COLUMN_NAME IS NOT NULL);`,
				function (err, data) {
					for (i in data) {
						if (!Array.isArray(constraints[data[i]['TABLE_NAME']])) {
							constraints[data[i]['TABLE_NAME']] = [];
						}
						constraints[data[i]['TABLE_NAME']].push(data[i]['COLUMN_NAME']);
						constraintsFlat.push(`[${data[i]['TABLE_NAME']}].[${data[i]['COLUMN_NAME']}]`);
					}
					callback(err, constraintsFlat);
				});
		},
		getTables: function (callback) {
			if (!options.tables || !options.tables.length) { // if not especifed, get all
				mysql.query("SHOW TABLES FROM `" + [options.database] + "`", function (err, data) {
					if (err) return callback(err);
					var resp = [];
					for (var i = 0; i < data.length; i++) resp.push(data[i]['Tables_in_' + options.database]);
					logger.debug(resp);
					callback(err, resp);
				});
			} else {
				callback(null, options.tables);
			}
		},
		createSchemaDump: ['getTables', function (results, callback) {
			if (!options.schema) {
				callback();
				return;
			}
			var run = [];
			results.getTables.forEach(function (table) {
				run.push(function (callback) {
					mysql.query("SHOW CREATE TABLE `" + table + "`", callback);
				})
			})
			async.parallel(run, function (err, data, fields) {
				if (err) return callback(err);
				var resp = [];
				for (var i in data) {
					var r = data[i][0]['Create Table'] + ";";

					if (options.dropTable) r = r.replace(/CREATE TABLE `/, 'DROP TABLE IF EXISTS `' + data[i][0]['Table'] + '`;\nCREATE TABLE `');
					if (options.ifNotExist) r = r.replace(/CREATE TABLE `/, 'CREATE TABLE IF NOT EXISTS `');
					if (!options.autoIncrement) r = r.replace(/AUTO_INCREMENT=\d+ /g, '');
					logger.debug(r);
					resp.push(r)
				}
				callback(err, resp);
			});
		}],
		createDataDump: ['createSchemaDump', 'loadConstraints', 'setMatcherHints', function (results, callback) {
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
			_.each(tbls, function (table) {
				run.push(function (callback) {
					var opts = {cols: '*', from: "`" + table + "`"};
					let selectSql = 'SELECT * FROM `' + table + '`';
					if ((options.where != null) && (typeof options.where[table] != 'undefined')) {
						opts.where = options.where[table];
						selectSql += ` WHERE ${options.where[table]}`;
					}
					if (options.orderBy) {
						opts.orderBy = '';
						if (typeof options.orderBy === 'string') {
							opts.orderBy = options.orderBy;
						}
						if (typeof options.orderBy['*'] === 'string') {
							opts.orderBy = options.orderBy['*'];
						}
						if (typeof options.orderBy[table] === 'string') {
							opts.orderBy = options.orderBy[table];
						}
						if (opts.orderBy.trim()) {
							selectSql += ` ORDER BY ${opts.orderBy}`;
						}
					}
					if (options.limit) {
						opts.limit = '';
						if (typeof options.limit === 'string') {
							opts.limit = options.limit;
						}
						if (typeof options.limit['*'] === 'string') {
							opts.limit = options.limit['*'];
						}
						if (typeof options.limit[table] === 'string') {
							opts.limit = options.limit[table];
						}
						if (opts.limit.toString().trim()) {
							selectSql += ` LIMIT ${opts.limit}`;
						}
					}
					logger.info(`Dumping table ${table}`);
					logger.debug(selectSql);

					mysql.execute(selectSql, function (err, data) {
						if (err) {
							logger.error(selectSql + ' => ' + err);
							return callback(err);
						}
						return buildInsert(data, table, callback);
					});
				});
			});
			async.parallelLimit(run, 3, callback);
		}],
		getDataDump: ['createSchemaDump', 'createDataDump', 'setMatcherHints', function (results, callback) {
			if (!results.createSchemaDump || !results.createSchemaDump.length) results.createSchemaDump = [];
			if (!results.createDataDump || !results.createDataDump.length) results.createDataDump = [];
			callback(null, results.createSchemaDump.concat(results.createDataDump).join("\n\n"));
		}]
	}, function (err, results) {
		anonymizer.printStats(true);
		if (err) return done(err);
		if (options.getDump) return done(err, results.getDataDump);
		fs.writeFile(options.dest, results.getDataDump, done);
	});
}

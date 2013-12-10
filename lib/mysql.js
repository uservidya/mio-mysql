/**
 * mio-mysql
 *
 * MySQL storage plugin for Mio.
 *
 * @author Alex Mingoia <talk@alexmingoia.com>
 * @link https://github.com/bloodhound/mio-mysql
 */

var async    = require('async')
  , extend   = require('extend')
  , mio      = require('mio')
  , lingo    = require('lingo').en
  , mosql    = require('mongo-sql')
  , mysql    = require('mysql');

module.exports = plugin;

// Expose `mysql` module.
// If you want to access the Model's db connection, use `Model.db` (a pool).
module.exports.mysql = mysql;

var adapter = module.exports.adapter = {};

/**
 * Initialize a new MySQL plugin with given `settings`.
 *
 * options
 *     - maxLimit  Maximum size of query limit parameter (Default: 200).
 *     - tableName MySQL table name for this Model.
 *
 * @param {Object} settings database settings for github.com/felixge/node-mysql
 * @param {Object} options options for this plugin instance
 * @return {Function}
 * @api public
 */

function plugin(settings, options) {
  options = options || {};

  // Models share connection pool through shared settings object
  if (!settings.pool) {
    settings.multipleStatement = true;
    settings.pool = mysql.createPool(settings);
    settings.pool.on('connection', configureConnection);
    process.once('exit', settings.pool.end.bind(settings.pool));
  }

  this.adapter.db = settings.pool;
  this.adapter.settings = settings;

  this.options.tableName = options.tableName;

  if (!this.options.tableName) {
    this.options.tableName = lingo.singularize(this.type.toLowerCase());
  }

  extend(this.options, options);

  this.adapter.buildSQL = adapter.buildSQL.bind(this);
  this.adapter.query = adapter.query.bind(this);
  this.adapter.findAll = adapter.findAll;
  this.adapter.find = adapter.find;
  this.adapter.count = adapter.count;
  this.adapter.removeAll = adapter.removeAll;
  this.adapter.save = adapter.save;
  this.adapter.update = adapter.update;
  this.adapter.remove = adapter.remove;

  this.on('setting', formatAttrs);
  this.on('initializing', formatAttrs);

  var toJSON = this.prototype.toJSON
  this.prototype.toJSON = function() {
    var json = toJSON ? toJSON.call(this) : {};
    if (typeof this.related == 'object') {
      json.related = this.related;
    }
    return json;
  };
};

/**
 * Find all models with given `query`.
 *
 * @param {Object} query
 * @param {Function(err, collection)} callback
 * @api public
 */

adapter.findAll = function(query, callback) {
  if (!query.offset) query.offset = 0;
  if (!query.limit) query.limit = 50;
  if (query.pageSize) {
    query.limit = query.pageSize;
    delete query.pageSize;
  }
  if (query.page) {
    query.offset = query.page * query.limit;
    delete query.page;
  }
  if (query.limit > this.options.maxLimit) {
    query.limit = this.options.maxLimit;
  }
  var Model = this;
  var ids = [];
  var results = [];
  results.limit = Number(query.limit || 50);
  results.offset = Number(query.offset || 0);
  results.total = 0;
  var include = query.include;
  async.series([
    function(next) {
      if (!query.include) return next();
      delete query.include;
      var sql = Model.adapter.buildSQL(extend({
        type: 'select',
        columns: [
          { name: 'id', table: Model.options.tableName }
        ],
        table: Model.options.tableName
      }, query));
      Model.adapter.query({ sql: sql.query }, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        results.total = rows[0]._count;
        for (var len = rows.length, i=0; i<len; i++) {
          ids.push(rows[i].id);
        }
        next();
      });
    },
    function(next) {
      extend(query, {
        type: 'select',
        columns: [
          { name: '*', table: Model.options.tableName }
        ],
        table: Model.options.tableName
      });
      if (include) {
        query.include = include;
        query.where = { id: { $in: ids } };
      }
      var sql = Model.adapter.buildSQL(query);
      Model.adapter.query(sql.query, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        for (var len = rows.length, i=0; i<len; i++) {
          results.push(stripTableName(rows[i], Model.options.tableName));
        }
        if (sql.relations) {
          // Remove dupes because outer join creates dupes
          var ids = {};
          var data = [];
          for (var len = results.length, i=0; i<len; i++) {
            if (!ids[results[i][Model.primaryKey]]) {
              ids[results[i][Model.primaryKey]] = 1;
              data.push(results[i]);
            }
          }
          results.length = 0;
          for (var len = data.length, i=0; i<len; i++) {
            results[i] = data[i];
          }
          for (var plural in sql.relations) {
            var relation = sql.relations[plural];
            for (var l = results.length, ii=0; ii<l; ii++) {
              var model = results[ii];
              model.related = model.related || {};
              model.related[plural] = model.related[plural] || [];
              for (var len = rows.length, i=0; i<len; i++) {
                var foreignkey = rows[i][(relation.through || relation.anotherModel).options.tableName + '_foreign_key'];
                if (foreignkey == model[Model.primaryKey]) {
                  var related = stripTableName(rows[i], relation.anotherModel.options.tableName);
                  model.related[plural].push(related);
                }
              }
            }
          }
        }
        next();
      });
    },
    function(next) {
      delete query.offset;
      delete query.limit;
      extend(query, {
        type: 'select',
        columns: [
          'COUNT(*) as _count'
        ],
        table: Model.options.tableName
      });
      var sql = Model.adapter.buildSQL(query);
      Model.adapter.query({ sql: sql.query }, sql.values, function(err, rows, fields) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        results.total = rows[0]._count;
        next();
      });
    }
  ], function(err) {
    if (err) return callback(err);
    callback(null, results);
  });
};

/**
 * Count models with given `query`.
 *
 * @param {Object} query
 * @param {Function(err, model)} callback
 * @api public
 */

adapter.count = function(query, callback) {
  var Model = this;
  var sql = Model.adapter.buildSQL(extend({
    type: 'select',
    columns: [
      'COUNT(*) as _count'
    ],
    table: Model.options.tableName
  }, query));
  Model.adapter.query({ sql: sql.query }, sql.values, function(err, rows) {
    if (err) return callback(err);
    if (!rows || !rows.length) return callback(null, 0);
    callback(null, rows[0]._count || 0);
  });
};

/**
 * Find model with given `id`.
 *
 * @param {Number|Object} id or query
 * @param {Function(err, model)} callback
 * @api public
 */

adapter.find = function(id, callback) {
  var Model = this;
  var query = typeof id == 'object' ? id : { where: { id: id } };
  var sql = this.adapter.buildSQL(extend({
    type: 'select',
    columns: [
      { name: '*', table: this.options.tableName }
    ],
    table: this.options.tableName
  }, query));
  this.adapter.query(sql.query, sql.values, function(err, rows, fields) {
    if (err) return callback(err);
    if (!rows || !rows.length) {
      var error = new Error("Could not find " + id + ".");
      error.code = error.status = 404;
      return callback(error);
    }
    var model;
    model = stripTableName(rows[0], Model.options.tableName);
    if (sql.relations) {
      model.related = {};
      for (var plural in sql.relations) {
        var relatedModel = sql.relations[plural].anotherModel;
        model.related[plural] = model.related[plural] || [];
        for (var len = rows.length, i=0; i<len; i++) {
          if (!rows[i][relatedModel.options.tableName + '_' + relatedModel.primaryKey]) {
            continue;
          }
          var related = stripTableName(rows[i], relatedModel.options.tableName);
          model.related[plural].push(related);
        }
      }
    }
    callback(null, model);
  });
};

/**
 * Remove all models matching given `query`.
 *
 * @param {Object} query
 * @param {Function(err)} callback
 * @api public
 */

adapter.removeAll = function(query, callback) {
  var sql = this.adapter.buildSQL(extend({
    type: 'delete',
    table: this.options.tableName
  }, query));
  this.adapter.query(sql.query, sql.values, function(err, rows) {
    if (err) return callback(err);
    callback();
  });
};

/**
 * Save.
 *
 * @param {Object} changed
 * @param {Function(err, attributes)} done
 * @api private
 */

adapter.save = function(changed, done) {
  if (this.primary) return adapter.update.call(this, changed, done);
  var model = this;
  var sql = this.constructor.adapter.buildSQL({
    type: 'insert',
    table: this.constructor.options.tableName,
    values: changed
  });
  this.constructor.adapter.query(sql.query, sql.values, function(err, rows, fields) {
    if (err) return done(err);
    var updated = {};
    formatAttrs(model, model.attributes);
    if (rows.insertId) {
      updated[model.constructor.primaryKey] = rows.insertId;
    }
    done(null, updated);
  });
};

/**
 * Update.
 *
 * @param {Object} changed
 * @param {Function(err, attributes)} done
 * @api private
 */

adapter.update = function(changed, done) {
  var model = this;
  var where = {};
  where[this.constructor.primaryKey] = this.primary;
  var sql = this.constructor.adapter.buildSQL({
    type: 'update',
    table: this.constructor.options.tableName,
    where: where,
    values: changed
  });
  this.constructor.adapter.query(sql.query, sql.values, function(err, rows, fields) {
    if (err) return done(err);
    done();
  });
};

/**
 * Remove.
 *
 * @param {Function(err)} done
 * @api private
 */

adapter.remove = function(done) {
  var model = this;
  var query = {
    type: 'delete',
    table: this.constructor.options.tableName,
    where: {}
  };
  query.where[this.constructor.primaryKey] = this.primary;
  var sql = this.constructor.adapter.buildSQL(query);
  this.constructor.adapter.query(sql.query, sql.values, function(err, rows) {
    if (err) return done(err);
    done();
  });
};

/**
 * Wrapper for `Model.db.query`. Transforms column/field names in results.
 */

adapter.query = function(statement, values, callback) {
  var Model = this;

  if (typeof statement == 'string') {
    statement = { sql: statement, nestTables: '_' };
  }

  var after = function(rows, fields) {
    if (rows.length) {
      var keys = Object.keys(fields);
      // Transform colum names
      var columnNamesToAttrNames = {};
      for (var attr in Model.attributes) {
        var columnName = Model.attributes[attr].columnName;
        if (columnName) columnNamesToAttrNames[columnName] = attr;
      }
      var columnNames = Object.keys(columnNamesToAttrNames);
      if (columnNames.length) {
        rows.forEach(function(row, i) {
          columnNames.forEach(function(columnName) {
            var tableColumnName = Model.options.tableName + '_' + columnName;
            if (row[tableColumnName]) {
              rows[i][Model.options.tableName + '_' + attr] = rows[i][tableColumnName];
              delete rows[i][tableColumnName];
            }
          });
        });
      }
      // Transform boolean values
      rows.forEach(function(row, i) {
        for (var key in row) {
          var attr = key.replace(Model.options.tableName + '_', '');
          if (!Model.attributes[attr] || !Model.attributes[attr].type) continue;
          if (Model.attributes[attr].type == 'boolean') {
            row[key] = Boolean(row[key]);
          }
        }
      });
    }
    callback.call(Model, null, rows, fields);
  };

  Model.adapter.db.query(statement, values, function(err, rows, fields) {
    if (err) {
      // Re-try query on DEADLOCK error
      if (~err.message.indexOf('DEADLOCK')) {
        return (function retry() {
          var attemptCount = 0;
          function attempt() {
            attemptCount++;
            Model.adapter.db.query(statement, values, function(err, rows, fields) {
              if (err) {
                if (attemptCount > 3) return callback(err);
                if (~err.message.indexOf('DEADLOCK')) return attempt();
                return callback(err);
              }
              after(rows, fields);
            });
          };
          attempt();
        })();
      }
      return callback(err);
    }
    after(rows, fields);
  });
};

/**
 * Build SQL query using MoSQL.
 *
 * @link https://github.com/goodybag/mongo-sql
 */

adapter.buildSQL = function(query) {
  var extras = prepareQuery(this, query);
  var sql = mosql.sql(query);

  // Convert query column names according to attribute defition.
  for (var attr in this.attributes) {
    var columnName = this.attributes[attr].columnName;
    if (columnName) {
      sql.query = sql.query.replace(
        new RegExp('"' + attr + '"', 'g'),
        '"' + columnName + '"'
      );
    }
  }

  if (extras.included) {
    sql.relations = extras.included;
  }

  return sql;
};

/**
 * Formats attributes when set
 *
 * @param {Model} model
 * @param {Object} attrs
 * @api private
 */

function formatAttrs(model, attrs) {
  for (var attr in attrs) {
    var def = model.constructor.attributes[attr];
    var val = attrs[attr];
    if (!def) continue;
    if (def.type == 'boolean') {
      attrs[attr] = Boolean(attrs[attr]);
    }
    if (def.type == 'date' || def.format == 'date') {
      if (typeof val == 'object') continue;
      if (isNaN(val)) {
        attrs[attr] = new Date(val);
      }
      else {
        attrs[attr] = new Date(val * 1000);
      }
    }
  }
};

/**
 * Prepare query.
 *
 * @param {Model} Model
 * @param {Object} query
 * @return {Object}
 * @api private
 */

function prepareQuery(Model, query) {
  var extras = {};
  var keywords = [];
  for (var key in query) {
    if (query.hasOwnProperty(key) && key.match(/(where|Join)$/)) {
      keywords.push(key);
    }
    if (typeof query[key] == 'string' && !isNaN(query[key])) {
      query[key] = Number(query[key]);
    }
  }
  // If no keywords, assume where query
  if (keywords.length == 0) {
    query.where = {};
    for (var param in query) {
      if (query.hasOwnProperty(param)) {
        if (!param.match(/(include|columns|table|type|values|where|offset|limit|sort|order|groupBy)$/)) {
          query.where[param] = query[param];
          delete query[param];
        }
      }
    }
  }
  if (query.sort) {
    query.order = query.sort;
    delete query.sort;
  }
  // Relations
  var relation, fkWhere;
  if (query.where) {
    for (var key in query.where) {
      for (var plural in Model.relations) {
        if (Model.relations[plural].anotherModel === Model
        &&  Model.relations[plural].foreignKey == key) {
          fkWhere = key;
          relation = Model.relations[plural];
        }
      }
    }
  }
  if (relation) {
    if (relation.through) {
      query.innerJoin = query.innerJoin || {};
      query.innerJoin[relation.through.options.tableName] = {};
      query.innerJoin[relation.through.options.tableName][relation.throughKey] = '$' + Model.options.tableName + '.' + Model.primaryKey + '$';
      query.where[relation.through.options.tableName + '.' + relation.foreignKey] = query.where[fkWhere];
    }
    else {
      query.where[relation.foreignKey] = query.where[fkWhere];
    }
    if (relation.through || relation.foreignKey != fkWhere) {
      delete query.where[fkWhere];
    }
  }
  if (query.include) {
    var include = query.include.split(',');
    delete query.include;
    extras.included = {};
    query.leftOuterJoin = query.leftOuterJoin || {};
    include.forEach(function(relation) {
      relation = Model.relations[relation];
      if (!relation) return;
      if (relation.through) {
        query.leftOuterJoin[relation.through.options.tableName] = {};
        query.leftOuterJoin[relation.anotherModel.options.tableName] = {};
        query.leftOuterJoin[relation.through.options.tableName][relation.foreignKey] = '$' + Model.options.tableName + '.' + Model.primaryKey + '$';
        query.leftOuterJoin[relation.anotherModel.options.tableName][relation.anotherModel.primaryKey] = '$' + relation.through.options.tableName + '.' + relation.throughKey + '$';
      }
      else {
        query.leftOuterJoin[relation.anotherModel.options.tableName] = {};
        query.leftOuterJoin[relation.anotherModel.options.tableName][relation.foreignKey] = '$' + Model.options.tableName + '.' + Model.primaryKey + '$';
      }
      query.columns.push({ name: '*', table: relation.anotherModel.options.tableName });
      query.columns.push({ name: relation.foreignKey, table: (relation.through || relation.anotherModel).options.tableName, as: 'foreign_key' });
      extras.included[relation.as] = relation;
    });
  }
  // Values
  if (query.values) {
    var values = query.values;
    for (var key in values) {
      var def = Model.attributes[key];
      if (def) {
        if (def.dataFormatter) {
          values[key] = def.dataFormatter(values[key], Model);
        }
        else if (def.format == 'date' || def.type == 'date') {
          switch (def.columnType) {
            case 'datetime':
              values[key] = values[key].toISOString();
              break;
            case 'timestamp':
              var d = values[key];
              values[key] = d.getFullYear() + '-' + pad(d.getMonth()) + '-'
                + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':'
                + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
              break;
            case 'integer':
            case 'number':
            default:
              if (values[key].unix) {
                values[key] = Math.floor(values[key].unix());
              }
              else {
                values[key] = Math.floor(values[key].getTime() / 1000);
              }
          }
        }
      }
      else if (typeof values[key] === 'object') {
        values[key] = JSON.stringify(values[key]);
      }
      else if (typeof values[key] === 'boolean') {
        values[key] = values[key] ? 1 : 'NULL';
      }
      else if (values[key] === undefined) {
        delete values[key];
      }
    }
  }
  if (!query.table) query.table = Model.options.tableName;
  if (!query.type) query.type = 'select';
  return extras;
}

/**
 * node-mysql query formatter.
 *
 * node-mysql uses `?` whereas mongo-sql uses `$1, $2, $3...`,
 * so we have to implement our own query formatter assigned
 * when extending the model class.
 *
 * @link https://github.com/felixge/node-mysql#custom-format
 *
 * @param {String} query
 * @param {Array} values
 * @return {String}
 * @api private
 */

function queryFormat(query, values) {
  if (!values || !values.length) return query;
  return query.replace(/\$\d+/g, function(match) {
    var i = Number(String(match).substr(1)) - 1;
    if (values[i] !== undefined) return this.escape(values[i]);
    return match;
  }.bind(this));
};

/**
 * Enable ANSI_QUOTES and set query formatter for new connections.
 *
 * @api private
 */

function configureConnection(connection) {
  // Set query value escape character to `$1, $2, $3..` to conform to
  // mongo-sql's query value escape character.
  connection.config.queryFormat = queryFormat;
  // Enable ANSI_QUOTES for compatibility with queries generated by mongo-sql
  connection.query('SET SESSION sql_mode=ANSI_QUOTES', [], function(err) {
    if (err) throw err;
  });
};

/**
 * Strip given `table` prefix from attribute names.
 *
 * @param {Object} attrs
 * @param {String} table
 * @return {Object}
 * @api private
 */

function stripTableName(attrs, table) {
  var stripped = {};
  for (var attr in attrs) {
    if (attr.indexOf(table + '_') === 0) {
      stripped[attr.replace(table + '_', '')] = attrs[attr];
    }
  }
  return stripped;
};

/**
 * Pad number with leading 0
 */

function pad(num) {
  num = String(num);
  if (num.length == 1) {
    num = '0' + num;
  }
  return num;
};

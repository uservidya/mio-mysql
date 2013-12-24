/**
 * mio-mysql
 *
 * MySQL storage plugin for Mio.
 *
 * @author Alex Mingoia <talk@alexmingoia.com>
 * @link https://github.com/alexmingoia/mio-mysql
 */

var async    = require('async')
  , extend   = require('extend')
  , mio      = require('mio')
  , lingo    = require('lingo').en
  , mosql    = require('mongo-sql')
  , mysql    = require('mysql');

module.exports = plugin;

// Expose `mysql` module.
// If you want to access the Model's db connection, use `Model.db`.
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

  // Models share connection through shared settings object
  if (!settings.connection) {
    settings.connection = connect(settings);
  }

  this.adapter.db = settings.connection;
  this.adapter.settings = settings;

  extend(this.options, options);

  if (!this.options.tableName) {
    this.options.tableName = lingo.singularize(this.type.toLowerCase());
  }

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
  this.on('initializing', addRelated);
};

/**
 * Find all models with given `query`.
 *
 * @param {Object} query
 * @param {Function(err, collection)} callback
 * @api public
 */

adapter.findAll = function(query, callback) {
  if (query.pageSize) {
    query.limit = query.pageSize;
    delete query.pageSize;
  }
  if (!query.limit) query.limit = 50;
  if (query.page) {
    query.offset = (query.page * query.limit) - query.limit;
    delete query.page;
  }
  if (!query.offset) query.offset = 0;
  if (query.limit > this.options.maxLimit) {
    query.limit = this.options.maxLimit;
  }
  var Model = this;
  var ids = [];
  var collection = [];
  collection.offset = Number(query.offset);
  collection.limit = Number(query.limit);
  collection.page = Math.ceil((query.offset + query.limit) / query.limit) || 1;
  collection.pageSize = collection.limit;
  collection.total = 0;
  collection.toJSON = collectionToJSON;
  async.series([
    function(next) {
      var countQuery = extend({}, query);
      extend(countQuery, {
        type: 'select',
        columns: [
          'COUNT(*) as _count'
        ],
        table: Model.options.tableName
      });
      delete countQuery.offset;
      delete countQuery.limit;
      var sql = Model.adapter.buildSQL(countQuery);
      Model.adapter.query({ sql: sql.query }, sql.values, function(err, rows, fields) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        collection.total = rows[0]._count;
        collection.pages = Math.ceil(collection.total / collection.pageSize);
        next();
      });
    },
    function(next) {
      if (!query.include) return next();
      var idQuery = extend({}, query);
      delete idQuery.include;
      var sql = Model.adapter.buildSQL(extend(idQuery, {
        type: 'select',
        columns: [
          { name: 'id', table: Model.options.tableName }
        ],
        table: Model.options.tableName
      }));
      Model.adapter.query({ sql: sql.query }, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        for (var len = rows.length, i=0; i<len; i++) {
          ids.push(rows[i].id);
        }
        next();
      });
    },
    function(next) {
      var include;
      extend(query, {
        type: 'select',
        columns: [
          { name: '*', table: Model.options.tableName }
        ],
        table: Model.options.tableName
      });
      if (query.include) {
        include = query.include.split(',');
        query.where = { id: { $in: ids } };
      }
      var sql = Model.adapter.buildSQL(query);
      Model.adapter.query(sql.query, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        if (!include) {
          for (var len = rows.length, i=0; i<len; i++) {
            collection.push(stripTableName(rows[i], Model));
          }
          return next();
        }
        collections = {};
        Model.relations.forEach(function(relation) {
          if (!~include.indexOf(relation.as) || relation.model != Model) {
            return;
          }
          var foreignKey = (relation.through || relation.anotherModel).options.tableName + '_foreign_key';
          var related_collection = collections[relation.as] = {};
          for (var len = rows.length, i=0; i<len; i++) {
            var id = rows[i][Model.options.tableName + '_' + Model.primaryKey];
            if (!related_collection[id]) {
              var model = stripTableName(rows[i], Model);
              model.related = {};
              collection.push(model);
              related_collection[id] = [];
            }
            var related_id = rows[i][foreignKey];
            if (related_id) {
              var related = stripTableName(
                transformColumnNames(rows[i], relation.anotherModel, true),
                relation.anotherModel
              );
              related_collection[related_id].push(related);
            }
          }
          for (var len = collection.length, i=0; i<len; i++) {
            collection[i].related[relation.as] = related_collection[collection[i][Model.primaryKey]] || [];
          }
        });
        next();
      });
    }
  ], function(err) {
    if (err) return callback(err);
    callback(null, collection);
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
  var sql = Model.adapter.buildSQL(extend(query, {
    type: 'select',
    columns: [
      'COUNT(*) as _count'
    ],
    table: Model.options.tableName
  }));
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
  var query = id && typeof id == 'object' ? id : { where: { id: id } };
  var include;
  if (query.include) {
    include = query.include.split(',');
  }
  var sql = this.adapter.buildSQL(extend(query, {
    type: 'select',
    columns: [
      { name: '*', table: this.options.tableName }
    ],
    table: this.options.tableName
  }));
  this.adapter.query(sql.query, sql.values, function(err, rows, fields) {
    if (err) return callback(err);
    if (!rows || !rows.length) {
      return callback();
    }
    var model = stripTableName(rows[0], Model);
    if (include) {
      model.related = {};
      Model.relations.forEach(function(relation) {
        if (!~include.indexOf(relation.as) || relation.model != Model) {
          return;
        }
        var relatedModel = relation.anotherModel;
        model.related[relation.as] = model.related[relation.as] || [];
        for (var len = rows.length, i=0; i<len; i++) {
          if (!rows[i][relatedModel.options.tableName + '_' + relatedModel.primaryKey]) {
            continue;
          }
          var related = stripTableName(
            transformColumnNames(
              rows[i], relatedModel, true
            ),
            relatedModel
          );
          model.related[relation.as].push(related);
        }
      });
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
 * Wrapper for `Model.db.query`.
 *
 * Transforms results and retries on deadlock.
 */

adapter.query = function(statement, values, callback) {
  var Model = this;

  if (typeof statement == 'string') {
    statement = { sql: statement, nestTables: '_' };
  }

  var after = function(rows, fields) {
    if (rows && rows.length) {
      rows.forEach(function(row, i) {
        for (var key in row) {
          var attr = key.replace(Model.options.tableName + '_', '');
          if (!Model.attributes[attr] || !Model.attributes[attr].type) continue;
          // Transform boolean values
          if (Model.attributes[attr].type == 'boolean') {
            row[key] = Boolean(row[key]);
          }
        }
        // Transform names using attribute definition's .columnName property
        var prefix;
        if (statement.nestTables) {
          prefix = Model.options.tableName;
        }
        rows[i] = transformColumnNames(rows[i], Model, prefix);
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
  prepareQuery(this, query);
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

  return sql;
};

/**
 * Add any related models from the response body.
 */

function addRelated(model, attrs) {
  if (typeof attrs.related === 'object') {
    model.related = {};
    for (var as in attrs.related) {
      var relations = model.constructor.relations;
      for (var len = relations.length, i=0; i<len; i++) {
        var params = relations[i];
        if (params.as == as) {
          hydrate(attrs.related[as], relations[i].anotherModel);
          model.related[as] = attrs.related[as];
        }
      }
    }
  }
};

/**
 * Hydrate collection of model data.
 */

function hydrate(collection, Model) {
  for (var len = collection.length, i=0; i<len; i++) {
    collection[i] = new Model(collection[i]);
  }
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
  var keywords = [];
  var where = query.where;
  for (var param in query) {
    if (!query.hasOwnProperty(param)) continue;
    if (!param.match(/(updates|join|joins|include|columns|table|type|values|where|offset|limit|sort|order|groupBy)$/i)) {
      where = where || {};
      where[param] = query[param];
      delete query[param];
    }
  }
  query.where = where;
  for (var param in query) {
    if (!query.hasOwnProperty(param)) continue;
    if (typeof query[param] == 'string' && !isNaN(query[param])) {
      query[param] = Number(query[param]);
    }
  }
  if (query.where) {
    for (var param in query.where) {
      if (!query.where.hasOwnProperty(param)) continue;
      if (typeof query.where[param] == 'string' && !isNaN(query.where[param])) {
        query.where[param] = Number(query.where[param]);
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
      Model.relations.forEach(function(params) {
        if (params.anotherModel === Model && params.foreignKey == key) {
          fkWhere = key;
          relation = params;
        }
      });
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
    query.leftOuterJoin = query.leftOuterJoin || {};
    include.forEach(function(name) {
      relation = null;
      Model.relations.forEach(function(params) {
        if (params.as === name && params.model === Model) {
          relation = params;
        }
      });
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
 * Transform column names to attribute names
 */

function transformColumnNames(row, Model, prefix) {
  prefix = prefix ? Model.options.tableName + '_' : '';
  var converted = {};
  columnsToAttributes = {};
  for (var key in Model.attributes) {
    if (Model.attributes[key].columnName) {
      columnsToAttributes[Model.attributes[key].columnName] = key;
    }
  }
  for (var key in row) {
    var attr = key;
    if (key.indexOf(prefix) === 0) {
      attr = key.replace(prefix, '');
    }
    if (columnsToAttributes[attr]) {
      converted[prefix + columnsToAttributes[attr]] = row[key];
      delete row[key];
    }
    else {
      converted[key] = row[key];
    }
  }
  return converted;
};

/**
 * Create node-mysql connection
 */

function connect(settings) {
  var connection = mysql.createConnection(settings);

  // Set query value escape character to `$1, $2, $3..` to conform to
  // mongo-sql's query value escape character.
  connection.config.queryFormat = queryFormat;

  connection.connect(function(err) {
    if (err) {
      setTimeout(function() {
        connect(settings);
      }, 2000);
    }
  });

  // Enable ANSI_QUOTES for compatibility with queries generated by mongo-sql
  connection.query('SET SESSION sql_mode=ANSI_QUOTES', []);

  connection.on('error', function(err) {
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      connect(settings);
    }
    else {
      throw err;
    }
  });

  return connection;
};

/**
 * Strip given `table` prefix from attribute names.
 *
 * @param {Object} attrs
 * @param {String} table
 * @return {Object}
 * @api private
 */

function stripTableName(attrs, Model) {
  var table = Model.options.tableName;
  var stripped = {};
  for (var attr in attrs) {
    if (attr.indexOf(table + '_') === 0 || attr == '_count') {
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

/**
 * Format collection for JSON transport.
 */

function collectionToJSON() {
  var collection = [];

  for (var len = this.length, i=0; i<len; i++) {
    collection[i] = this[i];
  }

  return {
    collection: collection,
    offset:     this.offset,
    limit:      this.limit,
    page:       this.page,
    pages:      this.pages,
    pageSize:   this.pageSize,
    total:      this.total
  };
};

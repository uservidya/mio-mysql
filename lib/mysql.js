/**
 * mio-mysql
 *
 * MySQL storage plugin for Mio.
 *
 * @author Alex Mingoia <talk@alexmingoia.com>
 * @link https://github.com/alexmingoia/mio-mysql
 */

var async    = require('async')
  , mio      = require('mio')
  , lingo    = require('lingo').en
  , mosql    = require('mongo-sql')
  , mysql    = require('mysql');

module.exports = plugin;

/**
 * Expose `mysql` module. If you want to access the Model's database
 * connection, use `Model.adapter.settings.db`.
 */

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
  return function() {
    // Models share connection through shared settings object
    if (!settings.db) {
      connect(settings);
    }

    this.adapter = {
      settings: settings,
      queryToSQL: adapter.queryToSQL.bind(this),
      query: adapter.query.bind(this),
      findAll: adapter.findAll,
      find: adapter.find,
      count: adapter.count,
      removeAll: adapter.removeAll,
      save: adapter.save,
      update: adapter.update,
      remove: adapter.remove
    };

    merge(this.options, options || {});

    if (!this.options.tableName) {
      this.options.tableName = lingo.singularize(this.type.toLowerCase());
    }

    this.on('initializing', addRelated);
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
  var limit = Number(query.pageSize || query.limit || 50);
  var offset = Number(query.page ? (query.page * limit) - limit : query.offset || 0);
  if (limit > this.options.maxLimit) {
    limit = this.options.maxLimit;
  }
  var Model = this;
  var ids = [];
  var collection = [];
  collection.offset = offset;
  collection.limit = limit;
  collection.page = Math.ceil((offset + limit) / limit) || 1;
  collection.pageSize = limit;
  collection.total = 0;
  collection.toJSON = collectionToJSON;
  async.series([
    function(next) {
      var sql = Model.adapter.queryToSQL(merge(merge({
        type: 'count',
        columns: [
          'COUNT(*) as count'
        ],
        table: Model.options.tableName
      }, query), { offset: null, limit: null }));
      Model.adapter.query(sql.query, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        collection.total = rows[0]._count;
        collection.pages = Math.ceil(collection.total / collection.pageSize);
        next();
      });
    },
    function(next) {
      if (!query.include) return next();
      var sql = Model.adapter.queryToSQL(merge(merge({
        type: 'select',
        columns: [
          { name: 'id', table: Model.options.tableName }
        ],
        table: Model.options.tableName,
      }, query), { offset: offset, limit: limit }));
      Model.adapter.query(sql.query, sql.values, function(err, rows) {
        if (err) return next(err);
        for (var len = rows.length, i=0; i<len; i++) {
          ids.push(stripTableName(rows[i], Model).id);
        }
        next();
      });
    },
    function(next) {
      var sql = Model.adapter.queryToSQL(merge(merge({
        type: 'select',
        columns: [
          { name: '*', table: Model.options.tableName }
        ],
        table: Model.options.tableName
      }, merge(ids.length ? { id: { $in: ids } } : {}, query)), {
        offset: offset,
        limit: limit
      }));
      Model.adapter.query(sql.query, sql.values, function(err, rows) {
        if (err) return next(err);
        if (!rows || !rows.length) return next();
        if (!query.include) {
          for (var len = rows.length, i=0; i<len; i++) {
            collection.push(stripTableName(rows[i], Model));
          }
          return next();
        }
        collections = {};
        Model.relations.forEach(function(relation) {
          if (!~query.include.indexOf(relation.as) || relation.model != Model) return;
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
              formatRowValues(rows[i], relation.anotherModel, true);
              var row = transformRowColumns(rows[i], relation.anotherModel, true);
              var related = stripTableName(row, relation.anotherModel);
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
  ],
  function(err) {
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
  var sql = this.adapter.queryToSQL(merge({
    type: 'select',
    columns: [ 'COUNT(*) as _count' ],
    table: this.options.tableName
  }, query));

  this.adapter.query(sql.query, sql.values, function(err, rows) {
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
  var query = typeof id == 'object' ? id : { where: { id: id } };
  var sql = this.adapter.queryToSQL(merge({
    type: 'select',
    columns: [
      { name: '*', table: this.options.tableName }
    ],
    table: this.options.tableName
  }, query));

  this.adapter.query(sql.query, sql.values, function(err, rows) {
    if (err) return callback(err);
    if (!rows || !rows.length) return callback();

    var model = stripTableName(rows[0], this);

    if (query.include) {
      model.related = {};

      for (var len = this.relations.length, i=0; i<len; i++) {
        var relation = this.relations[i];
        if (~query.include.indexOf(relation.as) && relation.model == this) {
          model.related[relation.as] = model.related[relation.as] || [];
          for (var len = rows.length, i=0; i<len; i++) {
            var anotherModel = relation.anotherModel;
            var relatedTable = anotherModel.options.tableName;
            var foreignKey = relatedTable + '_' + anotherModel.primaryKey;
            if (rows[i][foreignKey]) {
              formatRowValues(rows[i], anotherModel, true);
              var row = transformRowColumns(rows[i], anotherModel, true);
              var related = stripTableName(row, anotherModel);
              model.related[relation.as].push(related);
            }
          }
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
  var sql = this.adapter.queryToSQL(merge({
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
  var sql = this.constructor.adapter.queryToSQL({
    type: 'insert',
    table: this.constructor.options.tableName,
    values: changed
  });
  this.constructor.adapter.query(sql.query, sql.values, function(err, rows) {
    if (err) return done(err);
    var updated = {};
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
  var sql = this.constructor.adapter.queryToSQL({
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
  var sql = this.constructor.adapter.queryToSQL(query);
  this.constructor.adapter.query(sql.query, sql.values, function(err, rows) {
    if (err) return done(err);
    done();
  });
};

/**
 * Wrapper for `mysql.query`. Transforms results and retries on deadlock.
 */

adapter.query = function(statement, values, callback) {
  var Model = this;

  if (typeof statement == 'string') {
    statement = { sql: statement, nestTables: '_' };
  }

  var after = function(rows, fields) {
    if (rows && rows.length) {
      rows.forEach(function(row, i) {
        // Transform names using attribute definition's .columnName property
        rows[i] = transformRowColumns(row, Model, !!statement.nestTables);
        // Format row values
        formatRowValues(rows[i], Model, !!statement.nestTables);
      });
    }
    callback.call(Model, null, rows, fields);
  };

  Model.adapter.settings.db.query(statement, values, function(err, rows, fields) {
    if (err) {
      // Re-try query on DEADLOCK error
      if (~err.message.indexOf('DEADLOCK')) {
        return (function retry() {
          var attemptCount = 0;
          function attempt() {
            attemptCount++;
            Model.adapter.settings.db.query(statement, values, function(err, rows, fields) {
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

var queryKeywords = [
  'updates',
  'joins',
  'include',
  'columns',
  'table',
  'values',
  'where',
  'offset',
  'limit',
  'sort',
  'order',
  'page',
  'pageSize',
  'type',
  'groupBy'
];

/**
 * Process deserialized request query and return mongo-sql object.
 *
 * @link https://github.com/goodybag/mongo-sql/
 */

adapter.queryToSQL = function(query) {
  var Model = this;

  // Don't modify original query
  query = JSON.parse(JSON.stringify(query));

  // Merge top-level parameters into `query.where`
  for (var param in query) {
    if (!~queryKeywords.indexOf(param)) {
      query.where = query.where || {};
      query.where[param] = query[param];
      delete query[param];
    }
  }

  // `query.sort` is alias for `query.order`
  if (query.sort) {
    query.order = query.sort;
    delete query.sort;
  }

  /**
   * Find query parameters that match foreign key for model relation,
   * and transform to inner join if relation uses an intermediary model.
   */

  var relation;
  if (query.where) {
    for (var key in query.where) {
      Model.relations.forEach(function(params) {
        if (params.anotherModel === Model && params.foreignKey == key) {
          relation = params;
        }
      });
    }
  }
  if (relation && relation.through) {
    var table = Model.options.tableName;
    var throughTable = relation.through.options.tableName;
    var primaryKey = '$' + table + '.' + Model.primaryKey + '$';
    var id = query.where[relation.foreignKey];
    var join = {
      type: 'inner',
      target: throughTable,
      on: {}
    };
    join.on[relation.throughKey] = primaryKey;
    query.where[throughTable + '.' + relation.foreignKey] = id;
    query.joins = query.joins || [];
    query.joins.push(join);
    delete query.where[relation.foreignKey];
  }

  /**
   * Include related models in results. Related models are added to
   * `model.related[relation]`.
   */

  if (query.include && query.type != 'count') {
    if (typeof query.include == 'string') {
      query.include = query.include.split(',');
    }
    query.include.forEach(function(name) {
      relation = null;
      Model.relations.forEach(function(params) {
        if (params.as === name && params.model === Model) {
          relation = params;
        }
      });
      if (!relation) return;
      query.joins = query.joins || [];
      var joinRelated = {
        type: 'left',
        target: relation.anotherModel.options.tableName,
        on: {}
      };
      if (relation.through) {
        var joinThrough = {
          type: 'left',
          target: relation.through.options.tableName,
          on: {}
        };
        joinThrough.on[relation.foreignKey] = '$' + Model.options.tableName + '.' + Model.primaryKey + '$';
        joinRelated.on[Model.primaryKey] = '$' + relation.through.options.tableName + '.' + relation.throughKey + '$';
        query.joins.push(joinThrough);
      }
      else {
        joinRelated.on[relation.foreignKey] = '$' + Model.options.tableName + '.' + Model.primaryKey + '$';
      }
      query.joins.push(joinRelated);
      query.columns.push({ name: '*', table: relation.anotherModel.options.tableName });
      query.columns.push({ name: relation.foreignKey, table: (relation.through || relation.anotherModel).options.tableName, as: 'foreign_key' });
    });
  }

  formatQueryValues(query, Model);

  transformQueryColumns(query, Model);

  if (query.type == 'count') query.type = 'select';

  return mosql.sql(query);
};

/**
 * Transform query attribute names to column names.
 */

function transformQueryColumns(query, Model, attrName) {
  var attributes = Model.attributes;
  for (var key in query) {
    var attr = attributes[key];
    // Found attribute.. transform
    if (attr && attr.columnName) {
      query[attr.columnName] = query[key];
      delete query[key];
    }
    // Recurse objects/arrays
    if (typeof query[key] === 'object') {
      transformQueryColumns(query[key], Model);
    }
  }
};

/**
 * Format query values
 */

function formatQueryValues(query, Model, needle) {
  if (!query) return;

  for (var key in query) {
    var attr = Model.attributes[needle || key];
    var val = query[key];

    if (!needle && attr && attr.dataFormatter) {
      query[key] = attr.dataFormatter(val, Model);
      continue;
    }

    var attrType = attr ? (attr.type || attr.format) : attr;
    var queryType = typeof val;

    switch (attrType) {
      case 'date':
        if (queryType != 'object') {
          if (queryType == 'boolean') break;
          val = String(val);
          if (isNaN(val)) {
            if (!isNaN(val.substr(0, 4))) {
              val = new Date(val);
            }
            else { break; }
          }
          else {
            if (val.length === 4 || val.length > 10) {
              val = new Date(val);
            }
            else if (val.length === 10) {
              val = new Date(val * 1000);
            }
            else { break; }
          }
        }
        if (val.toISOString) {
          switch (attr.columnType) {
            case 'datetime':
              val = val.toISOString();
              break;
            case 'timestamp':
              var d = val;
              val = d.getFullYear() + '-' + pad(d.getMonth()) + '-'
                + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':'
                + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
              break;
            case 'integer':
            case 'number':
            default:
              val = Math.floor(val.valueOf() / 1000);
          }
        }
        else {
          formatQueryValues(val, Model, needle || key);
        }
        break;
      case 'array':
      case 'object':
      case 'json':
        val = JSON.stringify(val);
        break;
      default:
        if (queryType == 'object') {
          formatQueryValues(val, Model);
        }
        else if (queryType == 'boolean') {
          val = val ? 1 : 'NULL';
        }
    }
    query[key] = val;
  }
};

/**
 * Format row values
 */

function formatRowValues(row, Model, prefix) {
  prefix = prefix ? Model.options.tableName + '_' : '';
  for (var key in row) {
    var attr = Model.attributes[key.replace(prefix, '')];
    var val = row[key];
    var type = attr ? (attr.type || attr.format) : attr;
    // Transform boolean values
    if (type == 'boolean') {
      row[key] = Boolean(val);
    }
    // Transfoorm date values
    if (type == 'date' && (typeof val == 'string' || typeof val == 'number')) {
      row[key] = new Date(isNaN(val) ? val : val * 1000);
    }
  }
};

/**
 * Add and hydrate any related models from the response body.
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
 * node-mysql query formatter.
 *
 * node-mysql uses `?` whereas mongo-sql uses `$1, $2, $3...`,
 * so we have to implement our own query formatter assigned
 * when extending the model class.
 *
 * @link https://github.com/felixge/node-mysql#custom-format
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
 * Create node-mysql connection, and reconnect when disconnected.
 */

function connect(settings) {
  // Models share connection through shared settings object
  var db = settings.db = mysql.createConnection(settings);

  // Set query value escape character to `$1, $2, $3..` to conform to
  // mongo-sql's query value escape character.
  db.config.queryFormat = queryFormat;

  db.connect(function(err) {
    if (err) {
      setTimeout(function() {
        connect(settings);
      }, 2000);
    }
  });

  // Enable ANSI_QUOTES for compatibility with queries generated by mongo-sql
  db.query('SET SESSION sql_mode=ANSI_QUOTES', []);

  db.on('error', function(err) {
    if (err.code === 'PROTOCOL_CONNECTION_LOST') {
      connect(settings);
    }
    else {
      throw err;
    }
  });
};

/**
 * Transform column names in mysql result row to attribute names using
 * the `Model.attributes[attr].columnName` property.
 */

function transformRowColumns(row, Model, prefix) {
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
      delete converted[key];
    }
    else {
      converted[key] = row[key];
    }
  }
  return converted;
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

/**
 * Hydrate collection of model data.
 */

function hydrate(collection, Model) {
  for (var len = collection.length, i=0; i<len; i++) {
    collection[i] = new Model(collection[i]);
  }
};

/**
 * Strip given `table_` prefix from attribute names.
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
 * Merge properties from `b` into `a`.
 */

function merge(a, b) {
  for (var key in b) {
    a[key] = b[key];
  }
  return a;
};

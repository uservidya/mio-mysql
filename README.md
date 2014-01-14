# mio-mysql

[![Build Status](https://secure.travis-ci.org/alexmingoia/mio-mysql.png?branch=master)](http://travis-ci.org/alexmingoia/mio-mysql)
[![Coverage Status](https://coveralls.io/repos/alexmingoia/mio-mysql/badge.png?branch=master)](https://coveralls.io/r/alexmingoia/mio-mysql?branch=master)
[![NPM version](https://badge.fury.io/js/mio.png)](http://badge.fury.io/js/mio)
[![Dependency Status](https://david-dm.org/alexmingoia/mio-mysql.png)](http://david-dm.org/alexmingoia/mio-mysql)

MySQL storage plugin for [Mio][0].

## Installation

```sh
npm install mio-mysql
```

## API

See the [Mio][0] documentation for query methods like find, save, etc.

### exports(settings, options)

Create a new Mio mysql plugin with the given database `settings` and `options`.

`settings` same as settings for
[node-mysql](https://github.com/felixge/node-mysql/)

`options`
* `tableName` The table for this model. Defaults to singularized model name.
* `maxLimit` The maximum number of records to select at once. Default is 200.

```javascript
var mio = require('mio');

var User = mio.createModel('User');

User.use('server', 'mio-mysql', {
  database: 'mydb',
  user: 'root'
});
```

### Queries

The query syntax is a subset of [mongo-sql][1]. The `type`, `columns`,
and `table` properties are handled by mio-mysql.

### Pagination

Both `offset` and `limit`, and `page` and `pageSize` query parameters are
supported.

The collection returned by `Model.findAll()` has pagination properties on the
array:

```javascript
User.findAll({ page: 1, pageSize: 25 }, function(err, users) {
  console.log(users);
  // => [user1, user2, user3, ...]

  console.log(users.total);
  // => 73

  console.log(users.pages);
  // => 3

  console.log(users.page);
  // => 1

  console.log(users.pageSize);
  // => 25

  console.log(users.offset);
  // => 0

  console.log(users.limit);
  // => 25
});
```

### Custom table names

Custom table names are specified using the `tableName` option. For example:

```javascript
User.use(mysql({
  database: 'mydb',
  user: 'root'
}, {
  tableName: 'users'
}));
```

### Custom column names

Custom field names are provided by a `columnName` property in the attribute
definition. For example:

```javascript
User
  .attr('id')
  .attr('firstName', {
    type: 'string',
    length: 255,
    columnName: 'first_name'
  })
  .attr('lastName', {
    type: 'string',
    length: 255,
    columnName: 'last_name'
  });
```

### Date types

Attributes with `type: "date"` will be handled based on the `columnType`
property. This property can either be "datetime", "timestamp", or "integer",
corresponding to MySQL column type. If not specified, mio-mysql will assume
"integer".

### Data formatters

If you need to control exactly how a data-type is determined, set the attribute
definition's `dataFormatter` function:

```javascript
var Event = mio.createModel('Event');

Event.attr('date', { dataFormatter: function(value, Event) {
  value = Math.floor(value.getTime() / 1000);
  return value;
});
```

### Database connection

mio-mysql utilizes node-mysql's connection pool.

Models that share a settings object will share a connection pool, exposed via
`settings.pool`.

```javascript
var settings = {
  database: 'mydb',
  user: 'root'
};

// Both User and Post models will share the same connection.
User.use('server', 'mio-mysql', settings);
Post.use('server', 'mio-mysql', settings);

console.log(settings.pool);
// => node-mysql connection pool object...
```

### exports.mysql

[MySQL](https://github.com/felixge/node-mysql) module.

## Tests

Tests are written with [mocha](https://github.com/visionmedia/mocha) and
[should](https://github.com/visionmedia/should.js) using BDD-style assertions.

Run the tests with npm:

```sh
npm test
```

## MIT Licensed

[0]: https://github.com/alexmingoia/mio/
[1]: https://github.com/goodybag/mongo-sql/

/**
 * mio-mysql tests.
 */

var should = require('should');
var mio = require('mio');
var mysql = process.env.JSCOV ? require('../lib-cov/mysql') : require('../lib/mysql');

var settings = {};

mysql.adapter.createPool = function(settings) {
  return {
    on: function(event, callback) {},
    end: function() {},
    query: function(statement, values, done) {
      done(null, [], {});
    }
  };
};

describe('module', function(done) {
  it('exports plugin factory', function(done) {
    should.exist(mysql);
    mysql.should.be.a('function');
    done();
  });

  it('constructs new mysql plugins', function(done) {
    var plugin = mysql, settings;
    should.exist(plugin);
    plugin.should.be.a('function');
    done();
  });
});

describe('plugin', function() {
  it('exposes db connection on model', function(done) {
    var User = mio.createModel('User').attr('id').attr('name');
    User.use(mysql, settings);
    should.exist(User.adapter.db);
    done();
  });
});

describe('Model', function() {
  var User, Post;

  beforeEach(function(done) {
    User = mio.createModel('User').attr('id', { primary: true }).attr('name');
    Post = mio.createModel('Post').attr('id', { primary: true }).attr('title').attr('user_id');
    User.use(mysql, settings);
    Post.use(mysql, settings);
    done();
  });

  describe('.findAll()', function() {
    it('finds all models successfully', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        statement.sql.should.include(
          'from "user" where "user"."id" = $1 or "user"."name" = $2'
        );
        for (var key in userA.attributes) {
          userA.attributes[User.options.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.tableName + '_' + key] = userB.attributes[key];
        }
        callback(null, [userA.attributes, userB.attributes], userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }},
        function(err, found) {
          User.adapter.db.query = query;
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.pop().primary.should.equal(userB.primary);
          done();
        }
      );
    });

    it('supports limit and offset pagination parameters', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        User.adapter.db.query = function(statement, values, callback) {
          User.adapter.db.query = query;
          values.should.be.instanceOf(Array);
          values.should.have.property(2, 25);
          values.should.have.property(3, 75);
          statement.sql.should.include(
            'from "user" where "user"."id" = $1 ' +
            'or "user"."name" = $2 limit $3 offset $4'
          );
          callback(null, [userA.attributes, userB.attributes], userB.attributes);
        };
        for (var key in userA.attributes) {
          userA.attributes[User.options.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.tableName + '_' + key] = userB.attributes[key];
        }
        callback(null, [{ _count: 107 }], userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }, limit: 25, offset: 75 },
        function(err, found) {
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.should.have.property('limit', 25);
          found.should.have.property('offset');
          found.pop().primary.should.equal(userB.primary);
          done();
        }
      );
    });

    it('supports page and pageSize pagination parameters', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        User.adapter.db.query = function(statement, values, callback) {
          User.adapter.db.query = query;
          values.should.be.instanceOf(Array);
          values.should.have.property(2, 25);
          values.should.have.property(3, 75);
          statement.sql.should.include(
            'from "user" where "user"."id" = $1 ' +
            'or "user"."name" = $2 limit $3 offset $4'
          );
          callback(null, [userA.attributes, userB.attributes], userB.attributes);
        };
        for (var key in userA.attributes) {
          userA.attributes[User.options.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.tableName + '_' + key] = userB.attributes[key];
        }
        callback(null, [{ _count: 107 }], userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }, page: 4, pageSize: 25 },
        function(err, found) {
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.should.have.property('page', 4);
          found.should.have.property('pages', 5);
          found.should.have.property('pageSize', 25);
          found.pop().primary.should.equal(userB.primary);
          done();
        }
      );
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        callback(new Error('error finding users.'));
      };
      User.all(
        { where: { $or: { id: user.primary, name: "alex" }}},
        function(err, found) {
          User.adapter.db.query = query;
          should.exist(err);
          err.should.have.property('message', 'error finding users.');
          done();
        }
      );
    });

    it("uses attribute definition's columnName in queries", function(done) {
      User = mio.createModel('User')
        .attr('id', { primary: true })
        .attr('fullname', {
          type: 'string',
          length: 255,
          columnName: 'name'
        });
      User.use(mysql, settings);
      var user = new User({ fullname: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        User.adapter.db.query = query;
        statement.sql.should.equal(
          'insert into "user" ("name") values ($1)'
        );
        cb(null, { insertId: 1 }, {});
      };
      user.save(function(err) {
        if (err) return done(err);
        user.should.have.property('fullname');
        user.fullname.should.equal('alex');
        done();
      });
    });
  });

  describe('.find()', function() {
    it('finds model by id successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        statement.sql.should.equal(
          'select "user".* from "user" where "user"."id" = $1'
        );
        User.adapter.db.query = query;
        for (var key in user.attributes) {
          user.attributes[User.options.tableName + '_' + key] = user.attributes[key];
        }
        cb(null, [user.attributes], {});
      };
      User.find(user.primary, function(err, found) {
        if (err) return done(err);
        should.exist(found);
        user.primary.should.equal(found.primary);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        callback(new Error('error finding user.'));
      };
      User.find(user.primary, function(err, found) {
        User.adapter.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error finding user.');
        done();
      });
    });
  });

  describe('.count()', function() {
    it('counts models successfully', function(done) {
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        statement.sql.should.equal(
          'select COUNT(*) as _count from "user" where "user"."name" = $1'
        );
        cb(null, [{_count: 3}], {});
      };
      User.count({ name: 'alex' }, function(err, count) {
        User.adapter.db.query = query;
        if (err) return done(err);
        should.exist(count);
        count.should.equal(3);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        cb(new Error("error removing all models."));
      };
      User.count({ name: 'alex' }, function(err) {
        User.adapter.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error removing all models.');
        done();
      });
    });
  });

  describe('.removeAll()', function() {
    it('removes models successfully', function(done) {
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        statement.sql.should.equal(
          'delete from "user" where "user"."name" = $1'
        );
        cb(null, {}, {});
      };
      User.removeAll({ name: 'alex' }, function(err) {
        User.adapter.db.query = query;
        if (err) return done(err);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        cb(new Error("error removing all models."));
      };
      User.removeAll({ name: 'alex' }, function(err) {
        User.adapter.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error removing all models.');
        done();
      });
    });
  });

  describe('#save()', function() {
    it('saves new model successfully', function(done) {
      var user = new User({name: 'alex'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        User.adapter.db.query = query;
        values.should.include('alex');
        cb(null, { insertId: 1 }, {});
      };
      user.save(function(err) {
        should.not.exist(err);
        should.exist(user.primary);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        User.adapter.db.query = query;
        callback(new Error('error saving user.'));
      };
      user.save(function(err) {
        should.exist(err);
        err.should.have.property('message', 'error saving user.');
        done();
      });
    });
  });

  describe('#update()', function() {
    it('updates model successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      user.dirtyAttributes.length = 0;
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        User.adapter.db.query = query;
        statement.sql.should.equal(
          'update "user" set "name" = $1 where "user"."id" = $2'
        );
        values.should.include('jeff', 1);
        cb(null, [user], user.attributes);
      };
      user.name = 'jeff';
      user.save(function(err) {
        should.not.exist(err);
        user.name.should.equal('jeff');
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        callback(new Error('error updating user.'));
      };
      user.save(function(err) {
        User.adapter.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error updating user.');
        done();
      });
    });
  });

  describe('#remove', function() {
    it('removes model successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, cb) {
        User.adapter.db.query = query;
        statement.sql.should.equal(
          'delete from "user" where "user"."id" = $1'
        );
        values.should.include(1);
        cb(null, [], {});
      };
      user.remove(function(err) {
        should.not.exist(err);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ id: 1, name: 'alex' });
      var query = User.adapter.db.query;
      User.adapter.db.query = function(statement, values, callback) {
        User.adapter.db.query = query;
        callback(new Error('error removing user.'));
      };
      user.remove(function(err) {
        should.exist(err);
        err.should.have.property('message', 'error removing user.');
        done();
      });
    });
  });
});

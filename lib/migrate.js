'use strict';

const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const util = require('util');
const async = require('async');

require('dotenv').config({
  path: path.resolve(__dirname, '..', '..', '.env'),
  silent: true
});

process.env.TZ = 'UTC';

const config = {
  db: { url: process.env.DB_URL },
  redis: { url: process.env.REDIS_URL }
};

const MIGRATIONS_ROOT = path.join(__dirname, '..', '..', 'migrations');

const EXEC_ACTION = ['up', 'down'].indexOf(process.argv[2]) > -1 ? process.argv[2] : 'up';

buildContext(config, (err, context) => {
  if (err) {
      console.log(err);
      process.exit(1);
    }

  readMigrationFiles((err, files) => {
      if (err) {
            logger.error(err);
            process.exit(1);
          }
  
      let tasks = [
            async.apply(checkExecutedMigrations, context, files),
            async.apply(execution, context)
          ];
  
      async.waterfall(tasks, (err, results) => {
            if (err) {
                    logger.error(err);
                  }
      
            logger.info(`Executed Migrations: ${results.length}`);
            process.exit(0);
          });
    });
});

function readMigrationFiles (callback) {
  fs.readdir(path.resolve(MIGRATIONS_ROOT), (err, files) => {
      if (err) {
            return callback(err);
          }
  
      let migrations = files
        .filter((file) => {
                  return /\.js$/.test(file);
              })
        .map((migration) => {
                let migrationFile = require(path.resolve(MIGRATIONS_ROOT, migration));
        
                migrationFile.name = migration;
        
                return migrationFile;
              });
  
      return callback(null, migrations);
    });
}

function checkExecutedMigrations (dbContext, migrations, callback) {
  const migrationsName = migrations.map((migration) => migration.name);
  const conditions = {
      name: { $in:  migrationsName },
      action: EXEC_ACTION
    };

  dbContext
    .db
    .collection('migrations')
    .find(conditions)
    .toArray((err, results) => {
          if (err) {
                  return callback(err);
                }
    
          _.remove(migrations, function(n) {
                    return _.find(results, { name: n.name});
                });
    
          return callback(null, migrations);
        });
}

function execution (db, migrations, callback) {
  function iteratee (migration, cb) {
  
      logger.info(`${EXEC_ACTION.toUpperCase()} - ${migration.name}: Running`);
  
      const tasks = {
            execute: async.apply(migration[EXEC_ACTION], db),
            storeExecuted: async.apply(storeExecutedMigration, db, migration)
          };
  
      return async.series(tasks, (err, results) => {
            if (err) {
                    logger.debug(`Error on migration: ${migration.name}`);
                    return cb(err);
                  }
      
            logger.info(`${EXEC_ACTION.toUpperCase()} - ${migration.name}: Done`);
      
            return cb(null, migration.name);
      
          });
    }

  return async.mapSeries(migrations, iteratee, callback);
}

function storeExecutedMigration (dbContext, migration, callback) {
  const data = {
      name: migration.name,
      action: EXEC_ACTION,
      executed_at: new Date()
    };

  const filter = { name: migration.name };

  const opts = { upsert: true };

  dbContext
    .db
    .collection('migrations')
    .updateOne(filter, data, opts, (err, res) => {
          if (err) {
                  return callback(err);
                }
    
          if (res.lastErrorObject && res.lastErrorObject.n !== 1) {
                  return callback(new Error(`Cannot inserted this migration: ${migration.name}`))
                }
    
          return callback(null, res.value);
    
        });
}

function inspect (obj) {
  return util.inspect(obj, { depth: null });
}

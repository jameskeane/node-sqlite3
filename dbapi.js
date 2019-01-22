const sqlite3 = require('./lib/sqlite3');

// for type parsing
const PARSE_NONE = 0;
const PARSE_DECLTYPES = 1;
const PARSE_COLNAMES = 2;


const registered_converters = {};
const registered_adapters = new WeakMap();


class ExecuteIterator extends Promise {
  [Symbol.asyncIterator]() {
    return {
      next: () => this.then((cursor) => cursor.fetchone())
                      .then((row) => ({
                        value: row,
                        done: !row
                      }))
    };
  }
}


class Row {
  constructor(cols) {
    this.__cols = cols;

    for (let i = 0; i < cols.length; i++) {
      this[i] = cols[i].value;
      this[cols[i].name] = cols[i].value;
    }
  }

  slice(begin, end) {
    return new Row(this.__cols.slice(begin, end));
  }

  toArray() {
    return this.__cols.map((col) => col.value);
  }
}


class Cursor {
  constructor(db, detect_types) {
    this._active_stmt = null;
    this._wrapper = db;
    this._db = db._db;
    this._detect_types = detect_types;
    this._wrapper._register_cursor(this);
  }

  [Symbol.asyncIterator]() {
    return {
      next: () => this.fetchone().then((row) => ({
        value: row,
        done: !row
      }))
    };
  }

        // int type = sqlite3_column_type(stmt, i);
        // const char* name = sqlite3_column_name(stmt, i);


  execute(sql, params=[]) {
    const self = this;

    // map params
    params = params.map((p) => {
      if (typeof p !== 'object') return p;

      const adapter = registered_adapters.get(p.constructor);
      if (!adapter) return p;
      return adapter(p);
    });

    return new ExecuteIterator((resolve, reject) => {
      this._finalize_active()
        .then(() => {
          const stmt = this._db
              .prepare(sql, params)
              .run(function(err) {
                if (err) {
                  reject(err);
                  return;
                }
                // self._db.total_changes = this.changes;
                self.lastrowid = this.lastID;
              })
              .reset((err) => {  // seems like a but that I have to reset
                if (err) {
                  reject(err);
                  return;
                }

                self._active_stmt = stmt;
                resolve(self);
              });
        })
        .catch((err) => reject(err));
    });
  }

  async fetchone() {
    if (!this._active_stmt) return null;

    return new Promise((resolve, reject) => {
      this._active_stmt.get((err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve(this._convertRow(row));
        }
      });
    });
  }

  async fetchmany(count) {
    if (!this._active_stmt) return null;

    // todo implement this in c++
    const result = [];
    for (let i = 0; i < count; i++) {
      const row = await this.fetchone();
      if (!row) break;
      result.push(row);
    }
    return result;
  }

  async fetchall() {
    if (!this._active_stmt) return null;

    return new Promise((resolve, reject) => {
      this._active_stmt.all((err, rows) => {
        if (err) {
          reject(err);
        } else {
          // todo transform
          resolve(rows.map((row) => this._convertRow(row)));
        }
      });
    });
  }

  async close() {
    await this._finalize_active();
  }

  async _finalize_active() {
    if (!this._active_stmt) return Promise.resolve();

    return new Promise((resolve, reject) => {
      this._active_stmt.finalize((err) => {
        this._wrapper._unregister_cursor(this);
        this._active_stmt = null;

        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  _convertRow(row) {
    if (!row) return row;

    const converted = [];
    for (let { name, decltype, value } of row) {
      let converted_value = value;
      let colname_type = null;
      if (this._detect_types|PARSE_DECLTYPES && decltype in registered_converters) {
        converted_value = registered_converters[decltype](value.toString());
      }

      if ((converted_value === null) && this._detect_types|PARSE_COLNAMES) {
        colname_type = parse_colname_type(name);
        if (colname_type && colname_type in registered_converters) {
          converted_value = registered_converters[colname_type](value.toString());
        }
      }

      converted.push({ name, value: converted_value, decltype, colname_type });
    }

    return new Row(converted);
  }
}


class DatabaseWrapper {
  constructor(db, options) {
    this._db = db;
    this.detect_types = options.detect_types;
    this._isolation_level = null;
    this._begin_statement = null;
    this._cursors = new Set();
  }

  cursor(factory=Cursor) {
    return new factory(this, this.detect_types);
  }

  async _init() {
    await this.set_isolation_level('');
    return this;
  }

  async close() {
    for (let cursor of this._cursors) {
      try {
        await cursor.close();
      } catch (e) {
        console.error(e);
      }
    }
    this._cursors.clear();

    return new Promise((resolve, reject) => {
      this._db.close((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      })
    })
  }

  get isolation_level() {
    return this._isolation_level;
  }

  async set_isolation_level(isolation_level) {
    if (isolation_level === null) {
      this._isolation_level = null;
      await this.commit();
    } else {
      this._isolation_level = isolation_level;
      this._begin_statement = `BEGIN ${isolation_level}`;
    }
  }

  async commit() {
    if (this._db.autocommit) return;
    await this.execute('COMMIT');
  }

  async begin() {
    await this.execute(this._begin_statement);
  }

  async execute(sql, params=[]) {
    const cursor = this.cursor();
    await cursor.execute(sql, params);
    await cursor.close();
  }

  async executemany(sql, rows=[]) {
    const cursor = this.cursor();
    for (let row of rows) {
      await cursor.execute(sql, row);
    }
    await cursor.close();
  }

  async rollback() {
    await _exec_helper(this._db, 'ROLLBACK');
    await _exec_helper(this._db, 'BEGIN');
  }

  _register_cursor(cursor) {
    this._cursors.add(cursor);
  }

  _unregister_cursor(cursor) {
    this._cursors.delete(cursor);
  }
}


function connect(opts) {
  return new Promise((resolve, reject) => {
    const db = opts.mode ?
        new sqlite3.Database(opts.database, opts.mode, done) :
        new sqlite3.Database(opts.database, done); // lib checks on arg length :(

    function done(err) {
      if (err) {
        reject(err);
        return;
      }

      db.serialize(() => {
        const wrapper = new DatabaseWrapper(db, opts);
        wrapper._init().then(resolve, reject);
      });
    }
  });
}

function register_adapter(cls, adapter) {
  registered_adapters.set(cls, adapter);
}

function register_converter(type, converter) {
  registered_converters[type] = converter;
}

function parse_colname_type(colname) {
  const colname_regex = /^([^ ]+)( \[([^\]]+)\])?$/;
  const match = colname.match(colname_regex);

  if (match) {
    return match[3];
  }

  return null;
}



function _exec_helper(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}


module.exports = {
  ...{PARSE_NONE, PARSE_COLNAMES, PARSE_DECLTYPES },
  Cursor,
  connect, register_converter, register_adapter
};

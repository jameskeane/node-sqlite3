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
}


class Cursor {
  constructor(db, detect_types) {
    this._active_stmt = null;
    this._db = db;
    this._detect_types = detect_types;
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
          // todo transform
          resolve(this._convertRow(row));
        }
      });
    });
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
  }

  cursor(factory=Cursor) {
    return new factory(this._db, this.detect_types);
  }

  async commit() {
    await _exec_helper(this._db, 'COMMIT');
    await _exec_helper(this._db, 'BEGIN');
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
        db.exec('BEGIN', (err) => {
          if (err) {
            reject(err);
          } else {
            resolve(new DatabaseWrapper(db, opts));
          }
        });
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

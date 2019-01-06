const sqlite3 = require('../dbapi');


class Point {
  constructor(x, y) {
    this.x = x;
    this.y = y;
  }
}

sqlite3.register_adapter(Point, (point) => {
  return `${point.x};${point.y}`;
});

sqlite3.register_converter('point', (s) => {
  const parts = s.split(';').map(n => parseFloat(n));
  return new Point(...parts);
});



async function start() {
  const db = await sqlite3.connect(':memory:', {
    detect_types: sqlite3.PARSE_COLNAMES
  });

  console.log('connected');

  await db.execute('CREATE TABLE foo (p)');
  await db.executemany('INSERT INTO foo (p) values (?)', [
    [new Point(1, 2)],
    [new Point(5.5, 3.3)]
  ]);

  console.log('created table');

  const cur1 = db.cursor();

  await cur1.execute('SELECT p as "p [point]" FROM foo');
  console.log('cur1', await cur1.fetchall());
}

start()
    .catch((err) => {
      console.error(err);
    });

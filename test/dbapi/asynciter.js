const sqlite3 = require('../dbapi');


async function start() {
  const db = await sqlite3.connect(':memory:');

  await db.execute('CREATE TABLE foo (num int)');
  await db.executemany('INSERT INTO foo (num) values (?)', [
    [1],
    [2],
    [3]
  ]);
  const cur1 = db.cursor();

  for await (const row of cur1.execute('SELECT * FROM foo')) {
    console.log(row);
  }
}

start()
    .catch((err) => {
      console.error(err);
    });

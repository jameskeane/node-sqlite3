const sqlite3 = require('../dbapi');


async function start() {
  const db = await sqlite3.connect(':memory:');
  console.log('connected');

  await db.execute('CREATE TABLE foo (num int)');
  await db.executemany('INSERT INTO foo (num) values (?)', [
    [1],
    [2],
    [3]
  ]);

  console.log('created table');

  const cur1 = db.cursor();
  const cur2 = db.cursor();
  // await cur1.execute('SELECT * FROM foo');
  // console.log(await cur1.fetchall());

  await cur1.execute('SELECT * FROM foo');
  console.log('cur1', await cur1.fetchone());

  await cur2.execute('SELECT * FROM foo');
  console.log('cur2', await cur2.fetchone());

  console.log('cur1', await cur1.fetchone());
  console.log('cur2', await cur2.fetchone());
  console.log('cur1', await cur1.fetchone());
  console.log('cur2', await cur2.fetchone());

  console.log('cur1', await cur1.fetchone());
  console.log('cur2', await cur2.fetchone());
}

start()
    .catch((err) => {
      console.error(err);
    });

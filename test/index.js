const fs = require('fs');

const test = require('basictap');

const createDriver = require('../');

test('get: no records', async t => {
  t.plan(1);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  const result = await driver.get('tests');

  await driver.close();

  t.deepEqual(result, []);
});

test('post: records', async t => {
  t.plan(3);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  const postResult = await driver.post('tests', { a: 1 });
  const getResult = await driver.get('tests');

  await driver.close();

  t.ok(postResult.id, 'has id');
  t.deepEqual(postResult.a, 1);
  t.deepEqual(getResult, [postResult]);
});

test('get: records - with projection', async t => {
  t.plan(1);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  await Promise.all([
    driver.post('tests', { a: 1, b: 'yes' }),
    driver.post('tests', { a: 2, b: 'yes' }),
    driver.post('tests', { a: 3, b: 'yes' })
  ]);

  let result = await driver.get('tests', null, ['a']);
  result = result.sort((a, b) => a.a >= b.a ? 1 : -1);

  await driver.close();

  t.deepEqual(result, [
    { a: 1, id: result[0].id },
    { a: 2, id: result[1].id },
    { a: 3, id: result[2].id }
  ]);
});

test('put: record', async t => {
  t.plan(2);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  await Promise.all([
    driver.post('tests', { a: 1, b: 'yes' }),
    driver.post('tests', { a: 2, b: 'yes' }),
    driver.post('tests', { a: 3, b: 'yes' })
  ]);

  const { changes } = await driver.put('tests', { b: 'no' }, {});

  let result = await driver.get('tests');
  result = result.sort((a, b) => a.a >= b.a ? 1 : -1);

  await driver.close();

  t.equal(changes, 3);
  t.deepEqual(result, [
    { b: 'no', id: result[0].id },
    { b: 'no', id: result[1].id },
    { b: 'no', id: result[2].id }
  ]);
});

test('patch: record', async t => {
  t.plan(2);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  await Promise.all([
    driver.post('tests', { a: 1, b: 'yes' }),
    driver.post('tests', { a: 2, b: 'yes' }),
    driver.post('tests', { a: 3, b: 'yes' })
  ]);

  const { changes } = await driver.patch('tests', { b: 'no' }, {});

  let result = await driver.get('tests');
  result = result.sort((a, b) => a.a >= b.a ? 1 : -1);

  await driver.close();

  t.equal(changes, 3);
  t.deepEqual(result, [
    { a: 1, b: 'no', id: result[0].id },
    { a: 2, b: 'no', id: result[1].id },
    { a: 3, b: 'no', id: result[2].id }
  ]);
});

test('del: record', async t => {
  t.plan(2);

  await fs.promises.rmdir('./_tmpTestData', { recursive: true });

  const driver = createDriver({
    options: {
      dataDirectory: './_tmpTestData'
    }
  });

  await Promise.all([
    driver.post('tests', { a: 1, b: 'yes' }),
    driver.post('tests', { a: 2, b: 'yes' }),
    driver.post('tests', { a: 3, b: 'yes' })
  ]);

  const { changes } = await driver.del('tests', { a: 2 });

  let result = await driver.get('tests');
  result = result.sort((a, b) => a.a >= b.a ? 1 : -1);

  await driver.close();

  t.equal(changes, 1);
  t.deepEqual(result, [
    { a: 1, b: 'yes', id: result[0].id },
    { a: 3, b: 'yes', id: result[1].id }
  ]);
});

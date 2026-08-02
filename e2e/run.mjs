import { main as concurrency } from './concurrency.mjs';
import { main as crash } from './crash.mjs';
import { main as slowConsumer } from './slow-consumer.mjs';

const scenarios = [
  ['multi-instance concurrency', concurrency],
  ['power-cut & restart', crash],
  ['slow consumer', slowConsumer],
];

let failed = 0;
for (const [name, fn] of scenarios) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed++;
    console.error(`FAIL ${name}`);
    console.error(err);
  }
}
if (failed > 0) {
  console.error(`${failed} e2e scenario(s) failed`);
  process.exit(1);
}
console.log('all e2e scenarios passed');

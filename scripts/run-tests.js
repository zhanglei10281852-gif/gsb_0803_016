// Cross-version test discovery: collects compiled *.test.js files under a
// directory and hands them to node:test. Works on Node 20 (no --test glob
// support) and newer alike. Usage: node scripts/run-tests.js <dir>
"use strict";
const { spawnSync } = require("node:child_process");
const { readdirSync, statSync } = require("node:fs");
const { join } = require("node:path");

const rel = process.argv[2];
if (!rel) {
  console.error("usage: run-tests.js <build-test-subdir>");
  process.exit(2);
}
const root = join(process.cwd(), rel);

function collect(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    if (statSync(full).isDirectory()) out.push(...collect(full));
    else if (name.endsWith(".test.js")) out.push(full);
  }
  return out;
}

let files;
try {
  files = collect(root).sort();
} catch (err) {
  console.error(`no tests found under ${root}: ${err.message}`);
  process.exit(2);
}
if (files.length === 0) {
  console.error(`no *.test.js files under ${root}`);
  process.exit(2);
}

const timeout = process.env.TEST_TIMEOUT || "180000";
const args = ["--test", "--test-reporter=spec", `--test-timeout=${timeout}`, ...files];
const res = spawnSync(process.execPath, args, { stdio: "inherit" });
process.exit(res.status === null ? 1 : res.status);

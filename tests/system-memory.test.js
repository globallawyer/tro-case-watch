import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { getAvailableSystemMemoryMb, parseLinuxMemAvailableMb } from "../src/system-memory.js";

test("Linux memory pressure uses MemAvailable instead of only MemFree", () => {
  const meminfo = [
    "MemTotal:        4000000 kB",
    "MemFree:          700000 kB",
    "MemAvailable:    3200000 kB",
    "Cached:          2500000 kB"
  ].join("\n");

  assert.equal(parseLinuxMemAvailableMb(meminfo), 3125);
});

test("available memory falls back when Linux meminfo cannot be read", () => {
  const missingPath = path.join(os.tmpdir(), `missing-meminfo-${process.pid}-${Date.now()}`);
  assert.equal(fs.existsSync(missingPath), false);
  assert.equal(getAvailableSystemMemoryMb({
    platform: "linux",
    meminfoPath: missingPath,
    fallbackBytes: 768 * 1024 * 1024
  }), 768);
});

import fs from "node:fs";
import os from "node:os";

export function parseLinuxMemAvailableMb(meminfo = "") {
  const match = String(meminfo || "").match(/^MemAvailable:\s+(\d+)\s+kB$/im);
  if (!match) {
    return null;
  }

  const kilobytes = Number(match[1]);
  return Number.isFinite(kilobytes) && kilobytes >= 0
    ? Number((kilobytes / 1024).toFixed(1))
    : null;
}

export function getAvailableSystemMemoryMb({
  platform = process.platform,
  meminfoPath = "/proc/meminfo",
  fallbackBytes = os.freemem()
} = {}) {
  if (platform === "linux") {
    try {
      const availableMb = parseLinuxMemAvailableMb(fs.readFileSync(meminfoPath, "utf8"));
      if (availableMb !== null) {
        return availableMb;
      }
    } catch {}
  }

  return Number((Math.max(Number(fallbackBytes || 0), 0) / (1024 * 1024)).toFixed(1));
}

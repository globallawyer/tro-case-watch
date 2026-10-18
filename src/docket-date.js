const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

function calendarDate(year, month, day) {
  const y = Number(year), m = Number(month), d = Number(day);
  if (y < 1900 || y > new Date().getUTCFullYear() + 1 || m < 1 || m > 12 || d < 1) return null;
  const value = new Date(Date.UTC(y, m - 1, d));
  if (value.getUTCMonth() !== m - 1 || value.getUTCDate() !== d) return null;
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

export function parseDocketDate(value) {
  const text = String(value || "").trim();
  const iso = text.match(/\b(\d{4})-(\d{1,2})-(\d{1,2})(?!\d)/);
  if (iso) return calendarDate(iso[1], iso[2], iso[3]);
  const numeric = text.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (numeric) return calendarDate(numeric[3], numeric[1], numeric[2]);
  const named = text.match(/\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+(\d{1,2}),?\s+(\d{4})\b/i);
  return named ? calendarDate(named[3], MONTHS.indexOf(named[1].slice(0, 3).toLowerCase()) + 1, named[2]) : null;
}

export function normalizeDocketFiledAt(value) {
  const text = String(value || "").trim();
  const date = parseDocketDate(text);
  if (!date) return null;
  if (/^\d{4}-\d{2}-\d{2}T/.test(text)) return Number.isFinite(Date.parse(text)) ? text : null;
  return date;
}

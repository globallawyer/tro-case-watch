export function getDocketContentStatus(entry = {}) {
  const text = String(entry.description || entry.description_zh || "").trim();
  if (!text) return "missing";
  const plain = text.replace(/^\[|\]$/g, "").replace(/\s+/g, " ").trim();
  const type = String(entry.document_type || "").trim();
  if (/^\[[^\[\]\n]+\]$/.test(text) ||
      (type && plain.toLowerCase() === type.toLowerCase()) ||
      /^(?:docket entry(?:\s+\d+)?|complaint|summons|temporary restraining order|leave to file document|motion|order|notice|judgment|minute entry)$/i.test(plain)) {
    return "title-only";
  }
  return "descriptive";
}

export function docketContentScore(entry) {
  return ({ missing: 0, "title-only": 1, descriptive: 2 })[getDocketContentStatus(entry)];
}

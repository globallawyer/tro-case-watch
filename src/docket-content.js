import { getRssDocketEvidence } from "./docket-rss.js";

export const TITLE_ONLY_DOCKET_LABELS = [
  "docket entry", "complaint", "summons", "temporary restraining order",
  "leave to file document", "motion", "order", "notice", "judgment", "minute entry",
  "civil cover sheet", "cover sheet", "corporate disclosure statement",
  "certificate of service", "return of service", "notice of appearance",
  "seal document", "file excess pages", "order on motion for leave to file",
  "summons issued", "notice of motion for default judgment", "case assigned", "clerk's notice"
];

export function getDocketContentStatus(entry = {}) {
  const text = String(entry.description || entry.description_zh || "").trim();
  if (!text) return "missing";
  const mainText = text.split(/\s+(?:附件[:：]|Attachments?:)/i)[0].trim();
  const plain = mainText.replace(/^\[|\]$/g, "").replace(/\s+/g, " ").trim();
  const type = String(entry.document_type || "").trim();
  const shortLabel = String(entry.raw?.short_description || "").replace(/\s+/g, " ").trim();
  const shortOnly = entry.primary_source === "courtlistener" && shortLabel &&
    shortLabel.toLowerCase() === plain.toLowerCase() && !String(entry.raw?.description || "").trim();
  if (/^\[[^\[\]\n]+\]$/.test(mainText) ||
      getRssDocketEvidence(entry) ||
      shortOnly ||
      (type && plain.toLowerCase() === type.toLowerCase()) ||
      TITLE_ONLY_DOCKET_LABELS.includes(plain.toLowerCase()) ||
      /^docket entry\s+\d+$/i.test(plain)) {
    return "title-only";
  }
  return "descriptive";
}

export function docketContentScore(entry) {
  return ({ missing: 0, "title-only": 1, descriptive: 2 })[getDocketContentStatus(entry)];
}

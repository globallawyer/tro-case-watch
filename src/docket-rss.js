// Mirror table positions are not court document numbers. Only explicit RSS
// document evidence may supply a number for these notification-only records.
export function getRssDocketEvidence(entry = {}) {
  const text = String(entry.description || entry.description_zh || "").trim();
  const numberedNotice = text.match(/[（(]法院\s*RSS\s*更新[，,]\s*第\s*(\d+)\s*条[）)]/i);
  if (numberedNotice) return { number: String(Number(numberedNotice[1])) };
  const marker = text.match(/^\[court-rss:([^\]]+)\]/i);
  if (marker) {
    const number = String(entry.raw?.documentNumber || "").trim();
    return { number: /^[1-9]\d*$/.test(number) ? number : "", notificationId: marker[1] };
  }
  return null;
}

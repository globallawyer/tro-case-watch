export function normalizeDocketSequenceValue(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }

  const compact = text.replace(/,+/g, "");
  if (/^\d+(?:\.\d+)?$/.test(compact)) {
    const numeric = Number.parseFloat(compact);
    if (Number.isFinite(numeric)) {
      return String(numeric);
    }
  }

  return compact.replace(/^0+(?=\d)/, "");
}

export function getDocketSequenceValue(entry = {}) {
  const raw = entry.raw || {};
  const documentType = raw.document_type ?? raw.documentType;
  const isAttachment = Number(documentType) === 2 ||
    Number(raw.attachment_number || raw.attachmentNumber || 0) > 0;
  if (isAttachment && !normalizeDocketSequenceValue(entry.entry_number)) {
    return "";
  }
  return (
    normalizeDocketSequenceValue(entry.entry_number) ||
    normalizeDocketSequenceValue(entry.document_number) ||
    ""
  );
}

export function getIntegerDocketSequence(entry = {}) {
  const value = getDocketSequenceValue(entry);
  if (!/^\d+$/.test(value)) {
    return null;
  }

  const number = Number.parseInt(value, 10);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function normalizeUpperBound(value) {
  const text = normalizeDocketSequenceValue(value);
  if (!/^\d+$/.test(text)) {
    return 0;
  }

  const number = Number.parseInt(text, 10);
  return Number.isFinite(number) && number > 0 ? number : 0;
}

export function analyzeDocketSequence(entries = [], { upperBound = 0, maxRanges = 25 } = {}) {
  const sequenceValueCount = entries.filter((entry) => Boolean(getDocketSequenceValue(entry))).length;
  const numberedValues = entries
    .map((entry) => getIntegerDocketSequence(entry))
    .filter((value) => value !== null);
  const numbers = [...new Set(numberedValues)].sort((left, right) => left - right);
  const lastNumber = numbers.at(-1) || 0;
  const effectiveUpperBound = Math.max(normalizeUpperBound(upperBound), lastNumber);
  const missingRanges = [];
  let missingCount = 0;
  let missingRangeCount = 0;
  let nextExpected = 1;

  const appendRange = (start, end) => {
    if (end < start) {
      return;
    }

    const count = end - start + 1;
    missingCount += count;
    missingRangeCount += 1;
    if (missingRanges.length < Math.max(Number(maxRanges || 0), 0)) {
      missingRanges.push({ start, end, count });
    }
  };

  for (const number of numbers) {
    if (number > effectiveUpperBound) {
      break;
    }
    if (number > nextExpected) {
      appendRange(nextExpected, number - 1);
    }
    nextExpected = Math.max(nextExpected, number + 1);
  }
  if (effectiveUpperBound >= nextExpected) {
    appendRange(nextExpected, effectiveUpperBound);
  }

  return {
    numberedEntryCount: sequenceValueCount,
    uniqueNumberCount: numbers.length,
    unnumberedEntryCount: Math.max(entries.length - sequenceValueCount, 0),
    firstNumber: numbers[0] || 0,
    lastNumber,
    upperBound: effectiveUpperBound,
    missingCount,
    missingRanges,
    missingRangesTruncated: missingRangeCount > missingRanges.length,
    isComplete: numbers.length > 0 && missingCount === 0
  };
}

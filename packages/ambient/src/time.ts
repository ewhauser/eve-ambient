export function addMs(timestamp: string, milliseconds: number): string {
  if (!Number.isSafeInteger(milliseconds) || milliseconds <= 0) {
    throw new TypeError("milliseconds must be a positive safe integer");
  }
  const start = Date.parse(timestamp);
  if (!Number.isFinite(start) || new Date(start).toISOString() !== timestamp) {
    throw new TypeError("timestamp must be a canonical ISO timestamp");
  }
  return new Date(start + milliseconds).toISOString();
}

import type { JsonValue } from "./types.js";

/**
 * Produces the repository's canonical JSON representation.
 *
 * Objects are key-sorted recursively while array order remains significant.
 * Values outside the durable JSON model are rejected instead of being
 * silently coerced by `JSON.stringify`.
 *
 * This module intentionally imports no Node built-ins so the same canonical
 * representation can be used across Node and remote runtime realms.
 */
export function canonicalJson(value: unknown, name = "value"): string {
  const seen = new Set<object>();
  const normalize = (current: unknown, path: string): JsonValue => {
    if (current === null || typeof current === "string" || typeof current === "boolean") {
      return current;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) throw new TypeError(`${path} must contain finite numbers`);
      return current;
    }
    if (typeof current !== "object") {
      throw new TypeError(`${path} must be JSON-safe; received ${typeof current}`);
    }
    if (seen.has(current)) throw new TypeError(`${path} must not contain circular references`);
    seen.add(current);
    try {
      if (Array.isArray(current)) {
        const keys = Object.keys(current);
        if (
          keys.length !== current.length ||
          keys.some((key, index) => key !== String(index))
        ) {
          throw new TypeError(`${path} arrays must not contain holes or named properties`);
        }
        return current.map((item, index) => normalize(item, `${path}[${index}]`));
      }
      const prototype = Object.getPrototypeOf(current);
      // Remote runtimes may deserialize JSON into another realm. Their ordinary
      // Object.prototype is not reference-equal to this realm's prototype, so
      // recognize that shape without admitting class instances.
      const ordinaryCrossRealmObject =
        prototype !== null &&
        Object.getPrototypeOf(prototype) === null &&
        Object.prototype.hasOwnProperty.call(prototype, "constructor") &&
        typeof prototype.constructor === "function" &&
        prototype.constructor.name === "Object";
      if (
        prototype !== Object.prototype &&
        prototype !== null &&
        !ordinaryCrossRealmObject
      ) {
        throw new TypeError(`${path} must contain only plain JSON objects`);
      }
      // A null prototype preserves JSON keys such as "__proto__" as data
      // instead of invoking Object.prototype's legacy setter.
      const output = Object.create(null) as Record<string, JsonValue>;
      for (const key of Object.keys(current as Record<string, unknown>).sort()) {
        output[key] = normalize((current as Record<string, unknown>)[key], `${path}.${key}`);
      }
      return output;
    } finally {
      seen.delete(current);
    }
  };
  return JSON.stringify(normalize(value, name));
}

export function assertJsonValue(value: unknown, name = "value"): asserts value is JsonValue {
  canonicalJson(value, name);
}

export function cloneJson<T extends JsonValue>(value: T): T {
  return JSON.parse(canonicalJson(value)) as T;
}

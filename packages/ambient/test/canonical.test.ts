import { runInNewContext } from "node:vm";
import { describe, expect, it } from "vitest";
import { canonicalJson } from "../src/canonical.js";

describe("canonicalJson", () => {
  it("accepts ordinary JSON objects deserialized in another realm", () => {
    const value = runInNewContext('({ nested: { answer: 42 }, values: [true, null] })');

    expect(canonicalJson(value)).toBe(
      '{"nested":{"answer":42},"values":[true,null]}',
    );
  });

  it("still rejects class instances", () => {
    class Value {
      readonly answer = 42;
    }

    expect(() => canonicalJson(new Value())).toThrow("plain JSON objects");
  });
});

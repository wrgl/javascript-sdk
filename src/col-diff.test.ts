import { ColDiff, Column, longestIncreasingList, moveOps } from "./col-diff";

test("longestIncreasingList", () => {
  expect(longestIncreasingList([])).toStrictEqual([]);
  expect(longestIncreasingList([0])).toStrictEqual([0]);
  expect(longestIncreasingList([0, 1])).toStrictEqual([0, 1]);
  expect(longestIncreasingList([1, 0])).toStrictEqual([0]);
  expect(longestIncreasingList([2, 0, 1])).toStrictEqual([1, 2]);
  expect(longestIncreasingList([1, 2, 0])).toStrictEqual([0, 1]);
  expect(longestIncreasingList([1, 0, 2])).toStrictEqual([0, 2]);
  expect(longestIncreasingList([0, 1, 2])).toStrictEqual([0, 1, 2]);
  expect(longestIncreasingList([2, 1, 0])).toStrictEqual([1]);
  expect(longestIncreasingList([0, 4, 5, 1, 2, 3])).toStrictEqual([0, 3, 4, 5]);
  expect(longestIncreasingList([0, 4, 5, 1, 2])).toStrictEqual([0, 1, 2]);
  expect(longestIncreasingList([4, 5, 0, 2, 1, 3])).toStrictEqual([2, 3, 5]);
});

test("moveOps", () => {
  expect(moveOps([])).toStrictEqual([]);
  expect(moveOps([0])).toStrictEqual([]);
  expect(moveOps([0, 1])).toStrictEqual([]);
  expect(moveOps([0, 1, 2])).toStrictEqual([]);
  expect(moveOps([1, 0])).toStrictEqual([{ old: 0, new: 1 }]);
  expect(moveOps([1, 0, 2])).toStrictEqual([{ old: 0, new: 1 }]);
  expect(moveOps([1, 2, 0])).toStrictEqual([{ old: 0, new: 2 }]);
  expect(moveOps([2, 1, 0])).toStrictEqual([
    { old: 2, new: 0 },
    { old: 0, new: 2 },
  ]);
  expect(moveOps([0, 1, 2, 5, 3, 4])).toStrictEqual([{ old: 5, new: 3 }]);
  expect(moveOps([2, 1, 4, 5, 3, 0])).toStrictEqual([
    { old: 1, new: 1 },
    { old: 3, new: 4 },
    { old: 0, new: 5 },
  ]);
});

test.each([
  [
    { columns: ["a"], pk: [] },
    { columns: ["a"], pk: [] },
    [new Column({ name: "a", baseIdx: 0, layerIdx: { 0: 0 } })],
  ],
  [
    { columns: ["a"], pk: [] },
    { columns: ["b"], pk: [] },
    [
      new Column({
        name: "a",
        baseIdx: 0,
        layerIdx: {},
        removed: new Set([0]),
      }),
      new Column({ name: "b", layerIdx: { 0: 0 }, added: new Set([0]) }),
    ],
  ],
  [
    { columns: ["a", "b"], pk: [] },
    { columns: ["a", "b"], pk: [] },
    [
      new Column({ name: "a", baseIdx: 0, layerIdx: { 0: 0 } }),
      new Column({ name: "b", baseIdx: 1, layerIdx: { 0: 1 } }),
    ],
  ],
  [
    { columns: ["a", "b"], pk: [] },
    { columns: ["b", "a"], pk: [] },
    [
      new Column({ name: "b", baseIdx: 1, layerIdx: { 0: 0 } }),
      new Column({
        name: "a",
        baseIdx: 0,
        layerIdx: { 0: 1 },
        moved: { 0: { before: 0 } },
      }),
    ],
  ],
  [
    { columns: ["a", "b", "c"], pk: [] },
    { columns: ["c", "a"], pk: [] },
    [
      new Column({ name: "c", baseIdx: 2, layerIdx: { 0: 0 } }),
      new Column({
        name: "a",
        baseIdx: 0,
        layerIdx: { 0: 1 },
        moved: { 0: { before: 0 } },
      }),
      new Column({ name: "b", baseIdx: 1, removed: new Set([0]) }),
    ],
  ],
  [
    { columns: ["c", "b", "a"], pk: [] },
    { columns: ["a", "b", "c"], pk: [] },
    [
      new Column({
        name: "a",
        baseIdx: 2,
        layerIdx: { 0: 0 },
        moved: { 0: { after: 1 } },
      }),
      new Column({ name: "b", baseIdx: 1, layerIdx: { 0: 1 } }),
      new Column({
        name: "c",
        baseIdx: 0,
        layerIdx: { 0: 2 },
        moved: { 0: { before: 1 } },
      }),
    ],
  ],
  [
    { columns: ["a", "b"], pk: [] },
    { columns: ["b", "c", "a"], pk: [] },
    [
      new Column({
        name: "b",
        baseIdx: 1,
        layerIdx: { 0: 0 },
      }),
      new Column({ name: "c", layerIdx: { 0: 1 }, added: new Set([0]) }),
      new Column({
        name: "a",
        baseIdx: 0,
        layerIdx: { 0: 2 },
        moved: { 0: { before: 0 } },
      }),
    ],
  ],
  [
    { columns: ["a", "d", "e", "b", "c"], pk: [] },
    { columns: ["a", "b", "c", "d", "e"], pk: [] },
    [
      new Column({ name: "a", baseIdx: 0, layerIdx: { 0: 0 } }),
      new Column({ name: "b", baseIdx: 3, layerIdx: { 0: 1 } }),
      new Column({ name: "c", baseIdx: 4, layerIdx: { 0: 2 } }),
      new Column({
        name: "d",
        baseIdx: 1,
        layerIdx: { 0: 3 },
        moved: { 0: { after: 0 } },
      }),
      new Column({
        name: "e",
        baseIdx: 2,
        layerIdx: { 0: 4 },
        moved: { 0: { after: 0 } },
      }),
    ],
  ],
  [
    { columns: ["e", "b", "c", "d", "f"], pk: [] },
    { columns: ["a", "b", "c", "d", "e"], pk: [] },
    [
      new Column({ name: "a", layerIdx: { 0: 0 }, added: new Set([0]) }),
      new Column({ name: "b", baseIdx: 1, layerIdx: { 0: 1 } }),
      new Column({ name: "c", baseIdx: 2, layerIdx: { 0: 2 } }),
      new Column({ name: "d", baseIdx: 3, layerIdx: { 0: 3 } }),
      new Column({ name: "f", baseIdx: 4, removed: new Set([0]) }),
      new Column({
        name: "e",
        baseIdx: 0,
        layerIdx: { 0: 4 },
        moved: { 0: { before: 1 } },
      }),
    ],
  ],
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["a", "b", "c"], pk: ["a"] },
    [
      new Column({
        baseIdx: 0,
        basePK: true,
        layerIdx: {
          "0": 0,
        },
        layerPK: new Set([0]),
        name: "a",
      }),
      new Column({
        baseIdx: 1,
        layerIdx: {
          "0": 1,
        },
        name: "b",
      }),
      new Column({
        baseIdx: 2,
        layerIdx: {
          "0": 2,
        },
        name: "c",
      }),
    ],
  ],
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["b", "a", "d"], pk: ["d", "a"] },
    [
      new Column({
        added: new Set([0]),
        layerIdx: {
          "0": 2,
        },
        layerPK: new Set([0]),
        name: "d",
      }),
      new Column({
        baseIdx: 0,
        basePK: true,
        layerIdx: {
          "0": 1,
        },
        layerPK: new Set([0]),
        moved: {
          "0": {
            before: 0,
          },
        },
        name: "a",
      }),
      new Column({
        baseIdx: 1,
        layerIdx: {
          "0": 0,
        },
        name: "b",
      }),
      new Column({
        baseIdx: 2,
        name: "c",
        removed: new Set([0]),
      }),
    ],
  ],
  [
    {
      columns: [
        "a",
        "ab",
        "ac",
        "ad",
        "b",
        "c",
        "ca",
        "cb",
        "cd",
        "e",
        "ea",
        "eb",
        "ec",
      ],
      pk: [],
    },
    { columns: ["a", "b", "c", "d", "e", "f"], pk: [] },
    [
      new Column({ name: "a", baseIdx: 0, layerIdx: { 0: 0 } }),
      new Column({ name: "ab", baseIdx: 1, removed: new Set([0]) }),
      new Column({ name: "ac", baseIdx: 2, removed: new Set([0]) }),
      new Column({ name: "ad", baseIdx: 3, removed: new Set([0]) }),
      new Column({ name: "b", baseIdx: 4, layerIdx: { 0: 1 } }),
      new Column({ name: "c", baseIdx: 5, layerIdx: { 0: 2 } }),
      new Column({ name: "ca", baseIdx: 6, removed: new Set([0]) }),
      new Column({ name: "cb", baseIdx: 7, removed: new Set([0]) }),
      new Column({ name: "cd", baseIdx: 8, removed: new Set([0]) }),
      new Column({ name: "d", added: new Set([0]), layerIdx: { 0: 3 } }),
      new Column({ name: "e", baseIdx: 9, layerIdx: { 0: 4 } }),
      new Column({ name: "ea", baseIdx: 10, removed: new Set([0]) }),
      new Column({ name: "eb", baseIdx: 11, removed: new Set([0]) }),
      new Column({ name: "ec", baseIdx: 12, removed: new Set([0]) }),
      new Column({ name: "f", added: new Set([0]), layerIdx: { 0: 5 } }),
    ],
  ],
])("compareColumns(%j, %j)", (base, layer, columns) => {
  const cd = new ColDiff(base, layer);
  expect(cd.columns).toEqual(columns);
});

test.each([
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["a", "b", "c"], pk: ["a"] },
    ["1", "2", "3"],
    ["1", "2", "3"],
  ],
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["b", "a", "d"], pk: ["d", "a"] },
    ["1", "2", "3"],
    [null, "1", "2", "3"],
  ],
])("rearrangeBaseRow(%j, %j)", (base, layer, values, expected) => {
  const cd = new ColDiff(base, layer);
  expect(cd.rearrangeBaseRow(values)).toEqual(expected);
});

test.each([
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["a", "b", "c"], pk: ["a"] },
    ["1", "2", "3"],
    ["1", "2", "3"],
  ],
  [
    { columns: ["a", "b", "c"], pk: ["a"] },
    { columns: ["b", "a", "d"], pk: ["d", "a"] },
    ["1", "2", "3"],
    ["3", "2", "1", null],
  ],
])("rearrangeRow(%j, %j)", (base, layer, values, expected) => {
  const cd = new ColDiff(base, layer);
  expect(cd.rearrangeRow(0, values)).toEqual(expected);
});

test("combineRows", () => {
  const cd = new ColDiff(
    { columns: ["e", "b", "c", "d", "f"], pk: [] },
    { columns: ["a", "b", "c", "d", "e"], pk: [] }
  );
  expect(cd.columns).toEqual([
    new Column({ name: "a", added: new Set([0]), layerIdx: { 0: 0 } }),
    new Column({ name: "b", layerIdx: { 0: 1 }, baseIdx: 1 }),
    new Column({ name: "c", layerIdx: { 0: 2 }, baseIdx: 2 }),
    new Column({ name: "d", layerIdx: { 0: 3 }, baseIdx: 3 }),
    new Column({ name: "f", removed: new Set([0]), baseIdx: 4 }),
    new Column({
      name: "e",
      layerIdx: { 0: 4 },
      baseIdx: 0,
      moved: { 0: { before: 1 } },
    }),
  ]);
  expect(
    cd.combineRows(0, ["1", "2", "3", "4", "5"], ["6", "2", "7", "4", "5"])
  ).toEqual([["1"], ["2"], ["3", "7"], ["4"], ["5"], ["5", "6"]]);
});

test.each([
  [
    { columns: ["a", "b", "c"], pk: [] },
    { columns: ["a", "b", "c"], pk: [] },
    true,
  ],
  [
    { columns: ["a", "b", "c"], pk: [] },
    { columns: ["a", "b", "c", "d"], pk: [] },
    false,
  ],
  [
    { columns: ["a", "b", "c", "d"], pk: [] },
    { columns: ["a", "b", "c"], pk: [] },
    false,
  ],
  [
    { columns: ["a", "b", "c"], pk: [] },
    { columns: ["a", "c", "b"], pk: [] },
    false,
  ],
])("noColumnChanges(%j, %j)", (base, layer, expected) => {
  const cd = new ColDiff(base, layer);
  expect(cd.noColumnChanges()).toEqual(expected);
});

test("columnStatus", () => {
  const cd = new ColDiff(
    { columns: ["a", "b", "c", "e"], pk: [] },
    { columns: ["a", "e", "b", "d"], pk: [] }
  );
  expect(cd.columns[0].name).toBe("a");
  expect(cd.columns[0].isAdded()).toBe(false);
  expect(cd.columns[0].isRemoved()).toBe(false);
  expect(cd.columns[0].isMoved()).toBe(false);

  expect(cd.columns[2].name).toBe("b");
  expect(cd.columns[2].isAdded()).toBe(false);
  expect(cd.columns[2].isRemoved()).toBe(false);
  expect(cd.columns[2].isMoved()).toBe(true);

  expect(cd.columns[3].name).toBe("c");
  expect(cd.columns[3].isAdded()).toBe(false);
  expect(cd.columns[3].isRemoved()).toBe(true);
  expect(cd.columns[3].isMoved()).toBe(false);

  expect(cd.columns[4].name).toBe("d");
  expect(cd.columns[4].isAdded()).toBe(true);
  expect(cd.columns[4].isRemoved()).toBe(false);
  expect(cd.columns[4].isMoved()).toBe(false);
});

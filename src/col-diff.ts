import _ from "lodash";

// longestIncreasingList returns indices of longest increasing values
export const longestIncreasingList = (arr: number[]) => {
  interface Node {
    ind: number;
    prev: Node | null;
    len: number;
  }
  const nodes: { [key: string]: Node } = {};
  let root = null;
  for (let i = 0; i < arr.length; i++) {
    const v = arr[i];
    let prev = null;
    for (let j = v - 1; j >= 0; j--) {
      if (nodes[j] && (!prev || prev.len < nodes[j].len)) {
        prev = nodes[j];
      }
      if (prev && j < prev.len) {
        break;
      }
    }
    nodes[v] = { ind: i, prev, len: prev ? prev.len + 1 : 1 };
    if (
      !root ||
      root.len < nodes[v].len ||
      (root.len === nodes[v].len && v === i)
    ) {
      root = nodes[v];
    }
  }
  const result = [];
  while (root) {
    result.unshift(root.ind);
    root = root.prev;
  }
  return result;
};

// moveOps returns move operations that changed order of array indices
export const moveOps = (arr: number[]) => {
  const anchorIndices = longestIncreasingList(arr);
  const arr2: (number | null)[] = [...arr];
  for (const i of anchorIndices) {
    arr2[i] = null;
  }
  const ops = [];
  for (let i = 0; i < arr2.length; i++) {
    const old = arr2[i];
    if (old === null) continue;
    ops.push({ old, new: i });
  }
  return ops;
};

const arrayToMap = (arr: string[]) => _.fromPairs(arr.map((v, i) => [v, i]));

type ColumnOptions = {
  added?: Set<number>;
  removed?: Set<number>;
  moved?: {
    [key: string]: { before?: number; after?: number };
  };
  layerIdx?: {
    [key: string]: number;
  };
  layerPK?: Set<number>;
  name: string;
  baseIdx?: number;
  basePK?: boolean;
};

export class Column {
  added: Set<number>;
  removed: Set<number>;
  moved: {
    [key: string]: { before?: number; after?: number };
  };
  layerIdx: {
    [key: string]: number;
  };
  layerPK: Set<number>;
  name: string;
  baseIdx?: number;
  basePK?: boolean;

  constructor(args: ColumnOptions) {
    this.added = new Set();
    this.removed = new Set();
    this.moved = {};
    this.layerIdx = {};
    this.layerPK = new Set();
    this.name = args.name;
    if (args.added) {
      this.added = args.added;
    }
    if (args.removed) {
      this.removed = args.removed;
    }
    if (args.moved) {
      this.moved = args.moved;
    }
    if (args.layerIdx) {
      this.layerIdx = args.layerIdx;
    }
    if (args.layerPK) {
      this.layerPK = args.layerPK;
    }
    if (args.baseIdx !== undefined) {
      this.baseIdx = args.baseIdx;
    }
    if (args.basePK !== undefined) {
      this.basePK = args.basePK;
    }
  }

  isAdded() {
    return this.added.size > 0;
  }

  isRemoved() {
    return this.removed.size > 0;
  }

  isMoved() {
    return Object.keys(this.moved).length > 0;
  }
}

export type Table = {
  columns: string[];
  pk: string[];
};

type NumberMap = {
  [key: string]: number;
};

export class ColDiff {
  columns: Column[];
  nameMap: NumberMap;
  constructor(base: Table, ...layers: Table[]) {
    this.columns = [];
    this.nameMap = {};
    for (const layer of layers) {
      this.insertColumns(layer.columns);
    }
    this.insertColumns(base.columns);
    for (let i = 0; i < layers.length; i++) {
      this.assignColumnAttrs(base.columns, i, layers[i].columns);
    }
    this.hoistPKToStart(arrayToMap(layers[0].pk));
    this.assignIndex(base, ...layers);
  }

  hoistPKToStart(pk: NumberMap) {
    this.columns = _.sortBy(this.columns, ({ name }: Column) => {
      if (pk[name] === undefined) {
        return Infinity;
      }
      return pk[name];
    });
    this.nameMap = arrayToMap(this.columns.map((o) => o.name));
  }

  assignIndex(base: Table, ...layers: Table[]) {
    for (let i = 0; i < base.columns.length; i++) {
      const name = base.columns[i];
      this.columns[this.nameMap[name]].baseIdx = i;
    }
    for (const name of base.pk) {
      this.columns[this.nameMap[name]].basePK = true;
    }
    for (let i = 0; i < layers.length; i++) {
      const layer = layers[i];
      for (let j = 0; j < layer.columns.length; j++) {
        const name = layer.columns[j];
        this.columns[this.nameMap[name]].layerIdx[i] = j;
      }
      for (const name of layer.pk) {
        this.columns[this.nameMap[name]].layerPK.add(i);
      }
    }
  }

  insertColumns(columnNames: string[]) {
    let offset = -1;
    const insertMap: {
      [key: string]: Column[];
    } = {};
    for (const name of columnNames) {
      const i = this.nameMap[name];
      if (i !== undefined) {
        offset = i;
        continue;
      }
      if (insertMap[offset] === undefined) {
        insertMap[offset] = [];
      }
      insertMap[offset].push(new Column({ name }));
    }
    if (Object.keys(insertMap).length === 0) {
      return;
    }
    const inserts: [number, Column[]][] = _.sortBy(
      _.toPairs(insertMap).map(([i, o]: [string, Column[]]) => [
        parseInt(i),
        o,
      ]),
      (pair: [number, Column[]]) => -pair[0]
    );
    for (const [insertOff, cols] of inserts) {
      this.columns.splice(insertOff + 1, 0, ...cols);
    }
    this.nameMap = arrayToMap(this.columns.map((o) => o.name));
  }

  assignColumnAttrs(
    baseColumns: string[],
    layerIndex: number,
    layerColumns: string[]
  ) {
    const baseSet = new Set(baseColumns);
    const layerSet = new Set(layerColumns);
    for (const name of layerColumns) {
      if (!baseSet.has(name)) {
        this.columns[this.nameMap[name]].added.add(layerIndex);
      }
    }
    for (const name of baseColumns) {
      if (!layerSet.has(name)) {
        this.columns[this.nameMap[name]].removed.add(layerIndex);
      }
    }
    this.assignColumnMoved(baseColumns, layerIndex, layerSet);
  }

  assignColumnMoved(
    baseColumns: string[],
    layerIndex: number,
    layerSet: Set<string>
  ) {
    const commonCols = [];
    for (const name of baseColumns) {
      if (layerSet.has(name)) {
        commonCols.push(name);
      }
    }
    const commonMap = arrayToMap(commonCols);
    const oldIndices = [];
    const newIndices = [];
    for (let i = 0; i < this.columns.length; i++) {
      const { name } = this.columns[i];
      const j = commonMap[name];
      if (j !== undefined) {
        newIndices.push(i);
        oldIndices.push(j);
      }
    }
    const ops = moveOps(oldIndices);
    const nonAnchor = new Set(ops.map((v) => v.old));
    for (const op of ops) {
      const newIndex = newIndices[op.new];
      // search for anchor column before this column
      let after = null;
      for (let i = op.old - 1; i >= 0; i--) {
        if (nonAnchor.has(i)) continue;
        after = commonCols[i];
        if (this.nameMap[after] !== undefined) break;
      }
      if (after) {
        this.columns[newIndex].moved[layerIndex] = {
          after: this.nameMap[after],
        };
        continue;
      }

      // search for anchor column after this column
      let before = null;
      for (let i = op.old + 1; i < commonCols.length; i++) {
        if (nonAnchor.has(i)) continue;
        before = commonCols[i];
        if (this.nameMap[before] !== undefined) break;
      }
      if (before) {
        this.columns[newIndex].moved[layerIndex] = {
          before: this.nameMap[before],
        };
      }
    }
  }

  rearrangeRow(layer: number, row: string[]) {
    const res = new Array(this.columns.length).fill(null);
    for (let i = 0; i < this.columns.length; i++) {
      const col = this.columns[i];
      if (col.layerIdx && col.layerIdx[layer] !== undefined) {
        res[i] = row[col.layerIdx[layer]];
      }
    }
    return res;
  }

  rearrangeBaseRow(row: string[]) {
    const res = new Array(this.columns.length).fill(null);
    for (let i = 0; i < this.columns.length; i++) {
      const col = this.columns[i];
      if (col.baseIdx !== undefined) {
        res[i] = row[col.baseIdx];
      }
    }
    return res;
  }

  combineRows(layer: number, row: string[], oldRow: string[]) {
    const n = this.columns.length;
    const mergedRows = [];
    for (let i = 0; i < n; i++) {
      const col = this.columns[i];
      if (col.added.has(layer)) {
        mergedRows.push([row[col.layerIdx[layer]]]);
      } else {
        const baseIdx = col.baseIdx || 0;
        if (col.removed.has(layer)) {
          mergedRows.push([oldRow[baseIdx]]);
        } else if (row[col.layerIdx[layer]] === oldRow[baseIdx]) {
          mergedRows.push([oldRow[baseIdx]]);
        } else {
          mergedRows.push([row[col.layerIdx[layer]], oldRow[baseIdx]]);
        }
      }
    }
    return mergedRows;
  }

  noColumnChanges() {
    for (const col of this.columns) {
      if (
        col.added.size > 0 ||
        col.removed.size > 0 ||
        Object.keys(col.moved).length > 0
      ) {
        return false;
      }
    }
    return true;
  }
}

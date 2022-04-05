import _ from "lodash";
import { ColDiff } from "./col-diff";
import { TableProfileDiff } from "./diff";
import { Repository } from "./repository";

export class RowIterator {
  private _repo: Repository;
  private _tblSum: string;
  private _offsets: number[];
  private _off: number;
  private _fetchSize: number;
  private _batch: string[][];
  private _batchOff: number;

  public columns: string[];
  public primaryKey: string[];

  constructor(
    repo: Repository,
    tblSum: string,
    columns: string[],
    primaryKey: string[],
    fetchSize?: number
  ) {
    if (!fetchSize) {
      fetchSize = 100;
    }
    this._repo = repo;
    this._tblSum = tblSum;
    this._offsets = [];
    this._off = 0;
    this._batch = [];
    this._batchOff = 0;
    this.columns = columns;
    this.primaryKey = primaryKey;
    this._fetchSize = fetchSize;
  }

  public addOffset(offset: number) {
    this._offsets.push(offset);
  }

  public size() {
    return this._offsets.length;
  }

  public async next() {
    if (this._batchOff >= this._batch.length) {
      if (this._off >= this.size()) {
        return { done: true, value: null };
      }
      const reader = await this._repo.getTableRows(
        this._tblSum,
        this._offsets.slice(this._off, this._off + this._fetchSize)
      );
      this._batch = [];
      this._batchOff = 0;
      await reader.readAll((row) => {
        this._batch.push(row);
      });
    }
    const row = this._batch[this._batchOff];
    this._batchOff += 1;
    return { done: false, value: row };
  }
}

export class ModifiedRowIterator {
  private _repo: Repository;
  private _tblSum1: string;
  private _tblSum2: string;
  private _cd: ColDiff;
  private _fetchSize: number;
  private _offsets: [number, number][];
  private _off: number;
  private _batch: [string[], string[]][];
  private _batchOff: number;

  public columns: string[];
  public primaryKey: string[];

  constructor(
    repo: Repository,
    tblSum1: string,
    tblSum2: string,
    cd: ColDiff,
    columns: string[],
    primaryKey: string[],
    fetchSize?: number
  ) {
    if (!fetchSize) {
      fetchSize = 100;
    }
    this._repo = repo;
    this._tblSum1 = tblSum1;
    this._tblSum2 = tblSum2;
    this._offsets = [];
    this._off = 0;
    this._batch = [];
    this._batchOff = 0;
    this.columns = columns;
    this.primaryKey = primaryKey;
    this._fetchSize = fetchSize;
    this._cd = cd;
  }

  public addOffset(offset1: number, offset2: number) {
    this._offsets.push([offset1, offset2]);
  }

  public size() {
    return this._offsets.length;
  }

  public async next() {
    if (this._batchOff >= this._batch.length) {
      if (this._off >= this.size()) {
        return { done: true, value: null };
      }
      const offsets = this._offsets.slice(
        this._off,
        this._off + this._fetchSize
      );
      const rows1: string[][] = [];
      const rows2: string[][] = [];
      await Promise.all([
        this._repo
          .getTableRows(
            this._tblSum1,
            offsets.map((l) => l[0])
          )
          .then((reader) =>
            reader.readAll((row) => {
              rows1.push(row);
            })
          ),
        this._repo
          .getTableRows(
            this._tblSum2,
            offsets.map((l) => l[1])
          )
          .then((reader) =>
            reader.readAll((row) => {
              rows2.push(row);
            })
          ),
      ]);
      this._batch = _.zip(rows1, rows2) as [string[], string[]][];
      this._batchOff = 0;
    }
    const [row1, row2] = this._batch[this._batchOff];
    this._batchOff += 1;
    return { done: false, value: this._cd.combineRows(0, row1, row2) };
  }
}

export class ColumnChanges {
  newValues: string[];
  oldValues: string[];
  unchanged: Set<string>;
  added: Set<string>;
  removed: Set<string>;

  constructor(newCols: string[], oldCols: string[]) {
    this.newValues = newCols;
    this.oldValues = oldCols;
    const oldSet = new Set(oldCols);
    const newSet = new Set(newCols);
    this.unchanged = new Set(oldCols.filter((x) => newSet.has(x)));
    this.added = new Set(newCols.filter((x) => !oldSet.has(x)));
    this.removed = new Set(oldCols.filter((x) => !newSet.has(x)));
  }
}

const stringArrayEqual = (a: string[], b: string[]) => {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
};

export const collectChanges = async (
  repo: Repository,
  comSum1: string,
  comSum2: string,
  fetchSize?: number
) => {
  const dr = await repo.diff(comSum1, comSum2);
  const primaryKey = dr.oldPK.map((i) => dr.oldColumns[i]);
  const oldPrimaryKey = dr.pk.map((i) => dr.columns[i]);
  const cd = new ColDiff(
    {
      columns: dr.oldColumns,
      pk: primaryKey,
    },
    {
      columns: dr.columns,
      pk: oldPrimaryKey,
    }
  );
  const result: {
    dataProfile?: TableProfileDiff;
    columnChanges: ColumnChanges;
    pkChanges: ColumnChanges;
    addedRows?: RowIterator;
    removedRows?: RowIterator;
    modifiedRows?: ModifiedRowIterator;
  } = {
    dataProfile: dr.dataProfile,
    columnChanges: new ColumnChanges(dr.columns, dr.oldColumns),
    pkChanges: new ColumnChanges(primaryKey, oldPrimaryKey),
  };
  if (dr.rowDiff && stringArrayEqual(oldPrimaryKey, primaryKey)) {
    result.addedRows = new RowIterator(
      repo,
      dr.tableSum,
      dr.columns,
      primaryKey,
      fetchSize
    );
    result.removedRows = new RowIterator(
      repo,
      dr.oldTableSum,
      dr.oldColumns,
      oldPrimaryKey,
      fetchSize
    );
    result.modifiedRows = new ModifiedRowIterator(
      repo,
      dr.tableSum,
      dr.oldTableSum,
      cd,
      cd.columns.map((c) => c.name),
      primaryKey,
      fetchSize
    );
    for (const rd of dr.rowDiff) {
      if (!rd.off1) {
        result.removedRows.addOffset(rd.off2 as number);
      } else if (!rd.off2) {
        result.addedRows.addOffset(rd.off1);
      } else {
        result.modifiedRows.addOffset(rd.off1, rd.off2);
      }
    }
  }
  return result;
};

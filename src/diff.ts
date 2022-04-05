export type RowDiff = {
  off1?: number;
  off2?: number;
};

export type ScalarStatDiff = {
  name: string;
  shortName: string;
  old?: number;
  new?: number;
};

export type ValueCountDiff = {
  value?: string;
  oldCount?: number;
  newCount?: number;
  oldPct?: number;
  newPct?: number;
};

export type TopValuesStatDiff = {
  name: string;
  shortName: string;
  newAddition?: boolean;
  removed?: boolean;
  values?: ValueCountDiff[];
};

export type PercentileDiff = {
  old?: number;
  new?: number;
};

export type PercentilesStatDiff = {
  name: string;
  shortName: string;
  newAddition?: boolean;
  removed?: boolean;
  values?: PercentileDiff[];
};

export type ColumnProfileDiff = {
  name: string;
  newAddition?: boolean;
  removed?: boolean;
  stats: (ScalarStatDiff | TopValuesStatDiff | PercentilesStatDiff)[];
};

export type TableProfileDiff = {
  oldRowsCount: number;
  newRowsCount: number;
  columns: ColumnProfileDiff[];
};

export type DiffResult = {
  tableSum: string;
  oldTableSum: string;
  oldPK: number[];
  pk: number[];
  oldColumns: string[];
  columns: string[];
  rowDiff?: RowDiff[];
  dataProfile?: TableProfileDiff;
};

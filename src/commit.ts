export type CommitResult = {
  sum: string;
  table: string;
};

export type Table = {
  sum: string;
  exist?: boolean;
  columns?: string[];
  pk?: number[];
  rowsCount?: number;
};

export type CommitsDict = { [key: string]: Commit };

export type CommitInit = {
  sum: string;
  authorName: string;
  authorEmail: string;
  message: string;
  table: Table;
  time: string;
  parents?: string[];
  parentCommits?: CommitsDict;
};

export type Commit = Omit<CommitInit, "time"> & {
  time: Date;
};

export const commitPayload = (obj: CommitInit): Commit => {
  return {
    ...obj,
    time: new Date(obj.time),
  };
};

export type CommitTree = {
  sum: string;
  root: Commit;
};

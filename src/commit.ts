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

export type CommitInit = {
  sum: string;
  authorName: string;
  authorEmail: string;
  message: string;
  table: Table;
  time: string;
  parents?: string[];
  parentCommits?: Map<string, Commit>;
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

export class CommitTree {
  sum?: string;
  root?: Commit;

  public constructor(init?: Partial<CommitTree>) {
    Object.assign(this, init);
  }
}

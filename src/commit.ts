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

export type CommitTreeInit = {
  sum: string;
  root: Commit;
};

export class CommitTree {
  sum: string;
  root: Commit;

  constructor(obj: CommitTreeInit) {
    this.sum = obj.sum;
    this.root = obj.root;
  }

  public getNode(...indices: number[]): Commit | undefined {
    let commit = this.root;
    for (const ind of indices) {
      if (commit.parents && ind < commit.parents.length) {
        const sum = commit.parents[ind];
        commit = (commit.parentCommits as CommitsDict)[sum];
        commit.sum = sum;
      } else {
        return undefined;
      }
    }
    return commit;
  }

  public getLeftMostNodeAtDepth(depth: number): Commit | undefined {
    return this.getNode(...new Array(depth).fill(0));
  }
}

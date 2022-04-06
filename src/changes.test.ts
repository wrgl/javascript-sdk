import fs from "node:fs";
import { collectChanges, ColumnChanges } from "./changes";
import { Commit } from "./commit";
import { Repository } from "./repository";

import {
  commit,
  exhaustAsyncIterable,
  initRepo,
  startWrgld,
} from "./test-utils";

describe("collectChanges", () => {
  let repoDir: string;
  const email = "johndoe@domain.com";
  const name = "John Doe";
  const password = "password";

  beforeAll(async () => {
    repoDir = await initRepo(email, name, password);
  });

  afterAll(() => {
    fs.rmSync(repoDir, { recursive: true, force: true });
  });

  test("return changes object", async () => {
    await commit(
      repoDir,
      "main",
      "initial commit",
      ["a"],
      [
        ["a", "b", "c", "d"],
        ["1", "q", "w", "e"],
        ["2", "a", "s", "d"],
        ["3", "z", "x", "c"],
      ]
    );
    await commit(
      repoDir,
      "main",
      "second commit",
      ["a"],
      [
        ["a", "b", "c", "d"],
        ["1", "q", "w", "e"],
        ["2", "a", "s", "d"],
        ["3", "z", "x", "c"],
      ]
    );
    await commit(
      repoDir,
      "main",
      "third commit",
      ["a"],
      [
        ["a", "b", "c", "e"],
        ["1", "q", "u", "r"],
        ["2", "a", "s", "f"],
        ["4", "y", "u", "i"],
      ]
    );

    await startWrgld(repoDir, async (url) => {
      const repo = new Repository(url);
      await repo.authenticate(email, password);
      const comTree = await repo.getCommitTree("heads/main", 3);

      // test no changes
      const commit1 = comTree.getLeftMostNodeAtDepth(1) as Commit;
      expect(commit1).toBeTruthy();
      const commit2 = comTree.getLeftMostNodeAtDepth(2) as Commit;
      expect(commit2).toBeTruthy();
      let changes = await collectChanges(repo, commit1.sum, commit2.sum);
      expect(changes.columnChanges).toEqual(
        new ColumnChanges(
          ["a", "b", "c", "d"],
          ["a", "b", "c", "d"],
          new Set(["a", "b", "c", "d"]),
          new Set([]),
          new Set([])
        )
      );
      expect(changes.pkChanges).toEqual(
        new ColumnChanges(
          ["a"],
          ["a"],
          new Set(["a"]),
          new Set([]),
          new Set([])
        )
      );
      expect(changes.addedRows).toBeUndefined();
      expect(changes.removedRows).toBeUndefined();
      expect(changes.modifiedRows).toBeUndefined();

      // test with changes
      changes = await collectChanges(repo, comTree.sum, commit1.sum);
      expect(changes.columnChanges).toEqual(
        new ColumnChanges(
          ["a", "b", "c", "e"],
          ["a", "b", "c", "d"],
          new Set(["a", "b", "c"]),
          new Set(["e"]),
          new Set(["d"])
        )
      );
      expect(changes.pkChanges).toEqual(
        new ColumnChanges(
          ["a"],
          ["a"],
          new Set(["a"]),
          new Set([]),
          new Set([])
        )
      );

      expect(changes.addedRows?.size()).toEqual(1);
      expect(changes.addedRows?.columns).toEqual(["a", "b", "c", "e"]);
      expect(changes.addedRows?.primaryKey).toEqual(["a"]);
      expect(
        await exhaustAsyncIterable(changes.addedRows as AsyncIterable<string[]>)
      ).toEqual([["4", "y", "u", "i"]]);

      expect(changes.removedRows?.size()).toEqual(1);
      expect(changes.removedRows?.columns).toEqual(["a", "b", "c", "d"]);
      expect(changes.removedRows?.primaryKey).toEqual(["a"]);
      expect(
        await exhaustAsyncIterable(
          changes.removedRows as AsyncIterable<string[]>
        )
      ).toEqual([["3", "z", "x", "c"]]);

      expect(changes.modifiedRows?.size()).toEqual(2);
      expect(changes.modifiedRows?.columns).toEqual(["a", "b", "c", "d", "e"]);
      expect(changes.modifiedRows?.primaryKey).toEqual(["a"]);
      expect(
        await exhaustAsyncIterable(
          changes.modifiedRows as AsyncIterable<string[][]>
        )
      ).toEqual([
        [
          ["1", "1"],
          ["q", "q"],
          ["u", "w"],
          [null, "e"],
          ["r", null],
        ],
        [
          ["2", "2"],
          ["a", "a"],
          ["s", "s"],
          [null, "d"],
          ["f", null],
        ],
      ]);
      expect(changes.dataProfile).toBeTruthy();
      expect(changes.dataProfile?.oldRowsCount).toEqual(3);
      expect(changes.dataProfile?.newRowsCount).toEqual(3);
      expect(changes.dataProfile?.columns).toHaveLength(5);
    });
  });
});

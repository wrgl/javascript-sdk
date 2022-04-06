import fs from "node:fs";

import _ from "lodash";

import { CommitsDict } from "./commit";
import { Repository } from "./repository";
import { commit, csvStream, initRepo, readAll, startWrgld } from "./test-utils";
import { CreateTransactionRequest } from "./transaction";
import { randPastDate, randUuid } from "@ngneat/falso";

describe("repository", () => {
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

  test("commit", async () => {
    await commit(
      repoDir,
      "main",
      "initial commit",
      ["a"],
      [
        ["a", "b", "c"],
        ["1", "q", "w"],
        ["2", "a", "s"],
      ]
    );

    let token: string;
    await startWrgld(repoDir, async (url) => {
      const repo = new Repository(url);
      token = await repo.authenticate(email, password);
    });

    await startWrgld(repoDir, async (url) => {
      const repo = new Repository(url, token);

      const refs = await repo.getRefs();
      expect(_.keys(refs)).toHaveLength(1);
      const commit1 = await repo.getCommit(refs["heads/main"]);
      expect(commit1.time instanceof Date).toBeTruthy();
      const commit2 = await repo.getBranch("main");
      expect(commit2).toEqual(commit1);

      const rows = [
        ["a", "b", "c"],
        ["1", "e", "w"],
        ["2", "c", "s"],
      ];
      const cr = await repo.commit("main", "second commit", csvStream(rows), [
        "a",
      ]);
      expect(cr.sum).toBeTruthy();
      expect(cr.table).toBeTruthy();

      const comTree = await repo.getCommitTree(cr.sum, 2);
      expect(
        (comTree.root.parentCommits as CommitsDict)[commit1.sum].table
      ).toBeTruthy();

      const tbl = await repo.getTable(cr.table);
      expect(tbl.columns).toEqual(["a", "b", "c"]);
      expect(await readAll(await repo.getBlocks(cr.sum))).toEqual(rows);
      expect(await readAll(await repo.getTableBlocks(cr.table))).toEqual(rows);
      expect(await readAll(await repo.getRows(cr.sum, [0]))).toEqual([
        ["1", "e", "w"],
      ]);
      expect(await readAll(await repo.getTableRows(cr.table, [0]))).toEqual([
        ["1", "e", "w"],
      ]);

      const dr = await repo.diff(cr.sum, commit1.sum);
      expect(dr.pk).toEqual(dr.oldPK);
      expect(dr.rowDiff?.length).toBeGreaterThan(0);
    });
  });

  test("transaction", async () => {
    await commit(
      repoDir,
      "alpha",
      "initial commit",
      ["a"],
      [
        ["a", "b", "c"],
        ["1", "t", "w"],
        ["2", "g", "s"],
      ]
    );
    await startWrgld(repoDir, async (url) => {
      const repo = new Repository(url);
      await repo.authenticate(email, password);
      const com1 = await repo.getBranch("alpha");

      const { id } = await repo.createTransaction();
      const cr1 = await repo.commit(
        "alpha",
        "second commit",
        csvStream([
          ["a", "b", "c"],
          ["1", "q", "w"],
          ["2", "a", "s"],
        ]),
        ["a"],
        id
      );
      const cr2 = await repo.commit(
        "beta",
        "first commit",
        csvStream([
          ["a", "b", "c"],
          ["1", "e", "w"],
          ["2", "c", "s"],
        ]),
        ["a"],
        id
      );

      const tx = await repo.getTransaction(id);
      expect(tx.begin).toBeInstanceOf(Date);
      expect(tx.end).toBeUndefined();
      expect(tx.status).toEqual("in-progress");
      expect(tx.branches).toEqual([
        { name: "alpha", newSum: cr1.sum, currentSum: com1.sum },
        { name: "beta", newSum: cr2.sum },
      ]);

      await repo.commitTransaction(id);
      const com2 = await repo.getBranch("alpha");
      const com3 = await repo.getBranch("beta");
      expect(com2.table.sum).toEqual(cr1.table);
      expect(com3.table.sum).toEqual(cr2.table);
      const tx1 = await repo.getTransaction(id);
      expect(tx1.end).toBeInstanceOf(Date);
      expect(tx1.status).toEqual("committed");

      const tx2: CreateTransactionRequest = {
        id: randUuid(),
        begin: randPastDate(),
        status: "in-progress",
      };
      await repo.createTransaction(tx2);
      await repo.commit(
        "beta",
        "second commit",
        csvStream([
          ["a", "b", "c"],
          ["1", "e", "y"],
          ["2", "c", "h"],
        ]),
        ["a"],
        tx2.id
      );
      await repo.discardTransaction(tx2.id);
      expect(async () => {
        await repo.getTransaction(tx2.id);
      }).rejects.toThrow();
      const com4 = await repo.getBranch("beta");
      expect(com4).toEqual(com3);
    });
  });
});

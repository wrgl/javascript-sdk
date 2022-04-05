import { spawn } from "node:child_process";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { Readable } from "node:stream";
import net from "node:net";

import _ from "lodash";
import { Reader } from "gocsv";

import { version } from "../package.json";
import { CommitsDict } from "./commit";
import { Repository } from "./repository";

const plat = os.platform();
let arch = os.arch();
arch = arch === "x64" ? "amd64" : arch;

function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

const wrgl = (repoDir: string, args: string[]) =>
  new Promise((resolve, reject) => {
    const proc = spawn(
      path.join(
        "__testcache__",
        version,
        `wrgl-${plat}-${arch}`,
        "bin",
        "wrgl"
      ),
      args.concat(["--wrgl-dir", repoDir])
    );
    const stdout: string[] = [];
    const stderr: string[] = [];
    proc.stdout.on("data", (data) => {
      stdout.push(data);
    });
    proc.stderr.on("data", (data) => {
      stderr.push(data);
    });
    proc.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `wrgld exited with code ${code}:\n  stdout: ${stdout.join(
              ""
            )}\n  stderr: ${stderr.join("")}`
          )
        );
      } else {
        resolve(null);
      }
    });
  });

const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const srv = net.createServer(function (sock) {
      sock.end("");
    });
    let port: number;
    srv.listen(0, function () {
      port = (srv.address() as net.AddressInfo).port;
      srv.close();
    });
    srv.on("error", (err) => {
      reject(err);
    });
    srv.on("close", () => {
      resolve(port);
    });
  });

const startWrgld = async (
  repoDir: string
): Promise<{ url: string; abort: () => void }> => {
  const port: number = await freePort();
  const proc = spawn(
    path.join(
      "__testcache__",
      version,
      `wrgld-${plat}-${arch}`,
      "bin",
      "wrgld"
    ),
    [repoDir, "-p", port + ""]
  );
  await sleep(1000);
  return {
    url: `http://localhost:${port}`,
    abort: () => proc.kill(),
  };
};

const commit = async (
  repoDir: string,
  branch: string,
  message: string,
  primaryKey: string[],
  rows: string[][]
) => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "wrgl-javascript-sdk"));
  const filePath = path.join(tmpDir, "data.csv");
  fs.writeFileSync(filePath, rows.map((row) => row.join(",")).join("\n"));
  await wrgl(repoDir, [
    "commit",
    branch,
    filePath,
    message,
    "-p",
    primaryKey.join(","),
  ]);
  fs.rmSync(tmpDir, { recursive: true, force: true });
};

// @ts-ignore
const readAll = async (reader: Reader) => {
  const result: string[][] = [];
  await reader.readAll((row) => {
    result.push(row);
  });
  return result;
};

describe("repository", () => {
  let repoDir: string;
  const email = "johndoe@domain.com";
  const name = "John Doe";
  const password = "password";

  beforeAll(async () => {
    repoDir = fs.mkdtempSync(path.join(os.tmpdir(), "wrgl-javascript-sdk"));
    await wrgl(repoDir, ["init"]);
    await wrgl(repoDir, [
      "config",
      "set",
      "receive.denyNonFastForwards",
      "true",
    ]);
    await wrgl(repoDir, ["config", "set", "user.email", email]);
    await wrgl(repoDir, ["config", "set", "user.name", name]);
    await wrgl(repoDir, [
      "auth",
      "add-user",
      email,
      "--name",
      name,
      "--password",
      password,
    ]);
    await wrgl(repoDir, ["auth", "add-scope", email, "--all"]);
    await wrgl(repoDir, ["config", "set", "auth.type", "legacy"]);
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

    const { url, abort } = await startWrgld(repoDir);
    try {
      const repo = new Repository(url);
      await repo.authenticate(email, password);

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
      const file = Readable.from(rows.map((row) => row.join(",")).join("\n"));
      const cr = await repo.commit("main", "second commit", file, ["a"]);
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
    } catch (e) {
      throw e;
    } finally {
      abort();
    }
  });
});

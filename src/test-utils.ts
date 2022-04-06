import { spawn } from "node:child_process";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import net from "node:net";

import _ from "lodash";
import { Reader } from "gocsv";
import {
  randEmail,
  randFullName,
  randHexaDecimal,
  randPastDate,
  randText,
} from "@ngneat/falso";

import { version } from "../package.json";
import { Readable } from "node:stream";
import { Commit, CommitTree } from "./commit";

const plat = os.platform();
let arch = os.arch();
arch = arch === "x64" ? "amd64" : arch;

export const sleep = (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export const wrgl = (repoDir: string, args: string[]) =>
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
            `wrgl exited with code ${code}:\n  stdout: ${stdout.join(
              ""
            )}\n  stderr: ${stderr.join("")}`
          )
        );
      } else {
        resolve(null);
      }
    });
  });

export const freePort = (): Promise<number> =>
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

export const startWrgld = async (
  repoDir: string,
  callback: (url: string) => Promise<unknown>
) => {
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
  try {
    await callback(`http://localhost:${port}`);
  } catch (e) {
    throw e;
  } finally {
    proc.kill();
  }
};

export const commit = async (
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

export const readAll = async (reader: Reader) => {
  const result: string[][] = [];
  await reader.readAll((row) => {
    result.push(row);
  });
  return result;
};

export const initRepo = async (
  email: string,
  name: string,
  password: string
) => {
  const repoDir = fs.mkdtempSync(path.join(os.tmpdir(), "wrgl-javascript-sdk"));
  await wrgl(repoDir, ["init"]);
  await wrgl(repoDir, ["config", "set", "receive.denyNonFastForwards", "true"]);
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
  return repoDir;
};

export const csvStream = (rows: string[][]) =>
  Readable.from(rows.map((row) => row.join(",")).join("\n"));

export const exhaustAsyncIterable = async <T>(
  iterable: AsyncIterable<T>
): Promise<T[]> => {
  const result: T[] = [];
  for await (const value of iterable) {
    result.push(value);
  }
  return result;
};

export const randCommit = (): Commit => ({
  sum: randHexaDecimal({ length: 32 }).join(""),
  authorEmail: randEmail(),
  authorName: randFullName(),
  message: randText(),
  table: {
    sum: randHexaDecimal({ length: 32 }).join(""),
  },
  time: randPastDate(),
});

export const randCommitTree = (depth: number): CommitTree => {
  const root = randCommit();
  let commit = root;
  for (let i = 0; i < depth; i++) {
    const node = randCommit();
    commit.parentCommits = { [node.sum]: node };
    commit.parents = [node.sum];
    commit = node;
  }
  return new CommitTree({
    sum: root.sum,
    root,
  });
};

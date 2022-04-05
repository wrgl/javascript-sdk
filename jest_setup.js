import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import { promisify } from "node:util";
import got from "got";
import tar from "tar-fs";
import { pipeline, Readable } from "node:stream";
import { createUnzip } from "node:zlib";
import { readFile } from "node:fs/promises";

const pipe = promisify(pipeline);

const readVersion = async () => {
  const pkg = JSON.parse(await readFile("./package.json"));
  return pkg.version;
};

const downloadWrgl = async () => {
  const version = await readVersion();
  const dir = path.join("__testcache__", version);
  const plat = os.platform();
  let arch = os.arch();
  arch = arch === "x64" ? "amd64" : arch;
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    await Promise.all(
      ["wrgl", "wrgld"].map((binary) =>
        got
          .get(
            `https://github.com/wrgl/wrgl/releases/download/v${version}/${binary}-${plat}-${arch}.tar.gz`
          )
          .buffer()
          .then((buf) =>
            pipe(Readable.from(buf), createUnzip(), tar.extract(dir))
          )
      )
    );
  }
  return ["wrgl", "wrgld"].map((binary) =>
    path.join(dir, `${binary}-${plat}-${arch}`, "bin", binary)
  );
};

export default async () => {
  await downloadWrgl();
};

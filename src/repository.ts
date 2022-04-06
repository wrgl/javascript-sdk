import FormData from "form-data";
import got, { Got, ExtendOptions, Hooks } from "got";
import { pipeline, Readable } from "stream";
import { promisify } from "util";
import { createGzip } from "zlib";
import { Reader as CSVReader } from "gocsv";

import { AuthenticateResponse } from "./auth";
import {
  CommitInit,
  commitPayload,
  CommitResult,
  CommitTree,
  CommitTreeInit,
  Table,
} from "./commit";
import { InMemoryWritable } from "./io";
import { DiffResult } from "./diff";
import {
  CreateTransactionRequest,
  createTransactionRequestPayload,
  CreateTransactionResponse,
  getTransactionResponse,
} from "./transaction";

const pipe = promisify(pipeline);

type GetRefsResponse = {
  refs: { [key: string]: string };
};

export class Repository {
  endpoint: string;
  private _client: Got;
  hooks: Hooks;

  public constructor(endpoint: string, idToken?: string) {
    this.endpoint = endpoint.endsWith("/")
      ? endpoint.slice(0, endpoint.length - 1)
      : endpoint;
    const opts: ExtendOptions = {
      prefixUrl: this.endpoint,
    };
    if (idToken) {
      opts.headers = { Authorization: `Bearer ${idToken}` };
    }
    this._client = got.extend(opts);
    this.hooks = {
      beforeError: [
        (error) => {
          error.message = `${error.message}\n  url: ${error.request?.requestUrl}\n  payload: ${error.response?.body}`;
          return error;
        },
      ],
    };
  }

  private _isHubRepo() {
    return this.endpoint.startsWith("https://hub.wrgl.co/");
  }

  public async authenticate(email: string, password: string) {
    const endpoint = this._isHubRepo()
      ? "https://hub.wrgl.co/api/authenticate/"
      : this.endpoint + "/authenticate/";
    const response: AuthenticateResponse = await got
      .post(endpoint, {
        hooks: this.hooks,
        json: {
          email: email,
          password: password,
        },
      })
      .json();
    this._client = got.extend({
      prefixUrl: this.endpoint,
      headers: { Authorization: `Bearer ${response.idToken}` },
    });
    return response.idToken;
  }

  public async getRefs() {
    const resp: GetRefsResponse = await this._client
      .get("refs/", { hooks: this.hooks })
      .json();
    return resp.refs;
  }

  public async getBranch(branch: string) {
    return commitPayload(
      (await this._client
        .get(`refs/heads/${branch}/`, { hooks: this.hooks })
        .json()) as CommitInit
    );
  }

  public async commit(
    branch: string,
    message: string,
    file: Readable,
    primaryKey: string[],
    txid?: string
  ) {
    const dest = new InMemoryWritable({});
    await pipe(file, createGzip(), dest);
    const fd = new FormData();
    fd.append("branch", branch);
    fd.append("message", message);
    fd.append("file", dest.buffer, {
      filename: "data.csv.gz",
      contentType: "text/csv",
    });
    if (primaryKey.constructor === Array) {
      if (primaryKey.length > 0) {
        fd.append("primaryKey", primaryKey.join(","));
      }
    } else if (typeof primaryKey === "string") {
      fd.append("primaryKey", primaryKey);
    }
    if (txid) {
      fd.append("txid", txid);
    }
    return (await this._client
      .post("commits/", {
        hooks: this.hooks,
        body: fd.getBuffer(),
        headers: fd.getHeaders(),
      })
      .json()) as CommitResult;
  }

  public async getCommitTree(head: string, maxDepth: number) {
    return new CommitTree(
      (await this._client
        .get("commits/", {
          hooks: this.hooks,
          searchParams: { head: head, maxDepth: maxDepth },
        })
        .json()) as CommitTreeInit
    );
  }

  public async getCommit(commitSum: string) {
    return commitPayload(
      (await this._client
        .get(`commits/${commitSum}`, { hooks: this.hooks })
        .json()) as CommitInit
    );
  }

  public async getTable(tableSum: string) {
    return (await this._client
      .get(`tables/${tableSum}/`, { hooks: this.hooks })
      .json()) as Table;
  }

  public async getBlocks(
    commit: string,
    options?: { start?: number; end?: number; withColumnNames?: boolean }
  ) {
    const buf = await this._client
      .get("blocks/", {
        hooks: this.hooks,
        searchParams: {
          head: commit,
          start: options?.start,
          end: options?.end,
          columns: options?.withColumnNames === false ? "false" : "true",
        },
      })
      .buffer();
    return new CSVReader(buf);
  }

  public async getTableBlocks(
    tableSum: string,
    options?: { start?: number; end?: number; withColumnNames?: boolean }
  ) {
    const buf = await this._client
      .get(`tables/${tableSum}/blocks/`, {
        hooks: this.hooks,
        searchParams: {
          start: options?.start,
          end: options?.end,
          columns: options?.withColumnNames === false ? "false" : "true",
        },
      })
      .buffer();
    return new CSVReader(buf);
  }

  public async getRows(commit: string, offsets: number[]) {
    const buf = await this._client
      .get("rows/", {
        hooks: this.hooks,
        searchParams: {
          head: commit,
          offsets: offsets.join(","),
        },
      })
      .buffer();
    return new CSVReader(buf);
  }

  public async getTableRows(tableSum: string, offsets: number[]) {
    const buf = await this._client
      .get(`tables/${tableSum}/rows/`, {
        hooks: this.hooks,
        searchParams: {
          offsets: offsets.join(","),
        },
      })
      .buffer();
    return new CSVReader(buf);
  }

  public async diff(sum1: string, sum2: string) {
    return (await this._client
      .get(`diff/${sum1}/${sum2}/`, {
        hooks: this.hooks,
      })
      .json()) as DiffResult;
  }

  public async createTransaction(req?: CreateTransactionRequest) {
    return (await this._client
      .post("transactions/", {
        hooks: this.hooks,
        json: req ? createTransactionRequestPayload(req) : undefined,
      })
      .json()) as CreateTransactionResponse;
  }

  public async getTransaction(id: string) {
    return getTransactionResponse(
      await this._client
        .get(`transactions/${id}/`, {
          hooks: this.hooks,
        })
        .json()
    );
  }

  public async commitTransaction(id: string) {
    return await this._client.post(`transactions/${id}/`, {
      hooks: this.hooks,
      json: { commit: true },
    });
  }

  public async discardTransaction(id: string) {
    return await this._client.post(`transactions/${id}/`, {
      hooks: this.hooks,
      json: { discard: true },
    });
  }
}

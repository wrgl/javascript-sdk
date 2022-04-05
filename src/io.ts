import { Writable, WritableOptions } from "stream";

export const getFileContent = async (
  data: string | Buffer
): Promise<string> => {
  if (data instanceof Buffer) {
    return data.toString("utf8");
  }
  if (typeof data === "string") {
    return data;
  }
  throw new Error("unhandled data type: " + typeof data);
};

export class InMemoryWritable extends Writable {
  data: Buffer[];
  buffer?: Buffer;
  constructor(options: WritableOptions) {
    super(options);
    this.data = [];
  }

  _write(
    chunk: any,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    this.data.push(Buffer.from(chunk, encoding));
    callback();
  }

  _final(callback: (error?: Error | null) => void): void {
    this.buffer = Buffer.concat(this.data);
    callback();
  }

  _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.data = [];
    callback();
  }
}

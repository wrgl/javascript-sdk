import { Writable, WritableOptions } from "stream";

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
    // @ts-ignore
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.data = [];
    callback();
  }
}

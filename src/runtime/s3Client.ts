import { createRequire } from "node:module";
import { getBunGlobal, isBunRuntime } from "./engine";

const runtimeRequire = createRequire(import.meta.url);

/**
 * Runtime-neutral options used to initialize an S3 client adapter.
 * Next: pass to `createS3Client(...)`.
 */
export type S3ClientOptions = {
  /** Bucket name used for object operations. */
  bucket: string;
  /** Optional S3-compatible endpoint (with or without protocol). */
  endpoint?: string;
  /** Region used for signing and AWS defaults. */
  region?: string;
  /** Optional access key id. */
  accessKeyId?: string;
  /** Optional secret access key. */
  secretAccessKey?: string;
  /** Optional temporary session token. */
  sessionToken?: string;
  /** Use virtual-hosted-style addressing when true. */
  virtualHostedStyle?: boolean;
};

/**
 * List options accepted by the adapter.
 * Next: call `list(...)` for prefix and delimiter queries.
 */
export type S3ListOptions = {
  /** Optional object key prefix. */
  prefix?: string;
  /** Optional grouping delimiter. */
  delimiter?: string;
  /** Optional max number of keys. */
  maxKeys?: number;
};

/**
 * Normalized S3 list result consumed by `S3Depot`.
 * Next: iterate `contents` and `commonPrefixes`.
 */
export type S3ListResult = {
  /** Flat list of object keys. */
  contents?: Array<{ key: string }>;
  /** Grouped common prefixes when delimiter is supplied. */
  commonPrefixes?: Array<{ prefix: string }>;
};

/**
 * Runtime-neutral S3 client used by depots.
 * Next: pass into `S3Depot` for create/read/list/edit/delete flows.
 */
export interface S3ClientAdapter {
  /**
   * Write an object payload to a key.
   * Next: call `read(...)` or return the key to the caller.
   */
  write(key: string, payload: Blob | Response): Promise<void>;
  /**
   * Read an object payload as a Blob.
   * Next: wrap the blob in `DepotFile`.
   */
  read(key: string): Promise<Blob>;
  /**
   * Delete an object key.
   * Next: report deletion status to the calling depot.
   */
  unlink(key: string): Promise<void>;
  /**
   * List objects and prefixes.
   * Next: map keys back to RNs in depot code.
   */
  list(options: S3ListOptions): Promise<S3ListResult>;
}

type BunS3ListResult = {
  contents?: Array<{ key: string }>;
  commonPrefixes?: Array<{ prefix: string }>;
};

type BunS3ClientLike = {
  write: (key: string, payload: Blob | Response) => Promise<void>;
  file: (key: string) => { arrayBuffer: () => Promise<ArrayBuffer> };
  unlink: (key: string) => Promise<void>;
  list: (options: S3ListOptions) => Promise<BunS3ListResult>;
};

type BunS3Ctor = new (options: S3ClientOptions) => BunS3ClientLike;

type AwsS3Ctor = new (options: Record<string, unknown>) => {
  send(command: unknown): Promise<unknown>;
};

type AwsSdkModule = {
  S3Client: AwsS3Ctor;
  PutObjectCommand: new (params: Record<string, unknown>) => unknown;
  GetObjectCommand: new (params: Record<string, unknown>) => unknown;
  DeleteObjectCommand: new (params: Record<string, unknown>) => unknown;
  ListObjectsV2Command: new (params: Record<string, unknown>) => unknown;
};

function normalizeEndpoint(endpoint: string | undefined): string | undefined {
  if (!endpoint) return undefined;
  if (/^[a-z]+:\/\//i.test(endpoint)) return endpoint;
  return `https://${endpoint}`;
}

async function toBlob(payload: Blob | Response): Promise<Blob> {
  if (payload instanceof Blob) return payload;
  return await payload.blob();
}

async function streamBodyToBlob(body: unknown): Promise<Blob> {
  if (!body) return new Blob([]);
  if (body instanceof Blob) return body;

  const anyBody = body as Record<string | symbol, unknown>;
  const transformToByteArray = anyBody["transformToByteArray"] as
    | (() => Promise<Uint8Array>)
    | null;
  if (typeof transformToByteArray === "function") {
    const bytes = await transformToByteArray.call(body);
    return new Blob([bytes]);
  }

  const transformToWebStream = anyBody["transformToWebStream"] as
    | (() => ReadableStream<Uint8Array>)
    | null;
  if (typeof transformToWebStream === "function") {
    return await new Response(transformToWebStream.call(body)).blob();
  }

  const asyncIterator = anyBody[Symbol.asyncIterator] as
    | (() => AsyncIterator<unknown>)
    | null;
  if (typeof asyncIterator === "function") {
    const chunks: Uint8Array[] = [];
    for await (const chunk of body as AsyncIterable<unknown>) {
      if (chunk instanceof Uint8Array) {
        chunks.push(chunk);
        continue;
      }
      if (chunk instanceof ArrayBuffer) {
        chunks.push(new Uint8Array(chunk));
        continue;
      }
      if (typeof chunk === "string") {
        chunks.push(new TextEncoder().encode(chunk));
        continue;
      }
      if (
        chunk &&
        typeof chunk === "object" &&
        "buffer" in (chunk as Record<string, unknown>)
      ) {
        const possible = (chunk as { buffer?: ArrayBuffer }).buffer;
        if (possible instanceof ArrayBuffer) {
          chunks.push(new Uint8Array(possible));
          continue;
        }
      }
      throw new Error("Unsupported S3 body chunk type.");
    }
    return new Blob(chunks);
  }

  throw new Error("Unsupported S3 body type.");
}

function createBunS3Client(options: S3ClientOptions): S3ClientAdapter {
  const bun = getBunGlobal();
  if (!bun?.S3Client) {
    throw new Error("Bun runtime detected but Bun.S3Client is unavailable.");
  }
  const Ctor = bun.S3Client as unknown as BunS3Ctor;
  const client = new Ctor(options);
  return {
    async write(key: string, payload: Blob | Response): Promise<void> {
      await client.write(key, payload);
    },
    async read(key: string): Promise<Blob> {
      const file = client.file(key);
      return new Blob([await file.arrayBuffer()]);
    },
    async unlink(key: string): Promise<void> {
      await client.unlink(key);
    },
    async list(listOptions: S3ListOptions): Promise<S3ListResult> {
      const result = await client.list(listOptions);
      return {
        contents: result.contents?.map((entry) => ({ key: entry.key })),
        commonPrefixes: result.commonPrefixes?.map((entry) => ({
          prefix: entry.prefix,
        })),
      };
    },
  };
}

function loadAwsSdk(): AwsSdkModule {
  const loaded = runtimeRequire("@aws-sdk/client-s3") as
    | AwsSdkModule
    | { default?: AwsSdkModule };
  const sdk =
    typeof loaded === "object" &&
    loaded !== null &&
    "default" in loaded &&
    loaded.default
      ? loaded.default
      : (loaded as AwsSdkModule);
  if (!sdk || typeof sdk !== "object" || typeof sdk.S3Client !== "function") {
    throw new Error(
      'Failed to load "@aws-sdk/client-s3". Install it to use S3 depots under Node.',
    );
  }
  return sdk as AwsSdkModule;
}

function createNodeS3Client(options: S3ClientOptions): S3ClientAdapter {
  const sdk = loadAwsSdk();
  const client = new sdk.S3Client({
    region: options.region,
    endpoint: normalizeEndpoint(options.endpoint),
    forcePathStyle: options.virtualHostedStyle ? false : true,
    credentials:
      options.accessKeyId && options.secretAccessKey
        ? {
            accessKeyId: options.accessKeyId,
            secretAccessKey: options.secretAccessKey,
            sessionToken: options.sessionToken,
          }
        : undefined,
  });

  return {
    async write(key: string, payload: Blob | Response): Promise<void> {
      const blob = await toBlob(payload);
      await client.send(
        new sdk.PutObjectCommand({
          Bucket: options.bucket,
          Key: key,
          Body: new Uint8Array(await blob.arrayBuffer()),
          ContentType: blob.type || undefined,
        }),
      );
    },
    async read(key: string): Promise<Blob> {
      const result = (await client.send(
        new sdk.GetObjectCommand({
          Bucket: options.bucket,
          Key: key,
        }),
      )) as { Body?: unknown };
      return await streamBodyToBlob(result.Body);
    },
    async unlink(key: string): Promise<void> {
      await client.send(
        new sdk.DeleteObjectCommand({
          Bucket: options.bucket,
          Key: key,
        }),
      );
    },
    async list(listOptions: S3ListOptions): Promise<S3ListResult> {
      const result = (await client.send(
        new sdk.ListObjectsV2Command({
          Bucket: options.bucket,
          Prefix: listOptions.prefix,
          Delimiter: listOptions.delimiter,
          MaxKeys: listOptions.maxKeys,
        }),
      )) as {
        Contents?: Array<{ Key?: string }>;
        CommonPrefixes?: Array<{ Prefix?: string }>;
      };
      return {
        contents: (result.Contents ?? [])
          .map((entry) => entry.Key)
          .filter((key): key is string => typeof key === "string")
          .map((key) => ({ key })),
        commonPrefixes: (result.CommonPrefixes ?? [])
          .map((entry) => entry.Prefix)
          .filter((prefix): prefix is string => typeof prefix === "string")
          .map((prefix) => ({ prefix })),
      };
    },
  };
}

/**
 * Create a runtime-aware S3 adapter for Bun or Node.
 * Next: wire into `S3Depot` and call depot APIs.
 */
export function createS3Client(options: S3ClientOptions): S3ClientAdapter {
  if (isBunRuntime()) {
    return createBunS3Client(options);
  }
  return createNodeS3Client(options);
}

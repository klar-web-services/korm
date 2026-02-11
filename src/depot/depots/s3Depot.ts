import type { Depot } from "../depot";
import { DepotFile, type DepotFileBase } from "../depotFile";
import { RN } from "../../core/rn";
import { createDepotIdentifier } from "../depotIdent";
import crypto from "node:crypto";

function isReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return (
    !!value &&
    typeof value === "object" &&
    "getReader" in value &&
    typeof (value as ReadableStream).getReader === "function"
  );
}

/**
 * Options for S3-compatible depots.
 */
export type S3DepotOptions = {
  /** Optional explicit identifier used in RNs. */
  identifier?: string;
  /** Bucket name to store files in. */
  bucket: string;
  /** Custom endpoint (S3-compatible). */
  endpoint?: string;
  /** Region for signing and bucket creation. */
  region?: string;
  /** Access key id for signing requests. */
  accessKeyId?: string;
  /** Secret access key for signing requests. */
  secretAccessKey?: string;
  /** Optional session token for temporary credentials. */
  sessionToken?: string;
  /** Use virtual-hosted style URLs when true. */
  virtualHostedStyle?: boolean;
  /** Prefix applied to all stored keys. */
  prefix?: string;
  /** Auto-create the bucket when missing. Default true. */
  autoCreateBucket?: boolean;
};

/**
 * S3-compatible depot implementation backed by Bun.S3Client.
 * Use `korm.depots.s3(...)` to create and include in a pool.
 */
export class S3Depot implements Depot {
  readonly __DEPOT__: true = true;
  identifier: string;
  readonly type: "s3" = "s3";
  private _client: Bun.S3Client;
  private _prefix: string;
  private _bucket: string;
  private _endpoint?: string;
  private _region: string;
  private _accessKeyId?: string;
  private _secretAccessKey?: string;
  private _sessionToken?: string;
  private _virtualHostedStyle: boolean;
  private _autoCreateBucket: boolean;
  private _bucketEnsured: boolean = false;
  private _ensurePromise?: Promise<void>;

  /**
   * Create an S3 depot. Buckets are auto-created by default.
   */
  constructor(options: S3DepotOptions) {
    const identifier =
      options.identifier ??
      createDepotIdentifier("s3", [
        options.endpoint ?? "",
        options.bucket,
        options.region ?? "",
        options.prefix ?? "",
      ]);
    this.identifier = identifier;
    this._bucket = options.bucket;
    this._endpoint =
      options.endpoint ?? process.env.S3_ENDPOINT ?? process.env.AWS_ENDPOINT;
    this._region =
      options.region ??
      process.env.S3_REGION ??
      process.env.AWS_REGION ??
      "us-east-1";
    this._accessKeyId =
      options.accessKeyId ??
      process.env.S3_ACCESS_KEY_ID ??
      process.env.AWS_ACCESS_KEY_ID;
    this._secretAccessKey =
      options.secretAccessKey ??
      process.env.S3_SECRET_ACCESS_KEY ??
      process.env.AWS_SECRET_ACCESS_KEY;
    this._sessionToken =
      options.sessionToken ??
      process.env.S3_SESSION_TOKEN ??
      process.env.AWS_SESSION_TOKEN;
    this._virtualHostedStyle = options.virtualHostedStyle ?? false;
    this._autoCreateBucket = options.autoCreateBucket ?? true;
    this._prefix = this._normalizePrefix(options.prefix ?? "");
    this._client = new Bun.S3Client({
      bucket: options.bucket,
      endpoint: options.endpoint,
      region: options.region,
      accessKeyId: options.accessKeyId,
      secretAccessKey: options.secretAccessKey,
      sessionToken: options.sessionToken,
      virtualHostedStyle: options.virtualHostedStyle,
    });
  }

  private _normalizePrefix(prefix: string): string {
    const trimmed = prefix.trim().replace(/^\/+/, "").replace(/\/+$/, "");
    return trimmed;
  }

  private _keyFor(rn: RN): string {
    const value = rn.value();
    if (!this._prefix) return value;
    return `${this._prefix}/${value}`;
  }

  private _prefixForList(rn: RN): string {
    const value = rn.value();
    if (!value.endsWith("*")) {
      throw new Error(`Expected depot prefix RN but got "${value}".`);
    }
    const base = value.slice(0, -1);
    if (!this._prefix) return base;
    return `${this._prefix}/${base}`;
  }

  private _stripPrefix(key: string): string {
    if (!this._prefix) return key;
    const prefix = `${this._prefix}/`;
    return key.startsWith(prefix) ? key.slice(prefix.length) : key;
  }

  private _isNoSuchBucket(error: unknown): boolean {
    return Boolean(
      error &&
      typeof error === "object" &&
      "code" in (error as Record<string, unknown>) &&
      (error as { code?: string }).code === "NoSuchBucket",
    );
  }

  private _resolveEndpoint(): URL {
    if (this._endpoint) {
      const hasProtocol = /^[a-z]+:\/\//i.test(this._endpoint);
      return new URL(
        hasProtocol ? this._endpoint : `https://${this._endpoint}`,
      );
    }
    return new URL(`https://s3.${this._region}.amazonaws.com`);
  }

  private _buildBucketUrl(): { url: URL; host: string; canonicalPath: string } {
    const endpoint = this._resolveEndpoint();
    const basePath =
      endpoint.pathname === "/" ? "" : endpoint.pathname.replace(/\/$/, "");

    if (this._virtualHostedStyle) {
      const host = `${this._bucket}.${endpoint.host}`;
      const url = new URL(endpoint.toString());
      url.host = host;
      const path = basePath || "/";
      url.pathname = path;
      return { url, host, canonicalPath: path };
    }

    const bucketPath = `${basePath}/${this._bucket}`.replace(/\/+/g, "/");
    const path = bucketPath.startsWith("/") ? bucketPath : `/${bucketPath}`;
    const url = new URL(endpoint.toString());
    url.pathname = path;
    return { url, host: endpoint.host, canonicalPath: path };
  }

  private _hash(value: string): string {
    return crypto.createHash("sha256").update(value).digest("hex");
  }

  private _hmac(key: crypto.BinaryLike, value: string): Buffer {
    return crypto.createHmac("sha256", key).update(value).digest();
  }

  private _signRequest(opts: {
    method: string;
    url: URL;
    host: string;
    canonicalPath: string;
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
    payload: string;
    contentType?: string;
  }): { headers: Record<string, string> } {
    const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
    const dateStamp = amzDate.slice(0, 8);
    const payloadHash = this._hash(opts.payload);

    const canonicalHeadersParts = [
      `host:${opts.host}`,
      `x-amz-content-sha256:${payloadHash}`,
      `x-amz-date:${amzDate}`,
    ];
    if (opts.contentType) {
      canonicalHeadersParts.push(`content-type:${opts.contentType}`);
    }
    if (opts.sessionToken) {
      canonicalHeadersParts.push(`x-amz-security-token:${opts.sessionToken}`);
    }
    canonicalHeadersParts.sort();
    const canonicalHeaders = `${canonicalHeadersParts.join("\n")}\n`;
    const signedHeaders = canonicalHeadersParts
      .map((entry) => entry.split(":")[0]!)
      .join(";");

    const canonicalRequest = [
      opts.method,
      opts.canonicalPath,
      "",
      canonicalHeaders,
      signedHeaders,
      payloadHash,
    ].join("\n");

    const scope = `${dateStamp}/${opts.region}/s3/aws4_request`;
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      amzDate,
      scope,
      this._hash(canonicalRequest),
    ].join("\n");

    const kDate = this._hmac(`AWS4${opts.secretAccessKey}`, dateStamp);
    const kRegion = this._hmac(kDate, opts.region);
    const kService = this._hmac(kRegion, "s3");
    const kSigning = this._hmac(kService, "aws4_request");
    const signature = crypto
      .createHmac("sha256", kSigning)
      .update(stringToSign)
      .digest("hex");

    const authorization = `AWS4-HMAC-SHA256 Credential=${opts.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

    const headers: Record<string, string> = {
      Authorization: authorization,
      "x-amz-date": amzDate,
      "x-amz-content-sha256": payloadHash,
      host: opts.host,
    };
    if (opts.contentType) {
      headers["content-type"] = opts.contentType;
    }
    if (opts.sessionToken) {
      headers["x-amz-security-token"] = opts.sessionToken;
    }
    return { headers };
  }

  private async _createBucket(): Promise<void> {
    if (!this._autoCreateBucket) return;
    if (!this._accessKeyId || !this._secretAccessKey) {
      throw new Error("Missing S3 credentials required to create bucket.");
    }
    const { url, host, canonicalPath } = this._buildBucketUrl();
    const isRegional = this._region && this._region !== "us-east-1";
    const payload = isRegional
      ? `<?xml version="1.0" encoding="UTF-8"?><CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>${this._region}</LocationConstraint></CreateBucketConfiguration>`
      : "";
    const contentType = payload ? "application/xml" : undefined;

    const { headers } = this._signRequest({
      method: "PUT",
      url,
      host,
      canonicalPath,
      region: this._region,
      accessKeyId: this._accessKeyId,
      secretAccessKey: this._secretAccessKey,
      sessionToken: this._sessionToken,
      payload,
      contentType,
    });

    const response = await fetch(url.toString(), {
      method: "PUT",
      headers,
      body: payload || undefined,
    });

    if (response.ok) return;
    const text = await response.text();
    if (response.status === 409 || text.includes("BucketAlreadyOwnedByYou")) {
      return;
    }
    throw new Error(
      `Failed to create bucket "${this._bucket}": ${response.status} ${response.statusText} ${text}`,
    );
  }

  private async _ensureBucket(): Promise<void> {
    if (!this._autoCreateBucket || this._bucketEnsured) return;
    if (this._ensurePromise) {
      await this._ensurePromise;
      return;
    }
    this._ensurePromise = (async () => {
      try {
        await this._client.list({ maxKeys: 1 });
        this._bucketEnsured = true;
      } catch (error) {
        if (!this._isNoSuchBucket(error)) {
          throw error;
        }
        await this._createBucket();
        this._bucketEnsured = true;
      } finally {
        this._ensurePromise = undefined;
      }
    })();
    await this._ensurePromise;
  }

  /** @inheritdoc */
  async createFile(file: DepotFileBase): Promise<string> {
    if (file.rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${file.rn.value()}".`);
    }
    await this._ensureBucket();
    const key = this._keyFor(file.rn);
    const payload = isReadableStream(file.file)
      ? new Response(file.file)
      : (file.file as Blob);
    await this._client.write(key, payload);
    return key;
  }

  /** @inheritdoc */
  async getFile(rn: RN): Promise<DepotFile> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    await this._ensureBucket();
    const key = this._keyFor(rn);
    return new DepotFile(rn, this._client.file(key));
  }

  /** @inheritdoc */
  async deleteFile(rn: RN): Promise<boolean> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    await this._ensureBucket();
    const key = this._keyFor(rn);
    await this._client.unlink(key);
    return true;
  }

  /** @inheritdoc */
  async listFiles(rn: RN): Promise<DepotFile[]> {
    if (rn.pointsTo() !== "depotPrefix") {
      throw new Error(`Expected depot prefix RN but got "${rn.value()}".`);
    }
    await this._ensureBucket();
    const listPrefix = this._prefixForList(rn);
    const result = await this._client.list({ prefix: listPrefix });
    const contents = result.contents ?? [];
    const files: DepotFile[] = [];
    for (const entry of contents) {
      if (entry.key.endsWith(":")) continue;
      const rnRes = RN.create(this._stripPrefix(entry.key));
      if (rnRes.isErr()) {
        throw rnRes.error;
      }
      files.push(new DepotFile(rnRes.unwrap(), this._client.file(entry.key)));
    }
    return files;
  }

  /** @inheritdoc */
  async listDirs(rn: RN): Promise<string[]> {
    if (rn.pointsTo() !== "depotPrefix") {
      throw new Error(`Expected depot prefix RN but got "${rn.value()}".`);
    }
    await this._ensureBucket();
    const listPrefix = this._prefixForList(rn);
    const result = await this._client.list({
      prefix: listPrefix,
      delimiter: ":",
    });
    const prefixes = result.commonPrefixes ?? [];
    const base = this._stripPrefix(listPrefix);
    return prefixes.map((entry) => {
      const stripped = this._stripPrefix(entry.prefix);
      const relative = stripped.startsWith(base)
        ? stripped.slice(base.length)
        : stripped;
      return relative.replace(/:$/, "");
    });
  }

  /** @inheritdoc */
  async editFile(
    rn: RN,
    edit: (file: DepotFile) => Promise<DepotFileBase>,
  ): Promise<boolean> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    await this._ensureBucket();
    const file = await this.getFile(rn);
    const updated = await edit(file);
    const key = this._keyFor(rn);
    const payload = isReadableStream(updated.file)
      ? new Response(updated.file)
      : (updated.file as Blob);
    await this._client.write(key, payload);
    return true;
  }

  /** @internal */
  getPoolConfig(): { type: "s3"; options: S3DepotOptions } {
    return {
      type: "s3",
      options: {
        identifier: this.identifier,
        bucket: this._bucket,
        endpoint: this._endpoint,
        region: this._region,
        accessKeyId: this._accessKeyId,
        secretAccessKey: this._secretAccessKey,
        sessionToken: this._sessionToken,
        virtualHostedStyle: this._virtualHostedStyle,
        prefix: this._prefix,
        autoCreateBucket: this._autoCreateBucket,
      },
    };
  }
}

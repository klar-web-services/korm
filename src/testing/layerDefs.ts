import { resolve } from "node:path";
import { SqliteLayer } from "../sources/layers/sqlite";
import { PgLayer } from "../sources/layers/pg";
import { MysqlLayer } from "../sources/layers/mysql";
import { korm } from "../korm";
import { S3Depot } from "../depot/depots/s3Depot";
import { LocalDepot } from "../depot/depots/localDepot";

const sqlitePath = resolve(import.meta.dir, "test.sqlite");
export const sqll = new SqliteLayer(sqlitePath);
export const pg = new PgLayer(process.env.TESTING_PG_URL!);
export const mysql = new MysqlLayer(process.env.TESTING_MYSQL_URL!);

export const localDepot = new LocalDepot(resolve(import.meta.dir, "bucket"));

export const s3Depot = new S3Depot({
  bucket: "testing-bucket",
  endpoint: process.env.TESTING_S3_ENDPOINT!,
  accessKeyId: process.env.TESTING_S3_ACCESS_KEY_ID!,
  secretAccessKey: process.env.TESTING_S3_SECRET_ACCESS_KEY!,
});

export const layerPool = korm
  .pool()
  .setLayers(
    korm.use.layer(sqll).as("sqlite"),
    korm.use.layer(pg).as("pg"),
    korm.use.layer(mysql).as("mysql"),
  )
  .setDepots(korm.use.depot(localDepot).as("local"), korm.use.depot(s3Depot).as("s3"))
  .open();

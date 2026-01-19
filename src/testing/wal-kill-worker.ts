import { writeFileSync } from "node:fs";
import { korm } from "../korm";
import { sqll, localDepot } from "./layerDefs";

type SimpleRecord = { label: string; score: number };

const signalPath = process.env.KORM_WAL_KILL_SIGNAL;
const walNamespace = process.env.KORM_WAL_KILL_NAMESPACE;
const dataNamespace = process.env.KORM_WAL_KILL_DATA_NAMESPACE;
const dataKind = process.env.KORM_WAL_KILL_DATA_KIND;
const label = process.env.KORM_WAL_KILL_LABEL;

if (!signalPath || !walNamespace || !dataNamespace || !dataKind || !label) {
  throw new Error("Missing WAL kill worker configuration.");
}

const pool = korm
  .pool()
  .setLayers(korm.use.layer(sqll).as("sqlite"))
  .setDepots(korm.use.depot(localDepot).as("local"))
  .withWal({ depotIdent: "local", walNamespace, retention: "delete" })
  .open();

const originalInsert = sqll.insertItem.bind(sqll);
let signaled = false;
sqll.insertItem = (async (...args: Parameters<typeof originalInsert>) => {
  const result = await originalInsert(...args);
  if (!signaled) {
    signaled = true;
    writeFileSync(signalPath, "ready");
  }
  await new Promise(() => {});
  return result;
}) as typeof sqll.insertItem;

const item = korm
  .item<SimpleRecord>(pool)
  .from.data({
    namespace: dataNamespace,
    kind: dataKind,
    mods: [{ key: "from", value: "sqlite" }],
    data: { label, score: 1 },
  })
  .unwrap();

await item.create();

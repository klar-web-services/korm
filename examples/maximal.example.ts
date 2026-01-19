import { resolve } from "node:path";
import {
    korm,
    BackMan,
    // All these types are exposed
    type UncommittedItem, // Item types
    type FloatingItem,
    type Item,
    type UninitializedItem,
    type RN, // Resource Name type
    type SqliteLayer, // Source layers
    type PgLayer,
    type MysqlLayer,
    type Encrypt, // Symmetric encryption type
    type Password, // Password encryption type
    type DepotFileLike,
    // More layers soon
} from ".."; // In your app, that would be 'from "@fkws/korm"'

// --- SETUP ---

const { eq, and, or, gt } = korm.qfns; // Extract query functions for easier use later on

// I am using a sqlite db to hold my Car data
const sqlitePath = resolve(import.meta.dir, "../src/testing/test.sqlite");
const carDb = korm.layers.sqlite(sqlitePath);

// And a PG database to hold user data
const userDb = korm.layers.pg(process.env.PG_URL!)

// And a mysql database for very important documents:
const importantDb = korm.layers.mysql(process.env.MYSQL_URL!)

// Depots are used to store files
export const invoiceDepot = korm.depot.s3({
    bucket: "invoices",
    endpoint: process.env.S3_ENDPOINT!,
    accessKeyId: process.env.S3_ACCESS_KEY_ID!,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY!,
})

// Let's store WAL records in a local depot (we could also use the same depot as above or a different s3 depot):
const walDepotPath = resolve(import.meta.dir, "../src/testing/wal");
const walDepot = korm.depot.local(walDepotPath);

// These source layers are my layer pool
const pool = korm.pool()
    .setLayers(
        { layer: carDb, ident: "cardb" },
        { layer: userDb, ident: "userdb" },
        { layer: importantDb, ident: "importantDb" },
    )
    // Shared locks: persist RN locks in the selected layer for cross-process coordination.
    // If multiple instances write to the same RN space, enable this so only one instance mutates a given RN at a time.
    // acquire uses local mutexes plus the shared lock store (TTL-based) to serialize across instances.
    .withLocks({ layerIdent: "userdb" })
    .setDepots(
        { depot: invoiceDepot, ident: "invoiceDepot" },
        { depot: walDepot, ident: "walDepot" },
    )
    // WAL: undo/retry crash safety for create/commit/tx; on startup, pending ops are undone then retried.
    // Without shared locks, multiple instances may replay concurrently; with shared locks, recovery is coordinated so only one replays at a time and others wait.
    // Retention controls audit logs. WALs never contain cleartext values for encrypted fields.
    // Enable depotOps to snapshot depot file writes.
    .withWal({
        depotIdent: "walDepot",
        walNamespace: "demo",
        retention: "keep",
        depotOps: "record"
    })
    // Pool metadata: store the pool config in this layer so other instances can verify it and discover the pool later.
    // Backups require metadata because schedules and ownership live in __korm_backups__ on the meta layer.
    .withMeta({ layerIdent: "userdb" })
    // Backups: scheduled full snapshots per layer, written to a depot.
    // Each run exports __items__ tables plus __korm_meta__ and __korm_pool__ as JSON (encrypted fields stay encrypted).
    // Schedules are stored in __korm_backups__ on the meta layer; instances coordinate by locking schedule entries.
    // In-memory timers keep each instance on schedule; retention runs after each backup.
    // If you need to restore later, keep the BackMan instance and call manager.play(backupRn, { mode: "replace" | "merge" }).
    .backups("walDepot")
        .addInterval(
            "*", // Set the same interval for all source layers
            korm.interval.every("day").at(10, 0) 
        )
        .retain(10).days() // retain all backups younger than 10 days, discard older
    .open(); // Open the pool before you use it
        
        
// How else this can look:
// .backups()
//         .addInterval(
//              "cardb", // carDb target
//              korm.interval.every("day").at(10, 0).onSecond(35); 
//         )
//         .retain(10).days() // retain car backups younger than 10 days, discard older
//         .addInterval(
//              "importantdb",
//              korm.interval.every("week").on("saturday").at(0, 0)
//         )
//         .retain("all") // Retain all backups
//         .addInterval(
//              "userdb",
//              korm.interval.every("day").at(0, 0)
//         )
//         .retain(10).backups() // Retain 10 backups regardless of time

// We are talking about cars and users
type User = {
    firstName: string;
    lastName: string;
    password: Password<string>; // Mark for password encryption
    username: string;
}

type Car = {
    make: string;
    model: string;
    year: number;
    type: "sedan" | "suv"
    color: string;
    owner: RN<User>; // Reference to another Item by RN
    registered: boolean;
    registrationNumber: Encrypt<string>; // sensitive data, mark for symmetric encryption and redaction!
}

// --- USAGE ---
// Note: create/commit/tx automatically acquire in-process RN locks; use pool.locker for manual critical sections.

// I got a new user for my car management app:
const newUser = (await korm.item<User>(pool).from.data({
    namespace: "users",
    kind: "freetier",
    mods: [
        {
            key: "from", // Use the `from` RN Mod to target a specific source layer
            value: "userdb"
        }
    ],
    data: {
        firstName: "Fred",
        lastName: "Flintstone",
        password: await korm.password("iLoveDogs123"), // Immediately encrypts this with argon2 and makes the cleartext impossible to access.
        username: "freddieFlinny_69_420"
    }
}).create()) // This creates all needed tables with the correct columns to hold this item or inserts it if tables exist and have the correct shape
    .unwrap() // Unwrapping needed for test code; a real app would handle errors explicitly. (This is @fkws/klonk-result)

// The user is now in sync with the database.
// .from.data() is used to create an item from data, as you can see.
// This creates a FloatingItem<T>, which only has the .create() method.

// My user has gotten a new car:
const newCar = (await korm.item<Car>(pool).from.data({
    namespace: "cars",
    kind: "suv",
    mods: [{
        key: "from",
        value: "cardb"
    }],
    data: {
        make: "Citroen",
        model: "C4",
        year: 2014,
        color: "blue",
        type: "suv",
        owner: newUser.rn!, // Point to the user
        registered: true,
        registrationNumber: await korm.encrypt("1234567890") // Use AES-256-GCM to encrypt this value. Uses the KORM_ENCRYPTION_KEY environment variable to encrypt and later, decrypt. Outside of production, this also creates a new key for you with a warning and instructions, if you haven't set one yet. korm ships with a bin script that generates a new key for you for production / if you want to avoid auto-generating a new key in development.
    }
}).create()).unwrap();

// The car is now in sync with the database.

console.log(newCar.show({ color: true })); // Print the item for testing. This prints:
/**
┌─ [rn][from::cardb]:cars:suv:682599a4-1499-49a4-b339-7d315c57be10
│ { make: 'Citroen',
│   model: 'C4',
│   year: 2014,
│   color: 'blue',
│   type: 'suv',
│   owner:
│    RN {
│      _namespace: 'users',
│      _kind: 'freetier',
│      _id: '3dd91ede-37a4-4c25-a86a-6f1a9e132186',
│      _mods: Map(1) { 'from' => 'userdb' },
│      __RN__: true },
│   registered: true,
│   registrationNumber:
│    Encrypt { __ENCRYPT__: true,
│      _type: 'symmetric',
│      _value: '[REDACTED]', // Note that encrypted values are redacted when inspected. You must call .reveal() to access the cleartext in memory.
│      _encryptedValue:
│       Encrypted {
│         __ENCRYPTED__: true,
│         value: 'adbe293a0af013f982cd67de',
│         dataType: 'string',
│         type: 'symmetric',
│         salt: undefined,
│         iv: '60556460fe9939c9c79f7bfe',
│         authTag: 'b64884c3b74abcd54e39103da75813e2' } } }
└─
**/

const changeYear = (car: Car, year: number): Car => {
    // Complicated transformations can be done directly on data in TS
    car.year = year;
    return car;
}

const updatedCar = (
    await newCar.update(changeYear(newCar.data!, 2010)) // Update takes a DeepPartial<T>, in this case DeepPartial<Car> and produces an UncommittedItem<T>, an Item that exists in the DB but is out of sync here. It only has the .commit() method.
        .commit() // This saves an uncommited item to the source layer and returns an Item<T>
).unwrap();

// Someone is trying to log in as fred!
const incomingPassword = "badPassword123";
const incomingUsername = "freddieFlinny_69_420";

const userWantsLogin = await korm.item<User>(pool).from.query(korm.rn("[rn][from::userdb]:users:freetier:*")).where(
    eq("username", incomingUsername)
).get()

if (userWantsLogin.isErr()) {
    // send back 401
}

if (userWantsLogin.length > 1 || userWantsLogin.length < 1) {
    // 500 if > 1 because we did something wrong if a username is doubled
    // 401 if < 1 because user does not exist
}

const testResult = await userWantsLogin[0]?.data?.password.verifyPassword(incomingPassword);
if (!testResult) {
    // 401 -> Wrong password
} else {
    // 304 -> Allow login; redirect to dashboard
}

// ---

// Uh oh! Fred hasn't paid his fees. We should unregister all his cars.

// I need to get all of Fred's cars
const fredsCars = (
    await korm.item<Car>(pool).from.query(korm.rn("[rn][from::cardb]:cars:suv:*"))
        .where(
            eq("owner.username", "freddieFlinny_69_420") // Owner is an RN reference but korm will automatically resolve it for the query!
        ).get()
).unwrap()

console.log("One of Fred's cars:")
console.dir(fredsCars[0]?.data);
/**
One of Fred's cars:
{
  make: "Citroen",
  model: "C4",
  year: 2010,
  color: "blue",
  type: "suv",
  owner: "[rn][from::userdb]:users:freetier:3dd91ede-37a4-4c25-a86a-6f1a9e132186",
  registered: true,
  registrationNumber: Encrypt {
  __ENCRYPT__: true,
  _type: 'symmetric',
  _value: '[REDACTED]',
  _encryptedValue: Encrypted {
    __ENCRYPTED__: true,
    value: 'adbe293a0af013f982cd67de',
    dataType: 'string',
    type: 'symmetric',
    salt: undefined,
    iv: '60556460fe9939c9c79f7bfe',
    authTag: 'b64884c3b74abcd54e39103da75813e2'
  }
},
**/

const carUpdates = fredsCars.map((car) => car.update({ registered: false }).unwrap());

// We should also create a warning for the user which will be displayed in the app.
// Let's get Fred's RN first:
const fredRN = (await korm.item<User>(pool).from.query(korm.rn("[rn][from::userdb]:users:freetier:*")).where(
    eq("username", "freddieFlinny_69_420")
).get()).unwrap()[0]!.rn!;

type UserWarning = {
    user: RN<User>,
    warningText: string
}

const newWarning = korm.item<UserWarning>(pool).from.data({
    mods: [
        korm.mods.from("importantDb")
    ],
    namespace: "warnings",
    kind: "financial",
    data: {
        user: fredRN,
        warningText: "You have unpaid fees. To re-register your cars, please update your payment method."
    }
}).unwrap()

// We'll use a Tx. If any of these updates fail,
// all other operations in the Tx will be rolled
// back.
// 
// tx() accepts FloatingItem<T> and UncommittedItem<T>
// Depending on which one it sees, it creates (Floating)
// or updates (Uncommitted).
const txResult = await korm.tx(...carUpdates, newWarning).persist();

if (txResult.isErr()) {
    throw txResult.error;
}

console.log(`Touched:\n${txResult.items.map(i => {
    return "    " + i.item.rn?.value() + "\n";
}).join("").slice(0, -1)}`)
// Prints
/**
Touched:
    [rn][from::cardb]:cars:suv:682599a4-1499-49a4-b339-7d315c57be10
    [rn][from::importantDb]:warnings:financial:e4240304-de0a-42ea-9f5e-99b5a367e37f
**/

const theCar = (await korm.item<Car>(pool)
    .from.query(korm.rn("[rn][from::cardb]:cars:suv:*"))
    .where(eq("make", "Citroen"))
    .get(korm.resolve("owner")) // This path will be resolved across layers
).unwrap()[0]!;

// theCar now has type T (Car) extended with the resolved path's type (User), allowing us to type-safe access the resolved data.

console.log("The last name of the person who owns the car:")
console.dir(theCar.data?.owner.lastName);
// Prints:
// The last name of the person who owns the car:
// Flintstone


// If we modify the owner, korm will update the userdb row while keeping the cardb column a reference.
const theCarOwnerUpdate = theCar.update({ owner: { lastName: "Simpson" } }).unwrap();

// If we call .commit(), the changes will be persisted to the database.
const committedCar = (await theCarOwnerUpdate.commit()).unwrap();


// Let's get the car again to see the changes:
const theCarAgain = (await korm.item<Car>(pool)
    .from.rn(
        committedCar.rn!,
        korm.resolve("owner") // Like a JOIN across databases!
    )
).unwrap();

console.log("The last name of the person who owns the car after update:")
console.dir(theCarAgain.data?.owner.lastName);
// Prints:
// The last name of the person who owns the car after update:
// Simpson

// Let's create an invoice for Fred that points to a depot file.
type Invoice = {
    user: RN<User>;
    total: number;
    pdf: DepotFileLike;
};

const invoicePdf = korm.file({
    rn: korm.rn("[rn][depot::invoiceDepot]:invoices:fred:invoice-001.txt"),
    file: new Blob(["Invoice for Fred Flintstone - total: 199"], { type: "text/plain" }),
}); // You can also just save the file directly to the depot with .create().
// You don't need an Item<T> to work with files.

const newInvoice = (await korm.item<Invoice>(pool).from.data({
    mods: [
        korm.mods.from("importantDb")
    ],
    namespace: "invoices",
    kind: "customer",
    data: {
        user: fredRN,
        total: 199,
        pdf: invoicePdf
    }
}).create()).unwrap();

const invoiceWithFile = (await korm.item<Invoice>(pool).from.rn(
    newInvoice.rn!,
    korm.resolve("pdf")
)).unwrap();

console.log("Invoice file contents:");
console.log(await invoiceWithFile.data?.pdf.text());

await pool.close(); // Close the pool, so the program exits. If your program doesn't exit, you forgot to call this.

// Restore from a backup file (example): use a BackMan instance so you can call play(...) with a backup RN.
const restoreManager = new BackMan();
const restorePool = korm.pool()
    .setLayers({ layer: korm.layers.sqlite("./restore.sqlite"), ident: "restore" })
    .setDepots({ depot: walDepot, ident: "walDepot" })
    .withMeta({ layerIdent: "restore" })
    .open();
restorePool.configureBackups("walDepot", restoreManager);
await restoreManager.play(
    korm.rn("[rn][depot::walDepot]:__korm_backups__:userdb:20240102T030405Z:backup-00000000-0000-4000-8000-000000000000.json"),
    { mode: "replace" }
);
await restorePool.close();

import { korm, type RN } from "..";

// Single-layer pool: no WAL, backups, depots, or federation needed.
const carDb = korm.layers.sqlite("/home/fkws/workspaces/kws/klonk-orm/src/testing/test.sqlite");
const pool = korm.pool()
    .setLayers({ layer: carDb, ident: "cardb" })
    .open();

type User = {
    username: string;
};

type Car = {
    make: string;
    model: string;
    owner: RN<User>;
};

const user = (await korm.item<User>(pool).from.data({
    namespace: "users",
    kind: "basic",
    data: {
        username: "fred"
    }
}).create()).unwrap();

const car = (await korm.item<Car>(pool).from.data({
    namespace: "cars",
    kind: "suv",
    data: {
        make: "Citroen",
        model: "C4",
        owner: user.rn!
    }
}).create()).unwrap();

const { eq } = korm.qfns;

const cars = (await korm.item<Car>(pool)
    // No `from` mod needed with a single layer.
    .from.query(korm.rn("[rn]:cars:suv:*"))
    .where(eq("make", "Citroen"))
    .get()
).unwrap();

const updatedCar = (await car.update({ model: "C5" }).commit()).unwrap();

console.log(cars.map((item) => item.show({ color: true })).join("\n"));
console.log(updatedCar.show({ color: true }));

await pool.close();

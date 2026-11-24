# Databricks notebook source
# MAGIC %md
# MAGIC # Food delivery data generator
# MAGIC
# MAGIC Writes the raw files of our (fictional) Novi Sad food delivery app into the Unity Catalog
# MAGIC landing volume `/Volumes/<catalog>/landing/raw/`:
# MAGIC
# MAGIC | Folder | Content | Format |
# MAGIC |---|---|---|
# MAGIC | `orders/` | one event per delivered order (incl. rating and review text) | JSON lines |
# MAGIC | `gps/` | courier GPS pings (pickup, en route, delivered) | JSON lines |
# MAGIC | `customers_cdc/` | customer profile changes (INSERT / UPDATE / DELETE) | JSON lines |
# MAGIC | `restaurants/` | nightly full snapshot of the restaurant catalog | CSV |
# MAGIC
# MAGIC **Modes**
# MAGIC
# MAGIC | Mode | What it does |
# MAGIC |---|---|
# MAGIC | `backfill` | 7 days of history, the customer snapshot and the first restaurants file. Runs once (skipped if history exists). |
# MAGIC | `stream` | new orders and GPS pings every few seconds, customer address changes now and then |
# MAGIC | `nightly_restaurants` | drops a new restaurants CSV (one new restaurant, changed prep times). Starts the file-arrival job. |
# MAGIC | `bad_records` | planned failure: negative amount, missing restaurant, unparsable amount, GPS ping outside Novi Sad |
# MAGIC | `late_event` | planned failure: one order delivered 40 min ago (beyond the watermark) and one 3 min ago (within it) |
# MAGIC | `schema_change` | planned failure: orders with a new `tip_amount` field |
# MAGIC | `customer_changes` | on-demand CDC burst: `customer_changes` customers move, one changes the phone number, one signs up, one deletes the account |
# MAGIC
# MAGIC Order **#1042** (Liman, delivered late) is always part of the history: we follow it through every layer.

# COMMAND ----------

import csv
import json
import math
import os
import random
import time
import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Belgrade")

# District centers (approximate) and the extra delay a courier typically gets there.
DISTRICTS = {
    "Stari Grad": (45.2551, 19.8452),
    "Liman": (45.2417, 19.8380),
    "Grbavica": (45.2465, 19.8290),
    "Podbara": (45.2600, 19.8550),
    "Salajka": (45.2650, 19.8380),
    "Detelinara": (45.2620, 19.8170),
    "Novo Naselje": (45.2540, 19.7980),
    "Telep": (45.2370, 19.8100),
    "Petrovaradin": (45.2510, 19.8650),
    "Sremska Kamenica": (45.2230, 19.8410),
}
# Liman: roadworks on Bulevar cara Lazara. Petrovaradin / Kamenica: across the bridge.
DISTRICT_DELAY = {"Liman": 9, "Petrovaradin": 6, "Sremska Kamenica": 7}
# Kitchens that are sometimes very slow (minutes added on a bad day).
SLOW_KITCHENS = {"R001": 12, "R014": 10, "R012": 8}

STREETS = {
    "Stari Grad": ["Zmaj Jovina", "Dunavska", "Pašićeva", "Njegoševa"],
    "Liman": ["Bulevar cara Lazara", "Narodnog fronta", "Fruškogorska", "Balzakova"],
    "Grbavica": ["Gogoljeva", "Puškinova", "Braće Ribnikar"],
    "Podbara": ["Kosovska", "Vojvode Šupljikca"],
    "Salajka": ["Temerinska", "Partizanska"],
    "Detelinara": ["Rumenačka", "Kornelija Stankovića", "Primorska"],
    "Novo Naselje": ["Bulevar Jaše Tomića", "Seljačkih buna"],
    "Telep": ["Lovćenska", "Šajkaška"],
    "Petrovaradin": ["Preradovićeva", "Beogradska"],
    "Sremska Kamenica": ["Vojvode Putnika", "Karađorđeva"],
}

MENUS = {
    "Grill": [("Pljeskavica", 690), ("Ćevapi 10 kom", 780), ("Piletina na žaru", 720), ("Pomfrit", 250), ("Šopska salata", 350)],
    "Pizza": [("Pizza Capricciosa", 1150), ("Pizza Margherita", 950), ("Pizza Quattro Formaggi", 1250), ("Coca-Cola", 200)],
    "Bakery": [("Burek sa sirom", 320), ("Burek sa mesom", 350), ("Jogurt", 120), ("Pita sa višnjama", 280)],
    "Sushi": [("Sushi set 16 kom", 1900), ("California roll", 1100), ("Miso supa", 450), ("Edamame", 420)],
    "Mexican": [("Tacos al pastor", 890), ("Burrito", 990), ("Nachos", 650), ("Limunada", 280)],
    "Asian": [("Tonkotsu ramen", 1250), ("Gyoza", 690), ("Pad thai", 1090), ("Ledeni čaj", 260)],
    "Vegetarian": [("Falafel bowl", 890), ("Veggie burger", 940), ("Humus i pita", 550)],
    "Chinese": [("Piletina kung pao", 980), ("Prolećne rolnice", 450), ("Pirinač sa povrćem", 520)],
    "Serbian": [("Sarma", 850), ("Pasulj sa kobasicom", 790), ("Punjene paprike", 820), ("Proja", 220)],
    "Indian": [("Butter chicken", 1290), ("Dal makhani", 980), ("Garlic naan", 290), ("Mango lassi", 390)],
    "Desserts": [("Palačinke sa nutelom", 420), ("Palačinke sa plazmom", 450), ("Vafl", 520)],
    "Seafood": [("Riblja čorba", 690), ("Smuđ na žaru", 1650), ("Pržene lignje", 1390)],
    "Greek": [("Giros pita", 590), ("Giros tanjir", 890), ("Grčka salata", 550)],
    "Healthy": [("Cezar salata", 790), ("Poke bowl", 1150), ("Smoothie", 450)],
    "Italian": [("Pasta carbonara", 990), ("Lazanje", 1090), ("Tiramisu", 520)],
    "Burgers": [("Classic burger", 890), ("Cheeseburger", 950), ("Batat pomfrit", 380), ("Milkshake", 450)],
}
DELIVERY_FEE = 150.0

LATE_REVIEWS = {
    "kitchen": [
        "Late pickup, the courier waited ages at the restaurant.",
        "The order sat at the restaurant for 20 minutes before pickup.",
        "Restaurant was too slow, courier had to wait for the food.",
    ],
    "traffic": [
        "Courier was stuck in traffic because of the roadworks.",
        "Courier got lost looking for my building.",
        "Took forever, the courier had to go around the closed street.",
    ],
    "distance": [
        "Food arrived cold.",
        "Food arrived cold and the fries were soggy.",
        "Long wait and everything was lukewarm.",
    ],
}
GOOD_REVIEWS = [
    "Fast delivery, food was still hot!",
    "Great as always.",
    "Courier was very friendly.",
    "Arrived earlier than promised.",
    "Tasty and on time.",
]

FIRST_NAMES = ["Milica", "Jovana", "Ana", "Marija", "Teodora", "Katarina", "Nikola", "Marko", "Stefan", "Luka",
               "Lazar", "Nemanja", "Dušan", "Aleksandar", "Jelena", "Ivana", "Milan", "Petar", "Sara", "Mina"]
LAST_NAMES = ["Petrović", "Jovanović", "Nikolić", "Marković", "Đorđević", "Stojanović", "Ilić", "Pavlović",
              "Kovačević", "Popović", "Todorović", "Savić", "Mitrović", "Lukić", "Babić", "Tomić"]

# Order #1042 story: customer 77 (Milica Petrović) moved from Detelinara to Liman two days before
# the history starts, then orders grilled food from Stari Grad, and it arrives late.
STORY_ORDER_ID = 1042
STORY_CUSTOMER_ID = 77
STORY_RESTAURANT_ID = "R001"

NEW_RESTAURANTS = [
    {"restaurant_id": "R021", "name": "Poke Bowl Liman", "cuisine": "Healthy", "district": "Liman",
     "address": "Balzakova 20", "latitude": "45.2412", "longitude": "19.8371", "avg_prep_minutes": "11", "is_active": "true"},
    {"restaurant_id": "R022", "name": "Kebab Novo Naselje", "cuisine": "Grill", "district": "Novo Naselje",
     "address": "Seljačkih buna 30", "latitude": "45.2547", "longitude": "19.7969", "avg_prep_minutes": "12", "is_active": "true"},
    {"restaurant_id": "R023", "name": "Dim Sum Podbara", "cuisine": "Chinese", "district": "Podbara",
     "address": "Kosovska 8", "latitude": "45.2593", "longitude": "19.8547", "avg_prep_minutes": "18", "is_active": "true"},
    {"restaurant_id": "R024", "name": "Pizza Telep", "cuisine": "Pizza", "district": "Telep",
     "address": "Lovćenska 31", "latitude": "45.2361", "longitude": "19.8113", "avg_prep_minutes": "17", "is_active": "true"},
]

# Orders per hour of the day (lunch and dinner peaks). The app delivers 09:00-23:59.
HOUR_WEIGHTS = {9: 1, 10: 2, 11: 4, 12: 9, 13: 9, 14: 6, 15: 4, 16: 4, 17: 6, 18: 10, 19: 11, 20: 9, 21: 6, 22: 3, 23: 1}

# COMMAND ----------

# ---------- helpers (pure Python, no Spark) ----------

def now_local() -> datetime:
    """Wall-clock time in Novi Sad, without tz info (stored as-is, so students see local times)."""
    return datetime.now(TZ).replace(tzinfo=None, microsecond=0)


def ts(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def ascii_slug(text: str) -> str:
    table = str.maketrans({"č": "c", "ć": "c", "š": "s", "ž": "z", "đ": "dj", "Č": "C", "Ć": "C", "Š": "S", "Ž": "Z", "Đ": "Dj"})
    return text.translate(table).lower().replace(" ", "")


def km_between(a, b) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def round_up_5(minutes: float) -> int:
    return int(math.ceil(minutes / 5.0) * 5)


def jitter(point, rng, meters=400):
    d = meters / 111_000
    return (round(point[0] + rng.uniform(-d, d), 6), round(point[1] + rng.uniform(-d, d), 6))


def new_customer(customer_id: int, rng: random.Random, district: str = None) -> dict:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    district = district or rng.choice(list(DISTRICTS))
    return {
        "customer_id": customer_id,
        "first_name": first,
        "last_name": last,
        "email": f"{ascii_slug(first)}.{ascii_slug(last)}{customer_id}@example.com",
        "phone": f"+381 6{rng.randint(0, 6)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}",
        "address": f"{rng.choice(STREETS[district])} {rng.randint(1, 120)}",
        "district": district,
    }


def cdc_record(customer: dict, operation: str, change_ts: datetime) -> dict:
    return {**customer, "operation": operation, "change_ts": ts(change_ts)}


def move_customer(customer: dict, rng: random.Random, district: str = None) -> dict:
    district = district or rng.choice([d for d in DISTRICTS if d != customer["district"]])
    return {**customer, "district": district, "address": f"{rng.choice(STREETS[district])} {rng.randint(1, 120)}"}


def make_order(order_id, restaurant, customer, rng, order_ts=None, delivered_at=None, force_late=False, with_tip=False):
    """Simulate one delivered order. Give either order_ts (history) or delivered_at (live stream).

    Returns (order_record, gps_records)."""
    r_point = (float(restaurant["latitude"]), float(restaurant["longitude"]))
    district = customer["district"]
    c_point = jitter(DISTRICTS[district], rng)
    km = km_between(r_point, c_point)
    avg_prep = int(restaurant["avg_prep_minutes"])

    prep = max(4.0, rng.gauss(avg_prep, 3))
    kitchen_extra = 0.0
    if restaurant["restaurant_id"] in SLOW_KITCHENS and rng.random() < 0.45:
        kitchen_extra = SLOW_KITCHENS[restaurant["restaurant_id"]] * rng.uniform(0.7, 1.4)
    travel = max(3.0, 6 + km * 3.2 + rng.gauss(0, 2))

    ref = order_ts or delivered_at
    peak = 5.0 if 18 <= ref.hour < 21 else (2.0 if 12 <= ref.hour < 14 else 0.0)
    district_extra = DISTRICT_DELAY.get(district, 0) * rng.uniform(0.6, 1.4)

    if force_late:
        kitchen_extra = max(kitchen_extra, 18.0)

    actual = prep + kitchen_extra + travel + peak + district_extra
    promised = round_up_5(avg_prep + 6 + km * 3.2 + 10)

    if order_ts is None:
        order_ts = delivered_at - timedelta(minutes=actual)
    order_ts = order_ts.replace(microsecond=0)
    picked_up_ts = (order_ts + timedelta(minutes=prep + kitchen_extra)).replace(microsecond=0)
    delivered_ts = (order_ts + timedelta(minutes=actual)).replace(microsecond=0)
    promised_ts = order_ts + timedelta(minutes=promised)
    is_late = delivered_ts > promised_ts

    menu = MENUS[restaurant["cuisine"]]
    items = []
    for item, price in rng.sample(menu, k=rng.randint(1, min(3, len(menu)))):
        items.append({"item": item, "qty": rng.choice([1, 1, 1, 2]), "unit_price": float(price)})
    total = round(sum(i["qty"] * i["unit_price"] for i in items) + DELIVERY_FEE, 2)

    rating, review = None, None
    if is_late:
        causes = {"kitchen": kitchen_extra, "traffic": district_extra + peak, "distance": travel - 10}
        cause = max(causes, key=causes.get)
        rating = rng.choice([1, 2, 2, 3])
        if rng.random() < 0.8:
            review = rng.choice(LATE_REVIEWS[cause])
    else:
        rating = rng.choice([4, 5, 5]) if rng.random() < 0.6 else None
        if rating and rng.random() < 0.5:
            review = rng.choice(GOOD_REVIEWS)

    courier_id = f"C{rng.randint(1, 40):02d}"
    order = {
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "restaurant_id": restaurant["restaurant_id"],
        "courier_id": courier_id,
        "delivery_district": district,
        "delivery_address": customer["address"],
        "order_ts": ts(order_ts),
        "promised_delivery_ts": ts(promised_ts),
        "picked_up_ts": ts(picked_up_ts),
        "delivered_ts": ts(delivered_ts),
        "items": items,
        "delivery_fee": DELIVERY_FEE,
        "total_amount": total,
        "payment_method": rng.choice(["card", "card", "cash", "wallet"]),
        "rating": rating,
        "review_text": review,
    }
    if with_tip:
        order["tip_amount"] = float(rng.choice([0, 0, 50, 100, 100, 150, 200, 300]))

    mid_ts = picked_up_ts + (delivered_ts - picked_up_ts) / 2
    mid_point = ((r_point[0] + c_point[0]) / 2, (r_point[1] + c_point[1]) / 2)
    gps = [
        {"courier_id": courier_id, "order_id": order_id, "event_type": "pickup", "event_ts": ts(picked_up_ts),
         "latitude": r_point[0], "longitude": r_point[1], "speed_kmh": 0.0},
        {"courier_id": courier_id, "order_id": order_id, "event_type": "en_route", "event_ts": ts(mid_ts.replace(microsecond=0)),
         "latitude": round(mid_point[0], 6), "longitude": round(mid_point[1], 6), "speed_kmh": round(rng.uniform(15, 35), 1)},
        {"courier_id": courier_id, "order_id": order_id, "event_type": "delivered", "event_ts": ts(delivered_ts),
         "latitude": c_point[0], "longitude": c_point[1], "speed_kmh": 0.0},
    ]
    return order, gps


def order_times_for_day(day: date, rng: random.Random, until: datetime = None):
    base = 260 if day.weekday() in (4, 5) else 200
    n = int(rng.gauss(base, 15))
    hours = rng.choices(list(HOUR_WEIGHTS), weights=list(HOUR_WEIGHTS.values()), k=n)
    times = sorted(datetime(day.year, day.month, day.day, h, rng.randint(0, 59), rng.randint(0, 59)) for h in hours)
    return [t for t in times if until is None or t <= until]


def build_history(restaurants, days, rng, today=None):
    """Deterministic history: customers + CDC changes + orders + GPS for the last `days` days."""
    today = today or now_local().date()
    start_day = today - timedelta(days=days)
    active = [r for r in restaurants if r["is_active"].lower() == "true"]
    story_restaurant = next(r for r in restaurants if r["restaurant_id"] == STORY_RESTAURANT_ID)

    # Customer snapshot, 30 days before the history starts.
    customers = {cid: new_customer(cid, rng) for cid in range(1, 401)}
    customers[STORY_CUSTOMER_ID] = {**new_customer(STORY_CUSTOMER_ID, rng, "Detelinara"),
                                    "first_name": "Milica", "last_name": "Petrović",
                                    "email": f"milica.petrovic{STORY_CUSTOMER_ID}@example.com"}
    snapshot_ts = datetime.combine(start_day - timedelta(days=30), datetime.min.time()) + timedelta(hours=8)
    cdc = {"initial": [cdc_record(c, "INSERT", snapshot_ts + timedelta(seconds=i)) for i, c in enumerate(customers.values())]}

    # The story move: Detelinara -> Liman, two days before the history starts.
    move_ts = datetime.combine(start_day - timedelta(days=2), datetime.min.time()) + timedelta(hours=10, minutes=15)
    customers[STORY_CUSTOMER_ID] = move_customer(customers[STORY_CUSTOMER_ID], rng, "Liman")
    cdc["move"] = [cdc_record(customers[STORY_CUSTOMER_ID], "UPDATE", move_ts)]

    orders_by_day, gps_by_day = {}, {}
    next_id = 1001
    cutoff = now_local() - timedelta(minutes=30)
    for offset in range(days + 1):
        day = start_day + timedelta(days=offset)
        day_start = datetime.combine(day, datetime.min.time()) + timedelta(hours=8)
        changes = []
        # A few customers move or change their phone number every morning.
        for k in range(rng.randint(3, 7)):
            cid = rng.choice([c for c in customers if c != STORY_CUSTOMER_ID])
            if rng.random() < 0.7:
                customers[cid] = move_customer(customers[cid], rng)
            else:
                customers[cid] = {**customers[cid], "phone": f"+381 6{rng.randint(0, 6)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}"}
            changes.append(cdc_record(customers[cid], "UPDATE", day_start + timedelta(minutes=k)))
        if offset == days - 2:
            # Milica changes her phone number: TRACK HISTORY ON address, district -> no new history row.
            customers[STORY_CUSTOMER_ID] = {**customers[STORY_CUSTOMER_ID], "phone": "+381 64 555 1042"}
            changes.append(cdc_record(customers[STORY_CUSTOMER_ID], "UPDATE", day_start + timedelta(minutes=30)))
        if offset == 3:
            gone = customers.pop(399)
            changes.append(cdc_record(gone, "DELETE", day_start + timedelta(minutes=45)))
        cdc[day.isoformat()] = changes

        orders, gps = [], []
        for order_ts in order_times_for_day(day, rng, until=cutoff if day == today else None):
            if next_id == STORY_ORDER_ID:
                order_ts = order_ts.replace(hour=12, minute=37)
                order, pings = make_order(next_id, story_restaurant, customers[STORY_CUSTOMER_ID], rng,
                                          order_ts=order_ts, force_late=True)
                order["review_text"] = "Food arrived cold, the courier waited at the restaurant for ages."
                order["rating"] = 2
            else:
                customer = customers[rng.choice(list(customers))]
                order, pings = make_order(next_id, rng.choice(active), customer, rng, order_ts=order_ts)
            orders.append(order)
            gps.extend(pings)
            next_id += 1
        orders_by_day[day.isoformat()] = orders
        gps_by_day[day.isoformat()] = gps

    return {"customers": customers, "cdc": cdc, "orders": orders_by_day, "gps": gps_by_day,
            "next_order_id": next_id, "start_day": start_day}

# COMMAND ----------

# ---------- file I/O on the landing volume ----------

def write_jsonl(path: str, records) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n")


def write_restaurants_csv(path: str, rows) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fields = ["restaurant_id", "name", "cuisine", "district", "address", "latitude", "longitude", "avg_prep_minutes", "is_active"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def read_state(name: str, default=None):
    path = f"{STATE_DIR}/{name}.json"
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_state(name: str, value) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(f"{STATE_DIR}/{name}.json", "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False)


def stamp() -> str:
    return f"{now_local():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"


def load_base_restaurants(source: str):
    candidates = [source] if source else []
    candidates.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..", "data", "restaurants.csv")))
    for path in candidates:
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return list(csv.DictReader(f))
    raise FileNotFoundError(f"restaurants.csv not found, tried: {candidates}. Set the restaurants_source widget.")


def current_restaurants():
    state = read_state("restaurants")
    return state["rows"] if state else load_base_restaurants(RESTAURANTS_SOURCE)

# COMMAND ----------

# ---------- modes ----------

def run_backfill():
    if read_state("orders"):
        print("History already exists (found generator state). Run the reset job first to regenerate it.")
        return
    rng = random.Random(1042)
    restaurants = load_base_restaurants(RESTAURANTS_SOURCE)
    history = build_history(restaurants, BACKFILL_DAYS, rng)

    first_day = history["start_day"] - timedelta(days=1)
    write_restaurants_csv(f"{LANDING}/restaurants/restaurants_{first_day:%Y%m%d}_v000.csv", restaurants)
    write_state("restaurants", {"version": 0, "rows": restaurants})

    for key, records in history["cdc"].items():
        if records:
            write_jsonl(f"{LANDING}/customers_cdc/customers_{key.replace('-', '')}.json", records)
    for day, records in history["orders"].items():
        if records:
            write_jsonl(f"{LANDING}/orders/orders_{day.replace('-', '')}_history.json", records)
            write_jsonl(f"{LANDING}/gps/gps_{day.replace('-', '')}_history.json", history["gps"][day])

    write_state("customers", {str(k): v for k, v in history["customers"].items()})
    write_state("orders", {"next_order_id": history["next_order_id"]})
    n_orders = sum(len(v) for v in history["orders"].values())
    print(f"Backfill done: {n_orders} orders, {len(history['customers'])} customers, {len(restaurants)} restaurants "
          f"from {history['start_day']} until now. Order #{STORY_ORDER_ID} is in orders_{history['start_day']:%Y%m%d}_history.json")


def run_stream():
    rng = random.Random()
    customers = read_state("customers")
    order_state = read_state("orders")
    if not customers or not order_state:
        raise RuntimeError("No generator state found. Run the backfill (demo setup job) first.")
    next_id = order_state["next_order_id"]
    end = time.time() + DURATION_MINUTES * 60
    batch = 0
    print(f"Streaming for {DURATION_MINUTES} min, one batch every {INTERVAL_SECONDS}s, starting at order #{next_id}")
    while time.time() < end:
        now = now_local()
        customers = read_state("customers") or customers  # picks up changes from a customer_changes burst
        restaurants = [r for r in current_restaurants() if r["is_active"].lower() == "true"]
        orders, gps = [], []
        for _ in range(max(1, int(rng.gauss(ORDERS_PER_BATCH, 2)))):
            customer = customers[rng.choice(list(customers))]
            delivered_at = now - timedelta(seconds=rng.randint(0, 20))
            order, pings = make_order(next_id, rng.choice(restaurants), customer, rng,
                                      delivered_at=delivered_at, with_tip=WITH_TIP)
            orders.append(order)
            gps.extend(pings)
            next_id += 1
        tag = stamp()
        write_jsonl(f"{LANDING}/orders/orders_{tag}.json", orders)
        write_jsonl(f"{LANDING}/gps/gps_{tag}.json", gps)

        if batch % 12 == 11:  # roughly once a minute: someone moves
            cid = rng.choice([k for k in customers if int(k) != STORY_CUSTOMER_ID])
            customers[cid] = move_customer(customers[cid], rng)
            write_jsonl(f"{LANDING}/customers_cdc/customers_{tag}.json", [cdc_record(customers[cid], "UPDATE", now)])
            write_state("customers", customers)
            print(f"  customer {cid} moved to {customers[cid]['district']}")

        write_state("orders", {"next_order_id": next_id})
        print(f"[{now:%H:%M:%S}] batch {batch}: {len(orders)} orders (last #{next_id - 1})")
        batch += 1
        time.sleep(INTERVAL_SECONDS)
    print(f"Done after {batch} batches.")


def run_nightly_restaurants():
    state = read_state("restaurants") or {"version": 0, "rows": load_base_restaurants(RESTAURANTS_SOURCE)}
    rng = random.Random()
    version = state["version"] + 1
    rows = [dict(r) for r in state["rows"]]
    added = None
    if version <= len(NEW_RESTAURANTS):
        added = NEW_RESTAURANTS[version - 1]
        rows.append(dict(added))
    for r in rng.sample(rows, k=2):
        r["avg_prep_minutes"] = str(max(5, int(r["avg_prep_minutes"]) + rng.choice([-3, -2, 2, 4])))
    path = f"{LANDING}/restaurants/restaurants_{now_local():%Y%m%d_%H%M%S}_v{version:03d}.csv"
    write_restaurants_csv(path, rows)
    write_state("restaurants", {"version": version, "rows": rows})
    print(f"Wrote {path} ({len(rows)} restaurants)" + (f", new: {added['name']} ({added['district']})" if added else ""))
    print("The file-arrival job starts within about a minute.")


def run_customer_changes():
    """On-demand CDC burst: one file with UPDATEs (moves and a phone change), an INSERT and a DELETE."""
    rng = random.Random()
    customers = read_state("customers")
    if not customers:
        raise RuntimeError("No generator state found. Run the backfill (demo setup job) first.")
    now = now_local()
    candidates = [k for k in customers if int(k) != STORY_CUSTOMER_ID]
    picked = rng.sample(candidates, k=CUSTOMER_CHANGES + 2)
    movers, phone_changer, leaver = picked[:CUSTOMER_CHANGES], picked[-2], picked[-1]
    records, summary = [], []

    for cid in movers:  # new address -> new SCD2 history row
        old_district = customers[cid]["district"]
        customers[cid] = move_customer(customers[cid], rng)
        records.append(cdc_record(customers[cid], "UPDATE", now))
        summary.append(f"  UPDATE  customer {cid}: moved {old_district} -> {customers[cid]['district']} (new history row)")

    customers[phone_changer] = {**customers[phone_changer],
                                "phone": f"+381 6{rng.randint(0, 6)} {rng.randint(100, 999)} {rng.randint(1000, 9999)}"}
    records.append(cdc_record(customers[phone_changer], "UPDATE", now))
    summary.append(f"  UPDATE  customer {phone_changer}: new phone number (updated in place, no new history row)")

    new_id = str(max(int(k) for k in customers) + 1)
    customers[new_id] = new_customer(int(new_id), rng)
    records.append(cdc_record(customers[new_id], "INSERT", now))
    summary.append(f"  INSERT  customer {new_id}: {customers[new_id]['first_name']} {customers[new_id]['last_name']} "
                   f"signed up in {customers[new_id]['district']}")

    gone = customers.pop(leaver)
    records.append(cdc_record(gone, "DELETE", now))
    summary.append(f"  DELETE  customer {leaver}: deleted the account (history row closed with __END_AT, gone from gold.customers)")

    path = f"{LANDING}/customers_cdc/customers_{stamp()}_burst.json"
    write_jsonl(path, records)
    write_state("customers", customers)
    ids = ", ".join([*movers, phone_changer, new_id, leaver])
    print(f"Wrote {len(records)} change events to {path}")
    print("\n".join(summary))
    print("After the next pipeline update, check:\n"
          f"  SELECT customer_id, district, address, phone, __START_AT, __END_AT FROM silver.customers\n"
          f"  WHERE customer_id IN ({ids}) ORDER BY customer_id, __START_AT")


def inject_ids(n: int):
    base = 9_000_000 + (int(time.time()) % 100_000) * 10
    return [base + i for i in range(n)]


def run_bad_records():
    rng = random.Random()
    customers = read_state("customers")
    restaurants = current_restaurants()
    now = now_local()
    ids = inject_ids(3)
    orders, gps = [], []
    for oid in ids:
        order, pings = make_order(oid, rng.choice(restaurants), customers[rng.choice(list(customers))], rng, delivered_at=now)
        orders.append(order)
        gps.extend(pings)
    orders[0]["total_amount"] = -350.0          # fails positive_amount -> dropped + quarantined
    orders[1]["restaurant_id"] = None           # fails valid_restaurant -> dropped + quarantined
    orders[2]["total_amount"] = "twelve hundred"  # wrong type -> lands in _rescued_data -> quarantined
    gps[1]["latitude"], gps[1]["longitude"] = 0.0, 0.0  # GPS glitch: "Null Island", outside Novi Sad
    tag = stamp()
    write_jsonl(f"{LANDING}/orders/orders_{tag}_bad.json", orders)
    write_jsonl(f"{LANDING}/gps/gps_{tag}_bad.json", gps)
    print(f"Bad orders: {ids[0]} (negative amount), {ids[1]} (no restaurant), {ids[2]} (amount 'twelve hundred'); "
          f"GPS ping of order {ids[0]} at (0, 0).")
    print("Look for them in silver.orders_quarantine and in the pipeline's data quality metrics.")


def run_late_event():
    rng = random.Random()
    customers = read_state("customers")
    restaurants = current_restaurants()
    now = now_local()
    late_id, ok_id = inject_ids(2)
    very_late, _ = make_order(late_id, rng.choice(restaurants), customers[rng.choice(list(customers))], rng,
                              delivered_at=now - timedelta(minutes=40))
    slightly_late, _ = make_order(ok_id, rng.choice(restaurants), customers[rng.choice(list(customers))], rng,
                                  delivered_at=now - timedelta(minutes=3))
    write_jsonl(f"{LANDING}/orders/orders_{stamp()}_late.json", [very_late, slightly_late])
    print(f"Order {late_id}: delivered_ts {very_late['delivered_ts']} (40 min ago, behind the 5 min watermark -> "
          f"NOT in gold.avg_delivery_time_per_district_5min, but in silver.orders and the daily gold tables)")
    print(f"Order {ok_id}: delivered_ts {slightly_late['delivered_ts']} (3 min ago, within the watermark -> counted)")


def run_schema_change():
    rng = random.Random()
    customers = read_state("customers")
    restaurants = current_restaurants()
    now = now_local()
    orders = []
    for oid in inject_ids(5):
        order, _ = make_order(oid, rng.choice(restaurants), customers[rng.choice(list(customers))], rng,
                              delivered_at=now, with_tip=True)
        orders.append(order)
    write_jsonl(f"{LANDING}/orders/orders_{stamp()}_with_tip.json", orders)
    print("Wrote 5 orders with a new field tip_amount. Auto Loader adds the column to bronze.orders "
          "(the flow restarts once with the new schema). To use it downstream, add o.tip_amount to silver.orders.")

# COMMAND ----------

# ---------- parameters ----------

dbutils.widgets.dropdown(
    "mode", "stream",
    ["backfill", "stream", "nightly_restaurants", "customer_changes", "bad_records", "late_event", "schema_change"],
)
dbutils.widgets.text("catalog", "databricks_ftn_catalog")
dbutils.widgets.text("duration_minutes", "30")
dbutils.widgets.text("interval_seconds", "5")
dbutils.widgets.text("orders_per_batch", "6")
dbutils.widgets.text("backfill_days", "7")
dbutils.widgets.dropdown("with_tip", "false", ["false", "true"])
dbutils.widgets.text("customer_changes", "5")
dbutils.widgets.text("restaurants_source", "")

MODE = dbutils.widgets.get("mode")
CATALOG = dbutils.widgets.get("catalog")
DURATION_MINUTES = float(dbutils.widgets.get("duration_minutes"))
INTERVAL_SECONDS = float(dbutils.widgets.get("interval_seconds"))
ORDERS_PER_BATCH = int(dbutils.widgets.get("orders_per_batch"))
BACKFILL_DAYS = int(dbutils.widgets.get("backfill_days"))
WITH_TIP = dbutils.widgets.get("with_tip") == "true"
CUSTOMER_CHANGES = int(dbutils.widgets.get("customer_changes"))
RESTAURANTS_SOURCE = dbutils.widgets.get("restaurants_source")

LANDING = f"/Volumes/{CATALOG}/landing/raw"
STATE_DIR = f"{LANDING}/_state"
print(f"mode={MODE}, landing={LANDING}")

# COMMAND ----------

{
    "backfill": run_backfill,
    "stream": run_stream,
    "nightly_restaurants": run_nightly_restaurants,
    "bad_records": run_bad_records,
    "late_event": run_late_event,
    "schema_change": run_schema_change,
    "customer_changes": run_customer_changes,
}[MODE]()

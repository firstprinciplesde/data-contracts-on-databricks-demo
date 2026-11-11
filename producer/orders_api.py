"""A stand-in for the Orders API. get_orders() returns the JSON a GET /orders call would.

Flags simulate what a producer team might ship:
  cents          amount arrives in cents instead of dollars (same field, same type)
  extra_field    an extra field the contract does not have (discount_code)
  null_currency  some orders arrive without a currency
  renamed        currency is sent as currency_code
  drift          nothing breaks the contract, but the data moves: bigger orders, mostly USD, mostly the app
"""
import random
from datetime import datetime, timedelta


def get_orders(since="2026-11-01", n=2000, cents=False, extra_field=False, null_currency=False,
               renamed=False, drift=False, seed=7, start_id=1000):
    rnd = random.Random(seed)
    start = datetime.fromisoformat(since)
    orders = []
    for i in range(1, n + 1):
        order_id = str(start_id + i)
        amount = 59.90 if order_id == "1042" else round(rnd.uniform(80 if drift else 5, 150), 2)
        currency = rnd.choices(["USD", "CAD"], weights=[9, 1] if drift else [1, 1])[0]
        if null_currency and i % 5 == 0:
            currency = None
        order = {
            "order_id": order_id,
            "customer_id": f"c{rnd.randint(1, 200):04d}",
            "amount": round(amount * 100) if cents else amount,
            "created_at": (start + timedelta(minutes=i)).isoformat(),
            "quantity": rnd.randint(1, 5),
            ("currency_code" if renamed else "currency"): currency,
            "channel": rnd.choices(["web", "app"], weights=[2, 8] if drift else [1, 1])[0],
        }
        if extra_field:
            order["discount_code"] = rnd.choice(["", "WELCOME10", "BF2026"])
        orders.append(order)
    return {"orders": orders, "next": None}

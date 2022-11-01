from __future__ import annotations

import asyncio
import random
from operator import attrgetter, itemgetter

from asyncutils.reactive import ReactiveProperty


class Food:

    name = ReactiveProperty[str, "Food"]()
    price = ReactiveProperty[float, "Food"]()

    def __init__(self, name: str, price: float) -> None:
        self.name = name
        self.price = price
        # todo - because of this assignment, mypy thinks that name is a string and price is a float
        #  (as opposed to ReactiveProperty[str, "Food"] and ReactiveProperty[float, "Food"]).
        #  How do we fix this? Is a dataclass transform relevant?

    def __str__(self) -> str:
        return f"{self.name} {self.price}"

    def __repr__(self) -> str:
        return f"{self.name} {self.price}"


async def compare_food_prices() -> None:

    prices: dict[str, float] = {}
    async for update in Food.price.changes():
        # todo - find a way to get the type info for the update automatically
        print(f"{update.instance.name: <20} - ${update.old_value:.2f} --> ${update.new_value:.2f}")
        # todo - shouldn't have to cast to str for formatting

        prices[update.instance.name] = update.new_value
        # print(f"")
        # print(f"The cheapest food is now {min(prices.items(), key=itemgetter(1))[0]}")
        # print(f"The most expensive food is now {max(prices.items(), key=itemgetter(1))[0]}")
        # print(f"The average price is now {sum(prices.values()) / len(prices):.2f}")
        # print(f"=====================")


async def track_price(food: Food) -> None:
    async for update in food.price.changes():
        # todo - this currently does not work properly; it gets the price changes for all instances
        print(f"{food.name} changed price from {update.old_value} to {update.new_value}")


async def main() -> None:
    # todo - subscriptions are currently only created after the event loop starts.
    #  is it possible to create them before the loop starts?

    chocolate = Food("chocolate", 1.5)
    pizza = Food("pizza", 2.5)
    burger = Food("burger", 3.5)
    apple = Food("apple", 0.5)

    compare_task = asyncio.create_task(compare_food_prices())
    track_chocolate = asyncio.create_task(track_price(chocolate))
    await asyncio.sleep(0.1)

    all_food = [chocolate, pizza, burger, apple]

    for _ in range(15):
        random_food = random.choice(all_food)
        random_food.price += random.uniform(-0.5, 1.2)
        await asyncio.sleep(0.1)

    # New instances also have their property updates tracked when using the class watcher
    chips = Food("chips", 0.5)
    chips.price = 0.1

    chips.price = chips.price + 0.1

    await compare_task


if __name__ == "__main__":
    asyncio.run(main())

## 🔹 Introduction

In the next few lectures, we are going to learn about the **`flatMap`** operator.

This operator is **extremely important** in reactive programming — you will be using it often whether you like it or not!

If you’ve used the **Java Stream API**, then you’ve already seen something similar:
👉 `stream.flatMap(...)` — and the concept is *exactly the same* here.

So far, we have studied several operators like:

* `concat`
* `merge`
* `zip`

These operators are **great** when you’re dealing with **independent publishers (or producers)** — where each stream can be processed in parallel or combined independently.

---

## 🔹 Recap: Independent Publishers

Let’s take a few examples we’ve seen earlier:

* **Using `merge`**:
  We called two independent APIs — one for **Emirates flights** and another for **Qatar flights**.
  Both were independent I/O requests, and we used `merge()` to combine their results.

* **Using `zip`**:
  We made two parallel calls — one for **reviews** and another for **price** — and then combined their results.
  Again, both were *independent*.

---

## 🔹 When Independent Streams Are *Not Enough*

However, sometimes we have to make **dependent or sequential API calls** — where the output of one call becomes the input for another.

In such cases, operators like `merge()`, `concat()`, or `zip()` won’t help us.
They’re designed for parallel and independent sources — **not for dependent chains**.

---

## 🔹 Real-World Example: Dependent Microservice Calls

Let’s consider a real-world scenario involving **three microservices**:

### 1. 🧑‍💼 User Service

* **Endpoint 1** → Returns a list of all users.
  Example: `/users`
* **Endpoint 2** → Returns user details or ID for a given username.
  Example: `/users/{username}` → userId

### 2. 💳 Payment Service

* Returns the **user’s account balance** given their user ID.
  Example: `/payment/{userId}` → balance

### 3. 📦 Order Service

* Returns **all orders** placed by the user for a given user ID.
  Example: `/orders/{userId}` → list of orders

---

## 🔹 The Use Case

Suppose we have only the **username**, and we need to **fetch all orders** for that user.

We need to perform the following sequence of steps:

1. Call **User Service** → Get **user ID** by username
2. Call **Order Service** → Get **all orders** using that user ID

👉 These calls are **dependent** — the second can only execute after the first completes.

Hence, we need an operator that allows **chaining dependent calls** — that’s where `flatMap` comes in!

---

## 🔹 Simulating the Scenario in Code

To understand this better, let’s simulate the three services in our IDE.

We’ll create a package named `applications` (or `microservices` if you prefer).
Inside it, we’ll have the following classes:

---

### 🧑‍💼 `User` Record

```java
public record User(Integer id, String name) {}
```

---

### 🧑‍💻 `UserService`

This class simulates the **User microservice** with two endpoints.

```java
public class UserService {
    private static final Map<String, Integer> userTable = Map.of(
        "Sam", 1,
        "Mike", 2,
        "Jake", 3
    );

    // 1️⃣ Endpoint: Get all users
    public static Flux<User> getAllUsers() {
        return Flux.fromIterable(userTable.entrySet())
                   .map(entry -> new User(entry.getValue(), entry.getKey()));
    }

    // 2️⃣ Endpoint: Get user ID by username
    public static Mono<Integer> getUserId(String username) {
        return Mono.fromSupplier(() -> userTable.get(username));
    }
}
```

🟢 **Note:**
In real life, these calls would be **network calls** (e.g., REST API calls returning reactive types).
Here, we just simulate them using static maps.

---

### 📦 `Order` Record

```java
public record Order(Integer userId, String productName, double price) {}
```

---

### 📦 `OrderService`

This class simulates the **Order microservice**.

```java
public class OrderService {
    private static final Map<Integer, List<Order>> orderTable = Map.of(
        1, List.of(
            new Order(1, "Laptop", 950.0),
            new Order(1, "Mouse", 25.0)
        ),
        2, List.of(
            new Order(2, "Phone", 700.0),
            new Order(2, "Headphones", 100.0),
            new Order(2, "Charger", 20.0)
        ),
        3, List.of() // No orders for user 3
    );

    public static Flux<Order> getUserOrders(Integer userId) {
        return Flux.fromIterable(orderTable.get(userId))
                   .delayElements(Duration.ofMillis(500)); // simulate delay
    }
}
```

---

### 💳 `PaymentService`

This class simulates the **Payment microservice**.

```java
public class PaymentService {
    private static final Map<Integer, Integer> userBalanceTable = Map.of(
        1, 100,
        2, 200,
        3, 300
    );

    public static Mono<Integer> getUserBalance(Integer userId) {
        return Mono.fromSupplier(() -> userBalanceTable.get(userId));
    }
}
```

---

## 🔹 Why We Need `flatMap`

Now, suppose we want to:

> Get the list of orders for a given username.

Here’s what we need to do:

```java
UserService.getUserId("Sam")
    .flatMapMany(OrderService::getUserOrders)
    .subscribe(System.out::println);
```

### Explanation:

1. `getUserId("Sam")` returns a `Mono<Integer>` — the user ID.
2. We can’t directly pass that Mono into `OrderService.getUserOrders()` because it expects an `Integer`.
3. `flatMapMany()` helps us **flatten** the Mono into a new `Flux` — chaining the dependent call.

Thus:

* `map()` transforms the emitted value.
* `flatMap()` transforms **and flattens** the result, allowing **dependent asynchronous chaining**.

---

## 🔹 Summary

| Concept     | Description                                                       | Example Use Case                            |
| ----------- | ----------------------------------------------------------------- | ------------------------------------------- |
| `merge()`   | Combine multiple **independent** publishers emitting concurrently | Emirates + Qatar flight results             |
| `zip()`     | Combine emissions from multiple publishers **pairwise**           | Price + Review responses                    |
| `concat()`  | Combine publishers **sequentially**, one after another            | Reading logs from multiple sources in order |
| `flatMap()` | Chain **dependent** asynchronous operations                       | Fetch orders after fetching userId          |

---

## 🔹 Key Takeaways

* Use **`flatMap`** when one reactive call depends on the result of another.
* It allows you to perform **sequential, dependent, asynchronous** operations.
* It’s the reactive equivalent of **“then” chaining** in promises or **function composition** in streams.
* In Project Reactor:

    * `Mono.flatMap()` → used when chaining Monos.
    * `Mono.flatMapMany()` → used when chaining Mono → Flux.
    * `Flux.flatMap()` → used for transforming one Flux into another Flux.

---

## 🧩 Topic: Understanding `Mono.flatMap()` in Reactive Programming

### 🎯 Learning Objective

To understand **why and when to use `flatMap()`** with `Mono` instead of `map()` — especially when dealing with **asynchronous or non-blocking service calls** that return another `Mono`.

---

## 🧠 Concept Overview

In a reactive stream, operators like `map()` and `flatMap()` transform emitted items.
However, the key difference is in how they handle **nested publishers (Mono inside Mono)**.

Let’s explore this with a simple real-world analogy.

---

## 🧍 Scenario

You need to get a user’s account balance.

To achieve this:

1. You have a **username** (e.g., `"Sam"`).
2. You must call the **User Service** to get the **User ID**.
3. Once you have the **User ID**, call the **Payment Service** to get the **User Balance**.

---

### 🧾 Step-by-Step Flow

| Step | Action                        | Returns               | Remarks                              |
| ---- | ----------------------------- | --------------------- | ------------------------------------ |
| 1    | Call `getUserId(username)`    | `Mono<Integer>`       | Non-blocking call to user service    |
| 2    | Call `getUserBalance(userId)` | `Mono<Integer>`       | Non-blocking call to payment service |
| 3    | Chain both                    | Need to compose Monos | Requires `flatMap()`                 |

---

## ⚙️ Why `map()` Fails Here

Let’s see the problem in code logic:

```java
Mono<String> usernameMono = Mono.just("Sam");

Mono<Mono<Integer>> result = usernameMono.map(username -> getUserId(username));
```

* The **map()** operator wraps the result in another `Mono`.
* Since `getUserId()` already returns a `Mono<Integer>`,
  the type becomes **`Mono<Mono<Integer>>`** — a *nested Mono*.

🧩 That means:

* You now have **a Mono that emits another Mono**, not the integer itself.
* When you subscribe, you receive the **inner publisher**, not the **actual value**.

So, subscribing gives:

```
Mono<Mono<Integer>>  → emits → Mono<Integer> (inner publisher)
```

Your subscriber won’t see the integer until *someone subscribes to the inner Mono*.

---

## 🧪 Example: When `map()` Works Fine

If your transformation is **in-memory** (synchronous, not returning a Mono):

```java
Mono<String> greeting = Mono.just(1)
    .map(id -> "Hello user " + id);
```

✅ Output: `"Hello user 1"`

This works because:

* `map()` just transforms the data.
* It doesn’t deal with another reactive publisher.

---

## ⚠️ But When You Return Another Mono...

```java
Mono<String> message = Mono.just(1)
    .map(id -> getGreeting(id)); // getGreeting returns Mono<String>
```

Result type = `Mono<Mono<String>>`

❌ Not what we want.

You’ll need someone to “flatten” it — to unwrap the inner publisher.

---

## 🔄 Enter `flatMap()`

`flatMap()` solves exactly this.

It:

1. Subscribes to the inner publisher.
2. “Flattens” the nested structure.
3. Returns a single `Mono<T>` instead of `Mono<Mono<T>>`.

So the correct code becomes:

```java
Mono<String> message = Mono.just("Sam")
    .flatMap(username -> getUserId(username))
    .flatMap(userId -> getUserBalance(userId))
    .map(balance -> "User balance: " + balance);
```

✅ Output: `"User balance: 100"`

Now the flow is **flat and sequential**.

---

## ⚙️ Internal Mechanics

| Operator    | Input         | Output    | Behavior                                      |
| ----------- | ------------- | --------- | --------------------------------------------- |
| `map()`     | T → R         | `Mono<R>` | Transforms value directly                     |
| `flatMap()` | T → `Mono<R>` | `Mono<R>` | Flattens inner publisher by subscribing to it |

So if your mapping function **returns a Mono**, always use `flatMap()`.

---

## 🧩 Visualization

Without flatMap:

```
Mono<String>
   |
   map(user -> getUserId(user))  → Mono<Mono<Integer>>
                                        |
                                        | inner publisher not subscribed
```

With flatMap:

```
Mono<String>
   |
   flatMap(user -> getUserId(user)) → Mono<Integer>
                                          |
                                          | subscribed automatically
```

---

## 💡 Practical Example

```java
public Mono<Integer> getUserId(String username) {
    return Mono.just(1); // Simulate async call
}

public Mono<Integer> getUserBalance(Integer userId) {
    return Mono.just(100); // Simulate async call
}

public void example() {
    Mono.just("Sam")
        .flatMap(this::getUserId)
        .flatMap(this::getUserBalance)
        .subscribe(balance ->
            System.out.println("User balance: " + balance)
        );
}
```

🖥 Output:

```
User balance: 100
```

---

## 🧭 Summary

| Concept                 | Description                                                                    |
| ----------------------- | ------------------------------------------------------------------------------ |
| **map()**               | Transforms data **inside** a Mono (in-memory/synchronous).                     |
| **flatMap()**           | Used when the transformation function **returns another Mono** (asynchronous). |
| **Problem with map()**  | Produces a `Mono<Mono<T>>` when used with a function returning Mono.           |
| **flatMap() Solution**  | Automatically flattens `Mono<Mono<T>>` → `Mono<T>`.                            |
| **Real-world use case** | Sequential non-blocking service calls (e.g., `UserService → PaymentService`).  |

---

## 🔁 Quick Comparison

| Use Case                     | Operator        | Example                          |
| ---------------------------- | --------------- | -------------------------------- |
| Simple in-memory computation | `map()`         | `.map(id -> id + 1)`             |
| Asynchronous dependent call  | `flatMap()`     | `.flatMap(id -> getBalance(id))` |
| Multi-value transformation   | `flatMapMany()` | Converts Mono → Flux             |

---

## 🧩 Final Takeaway

* Use `map()` for **direct transformations** (synchronous).
* Use `flatMap()` for **chained reactive calls** (asynchronous).
* Think of `flatMap()` as a **subscriber to inner publishers** that ensures you always get a single flattened Mono.

---

# 🧩 Topic: Understanding `Mono.flatMapMany()` — The Bridge Between Mono and Flux

---

## 🎯 Learning Objective

To understand:

* What problem `flatMapMany()` solves,
* How it differs from `flatMap()`, and
* When to use it in reactive programming scenarios.

---

## 🧠 Recap: Before We Begin

In the previous lecture, we learned:

* `Mono.map()` is for **in-memory transformations**.
* `Mono.flatMap()` is for **asynchronous chaining**, where the inner result is also a **Mono**.

But what happens if your inner result is **a Flux (multiple elements)**?

That’s exactly what `flatMapMany()` solves.

---

## 📖 Problem Scenario

Let’s imagine the following use case:

### 🧍 You have:

* A **username** (e.g., `"Sam"`).

### 💡 You want:

* All the **orders** placed by that user.

### 🧱 The data flow is:

1. Call **User Service** → get **User ID** (returns a `Mono<Integer>`).
2. Call **Order Service** → get **Orders for User ID** (returns a `Flux<Order>`).

---

## ⚙️ Step-by-Step Breakdown

### Step 1️⃣: Starting with a Mono

You begin with:

```java
Mono<String> usernameMono = Mono.just("Sam");
```

### Step 2️⃣: Get the User ID

You call the user service:

```java
Mono<Integer> userIdMono = usernameMono.flatMap(this::getUserId);
```

This works fine — `getUserId()` returns a `Mono<Integer>`.

---

### Step 3️⃣: Get the User’s Orders

Here’s the tricky part:

```java
Mono<Flux<Order>> result = userIdMono.map(this::getOrdersByUserId);
```

Now, let’s observe the **type** carefully:

* `getOrdersByUserId()` returns `Flux<Order>`.
* So using `map()` gives you `Mono<Flux<Order>>` — a **Mono inside a Flux**, similar to the earlier `Mono<Mono>` problem!

🧩 You cannot subscribe directly to `Mono<Flux<Order>>` and expect to get the orders.

You’ll just get the **inner Flux** object, not the orders themselves.

---

## ⚠️ Why `flatMap()` Fails Here

You might think:
“Let’s just use `flatMap()` like before!”

```java
Mono<Flux<Order>> result = usernameMono.flatMap(name -> getUserId(name))
                                       .flatMap(userId -> getOrdersByUserId(userId));
```

But this won’t work.

Here’s why:

* `Mono.flatMap()` assumes the **inner publisher is also a Mono** — meaning it expects **exactly one value**.
* However, the `OrderService` returns a **Flux**, which can have **many values**.

So `flatMap()` doesn’t know how to “flatten” a `Mono<Flux<T>>` correctly.

In other words:

| Input Type  | Operator    | Works? | Reason                             |
| ----------- | ----------- | ------ | ---------------------------------- |
| Mono → Mono | `flatMap()` | ✅      | Flattens one-to-one                |
| Mono → Flux | `flatMap()` | ❌      | Expects single value, not multiple |

---

## 💡 Solution: `flatMapMany()`

When the **inner publisher** emits **multiple values** (Flux),
you must use `flatMapMany()`.

```java
Flux<Order> ordersFlux =
    Mono.just("Sam")
        .flatMap(this::getUserId)
        .flatMapMany(this::getOrdersByUserId);
```

✅ `flatMapMany()` **subscribes to the inner Flux**, extracts all its items,
and flattens the result into a **Flux<Order>**.

---

## 🔍 Internal Behavior

Let’s visualize what happens internally:

| Operator        | Input       | Output  | Meaning                       |
| --------------- | ----------- | ------- | ----------------------------- |
| `map()`         | T → R       | Mono<R> | Simple transformation         |
| `flatMap()`     | T → Mono<R> | Mono<R> | Flattens a Mono inside Mono   |
| `flatMapMany()` | T → Flux<R> | Flux<R> | Flattens a Flux inside a Mono |

---

## 🧪 Example Code

```java
public class FlatMapManyExample {

    public static void main(String[] args) {
        new FlatMapManyExample().processOrders();
    }

    public void processOrders() {
        Mono.just("Sam")
            .flatMap(this::getUserId)
            .flatMapMany(this::getOrdersByUserId)
            .subscribe(
                order -> System.out.println("Received order: " + order),
                err -> System.err.println("Error: " + err),
                () -> System.out.println("All orders received!")
            );

        // simulate non-blocking delay
        Util.sleepSeconds(3);
    }

    Mono<Integer> getUserId(String username) {
        return switch (username) {
            case "Sam" -> Mono.just(1);
            case "MC" -> Mono.just(2);
            case "Jake" -> Mono.just(3);
            default -> Mono.empty();
        };
    }

    Flux<String> getOrdersByUserId(Integer userId) {
        return switch (userId) {
            case 1 -> Flux.just("Order-101", "Order-102");
            case 2 -> Flux.just("Order-201", "Order-202", "Order-203");
            case 3 -> Flux.empty(); // no orders
            default -> Flux.empty();
        };
    }
}
```

### 🖥 Output

```
Received order: Order-101
Received order: Order-102
All orders received!

Received order: Order-201
Received order: Order-202
Received order: Order-203
All orders received!

Jake → no orders → immediate completion signal
```

---

## 🧩 What’s Really Happening

### For `Sam`:

* `Mono("Sam")`
* → `flatMap()` → `Mono(1)`
* → `flatMapMany()` → `Flux("Order-101", "Order-102")`

### For `MC`:

* → `Flux("Order-201", "Order-202", "Order-203")`

### For `Jake`:

* → `Flux.empty()` → immediate completion.

---

## 🧭 Key Differences Summary

| Operator        | Works On | Inner Type | Output Type | When to Use                                 |
| --------------- | -------- | ---------- | ----------- | ------------------------------------------- |
| `map()`         | Mono     | Value      | Mono        | Simple transformation                       |
| `flatMap()`     | Mono     | Mono       | Mono        | Asynchronous call returning Mono            |
| `flatMapMany()` | Mono     | Flux       | Flux        | Asynchronous call returning multiple values |

---

## 🧩 Analogy

Imagine:

* You call a **User Service** and get one **user ID** — this is **Mono**.
* Then, you use that user ID to call an **Order Service** that gives you **multiple orders** — this is **Flux**.

`flatMapMany()` acts like a **bridge** between the **single** and the **many** worlds.

---

## 🧭 Summary

| Concept             | Description                                                                           |
| ------------------- | ------------------------------------------------------------------------------------- |
| **Mono**            | Emits **one item** (or none).                                                         |
| **Flux**            | Emits **many items** (0 to N).                                                        |
| **`flatMap()`**     | Flattens `Mono<Mono<T>>` → `Mono<T>`.                                                 |
| **`flatMapMany()`** | Flattens `Mono<Flux<T>>` → `Flux<T>`.                                                 |
| **Use Case**        | When you start with one item (Mono) and want to get a stream (Flux) of related items. |
| **Example**         | Username → User ID → All User Orders.                                                 |

---

## 🧠 Final Takeaway

* Use `flatMap()` when your inner call returns a **Mono**.
* Use `flatMapMany()` when your inner call returns a **Flux**.
* Think of `flatMapMany()` as a **converter** that expands a single emission (Mono) into a stream (Flux).
* It’s the go-to operator when you move **from one-to-many relationships** in a reactive pipeline.

---
# 🧩 Lecture Notes: Understanding **Flux FlatMap** in Project Reactor

---

## 🎯 Learning Objective

To understand:

* How `flatMap()` works on a **Flux**,
* How it’s different from `Mono.flatMap()`,
* Why its behavior is **cool but tricky**,
  and how it handles **parallel subscriptions** in reactive streams.

---

## 🧠 Context

In earlier lectures, we learned:

* `Mono.flatMap()` → used when inner publisher returns another **Mono**.
* `Mono.flatMapMany()` → used when inner publisher returns a **Flux**.

Now, let’s move into **Flux.flatMap()**, which — good news! — **is not a new operator**.
It’s conceptually **the same** as `Mono.flatMap()` but behaves a bit differently in terms of **concurrency** and **asynchronous merging**.

---

## 🧱 Scenario: Fetch All Orders from All Users

### Problem Statement

> “Get all the orders from the Order Service.”

Sounds simple, right?

But here’s the catch:

* The **Order Service** does **not** have an endpoint to “get all orders”.
* You can only get orders **for a given user ID**.

So what do we do?

👉 Simple —
First, get **all users** from the **User Service**,
then, for **each user**, get **that user’s orders** from the **Order Service**.

---

## ⚙️ Step-by-Step Solution

### Step 1️⃣ — Get All Users

We start with a Flux of users:

```java
Flux<User> users = userService.getAllUsers();
```

Example users:

```
User(id=1), User(id=2), User(id=3)
```

---

### Step 2️⃣ — For Each User, Get Orders

We want to:

* Take each user (from Flux<User>),
* Call `orderService.getOrdersByUserId(user.getId())`,
* And finally get **all orders** from **all users** in a single stream.

Let’s try doing this using **map()** first (to see the problem).

---

### Step 3️⃣ — Using map() (Wrong Approach)

```java
Flux<Flux<Order>> fluxOfFlux = users.map(user ->
    orderService.getOrdersByUserId(user.getId())
);
```

Now check the type 👇
It’s **Flux<Flux<Order>>** — a **Flux inside a Flux**.

That means:

* You get an outer stream of users,
* Each emits an **inner Flux** of orders.

But no one’s subscribing to those inner publishers.
So if you subscribe to this, you won’t actually get the orders.

---

## 🧩 Problem

We now have **nested publishers** (Flux inside Flux).
To flatten them, someone needs to **subscribe to the inner publishers** and merge all the emitted items.

This is where **Flux.flatMap()** comes in.

---

## ✅ Step 4️⃣ — Using flatMap() (Correct Approach)

```java
Flux<Order> allOrders = users
    .flatMap(user -> orderService.getOrdersByUserId(user.getId()));
```

### What happens internally:

1. `flatMap()` subscribes to **each inner Flux** (from each user).
2. It merges all their emitted items into a **single Flux<Order>**.
3. The final result type is `Flux<Order>`.

So now, instead of getting `Flux<Flux<Order>>`,
you get a **flattened Flux<Order>** — perfect for subscribing directly.

---

### 🧪 Example Output

Let’s assume:

* User 1 → Orders [A, B]
* User 2 → Orders [C, D, E]

Final output Flux:

```
OrderA
OrderB
OrderC
OrderD
OrderE
```

✅ All orders merged together.

---

## 🧠 Important Note — Concurrency Behavior

Here’s the **cool** (and slightly confusing) part.

When you use **Flux.flatMap()**,
it **subscribes to all the inner publishers concurrently**.

That means:

* It doesn’t wait for one user’s orders to finish before moving to the next.
* It executes all those asynchronous streams **in parallel** and merges the results **as they arrive**.

---

### 🔍 How to Observe This Behavior

You can add logging to each inner call:

```java
Flux<Order> allOrders = userService.getAllUsers()
    .flatMap(user -> {
        return orderService.getOrdersByUserId(user.getId())
            .transform(Util.fluxLogger("order for user " + user.getId()));
    })
    .subscribe(Util.subscriber());
```

### Example Console Output

```
subscribing to order for user 2
subscribing to order for user 1
subscribing to order for user 3
Received order: Order2A
Received order: Order1A
Received order: Order3A
...
```

🧩 Notice — the results **interleave**!

You’re getting orders for multiple users at once because:

* **Flux.flatMap()** merges the emitted items from all inner streams **asynchronously**.
* It doesn’t enforce ordering of results.

---

## ⚡ Understanding the Difference

| Operator             | Input   | Inner Publisher | Output  | Behavior                       |
| -------------------- | ------- | --------------- | ------- | ------------------------------ |
| `Mono.flatMap()`     | Mono<T> | Mono<R>         | Mono<R> | Sequential, one item           |
| `Mono.flatMapMany()` | Mono<T> | Flux<R>         | Flux<R> | Converts Mono → Flux           |
| `Flux.flatMap()`     | Flux<T> | Publisher<R>    | Flux<R> | Concurrent, merges all streams |

---

## ⚙️ Example Code Summary

```java
Flux<Order> allOrders = userService.getAllUsers()
    .flatMap(user -> orderService.getOrdersByUserId(user.getId()))
    .subscribe(Util.subscriber());

Util.sleepSeconds(3); // To keep the main thread alive
```

✅ Output (unordered because of concurrency):

```
Received Order-101 for user 2
Received Order-102 for user 1
Received Order-103 for user 2
Received Order-104 for user 3
Completed!
```

---

## 🧩 Observations

* `Flux.flatMap()` subscribes to **all** inner publishers **in parallel**.
* The resulting sequence is **unordered** because items are emitted **as soon as they arrive**.
* If order matters, you should use **`concatMap()`** instead (covered later).

---

## 🧭 Summary

| Concept                 | Explanation                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------ |
| **Flux.flatMap()**      | Flattens nested publishers (Flux<Flux<T>>) into a single Flux<T>.                    |
| **Behavior**            | Concurrently subscribes to all inner publishers and merges their results.            |
| **Output**              | Flux of all items emitted by all inner publishers.                                   |
| **Type transformation** | Flux<Flux<T>> → Flux<T>.                                                             |
| **Use case**            | When you have multiple asynchronous sources and you want to merge them concurrently. |

---

## 💡 Final Takeaway

* Don’t worry — **Flux.flatMap()** is not a new concept.
* It’s conceptually the same as `Mono.flatMap()`,
  but powerful because it handles **multiple streams at once**.
* Remember:

  * `flatMap()` → **Concurrent merge**
  * `concatMap()` → **Sequential merge (ordered)**

---
# 🧩 Lecture Notes: Why Does **Flux.flatMap()** Behave Concurrently?

---

## 🎯 Learning Objective

To understand:

* Why `Flux.flatMap()` subscribes to **multiple inner publishers at once**,
* How this leads to **concurrent network calls**,
* How to **control the concurrency level**,
* And how it relates to **merge** and **concat** behaviors in Reactor.

---

## 🧠 Background

In the previous lecture, we saw this behavior:

> When using `Flux.flatMap()`, multiple user order requests were happening **simultaneously**.

Example log output:

```
Subscribing to order for user 1
Subscribing to order for user 2
Subscribing to order for user 3
```

All of them subscribed **at the same time**, not sequentially.

So…
🧩 **Why does this happen?**
Let’s break it down step by step.

---

## 🧱 Step 1 — The Setup

Imagine we have this Flux:

```java
Flux<Integer> userIds = Flux.just(1, 2, 3, 4);
```

Each number represents a **user ID**.

Now we want to get each user’s orders from an **Order Service**, which returns a Flux of orders:

```java
Flux<Order> getOrdersByUserId(Integer userId);
```

So we write:

```java
Flux<Order> allOrders = userIds.flatMap(userId ->
    orderService.getOrdersByUserId(userId)
);
```

At first glance, it looks straightforward —
but internally, something interesting happens.

---

## ⚙️ Step 2 — What FlatMap Actually Does

When `flatMap()` processes a source Flux (`userIds`):

1. Each **emitted item** (`userId`) triggers creation of an **inner Flux**
   → `orderService.getOrdersByUserId(userId)`.

2. The **flatMap() operator immediately subscribes** to that inner Flux
   → without waiting for previous ones to complete.

3. It **merges** all emissions from all active inner Fluxes into **one combined Flux**.

4. Whenever any inner Flux emits an item, it’s **instantly pushed downstream** to the subscriber.

So if user `1` and `2` both have open network calls,
their emitted orders will appear **interleaved** as they arrive asynchronously.

---

## 🧩 Step 3 — The Real-Life Analogy

Imagine 4 customers (User 1–4) walk into 4 different bank counters.

Each counter processes their requests independently.
As soon as one counter finishes, it gives you the result — no waiting for others.

That’s exactly how `flatMap()` behaves.

It **doesn’t queue or wait** — it **merges** all concurrent results into a single stream.

---

## 🧠 Step 4 — The Problem It Solves

If `flatMap()` waited for each Flux to complete before starting the next one,
it would severely limit performance — especially for **IO-bound network operations**.

Instead, it starts **all possible inner publishers in parallel**,
maximizing concurrency and throughput.

---

## ⚠️ Step 5 — The Question: When Will Each Flux Complete?

Remember — a Flux could be:

* Finite (few items), or
* Infinite (a stream that never completes).

If `flatMap()` waited for an **infinite Flux** to complete before moving on,
it would be stuck forever.

So instead, `flatMap()` says:

> “I won’t wait for any inner publisher to finish.
> As soon as a new item arrives from the source, I’ll start another one!”

That’s why you get **concurrent subscriptions**.

---

## 🔄 Step 6 — The Merge Analogy

The behavior of `flatMap()` is **very similar to `merge()`**.

Internally, `flatMap()` effectively does this:

```java
Flux.merge(
    orderService.getOrdersByUserId(1),
    orderService.getOrdersByUserId(2),
    orderService.getOrdersByUserId(3)
)
```

So as and when **each inner Flux emits an item**,
it merges it into the output stream.

That’s why your logs look **interleaved** —
you’re seeing results as they arrive from multiple concurrent sources.

---

## 🧩 Step 7 — How Many Concurrent Subscriptions?

By default, Reactor sets a **concurrency limit** of **256**.

That means `flatMap()` can subscribe to **up to 256 inner publishers concurrently**.

Where does this number come from?
→ It’s defined internally in Reactor’s **queue size configuration**.

But don’t memorize it — this value can change in future versions.

---

## ⚙️ Step 8 — Controlling Concurrency Manually

`flatMap()` provides an **overloaded version** that lets you define concurrency explicitly.

```java
Flux<Order> allOrders = userIds.flatMap(
    id -> orderService.getOrdersByUserId(id),
    2 // concurrency level
);
```

### 💡 What this means:

* Only **2 inner publishers** will be active at a time.
* Once one finishes, the next one will start.

So if you set concurrency to `1`,
it behaves **like concatMap** — processing one at a time, sequentially.

---

### 🔍 Example Demonstration

```java
Flux.just(1, 2, 3)
    .flatMap(id -> orderService.getOrdersByUserId(id)
        .doOnSubscribe(s -> System.out.println("Subscribing to user " + id)),
        1 // sequential
    )
    .subscribe(Util.subscriber());
```

🖥 Output:

```
Subscribing to user 1
... user 1 orders complete ...
Subscribing to user 2
... user 2 orders complete ...
Subscribing to user 3
... done ...
```

If you change concurrency to `3`:

```
Subscribing to user 1
Subscribing to user 2
Subscribing to user 3
... all emitting together ...
```

---

## ⚡ Step 9 — Concurrency vs. OS Limits

You might think:

> “Can I set concurrency to 1000 or 5000?”

Technically yes, but ⚠️ be careful.

Each concurrent subscription means:

* A separate **network connection**,
* Potential **TCP sockets**,
* Potential **HTTP connections**.

Operating systems usually limit the number of open sockets or file handles per process.
So, setting concurrency too high may lead to:

* Resource exhaustion,
* Timeouts, or
* Connection failures.

If you truly need thousands of concurrent streams,
you should use **HTTP/2 multiplexing** or a **connection pooling strategy**.

---

## 🧭 Step 10 — Summary

| Concept                    | Description                                                      |
| -------------------------- | ---------------------------------------------------------------- |
| **flatMap()**              | Creates and subscribes to inner publishers for each source item. |
| **Behavior**               | Merges all emissions concurrently (like `merge()`).              |
| **Default concurrency**    | 256 inner publishers.                                            |
| **Adjustable concurrency** | Use overloaded `flatMap()` with `concurrency` parameter.         |
| **Sequential alternative** | Use `concatMap()` (or `flatMap(..., 1)`).                        |
| **Why non-blocking?**      | Because waiting for infinite streams is impractical.             |

---

## 🧠 Mental Model Summary

```
Source Flux:   [1] ---- [2] ---- [3]
                    │        │        │
                    ▼        ▼        ▼
Inner Fluxes:  FluxA   FluxB   FluxC  (asynchronous)
                    \      |      /
                     \     |     /
                      \    |    /
                   flatMap merges → Flux<Order>
```

* Each inner Flux starts immediately.
* All run in parallel.
* Emissions are merged as they arrive.

---

## 💡 Final Takeaway

* `Flux.flatMap()` is **concurrent and non-blocking** by design.
* It launches **all possible streams** without waiting for earlier ones to finish.
* Use **concurrency control** if you want to **limit parallelism**.
* Use **concatMap()** if you need strict **ordering and sequential execution**.

---

# 🧩 Lecture Notes: Why Does **Flux.flatMap()** Behave Concurrently?

---

## 🎯 Learning Objective

To understand:

* Why `Flux.flatMap()` subscribes to **multiple inner publishers at once**,
* How this leads to **concurrent network calls**,
* How to **control the concurrency level**,
* And how it relates to **merge** and **concat** behaviors in Reactor.

---

## 🧠 Background

In the previous lecture, we saw this behavior:

> When using `Flux.flatMap()`, multiple user order requests were happening **simultaneously**.

Example log output:

```
Subscribing to order for user 1
Subscribing to order for user 2
Subscribing to order for user 3
```

All of them subscribed **at the same time**, not sequentially.

So…
🧩 **Why does this happen?**
Let’s break it down step by step.

---

## 🧱 Step 1 — The Setup

Imagine we have this Flux:

```java
Flux<Integer> userIds = Flux.just(1, 2, 3, 4);
```

Each number represents a **user ID**.

Now we want to get each user’s orders from an **Order Service**, which returns a Flux of orders:

```java
Flux<Order> getOrdersByUserId(Integer userId);
```

So we write:

```java
Flux<Order> allOrders = userIds.flatMap(userId ->
    orderService.getOrdersByUserId(userId)
);
```

At first glance, it looks straightforward —
but internally, something interesting happens.

---

## ⚙️ Step 2 — What FlatMap Actually Does

When `flatMap()` processes a source Flux (`userIds`):

1. Each **emitted item** (`userId`) triggers creation of an **inner Flux**
   → `orderService.getOrdersByUserId(userId)`.

2. The **flatMap() operator immediately subscribes** to that inner Flux
   → without waiting for previous ones to complete.

3. It **merges** all emissions from all active inner Fluxes into **one combined Flux**.

4. Whenever any inner Flux emits an item, it’s **instantly pushed downstream** to the subscriber.

So if user `1` and `2` both have open network calls,
their emitted orders will appear **interleaved** as they arrive asynchronously.

---

## 🧩 Step 3 — The Real-Life Analogy

Imagine 4 customers (User 1–4) walk into 4 different bank counters.

Each counter processes their requests independently.
As soon as one counter finishes, it gives you the result — no waiting for others.

That’s exactly how `flatMap()` behaves.

It **doesn’t queue or wait** — it **merges** all concurrent results into a single stream.

---

## 🧠 Step 4 — The Problem It Solves

If `flatMap()` waited for each Flux to complete before starting the next one,
it would severely limit performance — especially for **IO-bound network operations**.

Instead, it starts **all possible inner publishers in parallel**,
maximizing concurrency and throughput.

---

## ⚠️ Step 5 — The Question: When Will Each Flux Complete?

Remember — a Flux could be:

* Finite (few items), or
* Infinite (a stream that never completes).

If `flatMap()` waited for an **infinite Flux** to complete before moving on,
it would be stuck forever.

So instead, `flatMap()` says:

> “I won’t wait for any inner publisher to finish.
> As soon as a new item arrives from the source, I’ll start another one!”

That’s why you get **concurrent subscriptions**.

---

## 🔄 Step 6 — The Merge Analogy

The behavior of `flatMap()` is **very similar to `merge()`**.

Internally, `flatMap()` effectively does this:

```java
Flux.merge(
    orderService.getOrdersByUserId(1),
    orderService.getOrdersByUserId(2),
    orderService.getOrdersByUserId(3)
)
```

So as and when **each inner Flux emits an item**,
it merges it into the output stream.

That’s why your logs look **interleaved** —
you’re seeing results as they arrive from multiple concurrent sources.

---

## 🧩 Step 7 — How Many Concurrent Subscriptions?

By default, Reactor sets a **concurrency limit** of **256**.

That means `flatMap()` can subscribe to **up to 256 inner publishers concurrently**.

Where does this number come from?
→ It’s defined internally in Reactor’s **queue size configuration**.

But don’t memorize it — this value can change in future versions.

---

## ⚙️ Step 8 — Controlling Concurrency Manually

`flatMap()` provides an **overloaded version** that lets you define concurrency explicitly.

```java
Flux<Order> allOrders = userIds.flatMap(
    id -> orderService.getOrdersByUserId(id),
    2 // concurrency level
);
```

### 💡 What this means:

* Only **2 inner publishers** will be active at a time.
* Once one finishes, the next one will start.

So if you set concurrency to `1`,
it behaves **like concatMap** — processing one at a time, sequentially.

---

### 🔍 Example Demonstration

```java
Flux.just(1, 2, 3)
    .flatMap(id -> orderService.getOrdersByUserId(id)
        .doOnSubscribe(s -> System.out.println("Subscribing to user " + id)),
        1 // sequential
    )
    .subscribe(Util.subscriber());
```

🖥 Output:

```
Subscribing to user 1
... user 1 orders complete ...
Subscribing to user 2
... user 2 orders complete ...
Subscribing to user 3
... done ...
```

If you change concurrency to `3`:

```
Subscribing to user 1
Subscribing to user 2
Subscribing to user 3
... all emitting together ...
```

---

## ⚡ Step 9 — Concurrency vs. OS Limits

You might think:

> “Can I set concurrency to 1000 or 5000?”

Technically yes, but ⚠️ be careful.

Each concurrent subscription means:

* A separate **network connection**,
* Potential **TCP sockets**,
* Potential **HTTP connections**.

Operating systems usually limit the number of open sockets or file handles per process.
So, setting concurrency too high may lead to:

* Resource exhaustion,
* Timeouts, or
* Connection failures.

If you truly need thousands of concurrent streams,
you should use **HTTP/2 multiplexing** or a **connection pooling strategy**.

---

## 🧭 Step 10 — Summary

| Concept                    | Description                                                      |
| -------------------------- | ---------------------------------------------------------------- |
| **flatMap()**              | Creates and subscribes to inner publishers for each source item. |
| **Behavior**               | Merges all emissions concurrently (like `merge()`).              |
| **Default concurrency**    | 256 inner publishers.                                            |
| **Adjustable concurrency** | Use overloaded `flatMap()` with `concurrency` parameter.         |
| **Sequential alternative** | Use `concatMap()` (or `flatMap(..., 1)`).                        |
| **Why non-blocking?**      | Because waiting for infinite streams is impractical.             |

---

## 🧠 Mental Model Summary

```
Source Flux:   [1] ---- [2] ---- [3]
                    │        │        │
                    ▼        ▼        ▼
Inner Fluxes:  FluxA   FluxB   FluxC  (asynchronous)
                    \      |      /
                     \     |     /
                      \    |    /
                   flatMap merges → Flux<Order>
```

* Each inner Flux starts immediately.
* All run in parallel.
* Emissions are merged as they arrive.

---

## 💡 Final Takeaway

* `Flux.flatMap()` is **concurrent and non-blocking** by design.
* It launches **all possible streams** without waiting for earlier ones to finish.
* Use **concurrency control** if you want to **limit parallelism**.
* Use **concatMap()** if you need strict **ordering and sequential execution**.

---

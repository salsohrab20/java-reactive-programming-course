# 🧩 Lecture Notes: Understanding **Flux.concatMap()**

---

## 🎯 Learning Objective

To understand how **`concatMap()`** differs from **`flatMap()`**,
and how it enforces **sequential execution** — ensuring ordered and predictable behavior when working with asynchronous Flux streams.

---

## 🧠 Quick Recap

So far, we learned:

| Operator          | Behavior                  | Concurrency                | Output Order            |
| ----------------- | ------------------------- | -------------------------- | ----------------------- |
| `flatMap()`       | Merges all inner Fluxes   | Concurrent                 | Unordered (interleaved) |
| `flatMap(..., n)` | Limits concurrency        | Parallel up to *n* streams | Partially ordered       |
| `concatMap()`     | Concatenates inner Fluxes | Sequential                 | Ordered (deterministic) |

---

## 💡 The Core Idea

`concatMap()` works almost exactly like `flatMap()`,
but with **one key difference**:

> It waits for each inner Flux to **complete** before subscribing to the next one.

---

## ⚙️ Internal Behavior

When you use `concatMap()`, the operator behaves like this:

1. Takes the first item from the source Flux.
2. Creates the corresponding **inner Flux** (e.g., one network call).
3. Subscribes to that Flux.
4. Waits until it **completes**.
5. Only then moves to the **next item** in the source sequence.

Hence the name — *concatenate map* — because it **concatenates** one Flux’s emissions **after** the previous one completes.

---

## 🧱 Conceptual Visualization

```
Source Flux:   [1] ---- [2] ---- [3]
                    │
                    ▼
Inner Flux:   Orders for user 1 → completed
                    │
                    ▼
Next:         Orders for user 2 → completed
                    │
                    ▼
Next:         Orders for user 3 → completed
```

So, everything happens **one by one**, in a strictly **ordered** fashion.

---

## 🧪 Example Demo

```java
Flux.just(1, 2, 3)
    .concatMap(userId ->
        orderService.getOrdersByUserId(userId)
            .doOnSubscribe(s -> System.out.println("Fetching orders for user " + userId))
    )
    .subscribe(Util.subscriber());

Util.sleepSeconds(10); // ensure main thread stays alive
```

---

### 🖥 Example Output

```
Fetching orders for user 1
Received: Order-101
Received: Order-102
Fetching orders for user 2
Received: Order-201
Received: Order-202
Fetching orders for user 3
Received: Order-301
Completed!
```

✅ **Observations:**

* Each user's orders are fetched **sequentially**.
* No interleaving between user 1, user 2, and user 3.
* The entire stream takes longer, because each network call waits for the previous one to finish.

---

## 🧩 Why Use concatMap()

Use `concatMap()` when:

* **Order matters**, and you cannot process next items until current ones complete.
* You want to **preserve sequence** in emissions.
* You want to **avoid concurrency** issues with external systems (like databases or APIs).

Example:
If the API performs updates or writes, you don’t want concurrent requests — you want them serialized.

---

## ⚙️ Performance Note

| Operator      | Speed               | Use Case                  |
| ------------- | ------------------- | ------------------------- |
| `flatMap()`   | Fast (parallel)     | When order doesn’t matter |
| `concatMap()` | Slower (sequential) | When order matters        |

So you trade performance for determinism.

---

## 🧭 Summary

| Concept           | Description                                                    |
| ----------------- | -------------------------------------------------------------- |
| **`concatMap()`** | Similar to `flatMap()`, but subscribes sequentially.           |
| **Behavior**      | Waits for one inner Flux to complete before starting the next. |
| **Output Order**  | Always preserves source sequence.                              |
| **Concurrency**   | None — strictly one at a time.                                 |
| **When to use**   | When sequence and deterministic order are critical.            |

---

## 🧠 Quick Takeaway

* `flatMap()` → **Concurrent & unordered**
* `concatMap()` → **Sequential & ordered**
* Think of `concatMap()` as a **flatMap with built-in ordering guarantees.**

---

## 🔍 Practical Analogy

Imagine multiple people at a service desk:

* With `flatMap()`, all are served simultaneously at different counters.
* With `concatMap()`, they’re served **one by one** at a single counter — each waits for the previous request to finish.

---

## 💬 Final Thought

So, if your application involves **network calls** or **database updates** where **order matters**,
use `concatMap()` — it’s slower, but predictable and safe.

---

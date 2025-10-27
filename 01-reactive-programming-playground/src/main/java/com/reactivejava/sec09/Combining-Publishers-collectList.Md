# 🧩 Lecture Notes: Understanding `collectList()` in Reactor

---

## 🎯 Learning Objective

To understand the purpose of the **`collectList()`** operator in Reactor —
what it does, when to use it, and how it behaves with success or error signals.

---

## 🧠 The Concept

Sometimes, when you are working with a **Flux**, you may want to **collect all emitted items**
and get them **as a single list**.

That’s where the **`collectList()`** operator comes into play.

> It gathers all the elements emitted by a Flux and returns them as a **Mono containing a List**.

---

## ⚙️ Basic Behavior

Let’s say you have:

```java
Flux<Integer> numbers = Flux.range(1, 10);
```

This is a Flux emitting values from **1 to 10**.

Now, if you apply `collectList()`:

```java
Mono<List<Integer>> result = numbers.collectList();
```

🧩 Type transformation:

```
Flux<Integer>  →  Mono<List<Integer>>
```

That’s because once you collect everything into a list,
you’ll only have **one final item** (the complete list),
so it becomes a **Mono**.

---

## 🧪 Example Demo

```java
Flux.range(1, 10)
    .collectList()
    .subscribe(System.out::println);
```

### 🖥 Output:

```
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

✅ All elements are emitted together as one **list** once the Flux completes.

---

## 🧠 What Happens Internally

Let’s see how it behaves behind the scenes:

1. The Flux starts emitting items (1, 2, 3, …).
2. The `collectList()` operator **accumulates** all those items in an internal list.
3. It waits for the **onComplete()** signal.
4. Once it receives the **complete signal**, it emits the **entire list** downstream.
5. The subscriber then receives **one Mono** containing the full list.

---

## ⚠️ Important Points

### 1️⃣ `collectList()` is **non-blocking**

It’s not blocking the thread while collecting.

Instead, it **accumulates** items reactively and waits until the Flux completes.

When the complete signal comes:

> “Alright, I have everything — here’s your list!”

So it’s **non-blocking waiting**, not synchronous waiting.

---

### 2️⃣ Works only for **finite** Fluxes

It assumes that the Flux will eventually **complete**.

If the Flux is **infinite** or never completes,
`collectList()` will **never emit anything**, because it will keep waiting forever.

---

### 3️⃣ Behavior on **Error**

Let’s see what happens when an error occurs.

```java
Flux<Integer> faultyFlux = Flux.range(1, 5)
    .concatWith(Mono.error(new RuntimeException("Something went wrong!")));

faultyFlux.collectList()
    .subscribe(
        list -> System.out.println("Received list: " + list),
        err -> System.out.println("Error: " + err.getMessage())
    );
```

### 🖥 Output:

```
Error: Something went wrong!
```

Explanation:

* You **won’t** receive any list.
* The operator immediately forwards the **error signal** downstream.
* It discards all collected data because the Flux did not complete successfully.

🧩 **Rule:**
`collectList()` only emits when the upstream **completes successfully**.
If it ends in error, it emits the **error** and no list.

---

## 🧩 Practical Use Case

Let’s take an example from the **Order Service** scenario:

You have:

```java
Flux<Order> userOrders = orderService.getOrdersByUserId(userId);
```

You want all the orders as a single list:

```java
Mono<List<Order>> ordersList = userOrders.collectList();
```

✅ Use this when:

* You need to send all orders as a **single payload** to another service.
* You need to process or display all items together (like a summary).
* You want to store or log all results collectively after the Flux completes.

---

## ⚙️ Quick Comparison

| Operator            | Input | Output     | Emits    | Example          |
| ------------------- | ----- | ---------- | -------- | ---------------- |
| `Flux.range(1, 10)` | Flux  | Integer    | Multiple | 1, 2, 3, …, 10   |
| `.collectList()`    | Flux  | Mono<List> | One list | [1, 2, 3, …, 10] |

---

## 🧠 Analogy

Think of `collectList()` as a **shopping cart**.

Each item emitted by the Flux is like a product you drop into the cart.

Once you reach checkout (complete signal),
you get the **entire cart (the list)** as a single item.

If something goes wrong before checkout (error signal),
you lose the cart — no result is emitted.

---

## 🧭 Summary

| Concept            | Description                                          |
| ------------------ | ---------------------------------------------------- |
| **Operator**       | `collectList()`                                      |
| **Purpose**        | To collect all emitted items from a Flux into a List |
| **Return Type**    | `Mono<List<T>>`                                      |
| **Behavior**       | Waits for the complete signal, then emits one list   |
| **Blocking?**      | ❌ Non-blocking                                       |
| **Error Handling** | Forwards the error, no list emitted                  |
| **Use Case**       | When you need all Flux items together at once        |

---

## 🧩 Final Takeaway

* `collectList()` is a **terminal collector operator** that turns a **Flux into a Mono**.
* It’s **non-blocking** but waits until all items are emitted.
* Emits a **single list** only when the upstream completes successfully.
* Ideal when you want to **gather all results** and handle them as one object.

---

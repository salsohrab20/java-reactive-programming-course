### 1. Introduction

* In **Reactive Programming** (Project Reactor), **operators** are the fundamental building blocks for transforming, filtering, or controlling data as it flows through a **publisher** (`Flux` or `Mono`).
* Think of operators as **decorators** — they don’t change the publisher itself, but wrap around it, adding new behaviors.
* **Analogy**: Coffee drinks

    * Base espresso → Publisher (`Flux`/`Mono`)
    * Add water → Americano
    * Add steamed milk → Flat white
    * Add milk foam → Latte
    * Similarly, starting with a publisher, operators add “ingredients” (transformations, filters, logic) to shape the final output.

---

### 2. Key Concept

* Operators allow us to **build a pipeline** of transformations and actions.
* **Pipeline structure**:

  ```
  Producer (source) → Operators (decorators) → Subscriber (consumer)
  ```
* Each operator produces a **new Publisher instance** (immutable behavior).

    * Example: `Flux.range(1, 10).filter(...).map(...)`
    * Every step creates a **new Java object**.
    * Subscription happens on the final pipeline.

---

### 3. Example

```java
Flux.range(1, 10)   // producer: emits numbers 1–10
    .filter(i -> i % 2 == 0)   // operator: keep only even numbers
    .map(i -> i * 100)         // operator: transform each item
    .subscribe(System.out::println); // subscriber: consumes result
```

**Output**: 200, 400, 600, 800, 1000

---

### 4. Why Operators Matter

* **Declarative style**: Business requirements are expressed clearly in sequence.
* **Composability**: Small operators can be chained together to form complex pipelines.
* **Asynchronous flow**: Operators work seamlessly with the non-blocking, backpressure-aware nature of reactive streams.
* **Microservices relevance**:

    * Data flows from **UI → Business logic → Database** (and back).
    * Operators apply filters, validations, and transformations *in-stream* rather than in separate imperative steps.
    * Keeps memory footprint low by processing elements as they arrive instead of storing them in bulk.

---

### 5. Categories of Operators

1. **Creation**

    * `just()`, `fromIterable()`, `range()`, `empty()`, etc.
2. **Transformation**

    * `map()`, `flatMap()`, `concatMap()`, `buffer()`, etc.
3. **Filtering**

    * `filter()`, `take()`, `skip()`, `distinct()`, etc.
4. **Combining**

    * `merge()`, `zip()`, `concat()`, etc.
5. **Error Handling**

    * `onErrorReturn()`, `onErrorResume()`, `retry()`, etc.
6. **Utility / Side effects**

    * `log()`, `doOnNext()`, `doOnError()`, etc.
7. **Conditional / Boolean**

    * `any()`, `all()`, `hasElement()`, etc.

---

### 6. Important Points to Remember

* Operators do **not mutate** the original publisher. Each operator returns a **new pipeline**.
* The pipeline only starts executing when there is a **subscription**.
* Data flows **sequentially** through all operators, reaching the subscriber at the end.
* In real-world microservices:

    * Incoming data (from UI or API) → Operators apply business rules → Output goes to DB or API response.
    * Outgoing data (from DB/API) → Operators transform/filter → Sent to front-end for display.

---

### 1. Introduction

* In Project Reactor, **`handle`** is a **flexible operator** that behaves like a combination of **`filter` + `map`**.
* It allows you to:

    * Transform elements (`map`-like behavior).
    * Filter elements (`filter`-like behavior).
    * Emit errors or skip items as needed.
* Essentially, it gives **fine-grained control** over what gets passed downstream to the subscriber.

---

### 2. Motivation

* Many real-world requirements cannot be solved with a single `filter` or `map`.
* Example requirement:

    * If the item is `1`, send `-2`.
    * If the item is `4`, **don’t send it at all**.
    * If the item is `7`, throw an **error**.
    * For all other items, pass them as-is.

Clearly, this needs **more control** than what `filter()` or `map()` alone provide. → This is where **`handle`** comes in.

---

### 3. Signature of `handle`

```java
<T, R> Flux<R> handle(BiConsumer<T, SynchronousSink<R>> handler)
```

* Takes a **`BiConsumer`**:

    * First parameter → the **item** emitted by the source.
    * Second parameter → a **`SynchronousSink`**, which allows you to:

        * `sink.next(value)` → emit a transformed value.
        * `sink.error(Throwable)` → propagate an error.
        * (do nothing) → effectively drop the item.

---

### 4. Example Implementation

```java
Flux.range(1, 10)
// .filter(i -> i != 7)
                .handle((item, sink) -> {
        switch (item) {
        case 1 -> sink.next(-2);
                        case 4 -> {
                                }
                                case 7 -> sink.error(new RuntimeException("7 error"));
default -> sink.next(item);
                    }
                            })
                            .cast(Integer.class)
                .subscribe(Util.subscriber());
```

---

### 5. Observations

* Without special handling, the type inferred after `handle` may be `Flux<Object>`.

    * To enforce type safety, you can **cast** explicitly:

      ```java
      Flux<Integer> processed = numbers.handle(...).cast(Integer.class);
      ```
* Execution:

    * `1` → transformed into `-2`.
    * `4` → skipped (no emission).
    * `7` → triggers an error, stopping the pipeline.
    * Other numbers → flow through unchanged.

---

### 6. Key Points

* **`handle` = flexible hybrid** (filter + map + error).
* Great for **complex conditional logic**.
* Always produces a **new Publisher** instance → immutable and safe.
* Objects created are lightweight → no performance concerns.
* The pipeline only runs once **subscribed**.

---

# Lecture — **Do** hooks / callbacks (refined notes)

### Overview

Do-hooks (a.k.a. *side-effect* callbacks) are convenient operators in Reactor that let you observe lifecycle events on a reactive pipeline without changing the values flowing through it. They are **observational** — useful for logging, metrics, debugging, or releasing resources — but they should **not** be used to alter the pipeline’s data flow or to implement business logic.

---

# What do-hooks are good for

* Logging what’s emitted (`doOnNext`)
* Observing subscription / request lifecycle (`doFirst`, `doOnSubscribe`, `doOnRequest`)
* Observing completion/error/cancellation (`doOnComplete`, `doOnError`, `doOnCancel`, `doOnTerminate`, `doFinally`)
* Observing discarded items when upstream produced but downstream cancelled (`doOnDiscard`)
* Minimal, synchronous side effects only (they run on the pipeline’s thread unless you change schedulers).

---

# Common do-hooks (what they run and when)

* `doFirst(Runnable)`
  Runs **first** when the subscription starts — invoked from the *subscriber* side as the subscription traverses upstream. Useful to run something before anything else (e.g., set up context).

* `doOnSubscribe(Consumer<Subscription>)`
  Runs when the downstream receives the `Subscription` object. Receives the `Subscription`.

* `doOnRequest(LongConsumer)`
  Runs when the downstream requests items (e.g., `request(n)`); shows requested demand. Note: intermediate operators may change requests.

* `doOnNext(Consumer<T>)`
  Runs for **each** emitted item (before it reaches the downstream operator/subscriber). Good for debugging or very small side effects.

* `doOnError(Consumer<Throwable>)`
  Runs when the upstream signals an error.

* `doOnComplete(Runnable)`
  Runs when the upstream sends `onComplete()`.

* `doOnTerminate(Runnable)`
  Runs when the upstream terminates either with complete **or** error. (It does **not** run on cancel.)

* `doFinally(Consumer<SignalType>)`
  Always runs exactly once at the end with a `SignalType` (ON_COMPLETE, ON_ERROR, CANCEL). Use to cleanup regardless of how the sequence ended.

* `doOnCancel(Runnable)`
  Runs when downstream cancels. Cancellation flows upstream only — it does not produce an onComplete/onError.

* `doOnDiscard(Class<T>, Consumer<T>)` / `doOnDiscard(Consumer<Object>)`
  Runs when items are dropped/discarded (for example, because upstream keeps producing after downstream cancelled or an operator drops them). Receives discarded value(s).

---

# Important behavioral notes (execution order & direction)

* **Subscription & doFirst execution starts from subscriber and flows upstream**. If you have multiple `doFirst()` operators, the last one in the chain executes first (because subscription flows bottom-up).
* **Subscription object (`doOnSubscribe`) flows top-down** after the subscription request moves upstream; `doOnSubscribe` is invoked as the subscription reaches each operator.
* **Request events (`doOnRequest`) reflect the effective request passed to that operator** — operators like `take()` or `limitRate()` can change request values. Thus `doOnRequest` is useful to see how operators affect demand.
* **Emission events (`doOnNext`) flow top-down**: when the producer emits, `doOnNext` attached to each operator runs as the item passes through.
* **Terminate hooks**: `doOnComplete`, `doOnError`, `doOnTerminate` run when upstream sends completion/error and it propagates downstream.
* **doFinally** always runs after subscriber received the final signal (complete/error/cancel), so it’s ideal for final cleanup.

Concretely: the subscription path is bottom→top; the emission path is top→bottom. Keep this directionality in mind when reasoning about logs and counts.

---

# Example (illustrative)

```java
Flux<Integer> flux = Flux.create(sink -> {
    log.info("producer begins");
    for (int i = 0; i < 10; i++) {
        sink.next(i);
    }
    sink.complete();
    log.info("producer ends");
});

flux
  .doFirst(() -> log.info("doFirst #1"))
  .doFirst(() -> log.info("doFirst #2"))    // runs BEFORE doFirst #1
  .doOnSubscribe(s -> log.info("doOnSubscribe: {}", s))
  .doOnRequest(n -> log.info("doOnRequest: {}", n))
  .doOnNext(i -> log.info("doOnNext: {}", i))
  .doOnComplete(() -> log.info("doOnComplete"))
  .doOnTerminate(() -> log.info("doOnTerminate"))
  .doFinally(signal -> log.info("doFinally: {}", signal))
  .take(2)   // modifies request/demand and will cancel upstream after 2 items
  .doOnDiscard(Integer.class, discarded -> log.info("discarded: {}", discarded))
  .subscribe(
      i -> log.info("Subscriber got: {}", i),
      e -> log.error("Subscriber error", e),
      () -> log.info("Subscriber complete")
  );
```

Notes on this example:

* `doFirst #2` will print before `doFirst #1` (because subscription flows up).
* `take(2)` will reduce effective request to 2; upstream may still produce (if it ignores demand), causing discarded values — `doOnDiscard` logs them.
* When `take` cancels upstream after 2 items, `doOnCancel` (if present) and `doFinally(SignalType.CANCEL)` will run.

---

# Interaction with backpressure / operators

* Operators may **modify demand**. Example: `take(n)` will request only `n` from upstream; `doOnRequest` shows the effective request for the operator it’s attached to.
* If the producer ignores backpressure (e.g., a plain `Flux.create` that emits without checking demand), upstream may produce more items than requested. Those extra items can be discarded — `doOnDiscard` will notify you.
* `generate` respects downstream demand better than some naive `create` implementations. If you are testing backpressure behavior, prefer `Flux.generate` or `Flux.push` with correct demand handling.

---

# Best practices and caveats

* **Do not** use do-hooks to implement business logic — they are for side effects and observation only.
* Keep do-hook actions **fast** and **non-blocking**. Heavy work in a `doOnNext` can harm reactive throughput.
* If you need asynchronous side-effects, schedule them explicitly (e.g., publishOn/subscribeOn), or use operators designed for async tasks.
* `doOnDiscard` requires Reactor versions that include it — confirm your Reactor Core version if it doesn’t appear.
* Hooks execute in the pipeline’s thread by default. Consider thread-safety for any shared mutable state touched in hooks.
* `doFinally` is the most reliable place for cleanup because it runs on all terminal paths (complete/error/cancel).

---

# When to use which hook (quick cheat-sheet)

* Start-of-subscription initialization: `doFirst`
* Inspect subscription object: `doOnSubscribe`
* Inspect or log requests: `doOnRequest`
* Per-item logging/debugging: `doOnNext`
* Clean up on normal completion: `doOnComplete` (or `doFinally`)
* Observe errors: `doOnError`
* Observe cancellation: `doOnCancel`
* Run any final cleanup regardless of reason: `doFinally(SignalType)`
* Inspect values dropped due to cancellation/backpressure mismatch: `doOnDiscard`

---
## 🔑 Core Hooks

| Hook                                        | Triggered When...                                        | Notes                              |
| ------------------------------------------- | -------------------------------------------------------- | ---------------------------------- |
| **`doFirst(Runnable)`**                     | Right at the start of subscription (runs bottom → top)   | Last declared runs first           |
| **`doOnSubscribe(Consumer<Subscription>)`** | Subscriber receives a `Subscription`                     | Inspect subscription object        |
| **`doOnRequest(LongConsumer)`**             | Downstream requests `n` items                            | Shows demand; operators may adjust |
| **`doOnNext(Consumer<T>)`**                 | Each time an item is emitted                             | Good for logging/debugging         |
| **`doOnError(Consumer<Throwable>)`**        | Upstream emits error signal                              | Observes failure                   |
| **`doOnComplete(Runnable)`**                | Upstream sends complete signal                           | Runs once                          |
| **`doOnTerminate(Runnable)`**               | Upstream terminates (complete or error)                  | Runs in both cases, not cancel     |
| **`doFinally(Consumer<SignalType>)`**       | Always runs at the very end (complete, error, or cancel) | Best for cleanup                   |
| **`doOnCancel(Runnable)`**                  | Downstream cancels subscription                          | Cancel flows upstream only         |
| **`doOnDiscard(Class<T>, Consumer<T>)`**    | Items are discarded (e.g., take(), cancel)               | Gives you discarded value(s)       |

---

## 📊 Direction of Flow

* **Subscription events** (first, subscribe, request) → **subscriber → upstream**
* **Emission events** (next, complete, error, terminate) → **producer → downstream**
* **Cancel** → **downstream → upstream**

---

## 🧠 Tips

* `doFirst`: runs in reverse order (last added runs first).
* `doOnRequest`: request values may be modified (e.g., `take(2)`).
* `doOnDiscard`: useful for spotting wasted items when producer ignores backpressure.
* Use `doFinally` for resource cleanup (fires in all end cases).
* Keep hooks **lightweight, non-blocking, side-effect only**.
* Never implement business logic inside hooks.

---

✅ **Mnemonic**:

* *Start*: `doFirst`, `doOnSubscribe`, `doOnRequest`
* *During*: `doOnNext`
* *End*: `doOnComplete`, `doOnError`, `doOnTerminate`, `doOnCancel`, `doOnDiscard`, `doFinally`

---
### Why this lecture?

Many people get confused when they see **`doOnNext`** being used for mutation. Some even assume:

> “Mutation is always bad.”

That’s not true — **context matters**. Let’s clarify.

---

### 1. Functional programming vs. Reality

* **Functional programming** encourages immutability and pure functions.
* Yes, immutability is a good practice — it avoids side-effects, makes reasoning easier, and works well in concurrent systems.
* But → not everything in real-world Java is immutable.
* **Entity objects** (e.g., JPA entities, domain models) are typically **mutable** — and that’s perfectly fine.

---

### 2. Traditional Blocking Code

Example: Update a customer’s age

```java
Optional<Customer> customerOpt = repository.findById(123);
if (customerOpt.isPresent()) {
    Customer customer = customerOpt.get();
    customer.setAge(10);  // mutation
    repository.save(customer);
}
```

* Nobody complains here — this is the normal way in blocking style.

---

### 3. Reactive Code Equivalent

With a **Reactive Repository**:

```java
repository.findById(123) // returns Mono<Customer>
    .doOnNext(customer -> customer.setAge(10))  // mutation inside doOnNext
    .flatMap(repository::save)
    .subscribe();
```

* `Mono<Customer>` is asynchronous. You cannot call `.get()` like with `Optional`.
* Instead, you use `doOnNext` (or `map`) to act when the value arrives.
* Mutation happens at the right time — when the publisher emits the entity.

---

### 4. Is this safe?

Yes, because:

* Project Reactor provides guarantees:

    * A given signal (`onNext`) is processed by **one thread at a time**.
    * No two threads will mutate the *same emitted item* concurrently.
* It’s effectively no different from the traditional blocking example — just non-blocking and asynchronous.

---

### 5. When to prefer immutability

* **Pure transformations** (e.g., converting DTOs, mapping values) → prefer `map()` returning a new object.
* **Entity updates (DB objects)** → mutation is expected and normal.

---

### ✅ Key Takeaway

* **Immutability is good**, but **mutation is not evil**.
* Use immutability where it improves clarity and correctness.
* Use mutation naturally when dealing with **mutable entities** — especially in persistence layers.
* In Reactor, `doOnNext` is a natural place to perform such updates before saving.

---

### 1. Introduction

* By default, `Flux.range(1, 10)` emits items **as fast as possible**.
* What if we want to **slow down** emissions and deliver items one-by-one with a pause in between?
* For this, Reactor provides the **`delayElements`** operator.

---

### 2. Example

```java
Flux.range(1, 10)
    .delayElements(Duration.ofSeconds(1))
    .subscribe(Util.subscriber());

Util.sleepSeconds(12); // block main thread to see results
```

**Output behavior:**

* Instead of printing all numbers instantly,
* Subscriber receives: `1`, then after 1s → `2`, then after 1s → `3`, … until `10`.

---

### 3. How it Works

* `delayElements` schedules emissions on a **separate thread (Scheduler)**, because it needs to run at fixed intervals.
* Without blocking the main thread (e.g., `sleepSeconds`), your program would exit before you see the results.
* **Important**: `delayElements` does **not** buffer all items and drip-feed them.
    * Instead, it controls the **request pattern** to the upstream publisher.

---

### 4. With and Without Delay

Let’s inspect with `.log()` operator:

#### Case 1: Without delay

```java
Flux.range(1, 10)
    .log()
    .subscribe(Util.subscriber());
```

* Subscriber requests `Long.MAX_VALUE` (unbounded).
* Producer emits all `1 → 10` immediately.
* Then `onComplete` is signaled.

#### Case 2: With delay

```java
Flux.range(1, 10)
    .delayElements(Duration.ofSeconds(1))
    .log()
    .subscribe(Util.subscriber());

Util.sleepSeconds(12);
```
* Log shows: **request(1)** each second.
* `delayElements` keeps asking upstream:

  > “It’s time, give me just one more value.”
* Producer emits exactly one item per request, respecting the timing.

---

### 5. Why is this great?

* **Backpressure-friendly**: `delayElements` doesn’t overwhelm memory by prefetching all items.
* **Elegant design**: Upstream only works when requested, making it efficient.
* **Use cases**:

    * Throttling emissions (simulate slow data source).
    * Demo/teaching reactive streams.
    * UI updates (periodic refresh).
    * Rate-limiting in simple scenarios.

---

### 6. Key Points

* `delayElements` uses a **Scheduler (parallel thread)** internally.
* Always **block main thread** when running in a simple `main()` demo (e.g., `Util.sleepSeconds`).
* Requests are **one at a time**, not unbounded.
* It is *not* holding items in memory; instead, it requests per item at fixed intervals.

---

### 1. Introduction

* In all our demos so far, we used a **utility subscriber** (e.g., `Util.subscriber()`).
* But Reactor’s `subscribe()` method itself is very flexible — it allows you to pass **callback functions** directly.
* This can make code shorter and sometimes more readable.

---

### 2. `subscribe()` Variants

Reactor provides multiple overloads for `subscribe()`:

1. **Basic subscription (no handlers)**

   ```java
   Flux.range(1, 5)
       .subscribe();
   ```
    * Subscribes and requests `Long.MAX_VALUE`.
    * No logging, no handling of signals.

---

2. **With onNext handler**

   ```java
   Flux.range(1, 5)
       .subscribe(item -> System.out.println("Received: " + item));
   ```

    * Handles only emitted items.
    * Errors and completion are ignored.

---

3. **With onNext + onError handler**

   ```java
   Flux.range(1, 5)
       .subscribe(
           item -> System.out.println("Received: " + item),
           err -> System.err.println("Error: " + err)
       );
   ```

---

4. **With onNext + onError + onComplete handlers**

   ```java
   Flux.range(1, 5)
       .subscribe(
           item -> System.out.println("Received: " + item),
           err -> System.err.println("Error: " + err),
           () -> System.out.println("Stream complete")
       );
   ```

---

5. **With Subscription handler (onSubscribe)**

   ```java
   Flux.range(1, 5)
       .subscribe(
           item -> System.out.println("Received: " + item),
           err -> System.err.println("Error: " + err),
           () -> System.out.println("Stream complete"),
           sub -> sub.request(3) // manual control of demand
       );
   ```

    * Useful when you want to control **backpressure** manually.
    * For example, request only 3 items instead of all.

---

### 3. Comparison with `Util.subscriber()`

* Using `Util.subscriber()` gives us a **ready-made subscriber with logging**.
* Using explicit `subscribe(...)` callbacks gives you **control and readability** in small demos or business logic.
* Both approaches are valid — pick what fits the context.

---

## Error Handling in Reactive Programming

### 🔹 Introduction

Error handling is one of the most **important and practical topics** in reactive programming.
When building reactive applications, you deal with **producers (Flux/Mono)** and chain multiple **operators** to implement business logic.
However, things can fail at any point — for example:

* Network failures
* Invalid data
* Runtime exceptions (e.g., divide by zero)

In **traditional programming**, we use `try-catch` blocks to handle exceptions.
But in **Reactive Programming**, we handle errors using **reactive operators**.
This lecture focuses on **how to handle errors effectively in Reactor (Flux/Mono).**

---

### 🔹 Step 1: Creating a Simple Producer

We start by creating a simple producer using:

```java
Flux<Integer> flux = Flux.range(1, 10);
```

This produces numbers 1 through 10.

Then we subscribe to it using:

```java
flux.subscribe(Util.subscriber());
```

This simply emits all numbers from 1 to 10.

---

### 🔹 Step 2: Introducing an Error

Now, let’s intentionally cause an error in the data stream:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0; // Intentional divide-by-zero
        return i;
    })
    .subscribe(Util.subscriber());
```

✅ **What happens?**

* We get values `1, 2, 3, 4`
* Then an exception occurs when `i = 5` (`ArithmeticException`)
* The stream **terminates immediately** — no more values are emitted.

In reactive streams, once an **error signal** is emitted, the **publisher stops** producing further data.

---

### 🔹 Step 3: Handling Errors Using `onErrorReturn()`

Sometimes, you want the stream to **continue gracefully** and provide a **fallback value** when an error occurs.

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorReturn(-1)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
1
2
3
4
-1
```

Here, when an error occurs, instead of propagating the exception, the stream emits a **fallback value** (`-1`) and **completes normally**.

---

### 🔹 Step 4: Operator Placement Matters

The **placement** of the error-handling operator is **critical**.

Example — this won’t work as expected:

```java
Flux.range(1, 10)
    .onErrorReturn(-1) // Placed before map
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .subscribe(Util.subscriber());
```

❌ The `onErrorReturn` won’t catch the error because:

* It is attached to the **producer (Flux.range)**, which doesn’t throw the error.
* The error occurs **downstream** in the `map()` operator.

✅ Correct placement:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorReturn(-1)
    .subscribe(Util.subscriber());
```

Always place your **error-handling operator close to the subscriber**, to ensure it can intercept all upstream errors.

---

### 🔹 Step 5: Handling Specific Exception Types

You might not want to handle **all exceptions the same way**.
For example, you may want to handle only `IllegalArgumentException` or `ArithmeticException`.

Reactor allows specifying the **exception type** in `onErrorReturn()`:

```java
.onErrorReturn(IllegalArgumentException.class, -1)
.onErrorReturn(ArithmeticException.class, -2)
```

✅ Example:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorReturn(IllegalArgumentException.class, -1)
    .onErrorReturn(ArithmeticException.class, -2)
    .subscribe(Util.subscriber());
```

📊 **Output:**

```
1
2
3
4
-2
```

Explanation:

* When `i = 5`, an `ArithmeticException` occurs.
* The second `onErrorReturn` handles it and emits `-2`.
* The stream completes successfully.

---

### 🔹 Step 6: Works for Both Flux and Mono

All the error-handling concepts you see here work for **both `Flux` and `Mono`**.

Example with Mono:

```java
Mono.just(5)
    .map(i -> i / 0)
    .onErrorReturn(ArithmeticException.class, -2)
    .subscribe(Util.subscriber());
```

✅ Output:

```
-2
complete
```

---

### 🔹 Step 7: Layered Error Handling

You can chain multiple error handlers for different exception types:

```java
.onErrorReturn(IllegalArgumentException.class, -1)
.onErrorReturn(ArithmeticException.class, -2)
.onErrorReturn(-3); // fallback for any other exception
```

This makes it **clear and maintainable**, indicating exactly which fallback is applied for which exception.

---

### 🔹 Summary

| Concept                      | Description                                               | Example                                         |
| ---------------------------- | --------------------------------------------------------- | ----------------------------------------------- |
| `onErrorReturn(value)`       | Returns fallback value for **any** error                  | `.onErrorReturn(-1)`                            |
| `onErrorReturn(type, value)` | Returns fallback only for specific **exception type**     | `.onErrorReturn(ArithmeticException.class, -2)` |
| Operator order               | Must be placed **after** possible error-causing operators | `.map(...).onErrorReturn(...)`                  |
| Works with Mono              | Same syntax and behavior as Flux                          | `Mono.just(...).onErrorReturn(...)`             |
| Stream stops on error        | Unless handled by an operator, error terminates stream    | Without handling → error stops flow             |

---

### 🔹 Practical Use Case

Imagine a reactive **API call**:

```java
Flux<User> users = userService.getUsers()
    .map(this::enrichUser)
    .onErrorReturn(Collections.emptyList());
```

Here, if the network or parsing fails, you gracefully fallback to an empty list instead of breaking the stream.

---

### 🔹 Key Takeaway

* Traditional `try-catch` → Reactive `onErrorX()` operators.
* Use `onErrorReturn()` for **simple fallback** values.
* Always position error handlers **near the subscriber**.
* For complex logic, use other operators like:

    * `onErrorResume()` – dynamic fallback
    * `onErrorContinue()` – skip errors and continue
    * `doOnError()` – for logging/debugging

---
### 🔹 The Problem: Hardcoded Fallbacks Aren’t Always Enough

In real-world applications, especially in **microservice-based architectures**, errors are common.
For example:

* Your main **Product Service** fails due to network timeout.
* You have another **Backup or Fallback Service** that can still provide data.

In such cases, we can’t just return a hardcoded value — we need to **dynamically switch** to another data source.

---

### 🔹 The Solution: `onErrorResume()`

Reactor provides a powerful operator:

```java
onErrorResume()
```

This operator allows you to:

* **Catch the error**,
* **Inspect the exception**, and
* **Return another reactive stream** (`Mono` or `Flux`) as a **fallback source**.

So instead of returning a fixed value, you can call another microservice or perform any asynchronous operation.

---

### 🔹 Example: Fallback Service

Let’s define a fallback service method:

```java
private static Mono<Integer> fallback() {
    return Mono.fromSupplier(() -> {
        System.out.println("Calling fallback service...");
        return Faker.instance().number().numberBetween(10, 100);
    });
}
```

✅ This simulates another microservice call that returns a random number between 10 and 100.

---

### 🔹 Example Usage of `onErrorResume()`

Now, let’s integrate it into a reactive pipeline:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0; // Intentional divide-by-zero
        return i;
    })
    .onErrorResume(ex -> {
        System.out.println("Error: " + ex.getMessage());
        return fallback();
    })
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
1
2
3
4
Error: / by zero
Calling fallback service...
95
complete
```

👉 The stream failed at `i = 5`, triggered an error, and then **resumed** by switching to the fallback service.

---

### 🔹 Technical Insight: `onErrorResume()` Signature

```java
public final Flux<T> onErrorResume(Function<? super Throwable, ? extends Publisher<? extends T>> fallbackFunction)
```

So:

* It accepts a **function** that takes a `Throwable`
* Returns a **Publisher** (Mono or Flux)
* The returned Publisher acts as a **replacement sequence** after the error

---

### 🔹 Real-World Use Case

Suppose your **ProductService** fails; you can resume the stream using **FallbackProductService**:

```java
productService.getProductDetails()
    .onErrorResume(ex -> fallbackProductService.getBackupProductDetails())
```

This ensures high availability — your pipeline **continues gracefully** even if the primary service fails.

---

### 🔹 Handling Specific Exception Types

Just like `onErrorReturn`, `onErrorResume` also supports **exception type matching**.

```java
.onErrorResume(ArithmeticException.class, ex -> fallback())
.onErrorResume(RuntimeException.class, ex -> fallbackTwo())
```

✅ Example:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorResume(ArithmeticException.class, ex -> fallback())
    .onErrorResume(RuntimeException.class, ex -> fallbackTwo())
    .subscribe(Util.subscriber());
```

* If the error is `ArithmeticException` → call `fallback()`
* If it’s any other `RuntimeException` → call `fallbackTwo()`

---

### 🔹 Multiple Fallbacks Example

You can define multiple fallback methods — simulating **multiple microservices**:

```java
private static Mono<Integer> fallback1() {
    return Mono.fromSupplier(() -> Faker.instance().number().numberBetween(10, 100));
}

private static Mono<Integer> fallback2() {
    return Mono.fromSupplier(() -> Faker.instance().number().numberBetween(100, 1000));
}
```

And your error-handling logic could be:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorResume(ArithmeticException.class, ex -> fallback1())
    .onErrorResume(RuntimeException.class, ex -> fallback2())
    .onErrorReturn(-5) // Default handler if even fallback fails
    .subscribe(Util.subscriber());
```

✅ **Behavior Summary:**

| Error Type                          | Operator Invoked    | Fallback Output Range |
| ----------------------------------- | ------------------- | --------------------- |
| `ArithmeticException`               | `fallback1()`       | 10–100                |
| `RuntimeException`                  | `fallback2()`       | 100–1000              |
| Any other error or fallback failure | `onErrorReturn(-5)` | -5                    |

---

### 🔹 Chained Fallback Handling

What if your **fallback service itself fails**?
Reactor allows nesting fallback logic elegantly:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .onErrorResume(ArithmeticException.class, ex ->
        fallback1()
            .onErrorResume(innerEx -> fallback2())
            .onErrorReturn(-5)
    )
    .subscribe(Util.subscriber());
```

✅ This ensures:

1. Main service fails → try Fallback 1
2. Fallback 1 fails → try Fallback 2
3. Fallback 2 fails → emit `-5`

This is a **resilient fallback chain**, often seen in distributed systems with cascading services.

---

### 🔹 Comparison: `onErrorReturn()` vs `onErrorResume()`

| Feature            | `onErrorReturn()`                     | `onErrorResume()`                             |
| ------------------ | ------------------------------------- | --------------------------------------------- |
| Type of fallback   | Static value                          | Dynamic publisher                             |
| Input              | None                                  | Access to `Throwable`                         |
| Use case           | Simple fallback (e.g., default value) | Complex fallback (e.g., another service call) |
| Async support      | ❌ No                                  | ✅ Yes                                         |
| Real-world example | Return cached data                    | Call backup service                           |

---

### 🔹 Summary

✅ **`onErrorResume()` Key Points**

* Used when you need a **dynamic or reactive fallback**.
* Can call **another publisher** (Mono/Flux).
* Accepts a **function that returns a Publisher**.
* Supports **exception type-based matching**.
* Can chain multiple fallbacks for resilience.
* Often used in **microservice architectures** where backup APIs exist.

---

### 🔹 Code Summary

```java
private static Mono<Integer> fallback1() {
    return Mono.fromSupplier(() -> Faker.instance().number().numberBetween(10, 100));
}

private static Mono<Integer> fallback2() {
    return Mono.fromSupplier(() -> Faker.instance().number().numberBetween(100, 1000));
}

public static void onErrorResumeExample() {
    Flux.range(1, 10)
        .map( i-> i==5 ? i/0 : i)
        })
        .onErrorResume(ArithmeticException.class, ex -> fallback1())
        .onErrorResume(RuntimeException.class, ex -> fallback2())
        .onErrorReturn(-5)
        .subscribe(Util.subscriber());
}
```

---

### 🔹 Practical Analogy

Imagine your **main restaurant** runs out of food:

* You try your **backup kitchen (fallback1)**.
* If that fails too, you call a **partner restaurant (fallback2)**.
* If even that fails, you simply say, “Sorry, no food today.” → (`onErrorReturn()`)

That’s exactly what `onErrorResume()` does in reactive streams.

---


## Error Handling in Reactive Programming (Part 3)

### 🔹 The Scenario

Now imagine a case where you don’t want to:

* Return any hardcoded value (`onErrorReturn`), **or**
* Call another publisher or fallback (`onErrorResume`).

Instead, your subscriber’s expectation is simpler:

> “If everything goes fine, give me the data.
> If something goes wrong, just stop gracefully — don’t throw an exception.”

In other words, the subscriber wants the **stream to complete silently** when an error occurs, rather than fail with an exception.

---

### 🔹 The Solution: `onErrorComplete()`

Reactor provides an elegant operator for this scenario:

```java
onErrorComplete()
```

This operator **suppresses the error signal** and **converts it into a completion signal**.

---

### 🔹 Behavior Summary

| Situation        | Behavior                                                     |
| ---------------- | ------------------------------------------------------------ |
| Normal execution | Data flows as usual                                          |
| Error occurs     | Error signal is swallowed, replaced with `onComplete` signal |
| Subscriber view  | Looks as if the stream completed successfully                |

---

### 🔹 Example

Let’s take a simple example with an intentional error:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0; // Intentional divide-by-zero
        return i;
    })
    .onErrorComplete()
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
1
2
3
4
complete
```

Here’s what happened:

* Numbers 1–4 were emitted successfully.
* When `i = 5`, an `ArithmeticException` occurred.
* Instead of propagating the error to the subscriber,
  the `onErrorComplete()` operator **silenced the error** and emitted a **complete signal**.
* The subscriber thinks the producer completed normally.

---

### 🔹 Explanation

Under the hood:

* Normally, the Reactive Streams flow is:
  **onNext → onError → [terminate]**
* With `onErrorComplete()`, this becomes:
  **onNext → onComplete** (error is swallowed)

This is useful in cases where errors are *non-critical* or *expected* and you simply want to stop processing without failure.

---

### 🔹 Real-World Use Case

Imagine a background data-sync job that reads user data reactively:

```java
Flux<User> users = userService.getUsers()
    .map(this::enrichUserData)
    .onErrorComplete();
```

If a network error occurs or one user record fails, you may decide that’s not critical — just stop the pipeline quietly.

This prevents unnecessary “red error logs” and lets the system degrade gracefully.

---

### 🔹 Conditional Error Completion

You can also make `onErrorComplete()` **conditional**, based on exception type or custom logic.

#### Example:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        if (i == 7)
            throw new IllegalArgumentException("Bad data");
        return i;
    })
    .onErrorComplete(ArithmeticException.class) // Only swallow ArithmeticException
    .subscribe(Util.subscriber());
```

✅ **Behavior:**

* When `i = 5` → `ArithmeticException` → handled silently, stream completes.
* When `i = 7` → `IllegalArgumentException` → not handled → subscriber sees the error.

---

### 🔹 Signature Overview

```java
Flux<T> onErrorComplete()
Flux<T> onErrorComplete(Predicate<? super Throwable> predicate)
Flux<T> onErrorComplete(Class<? extends Throwable> type)
```

So you can:

* Suppress **all errors** → `.onErrorComplete()`
* Suppress **specific exception types** → `.onErrorComplete(ArithmeticException.class)`
* Suppress **based on custom logic** → `.onErrorComplete(ex -> ex.getMessage().contains("ignore"))`

---

### 🔹 Comparison Table

| Operator            | Purpose                     | Error Behavior                          | Fallback Output          |
| ------------------- | --------------------------- | --------------------------------------- | ------------------------ |
| `onErrorReturn()`   | Return a static value       | Suppresses error                        | Emits value              |
| `onErrorResume()`   | Switch to another publisher | Replaces error with alternate publisher | Emits new publisher data |
| `onErrorComplete()` | Silently complete stream    | Converts error → complete signal        | No value emitted         |

---

### 🔹 Example with Logging

It’s often useful to log the error before swallowing it:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0;
        return i;
    })
    .doOnError(ex -> System.out.println("Error occurred: " + ex.getMessage()))
    .onErrorComplete()
    .subscribe(Util.subscriber());
```

✅ Output:

```
1
2
3
4
Error occurred: / by zero
complete
```

This way, you **log the issue** but **don’t fail the stream**.

---

### 🔹 When to Use `onErrorComplete()`

| ✅ Use it when…                         | ❌ Avoid when…                                           |
| -------------------------------------- | ------------------------------------------------------- |
| You want to ignore non-critical errors | The error indicates data corruption or critical failure |
| You prefer graceful termination        | You need diagnostic visibility of errors                |
| You want to stop processing silently   | You need fallback or recovery behavior                  |

---

### 🔹 Summary

✅ **`onErrorComplete()`** converts an error into a **complete signal**.
✅ No fallback value or publisher is returned.
✅ You can handle all errors or specific exception types.
✅ Useful when:

* You want to **hide transient or non-critical failures**
* You prefer **graceful termination** instead of exception propagation
* You are running **background, best-effort, or cleanup tasks**

---

### 🔹 Code Summary

```java
private static void onErrorCompleteExample() {
    Flux.range(1, 10)
        .map(i -> {
            if (i == 5)
                return i / 0;
            return i;
        })
        .doOnError(ex -> System.out.println("Logging error: " + ex.getMessage()))
        .onErrorComplete()
        .subscribe(Util.subscriber());
}
```

✅ Output:

```
1
2
3
4
Logging error: / by zero
complete
```

---

### 🔹 The Problem

By design, **Reactive Streams** follow a strict contract:

> After an `onError` signal, no further `onNext` signals will be emitted.

That means — once an error happens, the stream stops completely.

But in some real-world scenarios, this may not be desirable.
Sometimes, you want to **skip the faulty element** and **continue processing the rest**.

For example:

* You are processing a list of 10 items from an API.
* If 1 item causes an exception (say due to bad data), you still want to process the remaining 9.

---

### 🔹 The Solution: `onErrorContinue()`

Project Reactor provides a special operator for this behavior:

```java
onErrorContinue();
```

This operator allows your stream to **skip over elements that cause errors** and **continue processing** the next ones.

---

### 🔹 Key Characteristics

| Property   | Description                                                                      |
| ---------- | -------------------------------------------------------------------------------- |
| Type       | Terminal operator (affects downstream flow)                                      |
| Input      | A `BiConsumer<Throwable, Object>`                                                |
| Parameters | `(Throwable error, Object element)`                                              |
| Behavior   | Skips the element that caused the error, logs/handles it, and continues the flow |
| Return     | No fallback value; next elements continue as usual                               |

---

### 🔹 Example

Let’s take our usual example of a simple producer that emits numbers 1–10:

```java
Flux.range(1, 10)
    .map(i -> {
        if (i == 5)
            return i / 0; // Intentional divide-by-zero
        return i;
    })
    .onErrorContinue((error, obj) -> {
        System.out.println("Error occurred for value: " + obj);
        System.out.println("Exception: " + error.getMessage());
    })
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
1
2
3
4
Error occurred for value: 5
Exception: / by zero
6
7
8
9
10
complete
```

---

### 🔹 Explanation

Here’s what happened:

1. The producer emitted values `1 → 4` successfully.
2. When it reached `5`, it caused an `ArithmeticException` (`divide by zero`).
3. Instead of stopping the stream:

    * `onErrorContinue()` caught the error.
    * Logged the error details (`value: 5` and exception message).
    * **Skipped** that faulty value.
4. The stream continued smoothly from `6 → 10`.
5. Finally, it emitted a **complete** signal.

This means the pipeline **did not terminate on error** — it **continued** processing remaining data.

---

### 🔹 Technical Signature

```java
Flux<T> onErrorContinue(BiConsumer<Throwable, Object> errorConsumer)
```

* The first parameter (`Throwable`) gives you the exception details.
* The second parameter (`Object`) represents the **element that triggered the exception**.

This allows fine-grained control:
You can **log, audit, or track** which elements caused issues without stopping the stream.

---

### 🔹 Real-World Use Case

Imagine an **ETL pipeline** that processes customer records:

```java
Flux<Customer> customers = getCustomers()
    .map(this::validateCustomer)
    .map(this::transformCustomer)
    .onErrorContinue((ex, obj) -> {
        System.out.println("Skipping invalid record: " + obj);
        log.error("Validation failed: {}", ex.getMessage());
    })
    .flatMap(this::saveCustomer)
    .subscribe();
```

✅ **Outcome:**

* Invalid or corrupted customer records are skipped.
* Remaining valid records continue through transformation and persistence.
* The overall pipeline remains **resilient** and **non-blocking**.

---

### 🔹 Key Behavior Notes

* **No fallback value:** The faulty element is skipped entirely — not replaced.
* **Error isolation:** Each element is processed independently; one failure doesn’t affect others.
* **Logging opportunity:** Ideal place to log or send alerts about problematic data.
* **Flow continues:** Subsequent elements continue to be emitted and processed normally.

---

### 🔹 Important Considerations

| Note                | Description                                                                     |
| ------------------- | ------------------------------------------------------------------------------- |
| ⚠️ Use with caution | Suppressing all errors may hide serious bugs or data issues                     |
| ⚙️ Local scope      | `onErrorContinue()` affects only downstream of where it’s applied               |
| 🧩 Compatibility    | Works only with certain operators (especially mapping or transformation stages) |
| 🚫 State safety     | Make sure skipped elements don’t cause inconsistencies downstream               |

---

### 🔹 Comparison Summary

| Operator            | Error Handling Strategy         | Stream Continues? | Fallback Behavior             |
| ------------------- | ------------------------------- | ----------------- | ----------------------------- |
| `onErrorReturn()`   | Replace error with static value | ❌ No              | Emits fallback value          |
| `onErrorResume()`   | Replace with another publisher  | ❌ No              | Emits publisher’s output      |
| `onErrorComplete()` | Suppress and complete silently  | ❌ No              | Completes without error       |
| `onErrorContinue()` | Skip faulty item and proceed    | ✅ Yes             | Skips element, continues flow |

---

### 🔹 Example with Multiple Errors

```java
Flux.just(1, 2, 3, 4, 5, 6)
    .map(i -> {
        if (i == 3 || i == 5)
            return i / 0; // Cause two errors
        return i;
    })
    .onErrorContinue((ex, obj) -> System.out.println("Skipping: " + obj))
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
1
2
Skipping: 3
4
Skipping: 5
6
complete
```

---

### 🔹 When to Use `onErrorContinue()`

| ✅ Use it when…                                          | ❌ Avoid when…                                |
| ------------------------------------------------------- | -------------------------------------------- |
| You process large datasets and want to skip bad records | You must maintain strict data integrity      |
| You need to log and move past transient issues          | You rely on every element being processed    |
| You want fault-tolerant streams                         | The operation must fail-fast for correctness |

---

### 🔹 Code Summary

```java
private static void onErrorContinueExample() {
    Flux.range(1, 10)
        .map(i -> {
            if (i == 5)
                return i / 0;
            return i;
        })
        .onErrorContinue((ex, obj) -> {
            System.out.println("Skipping faulty value: " + obj);
            System.out.println("Cause: " + ex.getMessage());
        })
        .subscribe(Util.subscriber());
}
```

✅ **Output:**

```
1
2
3
4
Skipping faulty value: 5
Cause: / by zero
6
7
8
9
10
complete
```

---

### 🔹 Summary

* `onErrorContinue()` allows a **non-fatal**, **graceful recovery** from item-level errors.
* It’s ideal for **data pipelines**, **stream processing**, and **batch operations**.
* Gives access to both the **error** and **faulty element**.
* Use carefully to avoid silently masking systemic issues.

---

### 🔹 Introduction

So far, we’ve seen how to handle **errors** in a reactive pipeline using:

* `onErrorReturn()` → for hardcoded fallback values
* `onErrorResume()` → for calling another publisher
* `onErrorComplete()` → for silent completion
* `onErrorContinue()` → for skipping faulty items

But what if there’s **no error**, yet your **producer emits nothing**?
That is — the producer completes successfully, but produces **no data**.

This situation is analogous to:

```java
Optional.empty()
```

in traditional Java — the producer didn’t fail, it just didn’t have any value to emit.

---

### 🔹 The Problem

Let’s say your reactive publisher is asked to emit something:

```java
Mono<String> mono = Mono.empty();
```

This means — the producer completed without emitting any value.

If you subscribe to this directly:

```java
mono.subscribe(Util.subscriber());
```

✅ **Output:**

```
complete
```

No error, no value — just a completion signal.

In many cases, this behavior is fine.
But sometimes you might want a **fallback value** when the publisher is empty.
For example:

> “If there’s no data, return a default value like ‘Unknown’.”

---

### 🔹 The Solution: `defaultIfEmpty()`

Reactor provides the operator:

```java
defaultIfEmpty()
```

It allows you to provide a **fallback value** that will be emitted **only if the source completes without emitting anything**.

---

### 🔹 Example 1: Using `Mono.empty()`

```java
Mono<String> mono = Mono.empty()
    .defaultIfEmpty("Fallback Value")
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Fallback Value
complete
```

Here’s what happens:

* The producer (`Mono.empty()`) emits **no value**.
* The `defaultIfEmpty()` operator intercepts this and emits **"Fallback Value"**.
* The subscriber receives this value followed by `complete`.

---

### 🔹 Example 2: Producer with a Value

Now let’s modify the producer:

```java
Mono<String> mono = Mono.just("Hello World")
    .defaultIfEmpty("Fallback Value")
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Hello World
complete
```

Here, because the producer emitted a value, the fallback was **not triggered**.

---

### 🔹 Key Rule

> `defaultIfEmpty()` activates **only when the publisher emits no values at all.**

If **at least one value** is emitted, the fallback **is not used**.

---

### 🔹 Example 3: Using with Flux and Filters

Let’s switch to `Flux` for a better understanding:

```java
Flux<Integer> flux = Flux.range(1, 10)
    .filter(i -> i > 11) // No value satisfies this
    .defaultIfEmpty(50)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
50
complete
```

Explanation:

* `Flux.range(1, 10)` generates numbers 1 to 10.
* The filter condition `i > 11` removes all values.
* So the stream becomes **empty**.
* `defaultIfEmpty(50)` emits the fallback value `50` and completes.

---

### 🔹 Example 4: Conditionally Non-Empty

If you modify the filter slightly:

```java
Flux<Integer> flux = Flux.range(1, 10)
    .filter(i -> i > 9)
    .defaultIfEmpty(50)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
10
complete
```

Explanation:

* The filter passes only `10`.
* Since the stream is not empty, `defaultIfEmpty()` **does not emit the fallback**.

---

### 🔹 Real-World Use Case

Imagine a **user lookup API**:

```java
Mono<User> user = userRepository.findById(userId)
    .defaultIfEmpty(new User("guest", "Guest User"))
    .subscribe(Util.subscriber());
```

✅ Behavior:

* If the user is found → returns actual user.
* If not found → returns the “guest” user as a fallback.

This is conceptually similar to:

```java
Optional.orElse(defaultValue)
```

---

### 🔹 Technical Signature

```java
public final Flux<T> defaultIfEmpty(T defaultValue)
```

It:

* Emits `defaultValue` **only** if the upstream completes without emitting anything.
* Otherwise, it just forwards all emitted elements as-is.

---

### 🔹 Comparison with Error Handling Operators

| Operator            | Trigger Condition | Emits Fallback            | Continues? | Typical Use           |
| ------------------- | ----------------- | ------------------------- | ---------- | --------------------- |
| `onErrorReturn()`   | On error          | ✅                         | ❌          | Handle runtime errors |
| `onErrorResume()`   | On error          | ✅ (via another publisher) | ❌          | Call fallback service |
| `onErrorComplete()` | On error          | ❌                         | ❌          | Suppress errors       |
| `defaultIfEmpty()`  | On empty          | ✅                         | ✅          | Handle empty streams  |

---

### 🔹 Example Summary Code

```java
private static void defaultIfEmptyExample() {
    Flux<Integer> flux = Flux.range(1, 10)
        .filter(i -> i > 11)
        .defaultIfEmpty(50)
        .subscribe(Util.subscriber());
}
```

✅ **Output:**

```
50
complete
```

And if we modify the filter:

```java
.filter(i -> i > 9)
```

✅ **Output:**

```
10
complete
```

---

### 🔹 Key Points to Remember

1. **`defaultIfEmpty()` only triggers when no elements are emitted.**
2. It works with both **Mono** and **Flux**.
3. It provides a simple, declarative way to **handle missing data**.
4. Commonly used as a **“default fallback”** for empty pipelines.
5. Does **not** handle errors — it’s for **empty** streams only.

---

### 🔹 Analogy

Think of `defaultIfEmpty()` like a **backup plan for silence**:

> “If you don’t say anything, I’ll assume you meant this.”

Or in everyday life:

> If the waiter forgets your order, the kitchen sends you a default meal 🍛 — you still get something instead of nothing.

---

### 🔹 Summary

| Concept             | Description                                                                  |
| ------------------- | ---------------------------------------------------------------------------- |
| **Operator**        | `defaultIfEmpty()`                                                           |
| **Purpose**         | Emit a fallback value when the publisher completes without emitting any data |
| **Works on**        | Both `Mono` and `Flux`                                                       |
| **Triggers when**   | Stream is empty (not errored)                                                |
| **Common use case** | Handling “no data found” or “empty response” cases gracefully                |

---
### 🔹 The Problem

With `defaultIfEmpty()`, you can only provide a **hardcoded** fallback value:

```java
Mono.empty()
    .defaultIfEmpty("Fallback Value")
```

But what if you want a **reactive fallback** — for example, calling **another publisher**, **another service**, or even **another database query** when the original source is empty?

That’s where `switchIfEmpty()` comes into play.

---

### 🔹 The Concept

**`switchIfEmpty()`** lets you specify another **Publisher** (Mono or Flux) that should be subscribed to **if the source is empty**.

> Think of it like:
> “If I don’t get data from my main source, switch to a backup source.”

---

### 🔹 Example 1: Using a Fallback Publisher

Let’s start with a simple example using `Flux`.

```java
Flux<Integer> primary = Flux.range(1, 10)
    .filter(i -> i > 10); // No elements satisfy this → Empty Flux

Flux<Integer> fallback = Flux.range(100, 3); // 100, 101, 102

primary
    .switchIfEmpty(fallback)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
100
101
102
complete
```

Here’s what happened:

* The **primary Flux** (`1–10`) became **empty** due to the filter (`i > 10`).
* Since it was empty, `switchIfEmpty()` subscribed to the **fallback Flux**.
* The subscriber then received elements from the fallback source.

---

### 🔹 Example 2: When Primary Emits Values

Now, if we change the filter so the primary stream is **not empty**:

```java
Flux<Integer> primary = Flux.range(1, 10)
    .filter(i -> i > 5); // Emits 6–10

Flux<Integer> fallback = Flux.range(100, 3); // 100, 101, 102

primary
    .switchIfEmpty(fallback)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
6
7
8
9
10
complete
```

This time:

* The **primary Flux** emitted values (6–10).
* Because it wasn’t empty, the **fallback Flux was ignored**.

---

### 🔹 Analogy to Error Handling Operators

| Empty Handling     | Error Handling    | Behavior                      |
| ------------------ | ----------------- | ----------------------------- |
| `defaultIfEmpty()` | `onErrorReturn()` | Emits a static fallback value |
| `switchIfEmpty()`  | `onErrorResume()` | Switches to another publisher |

So, conceptually:

* `defaultIfEmpty()` → *static fallback*
* `switchIfEmpty()` → *dynamic fallback (reactive publisher)*

---

### 🔹 Real-World Use Case: Multi-Tiered Data Retrieval

Consider a common **caching pattern** with **Redis** and **PostgreSQL**.

#### Use Case:

* Check **Redis cache** for data.
* If Redis is **empty (cache miss)**, query the **database**.

```java
Mono<Product> product = redisService.getProductById(productId)
    .switchIfEmpty(databaseService.getProductById(productId))
    .subscribe(Util.subscriber());
```

✅ **Explanation:**

1. The first publisher (`redisService.getProductById`) checks cache.
2. If Redis returns nothing → stream becomes empty.
3. `switchIfEmpty()` switches to `databaseService.getProductById`, ensuring data is retrieved from DB.

---

### 🔹 Example 3: Using with Mono

Let’s see a simple Mono example:

```java
Mono<String> primary = Mono.empty();
Mono<String> fallback = Mono.just("Fallback Data");

primary
    .switchIfEmpty(fallback)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Fallback Data
complete
```

If you replace `Mono.empty()` with `Mono.just("Primary Data")`, you get:

```
Primary Data
complete
```

---

### 🔹 Example 4: Realistic Example with Conditions

```java
Flux<String> mainService = Flux.just("A", "B", "C")
    .filter(val -> val.equals("Z")); // Empty due to filter

Flux<String> backupService = Flux.just("X", "Y", "Z");

mainService
    .switchIfEmpty(backupService)
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
X
Y
Z
complete
```

---

### 🔹 Technical Signature

```java
public final Flux<T> switchIfEmpty(Publisher<? extends T> alternate)
```

**Parameters:**

* `alternate`: Another `Publisher` (Mono or Flux) to subscribe to if the source completes empty.

**Behavior:**

* If the source emits at least one item → `alternate` is ignored.
* If the source is empty → it subscribes to and emits items from `alternate`.

---

### 🔹 Comparison Table

| Operator           | Triggers When   | Fallback Type      | Example                             |
| ------------------ | --------------- | ------------------ | ----------------------------------- |
| `defaultIfEmpty()` | Source is empty | Hardcoded value    | `.defaultIfEmpty(50)`               |
| `switchIfEmpty()`  | Source is empty | Reactive Publisher | `.switchIfEmpty(Flux.just(50, 60))` |
| `onErrorReturn()`  | Source errors   | Hardcoded value    | `.onErrorReturn(-1)`                |
| `onErrorResume()`  | Source errors   | Reactive Publisher | `.onErrorResume(ex -> fallback())`  |

---

### 🔹 Key Takeaways

1. **`switchIfEmpty()`** is the reactive equivalent of `onErrorResume()` — but for *empty* publishers.
2. It allows **switching to another data source** dynamically.
3. It works with both **Mono** and **Flux**.
4. It is often used in **multi-layered data access patterns** (cache → DB → external service).
5. The fallback publisher is **subscribed only if the main source emits nothing**.

---

### 🔹 Example Summary Code

```java
private static void switchIfEmptyExample() {
    Flux<Integer> fallback = Flux.range(100, 3); // 100, 101, 102

    Flux.range(1, 10)
        .filter(i -> i > 10) // No matching values
        .switchIfEmpty(fallback)
        .subscribe(Util.subscriber());
}
```

✅ **Output:**

```
100
101
102
complete
```

And if we change the filter:

```java
.filter(i -> i > 5)
```

✅ **Output:**

```
6
7
8
9
10
complete
```

---

### 🔹 Real-World Analogy

Think of `switchIfEmpty()` as your **backup plan**:

> “If Plan A doesn’t work, go with Plan B.”

Or, in simple terms:

> “If the main service says nothing, call the backup service.”

Just like how:

* `defaultIfEmpty()` gives a default “response message,”
* `switchIfEmpty()` gives you a whole “backup system” to handle the case.

---

### 🔹 Summary

| Concept           | Description                                                       |
| ----------------- | ----------------------------------------------------------------- |
| **Operator**      | `switchIfEmpty()`                                                 |
| **Purpose**       | Switch to another Publisher when the main Publisher emits nothing |
| **Use Case**      | Cache → Database fallback, or any backup data source pattern      |
| **Works On**      | `Mono` and `Flux`                                                 |
| **Similar To**    | `onErrorResume()` but for empty streams                           |
| **Fallback Type** | Another reactive Publisher (Mono/Flux)                            |

---
### 🔹 The Real-World Problem

When you make a network call — to a remote service, database, or cache — most of the time:

* The call succeeds ✅
* You get the response 📨

But sometimes, due to **load**, **network delay**, or **backend slowness**, the response might take too long.

If you don’t set any timeout:

* Your pipeline could **wait indefinitely**
* Your threads could **pile up**, leading to **resource exhaustion**

In reactive programming, we want to remain **non-blocking** but also **bounded** in time.

That’s where the **`timeout()` operator** comes in.

---

### 🔹 The Concept

The `timeout()` operator allows you to **set a maximum wait time** for a reactive stream.

If the producer doesn’t emit a value or terminal signal (complete/error) within the specified duration, the stream **fails with a `TimeoutException`**.

---

### 🔹 Example 1: Simulating a Slow External Service

Let’s start by simulating an external service call:

```java
private static Mono<String> getProductName() {
    return Mono.fromSupplier(() -> {
        System.out.println("Fetching product name from Service A...");
        return Faker.instance().commerce().productName();
    }).delayElement(Duration.ofSeconds(3)); // Simulated delay of 3 seconds
}
```

Now, let’s call this method and subscribe:

```java
getProductName()
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Fetching product name from Service A...
(3 seconds delay)
Received: "Intelligent Wooden Chair"
complete
```

The subscriber receives the product name **after 3 seconds**.

---

### 🔹 Example 2: Adding a Timeout

Now, let’s add a timeout of **1 second**:

```java
getProductName()
    .timeout(Duration.ofSeconds(1))
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Fetching product name from Service A...
Error: java.util.concurrent.TimeoutException
```

Here’s what happened:

* The producer took 3 seconds to emit.
* The timeout operator waited for 1 second.
* After 1 second, since no signal (item/complete/error) was received, it threw a `TimeoutException`.

---

### 🔹 How It Works Internally

The `timeout()` operator starts a **timer** when the subscription begins.

* If the upstream (producer) emits before the timer expires → ✅ stream continues.
* If no emission occurs within the duration → ❌ stream terminates with `TimeoutException`.

In other words, **the producer must emit something before the timeout**.

---

### 🔹 Example 3: Handling Timeout with Fallback (using `onErrorReturn`)

You can handle the timeout just like any other error:

```java
getProductName()
    .timeout(Duration.ofSeconds(1))
    .onErrorReturn("Fallback Product")
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Fetching product name from Service A...
Received: Fallback Product
complete
```

Because the service took longer than 1 second, the timeout triggered a `TimeoutException`.
The `onErrorReturn()` handled it gracefully by returning `"Fallback Product"`.

---

### 🔹 Example 4: When the Service Is Fast Enough

Now, if you reduce the delay to **900 milliseconds**, the timeout won’t trigger:

```java
private static Mono<String> getProductName() {
    return Mono.fromSupplier(() -> {
        System.out.println("Fetching product name...");
        return Faker.instance().commerce().productName();
    }).delayElement(Duration.ofMillis(900)); // Fast enough
}
```

✅ **Output:**

```
Fetching product name...
Received: "Ergonomic Steel Table"
complete
```

Because the producer emitted within the 1-second window, everything worked fine.

---

### 🔹 Example 5: Using a Fallback Publisher (Resilient Design)

Just like `onErrorResume()` allows a dynamic fallback for errors,
`timeout()` can also take a **fallback Publisher** directly.

Let’s define two “services”:

```java
private static Mono<String> serviceA() {
    return Mono.fromSupplier(() -> {
        System.out.println("Calling Service A...");
        return Faker.instance().commerce().productName();
    }).delayElement(Duration.ofSeconds(3));
}

private static Mono<String> serviceB() {
    return Mono.fromSupplier(() -> {
        System.out.println("Calling Fallback Service B...");
        return Faker.instance().commerce().productName();
    }).delayElement(Duration.ofMillis(300));
}
```

Now, use `timeout()` with fallback:

```java
serviceA()
    .timeout(Duration.ofSeconds(1), serviceB())
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Calling Service A...
(1 second passes)
Calling Fallback Service B...
Received: "Handcrafted Plastic Tuna"
complete
```

### 🔍 What Happened

1. `Service A` was too slow (3 seconds delay).
2. After 1 second, timeout triggered.
3. Instead of erroring, it switched to `Service B`.
4. `Service B` responded within 300 ms.
5. The subscriber received the fallback value.

This demonstrates **a graceful degradation pattern** — where slow services automatically failover to faster backups.

---

### 🔹 Example 6: When Primary Is Fast Enough

```java
serviceA()
    .timeout(Duration.ofSeconds(4), serviceB())
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Calling Service A...
Received: "Gorgeous Wooden Mouse"
complete
```

Here:

* Timeout duration = 4 seconds
* Service A delay = 3 seconds
  → The fallback was never called.

---

### 🔹 Behavior Summary

| Scenario                        | Result                                    |
| ------------------------------- | ----------------------------------------- |
| Producer emits within timeout   | Normal flow                               |
| Producer emits after timeout    | `TimeoutException`                        |
| Timeout with fallback Publisher | Switches to fallback                      |
| Timeout with error handler      | Can use `onErrorReturn` / `onErrorResume` |

---

### 🔹 Technical Signature

```java
timeout(Duration timeout)
timeout(Duration timeout, Publisher<? extends T> fallback)
```

* **`timeout(Duration)`** → Throws `TimeoutException` if the upstream delays too long.
* **`timeout(Duration, Publisher)`** → Switches to another publisher instead of erroring.

---

### 🔹 Real-World Use Case

Imagine an e-commerce system where you:

* Call an **inventory service** to get stock count.
* If it doesn’t respond within 500 ms, call a **cache service (Redis)** for approximate data.

```java
inventoryService.getStock(productId)
    .timeout(Duration.ofMillis(500), cacheService.getCachedStock(productId))
    .subscribe(Util.subscriber());
```

This pattern ensures **speed + resilience** — even under backend slowness.

---

### 🔹 Debugging Tip: Observing Subscriptions

You can observe when each publisher starts:

```java
serviceA()
    .doFirst(() -> System.out.println("Subscribed to Service A"))
    .timeout(Duration.ofSeconds(1), 
             serviceB().doFirst(() -> System.out.println("Subscribed to Service B")))
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Subscribed to Service A
Calling Service A...
(1 second delay)
Subscribed to Service B
Calling Fallback Service B...
Received: "Ergonomic Cotton Chair"
complete
```

Notice how **Service B only subscribes after timeout** — meaning no unnecessary work or resource usage happens before it’s actually needed.

---
### 🔹 Summary

| Concept              | Description                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------- |
| **Operator**         | `timeout()`                                                                               |
| **Purpose**          | Set a time limit for upstream emission                                                    |
| **Behavior**         | Throws `TimeoutException` or switches to fallback publisher                               |
| **Works On**         | `Flux` and `Mono`                                                                         |
| **Real-world usage** | Remote service, DB, or cache fallbacks                                                    |
| **Analogy**          | “If the waiter doesn’t bring food in 10 minutes, I’ll order from another restaurant.” 🍽️ |

---

### 🔹 The New Question: What Happens with Multiple Timeouts?

Now, let’s consider a more advanced scenario.

You may not always control all parts of the reactive pipeline — for example:

* A **shared library** defines a publisher and already includes a timeout.
* You, as a consumer, want to **add your own timeout constraint** (e.g., stricter SLA, lower latency limit).

So what happens when you have **multiple `timeout()` operators** in your reactive chain?

---

### 🔹 Example Setup

Let’s simulate this with a model:

```java
private static Mono<String> getProductService() {
    return Mono.fromSupplier(() -> {
        System.out.println("Calling Product Service...");
        return Faker.instance().commerce().productName();
    })
    .delayElement(Duration.ofSeconds(1))
    .timeout(Duration.ofSeconds(1)); // Timeout inside the library or model
}
```

Here:

* The internal library (or service layer) already defines a **1-second timeout**.
* You, as the subscriber, might want to add **your own timeout** externally.

---

### 🔹 Scenario 1: Adding an Outer Timeout (Stricter Constraint)

Now, in your consumer code, you wrap this publisher:

```java
getProductService()
    .timeout(Duration.ofMillis(200)) // Your custom stricter timeout
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Calling Product Service...
Error: java.util.concurrent.TimeoutException
```

### 🔍 Explanation

* The **inner timeout (1s)** belongs to the original producer — but the **outer timeout (200ms)** applies first.
* Since **no signal** (data/complete/error) was received within **200 ms**, the **outer timeout** triggered.
* The stream failed before the inner timeout had a chance to do anything.

So, effectively:

> The **nearest timeout to the subscriber** always wins — because that’s the one controlling your view of the stream.

---

### 🔹 Scenario 2: Handling Outer Timeout Gracefully

You can, of course, handle it as part of normal error handling:

```java
getProductService()
    .timeout(Duration.ofMillis(200))
    .onErrorReturn("Fallback Product")
    .subscribe(Util.subscriber());
```

✅ **Output:**

```
Calling Product Service...
Received: Fallback Product
complete
```

Here, instead of propagating the `TimeoutException`, we handled it gracefully by providing a fallback value.

---

### 🔹 Scenario 3: Increasing Timeout Duration (Less Strict)

What if you do the opposite — you want to **increase** the waiting time externally?

```java
getProductService()
    .timeout(Duration.ofSeconds(5)) // More lenient timeout
    .subscribe(Util.subscriber());
```

You might expect this to give more time, but let’s check what happens.

❌ **Output:**

```
Calling Product Service...
Error: java.util.concurrent.TimeoutException
```

Even though you’re willing to wait **5 seconds**, the **inner producer** (library) already has a **1-second timeout**.

That means:

* The **inner timeout** throws an error after 1 second.
* The **outer timeout** never even comes into play.

---

### 🔍 Explanation

This is an important behavioral rule:

> You can make the timeout **stricter**, but you cannot make it **looser** than the upstream producer’s timeout.

Because:

* Each `timeout()` creates its own **independent control mechanism** for how long it waits for a signal.
* The upstream publisher already has its own **“patience limit.”**
* Once the upstream times out and emits an error, the downstream can’t undo it.

---

### 🔹 Visual Representation

```
Producer Timeout (1s) ──► [ TimeoutException emitted after 1s ]
         │
         ▼
Consumer Timeout (5s)
   ⟶ Will not extend producer's time
```

If you reduce it (e.g., 200ms), the consumer timeout will trigger first:

```
Consumer Timeout (200ms) ──► [ TimeoutException after 200ms ]
         │
         ▼
Producer Timeout (1s) never executes
```

---

### 🔹 Key Takeaways

| Situation                         | Result                       | Explanation                         |
| --------------------------------- | ---------------------------- | ----------------------------------- |
| **Outer timeout < Inner timeout** | Outer timeout triggers first | Stricter constraint dominates       |
| **Outer timeout > Inner timeout** | Inner timeout triggers first | Producer’s limit reached first      |
| **Multiple timeouts**             | Whichever expires first wins | Each timeout operates independently |
| **Adding external timeout**       | Always possible              | Helps customize latency goals       |
| **Extending (increasing)**        | Not effective                | Upstream already stops earlier      |

---

### 🔹 Example with Logging for Clarity

Let’s log both timeouts to observe the timing.

```java
private static Mono<String> getProductService() {
    return Mono.fromSupplier(() -> {
        System.out.println("[Service] Starting call...");
        return Faker.instance().commerce().productName();
    })
    .delayElement(Duration.ofSeconds(1))
    .doFirst(() -> System.out.println("[Service] Subscribed"))
    .timeout(Duration.ofSeconds(1)); // Internal library timeout
}

public static void main(String[] args) {
    getProductService()
        .doFirst(() -> System.out.println("[Consumer] Subscribed"))
        .timeout(Duration.ofMillis(200)) // Consumer’s timeout
        .onErrorReturn("[Consumer] Fallback Product")
        .subscribe(Util.subscriber());
}
```

✅ **Output:**

```
[Consumer] Subscribed
[Service] Subscribed
[Service] Starting call...
Error: java.util.concurrent.TimeoutException
[Consumer] Fallback Product
complete
```

You can see:

* Both timeouts are active.
* The **outer 200 ms** timeout fired first, stopping the stream.

---

### 🔹 Real-World Analogy

Think of **nested timeouts** like this:

🕐 **Inner timeout (Service Level):**

> “I won’t wait more than 1 second to fetch the data from the database.”

🕑 **Outer timeout (Consumer Level):**

> “I won’t wait more than 200 milliseconds for *you* to respond.”

So even if the service is willing to wait longer, the consumer might cut it short due to stricter latency requirements.

But if the service fails earlier (say at 1s), the consumer can’t override it to wait longer — because the service has already *given up*.

---

### 🔹 Practical Recommendation

| Recommendation                                                 | Why                                               |
| -------------------------------------------------------------- | ------------------------------------------------- |
| ✅ Set timeouts close to the producer for service-level control | Prevents internal overloads                       |
| ✅ Add external timeout if you need stricter client-side SLAs   | Avoids waiting too long downstream                |
| ⚠️ Don’t rely on downstream timeout to “extend” upstream       | Inner timeout will fire first                     |
| ✅ Always combine with fallback or error handling               | To handle slow or unavailable services gracefully |

---

### 🔹 Summary

| Concept               | Description                                                                                |
| --------------------- | ------------------------------------------------------------------------------------------ |
| **Operator**          | `timeout(Duration)` or `timeout(Duration, Publisher)`                                      |
| **Purpose**           | Define max waiting time for emission                                                       |
| **Multiple Timeouts** | Independent — whichever expires first wins                                                 |
| **Behavior**          | You can make timeout stricter but not looser                                               |
| **Best Practice**     | Combine producer-level timeout (service) and consumer-level timeout (client) carefully     |
| **Common Use Case**   | Client SLAs, service rate-limiting, or nested microservices with different latency budgets |

---

##  The `transform()` Operator — Building Reusable Pipeline Components

### 🔹 Introduction

In **object-oriented programming**, everything revolves around **objects** interacting with each other.
Similarly, in **reactive programming**, everything revolves around **publishers and subscribers** — data flows through a **pipeline** composed of multiple **operators**.

As you build complex business workflows such as:

* Order creation
* Order modification
* Order cancellation
* Refund processing
* Order fulfillment

you’ll start noticing something interesting:

> Certain sets of operations or transformations repeat across multiple reactive pipelines.

For example:

* You might have a **validation step** common to multiple workflows.
* Or maybe a **debugging/logging step** you use across many `Flux` or `Mono` chains.

Copy-pasting these steps everywhere is not only redundant but also hard to maintain.

So, how can we **reuse** such repeated pieces of logic **across different pipelines**?

That’s where the **`transform()` operator** comes in.

---

### 🔹 The Problem: Repeated Operators Across Pipelines

Let’s imagine you have two different publishers:

1. **Customers** coming from a customer service
2. **Purchase orders** coming from an order service

Example:

```java
private static Flux<Customer> getCustomers() {
    return Flux.range(1, 3)
            .map(i -> new Customer(i, Faker.instance().name().firstName()));
}

private static Flux<PurchaseOrder> getPurchaseOrders() {
    return Flux.range(1, 5)
            .map(i -> new PurchaseOrder(Faker.instance().commerce().productName(), i * 10));
}
```

Now, in multiple places you might do something like this:

```java
getCustomers()
    .doOnNext(System.out::println)
    .doOnComplete(() -> System.out.println("Customer stream completed"))
    .subscribe();

getPurchaseOrders()
    .doOnNext(System.out::println)
    .doOnComplete(() -> System.out.println("Purchase order stream completed"))
    .subscribe();
```

✅ It works perfectly — but notice something:

> The same `doOnNext` and `doOnComplete` logging steps are repeated.

If you have 10 such pipelines, this code duplication can easily grow — and maintaining it becomes painful.

---

### 🔹 The Solution: The `transform()` Operator

The `transform()` operator allows you to **encapsulate a set of operators** into a reusable function and **inject it** anywhere in your pipeline.

Conceptually:

> You define a “pipeline function” that takes a `Flux` (or `Mono`) and returns a transformed `Flux` (or `Mono`).

You can then reuse this function anywhere you like — just like a **custom operator**.

---

### 🔹 Example 1: Creating a Reusable “Debug” Transformation

Let’s create a reusable debug function:

```java
private static <T> UnaryOperator<Flux<T>> addDebugger() {
    return flux -> flux
            .doOnNext(item -> System.out.println("Received: " + item))
            .doOnComplete(() -> System.out.println("Stream completed"))
            .doFirst(() -> System.out.println("Subscribed to stream"));
}
```

Then you can use it like this:

```java
getCustomers()
    .transform(addDebugger())  // Applying our reusable logic
    .subscribe();
```

✅ **Output:**

```
Subscribed to stream
Received: Customer[id=1, name=Alice]
Received: Customer[id=2, name=Bob]
Received: Customer[id=3, name=Charlie]
Stream completed
```

---

### 🔹 Example 2: Applying the Same Transform to Multiple Streams

Now you can use the same `addDebugger()` anywhere:

```java
getCustomers().transform(addDebugger()).subscribe();
getPurchaseOrders().transform(addDebugger()).subscribe();
```

✅ **Output (simplified):**

```
Subscribed to stream
Received: Customer[id=1, name=Alice]
...
Stream completed

Subscribed to stream
Received: PurchaseOrder[productName=Table, quantity=10]
...
Stream completed
```

See how clean and reusable this is?
You no longer have to repeat `doOnNext` and `doOnComplete` in every pipeline.

---

### 🔹 How It Works Internally

The **`transform()`** operator accepts a **Function**:

```java
public final <V> Flux<V> transform(Function<? super Flux<T>, ? extends Publisher<V>> transformer)
```

In simpler terms:

* You get the **current Flux** as input.
* You return a **transformed Publisher** (usually another Flux).
* Reactor automatically replaces the current pipeline with the returned one.

This makes it easy to inject **custom reusable logic** anywhere in the reactive flow.

---

### 🔹 Example 3: Conditional Transformations at Runtime

Let’s make it even more dynamic.
Sometimes, you might want to **conditionally** apply a transformation — for example, based on a configuration flag.

```java
boolean debugEnabled = true; // Could be a system property or environment flag

UnaryOperator<Flux<Customer>> debugOrPassThrough =
        debugEnabled ? addDebugger() : Function.identity();
```

Now use it:

```java
getCustomers()
    .transform(debugOrPassThrough)
    .subscribe();
```

✅ If `debugEnabled = true`, the logs will print.
✅ If `debugEnabled = false`, it behaves as a plain stream — no extra operations.

**Explanation:**

* `Function.identity()` simply returns the input as-is.
* So, you can toggle debugging dynamically **without modifying the original code**.

---

### 🔹 Real-World Use Case

Let’s imagine a **microservices-based e-commerce system**.

You may have multiple reactive pipelines such as:

* Order creation
* Order modification
* Order refund
* Inventory updates

Each pipeline may include **common steps** like:

* Logging the request
* Validating the payload
* Auditing
* Metrics collection

Instead of repeating these steps everywhere, you can define reusable transformers:

```java
private static <T> UnaryOperator<Flux<T>> addAuditInfo() {
    return flux -> flux
            .doOnNext(item -> System.out.println("[AUDIT] Processed item: " + item))
            .doFinally(signal -> System.out.println("[AUDIT] Pipeline finished with: " + signal));
}
```

Now simply plug it into any reactive flow:

```java
orderService.createOrder()
    .transform(addAuditInfo())
    .subscribe();
```

This creates clean, modular, and maintainable reactive code.

---

### 🔹 Example 4: Combining Multiple Transformations

You can also **chain multiple reusable transformations** for layered behaviors:

```java
getCustomers()
    .transform(addDebugger())
    .transform(addAuditInfo())
    .subscribe();
```

✅ Output:

```
[AUDIT] Subscribed
Subscribed to stream
Received: Customer[id=1, name=Alice]
[AUDIT] Processed item: Customer[id=1, name=Alice]
...
Stream completed
[AUDIT] Pipeline finished with: onComplete
```

---

### 🔹 Comparison with `compose()` in Reactor

* **`transform()`** applies the function **once** when the pipeline is assembled.
  It’s a **static transformation**.
* **`compose()`** (used mostly in Flux/Mono extensions) applies the function **for every new subscriber**.
  It’s a **dynamic, per-subscription transformation**.

For most business scenarios, **`transform()`** is exactly what you want — clean, reusable, and efficient.

---

### 🔹 Key Takeaways

| Concept               | Description                                                    |
| --------------------- | -------------------------------------------------------------- |
| **Operator**          | `transform()`                                                  |
| **Purpose**           | Reuse pipeline steps across different reactive flows           |
| **Input**             | Function that accepts a `Flux`/`Mono` and returns another      |
| **Use case**          | Create custom “operators” for logging, validation, audit, etc. |
| **Advantage**         | Reduces redundancy, improves modularity                        |
| **Conditional Usage** | You can apply transformations dynamically using flags          |
| **Similar To**        | `compose()` but `transform()` applies once at assembly time    |

---

### 🔹 Summary

| Feature               | Example                                                                                |
| --------------------- | -------------------------------------------------------------------------------------- |
| **Reusable function** | `private static <T> UnaryOperator<Flux<T>> addDebugger()`                              |
| **Usage**             | `.transform(addDebugger())`                                                            |
| **Dynamic inclusion** | `debugEnabled ? addDebugger() : Function.identity()`                                   |
| **Result**            | Cleaner, modular, and reusable reactive pipelines                                      |
| **Analogy**           | Like defining a *utility function* for recurring business logic in a reactive workflow |

---

### 🔹 Real-World Analogy

Think of `transform()` as a **plug-in slot** for your reactive pipeline:

> “I have a stream of data — I want to apply this reusable transformation before sending it to the subscriber.”

Just like how in OOP you might create a *utility method* or *helper class* for reusable behavior,
in reactive programming you create a **transformer function** and plug it in using `transform()`.

---








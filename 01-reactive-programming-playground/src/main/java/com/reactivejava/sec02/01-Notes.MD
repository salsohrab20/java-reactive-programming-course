# Working with `Mono.just()`

### 2. Creating a Mono – Factory Methods

Since many projects already have **non-reactive code and libraries**, Reactor provides **factory methods** to quickly create a `Mono`.

* The most common one is **`Mono.just(T value)`**.
* 
```java
Mono<String> mono = Mono.just("Wins");
```

* Here, `"Wins"` is wrapped into a `Mono<String>`.
* When subscribed, this Mono emits `"Wins"` and then completes.

---

### 3. Behavior of `Mono.just()`

* **Printing directly**:

    * If you do `System.out.println(mono)`, it won’t print the value.
    * Instead, it prints something like `MonoJust`, because the value is only available when subscribed.
* **Subscribing**:

  ```java
  mono.subscribe(value -> System.out.println("Received: " + value));
  ```

    * Output:

      ```
      Received: Wins
      ```
* **Subscriber with backpressure**:

    * If you attach a custom `Subscriber`, you still need to explicitly **request** items:

      ```java
      subscription.request(10);  // Mono will emit only 1 item + complete
      ```
    * Even if you request more, Mono only emits **one item** (if available).

---

### 4. Canceling the Subscription

* If you call `subscription.cancel()` **before requesting**, the Mono will never emit the value.
* If you cancel **after emission**, nothing further happens because Mono has already completed.

---

### 5. Why Do We Need `Mono.just()`?

The **real power** of `Mono.just()` comes when integrating with **existing libraries**:

* In Reactive programming, *everything is a Publisher*.
* Sometimes you have a value **already in memory**, but the API you’re working with expects a `Publisher`.
* Example:

```java
// A library method expects a Publisher<String>
public void saveRecord(Publisher<String> recordPublisher) {
    // ...
}

// You already have the value in memory
String value = "Wins";

// Direct value won't work
// saveRecord(value); ❌

// Wrap it as a Mono
saveRecord(Mono.just(value)); ✅
```
---

# Exploring `Mono.subscribe()`

### 1. Setting Up

```java
Mono<Integer> mono = Mono.just(1);
```

* This `Mono` will emit the value `1` and then complete.

---

### 2. Subscribing to a Mono

#### a) Simplest form – Consumer for `onNext`

```java
mono.subscribe(value -> log.info("Received: {}", value));
```

* Output:

  ```
  Received: 1
  ```
* **Question**: Where is the `onComplete` message?

    * Not printed, because we only provided an `onNext` consumer.
    * The complete signal is still sent, but we didn’t attach a handler for it.

---

#### b) Adding error and completion handlers

```java
mono.subscribe(
    value -> log.info("Received: {}", value),          // onNext
    error -> log.error("Error: {}", error.getMessage()), // onError
    () -> log.info("Completed")                        // onComplete
);
```

* Output:

  ```
  Received: 1
  Completed
  ```

* If the Mono fails, the `onError` consumer will handle the exception.

---

#### c) Automatic request handling

* **Observation**: We didn’t call `subscription.request(n)` but still received the value.
* **Why?**

    * Reactor automatically requests `Long.MAX_VALUE` on behalf of the subscriber **when you use these overloaded `subscribe()` methods**.
    * This means: “Give me everything you can emit.”
* So, in this form, we don’t need manual request management.

---

#### d) Controlling the subscription manually

If we want the `Subscription` object:

```java
mono.subscribe(
    value -> log.info("Received: {}", value),
    error -> log.error("Error: {}", error.getMessage()),
    () -> log.info("Completed"),
    subscription -> {
        log.info("Subscribed, but cancelling immediately...");
        subscription.cancel(); // No value emitted
    }
);
```

* If we instead do:

```java
subscription.request(1);
```

* Output:

  ```
  Received: 1
  Completed
  ```

---

### 3. Handling Errors – Example with Transformation

* You can transform the value using operators like `map()`.
* Example – simulate division by zero:

```java
Mono<Integer> faultyMono = Mono.just(1)
    .map(value -> value / 0); // triggers ArithmeticException

faultyMono.subscribe(
    value -> log.info("Received: {}", value),
    error -> log.error("Error occurred: {}", error.getMessage()),
    () -> log.info("Completed")
);
```

* Output:

  ```
  Error occurred: / by zero
  ```

---


# Creating a Reusable Generic Subscriber

### 1. Motivation
* For every demo, we will need to **subscribe** repeatedly.
* Writing full subscriber implementations each time will slow us down.
* Solution: Create a **generic, reusable subscriber** that can be used across all demos.

---

### 3. Step 2 – Generic Default Subscriber

* Copy your earlier `SubscriberImpl` and refactor it into a **generic subscriber**:

```java
package calmwinds.guru.common;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class DefaultSubscriber<T> implements Subscriber<T> {

    private final String name;

    public DefaultSubscriber(String name) {
        this.name = name;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        // Automatically request everything (like Reactor does internally)
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T item) {
        System.out.println(name + " received: " + item);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println(name + " received error: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println(name + " received: Completed");
    }
}
```

#### Key points:

* Made **generic**: `DefaultSubscriber<T>` can handle `String`, `Integer`, `Double`, or any type.
* Added a **name field**: helps differentiate multiple subscribers when subscribed to the same publisher.
* Uses `subscription.request(Long.MAX_VALUE)` automatically so we don’t need to call `request()` manually in every demo.

---

### 4. Step 3 – Utility Class

To avoid creating `new DefaultSubscriber(...)` manually every time, create a **utility class**:

```java
package calmwinds.guru.common;

public class Util {

    public static <T> DefaultSubscriber<T> subscriber() {
        return new DefaultSubscriber<>("");
    }

    public static <T> DefaultSubscriber<T> subscriber(String name) {
        return new DefaultSubscriber<>(name);
    }
}
```

---

### 5. Step 4 – Testing the Setup

Create a quick test class with a `Mono` publisher:

```java
package calmwinds.guru.section02;

import calmwinds.guru.common.Util;
import reactor.core.publisher.Mono;

public class Lecture04TestSubscriber {

    public static void main(String[] args) {

        Mono<String> mono = Mono.just("Hello Reactive World");

        // Using default subscriber (no name)
        mono.subscribe(Util.subscriber());

        // Using subscribers with names
        mono.subscribe(Util.subscriber("Subscriber 1"));
        mono.subscribe(Util.subscriber("Subscriber 2"));
    }
}
```

#### Expected Output:

```
 received: Hello Reactive World
 received: Completed

Subscriber 1 received: Hello Reactive World
Subscriber 1 received: Completed

Subscriber 2 received: Hello Reactive World
Subscriber 2 received: Completed
```

---

### 6. Why This Matters

* Saves time: no need to reimplement `Subscriber` for every demo.
* Helps readability: just call `Util.subscriber("name")`.
* Useful for **multi-subscriber scenarios**:

    * When multiple subscribers attach to the same publisher, you can clearly see **who received what**.

---

# Project Reactor – Default OnErrorDropped Issue

### 1. Setup

```java
Mono<String> mono = Mono.error(new RuntimeException("Something went wrong"));

mono.subscribe(name -> System.out.println("Received: " + name));
```

* Here, we only provided the **onNext handler** (`Consumer<String>`).
* No error handler (`Consumer<Throwable>`) or completion handler.

---

### 2. What Happens?

* Since the publisher (`Mono.error(...)`) immediately emits an **error**, and we **did not provide an error handler**:
* Reactor logs:

  ```
  Operator called default onErrorDropped
  ```
* This means:

  * The error signal was generated.
  * But since there was **no error consumer**, Reactor had nowhere to deliver it.
  * So it **dropped the error** and printed this diagnostic message.

---

### 3. Why Reactor Does This

* Reactor is strict about the **Reactive Streams specification**:

  * Every signal (`onNext`, `onError`, `onComplete`) must either be handled or explicitly ignored.
* If you don’t provide an `onError` handler:

  * Reactor assumes you don’t care.
  * But to avoid silently losing errors, it logs the warning.

---

### 4. The Fix

Provide an error handler in your subscription:

```java
mono.subscribe(
    name -> System.out.println("Received: " + name),   // onNext
    error -> System.err.println("Error: " + error.getMessage()) // onError
);
```

#### Output:

```
Error: Something went wrong
```

---

### 5. Key Takeaways

* If a publisher may emit an error, you must **handle it explicitly**.
* `subscribe()` has **multiple overloads**:

  * `subscribe(onNext)`
  * `subscribe(onNext, onError)`
  * `subscribe(onNext, onError, onComplete)`
  * `subscribe(onNext, onError, onComplete, onSubscribe)`
* Use the right overload depending on what signals you care about.

---

✅ **Summary:**
If you see `Operator called default onErrorDropped`, it just means **you subscribed without providing an error handler, and the publisher emitted an error**. Add an error consumer, and the warning disappears.

---


# 📘 Reactive Programming – Laziness and `Mono.fromSupplier`

## Core Concept

In **reactive programming**, we should strive to be as **lazy** as possible:

* **Do work only when required.**
* **Delay execution** until a subscriber explicitly asks for the result.
* Avoid **eager computation** that wastes CPU cycles if no one consumes the result.

---

## Problem Statement

Let’s say we have a compute-intensive operation like summing a list of integers.

```java
private static int sum(List<Integer> numbers) {
    System.out.println("Finding the sum of " + numbers);
    return numbers.stream()
                  .mapToInt(Integer::intValue)
                  .sum();
}
```

Now, consider:

```java
List<Integer> numbers = List.of(1, 2, 3);
Mono<Integer> mono = Mono.just(sum(numbers));
```

### What happens here?

* The **sum is computed immediately** when creating the `Mono`, even if there’s no subscriber.
* This wastes CPU cycles if the subscriber never actually requests the value.

---

## Solution: `Mono.fromSupplier`

Instead of computing eagerly with `Mono.just()`, we can use **`Mono.fromSupplier()`**:

```java
List<Integer> numbers = List.of(1, 2, 3);

Mono<Integer> mono = Mono.fromSupplier(() -> sum(numbers));
```

### Why is this better?

* **Lazy execution** → `sum()` is not executed at creation time.
* **Execution happens only when a subscriber subscribes.**

---

## Execution Flow

1. `Mono.just(value)`

  * Executes **immediately**.
  * Good when the value is already in memory.
  * Example: `Mono.just("Hello World")`.

2. `Mono.fromSupplier(Supplier<T>)`

  * Executes **only when subscribed to**.
  * Perfect for **expensive computations** or **deferred logic**.

---

## Demo Flow

1. **Without subscriber**:

   ```java
   Mono<Integer> mono = Mono.fromSupplier(() -> sum(numbers));
   // No subscriber → no execution
   ```

2. **With subscriber**:

   ```java
   mono.subscribe(result -> System.out.println("Result: " + result));
   // sum() executes here → "Finding the sum of [1, 2, 3]" + Result printed
   ```

---

## Key Takeaways

* **Use `Mono.just()`** when the value is **already available** in memory.
* **Use `Mono.fromSupplier()`** when the value requires **computation** or should be deferred until a subscription.
* Laziness is **crucial** in reactive programming for efficiency and scalability.

---

## Interview-Style Questions & Answers

**Q1.** What is the difference between `Mono.just()` and `Mono.fromSupplier()`?
**A1.**

* `Mono.just()` → Eager execution, computes immediately.
* `Mono.fromSupplier()` → Lazy execution, computes only on subscription.

---

**Q2.** Why is laziness important in reactive programming?
**A2.**
Because reactive systems deal with potentially large-scale data and asynchronous flows. Laziness ensures CPU, memory, and IO resources are consumed **only when needed** and **only if a subscriber requests data**.

---

**Q3.** Can you give a real-world example of using `Mono.fromSupplier()`?
**A3.**
Fetching a record from a database or performing a complex calculation that you don’t want to execute unless the downstream pipeline actually requires it.

---

**Q4.** What happens if you create a `Mono` with `fromSupplier()` but never subscribe?
**A4.**
Nothing happens. The supplier is never invoked. Reactive streams **require a subscriber** to trigger execution.

---


# 📘- `Mono.fromSupplier()` vs `Mono.fromCallable()`

## 1. Core Idea

Project Reactor supports creating a `Mono` from two functional types:

* **`Mono.fromSupplier(Supplier<T>)`**
* **`Mono.fromCallable(Callable<T>)`**

Both:

* Delay execution until **subscription** (i.e., they are lazy).
* Wrap logic that produces a single value.

But there is a **key difference** between them.

---

## 2. Supplier vs Callable in Java

### `Supplier<T>`

* Introduced in **Java 8** (functional interfaces package).
* Method signature:

  ```java
  T get();
  ```
* Does **not declare any checked exception** in its signature.
* Can only throw **unchecked exceptions** (`RuntimeException`).

---

### `Callable<T>`

* Introduced in **Java 5** (for `ExecutorService` task submission).
* Method signature:

  ```java
  T call() throws Exception;
  ```
* Declares `throws Exception` → can throw **checked exceptions**.
* Often used for wrapping tasks submitted to threads.

---

## 3. Example – Using `Mono.fromSupplier`

```java
Mono<Integer> mono = Mono.fromSupplier(() -> sum(numbers));
```

✅ Works fine if `sum()` does not throw checked exceptions.
❌ If `sum()` declares a checked exception, compiler forces you to handle it:

```java
private static int sum(List<Integer> numbers) throws Exception {
    // ...
}

Mono<Integer> mono = Mono.fromSupplier(() -> sum(numbers)); 
// ❌ Compilation error → must handle checked exception
```

To fix:

* Wrap inside `try-catch`, or
* Convert to `RuntimeException`.

---

## 4. Example – Using `Mono.fromCallable`

```java
Mono<Integer> mono = Mono.fromCallable(() -> sum(numbers));
```

✅ Works even if `sum()` declares `throws Exception`.
Because `Callable.call()` itself declares `throws Exception`.

So if you have methods with **checked exceptions**, `Mono.fromCallable` is the better choice.

---

## 5. Summary Table

| Aspect                  | Supplier (`Mono.fromSupplier`) | Callable (`Mono.fromCallable`)          |
| ----------------------- | ------------------------------ | --------------------------------------- |
| Introduced in           | Java 8                         | Java 5                                  |
| Method signature        | `T get()`                      | `T call() throws Exception`             |
| Checked exceptions      | ❌ Not allowed (must wrap)      | ✅ Allowed                               |
| Common usage            | Simple, no checked exceptions  | Tasks that may throw checked exceptions |
| Laziness (Mono context) | ✅ Executes only on subscribe   | ✅ Executes only on subscribe            |

---

## 6. Demo Flow

```java
public class Lecture06MonoFromCallable {
    public static void main(String[] args) {
        List<Integer> numbers = List.of(1, 2, 3);

        // Supplier example (no checked exception)
        Mono<Integer> supplierMono = Mono.fromSupplier(() -> numbers.stream().mapToInt(i -> i).sum());

        // Callable example (checked exception allowed)
        Mono<Integer> callableMono = Mono.fromCallable(() -> sum(numbers));

        supplierMono.subscribe(result -> System.out.println("Supplier Result: " + result));
        callableMono.subscribe(result -> System.out.println("Callable Result: " + result));
    }

    private static int sum(List<Integer> numbers) throws Exception {
        System.out.println("Computing sum of: " + numbers);
        if (numbers.isEmpty()) throw new Exception("Empty list!");
        return numbers.stream().mapToInt(Integer::intValue).sum();
    }
}
```

---

## 7. Interview-Style Q\&A

**Q1.** When should you use `Mono.fromSupplier` vs `Mono.fromCallable`?
**A1.** Use `fromSupplier` when no checked exceptions are involved. Use `fromCallable` when the logic may throw checked exceptions.

---

**Q2.** Why does `Supplier` not allow checked exceptions?
**A2.** Because `Supplier.get()` is designed as a simple functional interface in Java 8’s streams/functional APIs. It keeps the functional style clean. Checked exceptions must be explicitly handled.

---

**Q3.** Can both `fromSupplier` and `fromCallable` defer execution until subscription?
**A3.** Yes. Both are lazy and will execute only when a subscriber is attached.

---

**Q4.** In a multithreaded context, which one would be more naturally compatible with `ExecutorService`?
**A4.** `Callable`, since it was designed for that purpose. `Supplier` is more for functional pipelines and lambda-based data access.

---


# 📘 - `Mono.fromRunnable()`

## 1. Background

In Project Reactor, we have multiple factory methods for creating a `Mono`.
Not every factory method will be useful in every project — think of it like a **buffet**:
you don’t have to eat everything, only pick what fits your needs.

We’ve already seen:

* `Mono.just(value)` → Eager, emit existing value in memory.
* `Mono.fromSupplier(supplier)` → Lazy, perform computation only when subscribed.
* `Mono.empty()` → Emit an empty signal immediately.

So where does **`Mono.fromRunnable()`** fit?

---

## 2. When to use `Mono.fromRunnable()`

* It is **similar to `Mono.empty()`**, except it allows you to **invoke a method before completing**.
* It emits **no value** — only an **empty completion signal**.
* Execution is **delayed until a subscriber subscribes**.

👉 Think of it as:
`empty + delayed side-effect`.

---

## 3. Analogy

* `Mono.just()` ↔ Emit **value directly**.
* `Mono.fromSupplier()` ↔ **Invoke method** to compute a value, then emit.
* `Mono.empty()` ↔ Emit **empty signal directly**.
* `Mono.fromRunnable()` ↔ **Invoke method first**, then emit **empty signal**.

---

## 4. Example – E-commerce Product Lookup

### Step 1: Utility with Faker

```java
private static final Faker faker = new Faker();

public static Faker faker() {
    return faker;
}
```

---

### Step 2: Get Product Name

```java
private static Mono<String> getProductName(int productId) {
    if (productId == 1) {
        // Product found → return product name
        return Mono.fromSupplier(() -> faker().commerce().productName());
    } else {
        // Product not found → notify business first, then return empty
        return Mono.fromRunnable(() -> notifyBusiness(productId));
    }
}
```

---

### Step 3: Notify Business

```java
private static void notifyBusiness(int productId) {
    System.out.println("Notifying business: unavailable product with id " + productId);
}
```

---

### Step 4: Test

```java
public static void main(String[] args) {
    getProductName(1).subscribe(Util.subscriber("Case 1"));
    getProductName(2).subscribe(Util.subscriber("Case 2"));
}
```

---

### Output (example)

```
Case 1: onNext -> "Plastic Car"
Case 1: onComplete
Notifying business: unavailable product with id 2
Case 2: onComplete
```

---

## 5. Why not just call `notifyBusiness()` directly?

* If we wrote `notifyBusiness(productId)` directly before `Mono.empty()`, it would execute **immediately**, even if no subscriber is present.
* Using `Mono.fromRunnable()`, the execution is **deferred** until subscription.
* This ensures **true laziness**, same as with `fromSupplier`.

---

## 6. Key Takeaways

* `Mono.fromRunnable()` is useful when:

  * You want to **perform a side effect** (like logging, auditing, or notifying)
  * …and then return **only a completion signal (no data)**.
* If you don’t have such a use case in your project, it’s fine to **ignore it**.
* But it’s important to understand **when and why you might use it**.

---

## 7. Interview-Style Q\&A

**Q1.** What does `Mono.fromRunnable()` emit?
**A1.** It emits only an **empty completion signal** (no value).

---

**Q2.** How is `Mono.fromRunnable()` different from `Mono.empty()`?
**A2.**

* `Mono.empty()` → emits empty immediately.
* `Mono.fromRunnable()` → **invokes a method first** (when subscribed), then emits empty.

---

**Q3.** Why do we need laziness with `Runnable`?
**A3.** To ensure side effects like logging, auditing, or notifications happen **only when someone subscribes**, not when the `Mono` is created.

---

# 📘  Laziness with `Supplier` in Reactor

## 1. Why this lecture?

* Sometimes, students (especially beginners) ask:

  > “You said using `Supplier` we delay execution. But if I just call my method (like `getName()`), isn’t that already executed immediately?”
* Experienced Java developers may find this obvious, but it’s important to **clear the confusion** for everyone.

---

## 2. The Scenario

Let’s say we have this method:

```java
private static String getName() {
    System.out.println("Generating name...");
    return Util.faker().name().firstName();
}
```

Now consider two different usages:

---

### Case 1: Calling `getName()` directly

```java
Mono<String> mono = Mono.fromSupplier(() -> getName());
```

* Here, notice we are **not calling `getName()` immediately**.
* Instead, we are passing a **lambda expression** (a `Supplier`) to Reactor.
* This means:

  * Nothing executes yet.
  * `getName()` will only be called **when someone subscribes**.

---

### Case 2: Calling `getName()` outside

```java
Mono<String> mono = Mono.just(getName());
```

* Here, we are **calling `getName()` immediately**.
* Execution happens at the time of `Mono` creation.
* The generated name is already in memory, and `Mono.just()` simply wraps that value.
* This is **not lazy**.

---

## 3. Key Clarification

* If you write `getName()`, you are **invoking the method immediately**.
* If you write `() -> getName()`, you are **deferring invocation** until later (when subscribed).

This is the **core difference** between:

* **`Mono.just(getName())`** → Eager execution.
* **`Mono.fromSupplier(() -> getName())`** → Lazy execution.

---

## 4. Demo

```java
public static void main(String[] args) {
    // Case 1: eager
    Mono<String> eager = Mono.just(getName()); 
    // Prints: "Generating name..." immediately

    // Case 2: lazy
    Mono<String> lazy = Mono.fromSupplier(() -> getName()); 
    // Nothing prints yet

    lazy.subscribe(name -> System.out.println("Subscriber received: " + name));
    // Now prints: "Generating name..." followed by subscriber output
}
```

---

## 5. Takeaway

* **Direct method call (`getName()`)** → executes immediately.
* **Lambda/supplier (`() -> getName()`)** → delays execution until subscription.
* This is what we mean when we say **“using supplier we are delaying the execution.”**

---

## 6. Interview-Style Q\&A

**Q1.** What is the difference between `Mono.just(getName())` and `Mono.fromSupplier(() -> getName())`?
**A1.** `Mono.just()` executes the method immediately and wraps the result. `Mono.fromSupplier()` defers method execution until a subscriber subscribes.

---

**Q2.** Why is `fromSupplier` preferred for compute-heavy methods?
**A2.** Because it ensures the work is done only when necessary, avoiding wasted CPU cycles if no one subscribes.

---

# 📘 Reactive Programming – `Mono.defer()`

## 1. Background

* In **reactive programming**, creating a `Mono` is usually just creating an object — a **lightweight operation**.
* The actual **business logic** (time-consuming computation, IO, etc.) is usually inside a `Supplier`, `Callable`, `Future`, etc.
* In most cases:

  * Publisher creation = lightweight
  * Business logic execution = heavyweight (delayed until subscription)

But… sometimes **even publisher creation itself can be expensive**.
👉 In such cases, you may want to delay **publisher creation** until **someone subscribes**.
That’s where `Mono.defer()` comes in.

---

## 2. Without `defer`

Let’s simulate an expensive publisher creation:

```java
private static Mono<Integer> createPublisher() {
    System.out.println("Creating publisher...");
    Util.sleepSeconds(1);  // simulate expensive setup

    return Mono.fromSupplier(() -> {
        Util.sleepSeconds(3);  // simulate heavy business logic
        return 42;
    });
}
```

### Case 1: Normal creation

```java
Mono<Integer> mono = createPublisher();
```

* As soon as `createPublisher()` is called:

  * `"Creating publisher..."` is printed
  * 1 second is wasted (even with no subscriber)

If we never subscribe, we still **paid the cost of publisher creation** unnecessarily.

---

## 3. With `Mono.defer()`

We can **wrap publisher creation in a supplier**:

```java
Mono<Integer> mono = Mono.defer(() -> createPublisher());
```

Now:

* No publisher is created upfront.
* `createPublisher()` runs **only when a subscriber subscribes**.
* If no one subscribes → no wasted work.

---

## 4. Demo Code

```java
public class Lecture08MonoDefer {
    public static void main(String[] args) {
        // Lazy publisher creation
        Mono<Integer> mono = Mono.defer(() -> createPublisher());

        System.out.println("Before subscription...");

        // Uncomment to trigger execution
        mono.subscribe(Util.subscriber("sub1"));
    }

    private static Mono<Integer> createPublisher() {
        System.out.println("Creating publisher...");
        Util.sleepSeconds(1); // expensive setup

        return Mono.fromSupplier(() -> {
            System.out.println("Executing business logic...");
            Util.sleepSeconds(3); // heavy computation
            return 42;
        });
    }
}
```

---

## 5. Output (with subscription)

```
Before subscription...
Creating publisher...
Executing business logic...
sub1 received: 42
sub1 completed
```

## 6. Output (without subscription)

```
Before subscription...
# Nothing else prints, no time wasted
```

---

## 7. Key Takeaways

* `Mono.defer(Supplier<Mono<T>>)`

  * Delays **publisher creation** until subscription.
* Use it when:

  * Creating the publisher itself is expensive.
  * You want **fresh publisher instances** per subscription (instead of reusing a pre-created one).
* In most cases, publisher creation is lightweight and you don’t need `defer`.
* Think of `defer` as the **“ultimate laziness” tool** in Reactor.

---

## 8. Interview-Style Q\&A

**Q1.** What is the difference between `Mono.fromSupplier()` and `Mono.defer()`?
**A1.**

* `fromSupplier()` delays **business logic execution** but the `Mono` itself is already created.
* `defer()` delays the **entire publisher creation**, not just the business logic.

---

**Q2.** Why might publisher creation itself be expensive?
**A2.** Examples:

* Setting up a database connection.
* Preparing large in-memory objects.
* Starting an external service client.

---

**Q3.** Does `Mono.defer()` create a new publisher for each subscription?
**A3.** Yes. Each subscriber gets a **fresh publisher** created via the supplier.

---

# 📘 Wrapping Up Mono Factory Methods & Moving Toward Reactive IO

## 1. Recap of Mono Factory Methods

So far, we have seen various **factory methods** that `Mono` provides to create publishers quickly:

* **`Mono.just(value)`** → emit a value already in memory (no computation).
* **`Mono.empty()`** → emit an empty/complete signal directly.
* **`Mono.error(Throwable)`** → emit an error signal.
* **`Mono.fromSupplier(...)`** / **`Mono.fromCallable(...)`** → delay execution until subscription.
* **`Mono.fromFuture(...)`** → adapt a `CompletableFuture` into a `Mono`.
* **`Mono.fromRunnable(...)`** → invoke a method for side effects, then emit empty.
* **`Mono.defer(...)`** → delay even the publisher creation itself.

These methods are extremely handy when:

* You already have a **value, supplier, callable, runnable, or future**.
* You want to **quickly create a reactive publisher** that some library or framework can subscribe to.

👉 In this setup: **you become the publisher** and the **framework becomes the subscriber**.

---

## 2. But What About External Data?

Up to now, we’ve mostly simulated data with **Faker** or static methods.
But in real-world applications, we often need data from **external systems** like:

* Remote **HTTP services** (APIs).
* **Databases** (Postgres, MySQL, Mongo, Redis, etc.).
* **Messaging systems** (Kafka, RabbitMQ, etc.).
* **Search engines** (Elasticsearch, etc.).

So the natural question is:

> Can we just use `Mono.fromSupplier()` and make a blocking HTTP or DB call inside it?

❌ **Answer: No.**

* `Supplier`, `Callable`, etc. are **blocking & synchronous**.
* Reactive programming is all about **non-blocking IO**.
* If you block inside `fromSupplier`, you defeat the purpose of reactive.

---

## 3. The Solution: Reactive IO Clients

For true non-blocking communication, we need **reactive drivers and clients**:

* **HTTP** → Spring WebFlux (`WebClient`), Reactor Netty (`HttpClient`).
* **Databases** → R2DBC drivers (Postgres, MySQL, H2, etc.).
* **NoSQL** → Reactive drivers for MongoDB, Redis.
* **Messaging** → Reactive Kafka, RabbitMQ integrations.

These tools allow you to:

* Send/receive requests **asynchronously**.
* Get back a `Mono` or `Flux` **without blocking**.
* Integrate external systems naturally into a reactive pipeline.

---

## 4. The Chicken-and-Egg Problem

* To use **WebFlux** or **R2DBC**, you must already understand **Reactive Programming basics**.
* But at the same time, if we only use `Faker` and static methods in demos, things feel artificial.

👉 That’s why in this course:

* We **first focus on core reactive concepts** (Mono, Flux, backpressure, etc.).
* But to make demos more **realistic and engaging**, we’ll also introduce **Reactor Netty**, which lets us send non-blocking HTTP requests directly.

---

## 5. Next Step: Reactor Netty

* We’ll add a dependency for **Reactor Netty**.
* Use it to perform **non-blocking HTTP calls** in our demos.
* This will keep things practical, without requiring you to jump straight into Spring WebFlux yet.

---

## 6. Takeaway

* Mono factory methods are great for **wrapping existing synchronous constructs**.
* But for **IO operations** (HTTP, DB, messaging), you need proper **reactive clients/drivers**.
* Reactor Netty will be our first step into that world before moving to **Spring WebFlux** and beyond.

---

# 📘 Setup: Local Product Service for Reactive HTTP Calls

## 1. Why this step?

* So far, our demos used **Faker** or static methods.
* To make things more realistic, we now want to call an **actual HTTP service**.
* For this, I’ve prepared a **Spring Boot JAR file** for you to run locally.
* This service will act as a **dummy product microservice** we can experiment with.

---

## 2. Running the JAR

* Download the JAR file I provided earlier.
* Run it using the command I shared (it’s a standard Spring Boot run command).
* Don’t worry — it’s safe. This JAR simply exposes a few HTTP APIs for learning purposes.

By default, the service runs on **port 7070**.
👉 Please keep the default port unless you have conflicts (to avoid confusion in upcoming demos).

---

## 3. Swagger UI

Once the app is running:

* Open your browser and go to:

  ```
  http://localhost:7070
  ```
* You’ll see a **Swagger UI page** listing the available APIs.

---

## 4. The API We’ll Use

In this section, we care about one endpoint:

**`GET /demo01/product/{id}`**

* Accepts a **product ID** (1–100).
* Returns the product name.
* Example:

  ```
  GET http://localhost:7070/demo01/product/15
  ```

  → Response: `"product15"`

### Behavior

* The service **intentionally delays by 1 second** before responding.
* This simulates real-world network latency and shows why **non-blocking communication** matters.
* If you request:

  * `/demo01/product/1` → `"product1"`
  * `/demo01/product/50` → `"product50"`
* Simple mapping: request ID → `"product{id}"`.

---

## 5. Notes on Content-Type

* The response currently uses **text/plain** instead of **application/json**.
* This is intentional for simplicity.
* Proper API design (content-type, structured responses, error handling, etc.) is part of **Spring WebFlux** (covered in the next course).
* For now, our focus is purely on:
  ✅ **Sending HTTP requests non-blocking way**
  ✅ **Receiving responses reactively**

---

## 6. Why is this important?

* We need a **real API** to demonstrate reactive HTTP calls.
* With this service in place, we can now:

  * Send multiple requests concurrently.
  * See how Reactor handles them non-blockingly.
  * Compare with blocking approaches.

---
# 📘 Reactor Netty: calling an external HTTP product service

## Goals

* Create a reusable **abstract HTTP client** using Reactor Netty’s `HttpClient`.
* Override loop resources so we can demonstrate single-threaded event-loop behavior.
* Implement an `ExternalServiceClient` that requests `/demo01/product/{id}` and returns a `Mono<String>` with the product name.
* Show how Reactor Netty returns a `Flux<ByteBuf>` (stream of bytes) and how to convert that into a `Mono<String>`.

---

## 1) Dependency

(You already added this for the course.)
Typical artifact (for reference):

```xml
<dependency>
  <groupId>io.projectreactor.netty</groupId>
  <artifactId>reactor-netty-http</artifactId>
  <version>YOUR_VERSION</version>
</dependency>
```

---

## 2) Why Reactor Netty (low-level) vs Spring WebFlux (higher-level)

* Reactor Netty gives you the raw non-blocking HTTP client (`HttpClient`) used under the hood by WebFlux.
* **Pros:** lightweight, minimal dependencies, clearer view of the reactive primitives (ByteBuf flux, loop threads).
* **Cons:** you work at the byte/flux level (manual conversion, less convenience).
* Use Netty for demos and to understand how non-blocking IO works. Use WebFlux in real apps for convenience.

---

## 3) Abstract HTTP client (reusable base)

Create a base class that:

* holds a `HttpClient` configured with a base URL,
* lets subclasses call `httpClient` to issue requests.

Example:

```java
public abstract class AbstractHttpClient {
    private static final String BASE_URL = "http://localhost:7070";
    protected final reactor.netty.http.client.HttpClient httpClient;

    protected AbstractHttpClient() {
        // Create loop resources with custom configuration (e.g., single worker thread)
        var loopResources = LoopResources.create("my-evt-loop", 1, true);

        this.httpClient = reactor.netty.http.client.HttpClient.create()
            .runOn(loopResources)
            .baseUrl(BASE_URL);
    }
}
```

**Notes:**

* `LoopResources.create(namePrefix, workerCount, daemon)` lets you control how many event-loop threads are used. For demos we often set `workerCount = 1` to show how one thread can handle many concurrent requests non-blockingly.
* `runOn(loopResources)` instructs the `HttpClient` to use those loop resources.
* `baseUrl(...)` lets you provide relative URIs later.

---

## 4) ExternalServiceClient: call product API

Implement the client that extends the abstract client and exposes:

```java
public class ExternalServiceClient extends AbstractHttpClient {

    public Mono<String> getProductName(int productId) {
        // path: /demo01/product/{id}
        return httpClient
            .get()
            .uri(uriSpec -> uriSpec.path("/demo01/product/{id}").addPathParam("id", Integer.toString(productId)).build())
            .responseContent()              // Flux<ByteBuf> (stream of raw bytes)
            .aggregate()                    // optional: aggregate ByteBufs into a single ByteBuf then asString()
            .asString()                     // returns Flux<String> where each element is a decoded string chunk
            .next();                        // take the first element -> Mono<String>
    }
}
```

### Explanation of pipeline

* `.get().uri(...)` — builds the GET request (base URL already set in abstract client).
* `.responseContent()` — returns a `Flux<io.netty.buffer.ByteBuf>`: raw bytes from the response body.
* `.aggregate()` — (optional but handy) merges all incoming byte chunks into one `ByteBuf` (useful for small responses).
* `.asString()` — decodes `ByteBuf` to `String`. The return type after `asString()` is `Flux<String>` (stream of decoded string chunks).
* `.next()` — returns a `Mono<String>` that will emit the first string chunk (suitable when you expect a single response body).

> Alternative: you can also do `.responseContent().asString().single()` or `.collectList().map(list -> String.join("", list))` — but `.next()` or `.single()` are the idiomatic conversions when expecting a single element.

---

## 5) Dealing with errors & timeouts

* Add error handling where appropriate:

```java
.return httpClient.get()...
    .responseContent()
    .aggregate()
    .asString()
    .next()
    .timeout(Duration.ofSeconds(5))                     // avoid hanging waits
    .onErrorResume(t -> Mono.error(new RuntimeException("Remote call failed", t)));
```

* To handle HTTP status codes (e.g., 4xx/5xx), use `.responseSingle((res, bytes) -> { ... })`:

```java
.responseSingle((res, byteBufMono) -> {
    if (res.status().code() >= 400) {
        return byteBufMono.asString()
                           .flatMap(body -> Mono.error(new RuntimeException("Remote error: " + res.status() + " body: " + body)));
    }
    return byteBufMono.asString();
});
```

---

## 6) Demo usage (test in `main`)

Simple demo showing concurrent requests:

```java
public static void main(String[] args) {
    ExternalServiceClient client = new ExternalServiceClient();

    // single call
    client.getProductName(1)
          .subscribe(Util.subscriber("single"));

    // concurrent calls
    Flux.range(1, 10)
        .flatMap(id -> client.getProductName(id))
        .subscribe(Util.subscriber("concurrent"));

    // keep main thread alive for demo (not needed in app server)
    Util.sleepSeconds(3);
}
```

* Because the server intentionally delays responses by 1 second, you can observe behavior for many concurrent requests and how one event loop thread handles them.

---

## 7) Why `.next()`?

* Reactor Netty gives a `Flux<String>` because the response body may stream multiple chunks.
* But when you expect **one logical response** (small JSON/text), `.next()` selects the first emitted item and converts the stream to `Mono<String>` — the simplest choice for single-value APIs.

---

## 8) Summary / teaching points

* **`HttpClient` is low level**: you work with `ByteBuf` fluxes and event loop resources. Helpful to learn how reactive IO layers work.
* **Loop resources** let you control concurrency at the event-loop level; setting `workerCount=1` is a neat demo to show a single thread handling multiple concurrent IO tasks.
* **Convert `Flux<ByteBuf>` → `Mono<String>`** via `.aggregate().asString().next()` (or `responseSingle(...)` for status-aware handling).
* **`Mono` is the right return type** when a single resource is expected; `Flux` when you expect streaming multiple elements.
* For most production work, **use Spring WebFlux** (`WebClient`) which wraps these details and gives convenient body-to-object mapping and status handling.

---

# 📘 Non-blocking I/O in practice (calling product service concurrently)

## Goal

Show how a single event-loop thread with Reactor Netty can send many HTTP requests concurrently (non-blocking), get responses roughly in one second, and handle hundreds of requests without creating hundreds of threads.

---

## Key points / recap from the demo

1. **Non-blocking I/O** means we issue many requests without blocking the main thread for each response.

  * You create publishers (`Mono<String>` for each request) and subscribe.
  * The event loop / Reactor Netty handles I/O asynchronously on one or a few threads.

2. **Main thread exits unless you keep it alive** (for small demos).

  * In the demo we used `Util.sleepSeconds(...)` to give time for async responses. In a real server app you wouldn’t do that.

3. **Concurrent requests complete in parallel** (in wall-clock time) even when initiated in a tight loop:

  * Sending 5, 50, 100 requests all returned within \~1 second (server adds 1s delay) — showing concurrency.
  * Only one thread handled the work (because we configured `LoopResources` with `workerCount = 1`).

4. **Responses may arrive out of order**.

  * Network + remote processing determine arrival order, so response ordering is not guaranteed.
  * If you require ordered results, you must enforce it on the client side (see tips).

---

## Why this works (brief, intuitive explanation)

* Traditional blocking I/O: each request blocks a thread until the response arrives → to get N concurrent requests you need N threads.
* Non-blocking I/O: a single event loop uses non-blocking sockets and callbacks (or epoll/kqueue). Threads don’t block waiting for network; they react to readiness events and handle many connections concurrently.
* Result: one thread can service many requests concurrently; the CPU is not wasted waiting on I/O.

---

## Example runnable demo (structure)

```java
public class Lecture11NonBlockingIO {
    public static void main(String[] args) {
        ExternalServiceClient client = new ExternalServiceClient();

        System.out.println("Starting...");

        // send N requests concurrently and subscribe each
        int N = 100;  // change to 5, 50, 100 as you like
        for (int i = 1; i <= N; i++) {
            final int id = i;
            client.getProductName(id)
                  .subscribe(name -> System.out.println("response for " + id + " -> " + name),
                             err  -> System.err.println("error for " + id + " -> " + err.getMessage()));
        }

        // keep main alive for demo only (server apps don't need this)
        Util.sleepSeconds(3); // enough for 1s server delay + overhead
    }
}
```

**What you’ll observe**

* `Starting...` prints immediately.
* After about 1 second the responses start arriving — often most responses arrive together.
* A single event-loop thread handles the I/O (you can print thread names in your client to verify).

---

## Why responses are out of order

* You dispatched all requests almost simultaneously. The network, server scheduling, serialization, and queuing cause variable latencies.
* Non-blocking I/O does not impose ordering — it processes events as they complete.

---

## If you need ordered results

* Use reactive operators that preserve order:

  * `Flux.range(1, N).concatMap(id -> client.getProductName(id))` — *serializes* requests (one after another), keeps order but loses concurrency.
  * `Flux.range(1, N).flatMapSequential(id -> client.getProductName(id), concurrency)` — runs up to `concurrency` in parallel *but emits results in the original order*.
  * `flatMap` (default) provides maximum concurrency but **does not** preserve order.
* Example (preserve order while parallelizing with concurrency limit 10):

  ```java
  Flux.range(1, 100)
      .flatMapSequential(id -> client.getProductName(id), 10)
      .subscribe(System.out::println);
  ```

---

## Controlling concurrency and resource use

* **Do not fire unlimited requests**; limit concurrency to avoid overwhelming client, server or network:

  * `flatMap(..., concurrency)` or `flatMapSequential(..., concurrency)`.
* Use Reactor operators to **rate limit** or **buffer** (`limitRate`, `onBackpressureBuffer`, `window`).

---

## Production considerations / best practices

1. **Timeouts** — always set timeouts for remote calls: `.timeout(Duration.ofSeconds(…))`.
2. **Retries with backoff** — use `retryBackoff` for transient errors.
3. **Connection pooling and limits** — configure connection pool max connections; don’t create unbounded connections.
4. **Circuit breaker** — protect downstream services (Resilience4j or similar).
5. **Error handling** — translate errors to meaningful business errors; avoid letting exceptions bubble unobserved.
6. **Monitoring** — track request rates, latencies, failures.
7. **Avoid blocking calls** on reactive event-loop threads (no JDBC blocking calls; use R2DBC or offload to dedicated thread pool if unavoidable).
8. **Prefer higher-level clients** in real apps (Spring `WebClient` / WebFlux) for convenience, serialization, and declarative features. Reactor Netty is great for learning and lightweight use-cases.

---

## Small checklist for students to reproduce the demo

* Make sure the product service JAR is running on `localhost:7070`.
* Confirm the endpoint `/demo01/product/{id}` returns `product{id}` after \~1s delay.
* Run the demo class, watch console lines and timestamps.
* Try `N = 5`, `50`, `100` and see the difference.
* Try `flatMapSequential` or `concatMap` to see ordering behavior.

---

## Quick Q\&A (common student questions)

* **Q:** Why did we need `sleep` in `main()`?
  **A:** Demo-only convenience. In a server process (Spring Boot app) the process keeps running; you don’t block to wait for results.
* **Q:** Do we need many threads to make many simultaneous requests?
  **A:** No — with non-blocking I/O a small number of threads (often 1 per CPU or even 1 for demo) can handle many concurrent connections.
* **Q:** If order matters, how do I guarantee it?
  **A:** Use `concatMap` (serial) or `flatMapSequential` (parallel execution, ordered emission), or reorder results downstream by IDs.

---


# 📘 How Non-blocking I/O Works Behind the Scenes (Event Loop)

## 1. Big Picture

* In Reactor Netty (and most reactive frameworks), **one event-loop thread** is enough to manage many concurrent requests.
* This works by combining:

  * **Outbound queue** → tasks to send (HTTP requests).
  * **Inbound queue** → events/responses received (network packets).
* The event loop continuously cycles (loops) to process both queues.

---

## 2. Outbound Flow (sending requests)

1. Suppose you request product names for IDs `1, 2, 3, 4, …, 100`.
2. Each of these requests is placed into the **outbound queue**.
3. The event-loop thread:

  * Picks task `1` → sends HTTP request to remote service.
  * **Does not wait** for the response (no blocking).
  * Immediately picks task `2`, sends it.
  * Continues until all 100 requests are dispatched.
4. Effectively, all requests are “in flight” at nearly the same time.
   👉 Like throwing 100 balls into the air at once.

---

## 3. Inbound Flow (receiving responses)

* Meanwhile, the remote service starts responding (with \~1s delay).
* When a response arrives:

  * The **OS kernel notifies the event loop** (e.g., via epoll/kqueue).
  * The event loop puts the response into the **inbound queue**.
* The event-loop thread processes inbound messages as they arrive:

  * Response for product `2` might come first.
  * Then product `7`.
  * Then product `1`.
  * 👉 **Order is not guaranteed** because it depends on network latency, server scheduling, and routing.

---

## 4. Why only one thread is enough?

* The event loop **never blocks waiting** for a response.
* While one request is “waiting on the wire,” the thread continues sending or processing other tasks.
* CPU cycles are used only when there is actual work (sending/receiving), not wasted sitting idle.
* This is why **one event-loop thread** can handle hundreds or even thousands of concurrent I/O tasks.

---

## 5. Comparison with Blocking I/O

* **Blocking model:**

  * Each request blocks a thread until response arrives.
  * 100 requests → either 100 threads (parallel) or 100 seconds (sequential).
* **Non-blocking model:**

  * 1 thread → all 100 requests sent in \~1 second, responses handled as they arrive.
  * Far more scalable and resource efficient.

---

## 6. Key Analogy

* Imagine a **single chef in a kitchen**:

  * They put 100 dishes in the oven one after another.
  * They don’t stand staring at the oven waiting; they keep loading ovens with new dishes.
  * As timers go off, they take out finished dishes (order depends on cooking time).
* That’s exactly what the event loop does with outbound requests and inbound responses.

---

## 7. Important Notes

* **Order is not guaranteed** → if you need responses in order, use operators like `concatMap` or `flatMapSequential`.
* This pattern is **universal**:

  * Node.js event loop, Python’s asyncio, Golang goroutines with async network ops, Java NIO.
* Not specific to Java/Reactor — it’s how non-blocking network programming works across languages.

---

## 8. Takeaway

* Non-blocking I/O works by separating **sending** (outbound) and **receiving** (inbound) into queues, with one event loop thread coordinating both.
* The thread never blocks → it can handle massive concurrency with minimal threads.
* This is the foundation for **reactive programming scalability**.

---

# 📊 Event Loop & Non-blocking I/O (Simplified)

```
                 ┌─────────────────────────────┐
                 │        Outbound Queue       │
                 │ [Req1][Req2][Req3]...[ReqN] │
                 └──────────────┬──────────────┘
                                │
                                ▼
                        ┌────────────┐
                        │ Event Loop │  (single thread)
                        └─────┬──────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     ┌──────────────────┐             ┌──────────────────┐
     │ Sends HTTP Req → │   Network   │ ← Receives Resp  │
     └──────────────────┘             └──────────────────┘
              │                               │
              ▼                               ▼
      ┌───────────────┐               ┌───────────────┐
      │ Remote Server │               │ Inbound Queue │
      │  (delays 1s)  │               │ [Resp2][Resp7][Resp1]... │
      └───────────────┘               └───────────────┘
```

---

## How to read this:

1. **Outbound queue** → tasks to be sent (Product ID 1…100).
2. **Event loop thread** → pulls tasks, sends HTTP requests.

  * Doesn’t wait for responses.
  * Keeps pulling the next task.
3. **Remote server** → processes requests (slow, \~1s delay).
4. **Inbound queue** → responses arrive *asynchronously* in arbitrary order.
5. Event loop processes inbound responses and hands them back to subscribers.

---

# 📊 Blocking I/O vs Non-blocking I/O

```
          BLOCKING MODEL (Thread-per-request)        NON-BLOCKING MODEL (Event Loop)

         ┌─────────────┐                          ┌─────────────┐
 Req 1 → │ Thread 1    │───wait(1s)─────────┐     │             │
         └─────────────┘                     │     │             │
                                             │     │             │
 Req 2 → ┌─────────────┐───wait(1s)──────────┼───► │             │
         │ Thread 2    │                     │     │             │
         └─────────────┘                     │     │             │
                                             │     │ Event Loop  │
 Req 3 → ┌─────────────┐───wait(1s)──────────┼───► │   Thread    │
         │ Thread 3    │                     │     │             │
         └─────────────┘                     │     │             │
                                             │     │             │
  ...    ┌─────────────┐                     │     │             │
 Req N → │ Thread N    │───wait(1s)──────────┘     │             │
         └─────────────┘                           └─────────────┘
```

---

## 🔎 Explanation

### Blocking I/O

* Each request blocks its own thread until response arrives.
* 100 concurrent requests = 100 threads (or 100 seconds sequentially).
* Threads are expensive → heavy memory, context switching, poor scalability.

### Non-blocking I/O (Event Loop)

* One thread manages **all requests**.
* It sends request #1 → doesn’t wait → sends #2 → #3 → … → #N.
* Responses arrive asynchronously and are processed as events.
* Only **1 (or a few) threads** needed → lightweight, scalable.

---

## 💡 Key takeaway for students

* Blocking = **1 thread per request**.
* Non-blocking = **1 thread for thousands of requests**.
* That’s the **superpower** of reactive programming and why we can handle large concurrent workloads without 100x hardware.

---

# 📘 Why You Should Not Use `.block()` in Reactive Programming

## 1. What is `.block()`?

* Reactive types (`Mono`, `Flux`) are **asynchronous publishers**.
* Normally, we consume them by:

  * Providing a **subscriber** implementation, or
  * Using **functional callbacks** like `.subscribe(item -> ...)`.
* But `Mono` and `Flux` also expose a **blocking method**:

  ```java
  String value = mono.block();
  ```
* This **forces the async pipeline to stop and wait** until the result is available.

---

## 2. Why `.block()` is discouraged

1. **Breaks the reactive model**

  * Reactive programming = **non-blocking, event-driven**.
  * `.block()` **turns async back into sync**, defeating the purpose.

2. **Thread starvation**

  * Event-loop threads (Netty) are meant for non-blocking tasks.
  * If you call `.block()` inside the reactive pipeline, you block the event loop itself.
  * This prevents the thread from serving other requests → bottleneck.

3. **Performance issues**

  * Instead of one thread serving thousands of requests, you now need many threads again.
  * You lose scalability benefits of reactive programming.

4. **Deadlocks**

  * In certain contexts (e.g., running inside the same event loop), `.block()` can deadlock.
  * Example: blocking inside a WebFlux controller can freeze the request.

---

## 3. Demo (for teaching)

```java
Mono<String> mono = Mono.fromSupplier(() -> {
    Util.sleepSeconds(1);
    return "Hello";
});

// BAD: blocking call
String result = mono.block();  
System.out.println("Result = " + result);
```

* Works for demos, but **not safe in production pipelines**.
* You’ll notice the thread is stuck for 1 second. In real apps, hundreds of blocked threads = disaster.

---

## 4. When is `.block()` acceptable?

* **At application boundaries only**:

  * Small **unit tests** (to assert results easily).
  * Bootstrapping code in `main()` before application startup.
* **Never inside a reactive pipeline** or request handler.

---

## 5. Correct approach

Instead of:

```java
String result = mono.block();
```

Use reactive style:

```java
mono.subscribe(result -> System.out.println("Result = " + result));
```

Or, in a pipeline:

```java
mono.map(value -> value.toUpperCase())
    .subscribe(System.out::println);
```

---

## 6. Takeaway

* `.block()` is like **pressing the brake** in a high-speed reactive system.
* It cancels out the benefits of non-blocking I/O.
* **Golden rule:**

  * ❌ Don’t `.block()` inside your reactive pipelines.
  * ✅ Use `.subscribe()`, `.map()`, `.flatMap()`, and other operators instead.

---

# 📘 Why Reactor Netty (instead of plain Java NIO)?

## 1. The Question

Students often ask:
👉 *“Why do we need Reactor Netty? Can’t we just use plain Java to do non-blocking I/O ourselves?”*

Answer: **Yes, it’s possible — but it’s very hard.**

---

## 2. How Plain Java Non-blocking I/O Works

Java provides **NIO (New I/O)** and **AsynchronousSocketChannel** APIs that allow non-blocking networking.

To send a simple HTTP request manually, you’d need to:

1. **Open an `AsynchronousSocketChannel`**

   ```java
   AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
   channel.connect(new InetSocketAddress("localhost", 7070), null, new CompletionHandler<Void,Void>() {
       public void completed(Void result, Void attachment) {
           // connection established
       }
       public void failed(Throwable exc, Void attachment) { ... }
   });
   ```

2. **Build the HTTP request by hand** (raw text):

   ```
   GET /demo01/product/1 HTTP/1.1
   Host: localhost:7070
   Connection: keep-alive
   ```

3. **Write the request into a `ByteBuffer`**:

   ```java
   ByteBuffer buffer = ByteBuffer.wrap(request.getBytes(StandardCharsets.UTF_8));
   channel.write(buffer, null, new CompletionHandler<Integer,Void>() { ... });
   ```

4. **Register another callback to read the response**:

   ```java
   ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
   channel.read(responseBuffer, null, new CompletionHandler<Integer,Void>() {
       public void completed(Integer bytesRead, Void attachment) {
           responseBuffer.flip();
           String response = StandardCharsets.UTF_8.decode(responseBuffer).toString();
           System.out.println("Response: " + response);
       }
       public void failed(Throwable exc, Void attachment) { ... }
   });
   ```

5. Handle **connection close, errors, retries, timeouts**… all manually.

---

## 3. The Problem

* This quickly becomes **callback hell**:
  Connect → Write → Read → Process → Close.
  Each step requires a `CompletionHandler`.
* You’re working at a **very low level**: raw `ByteBuffer`s, manual request formatting, response parsing.
* Features like:

  * Connection pooling
  * SSL/TLS
  * Keep-alive
  * Backpressure
    … must all be built by hand.

---

## 4. What Reactor Netty Gives You

Reactor Netty **abstracts all of this**:

```java
HttpClient.create()
    .baseUrl("http://localhost:7070")
    .get()
    .uri("/demo01/product/1")
    .responseContent()
    .aggregate()
    .asString()
    .subscribe(System.out::println);
```

✅ No raw `ByteBuffer`s.
✅ No manual callbacks — Reactor handles them under the hood.
✅ Integrated with Reactor → you get `Mono`/`Flux` directly.
✅ Scales to thousands of concurrent requests using event loop model.

---

## 5. Takeaway

* **Yes, you *can* do non-blocking IO in plain Java.**
* But you’d end up writing hundreds of lines of boilerplate, managing callbacks, and handling tricky concurrency issues.
* **Reactor Netty** saves you from that pain by giving a clean, reactive API on top of Netty’s event loop.


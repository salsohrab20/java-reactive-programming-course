# **Flux in Project Reactor**

### 1. **Introduction**

* `Flux<T>` is a **Publisher** that can emit:

    * **Zero items**
    * **One item**
    * **Multiple items (`n`)**
* After emitting, it ends with:

    * `onComplete()` signal **or**
    * `onError()` signal
* A `Flux` can also represent an **infinite stream** (never-ending sequence).

    * In such cases, you may never receive `onComplete()` or `onError()` signals.

---

### 2. **Publisher–Subscriber Relationship**

* **Publisher** (`Flux`) gives a **Subscription** object to the **Subscriber**.
* The **Subscriber** uses the `Subscription` to request items.
* **Flux** emits items based on the subscriber’s request (demand-driven / backpressure-aware).
* Same principle as with `Mono` (Publisher-Subscriber contract).

---

### 3. **Factory Methods to Create a Flux**

Flux provides several static factory methods to quickly create publishers.
Some of the most common ones:

1. **`Flux.just(T... data)`**

    * Creates a `Flux` that emits the given items sequentially and then completes.
    * Example:

      ```java
      Flux.just(1, 2, 3, "Sam")
          .subscribe(System.out::println);
      // Output: 1, 2, 3, Sam
      ```

2. **`Flux.fromIterable(Iterable<T>)`**

    * Converts an existing collection (e.g., `List`, `Set`) into a `Flux`.
    * Example:

      ```java
      List<String> names = List.of("John", "Mike", "Sara");
      Flux.fromIterable(names).subscribe(System.out::println);
      ```

3. **`Flux.fromArray(T[] array)`**

    * Emits items from a Java array.

4. **`Flux.fromStream(Stream<T>)`**

    * Wraps a Java `Stream` into a `Flux`.

5. **Other advanced methods**

    * `Flux.empty()` → emits nothing and completes.
    * `Flux.error(Throwable)` → immediately terminates with an error.
    * `Flux.range(int start, int count)` → emits a range of integers.
    * `Flux.interval(Duration)` → emits increasing `Long` values at given time intervals (useful for infinite/periodic streams).

---

### 4. **Subscribing to a Flux**

* Similar to `Mono`, you can subscribe in different ways:

    1. **Default Subscriber**

       ```java
       Flux.just(1).subscribe();
       ```
    2. **With `onNext` handler**

       ```java
       Flux.just(1, 2, 3).subscribe(item -> System.out.println("Received: " + item));
       ```
    3. **With `onNext`, `onError`, and `onComplete` handlers**

       ```java
       Flux.just("A", "B")
           .subscribe(
               data -> System.out.println("OnNext: " + data),
               error -> System.err.println("Error: " + error),
               () -> System.out.println("Completed!")
           );
       ```

---

### 5. **Example Walkthrough**

```java
Flux.just(1, 2, 3, "Sam")
    .subscribe(
        value -> System.out.println("Received: " + value),
        error -> System.err.println("Error: " + error),
        () -> System.out.println("Stream completed")
    );
```

**Output:**

```
Received: 1
Received: 2
Received: 3
Received: Sam
Stream completed
```

---

# **Flux – Multiple Subscribers**

### 1. **Overview**

* A single `Flux` can have **multiple subscribers**.
* Each subscriber will receive the items emitted by the `Flux`, independently.
* Subscribers can also apply **transformations** or **filters** before consuming the data.

---

### 2. **Creating a Flux**

We first create a simple `Flux` using the `just()` factory method:

```java
Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6);
```

This flux emits the numbers `1` through `6`, then sends an `onComplete()` signal.

---

### 3. **Multiple Subscribers**

#### **Subscriber 1 – Simple Subscriber**

```java
flux.subscribe(value -> System.out.println("Subscriber 1: " + value));
```

**Output:**

```
Subscriber 1: 1
Subscriber 1: 2
Subscriber 1: 3
Subscriber 1: 4
Subscriber 1: 5
Subscriber 1: 6
```

---

#### **Subscriber 2 – Another Subscriber**

```java
flux.subscribe(value -> System.out.println("Subscriber 2: " + value));
```

**Output (same as Subscriber 1):**

```
Subscriber 2: 1
Subscriber 2: 2
Subscriber 2: 3
Subscriber 2: 4
Subscriber 2: 5
Subscriber 2: 6
```

> 🔑 Each subscriber gets the **full sequence** of items from the source Flux.

---

### 4. **Subscribers with Operators**

#### **Subscriber 3 – Filtering Even Numbers**

```java
flux.filter(i -> i % 2 == 0)
    .subscribe(value -> System.out.println("Subscriber 3 (Even only): " + value));
```

**Output:**

```
Subscriber 3 (Even only): 2
Subscriber 3 (Even only): 4
Subscriber 3 (Even only): 6
```

---

#### **Subscriber 4 – Transforming Items (map)**

```java
flux.filter(i -> i % 2 == 0)
    .map(i -> i + "A")
    .subscribe(value -> System.out.println("Subscriber 4 (Mapped): " + value));
```

**Output:**

```
Subscriber 4 (Mapped): 2A
Subscriber 4 (Mapped): 4A
Subscriber 4 (Mapped): 6A
```

---

# **Flux from Iterable or Array**

### 1. **Overview**

* Often, you already have data stored in a **collection** (like `List` or `Set`) or an **array**.
* Reactor’s `Flux` provides convenient **factory methods** to convert them directly into a reactive stream:

    * `Flux.fromIterable(Iterable<T>)`
    * `Flux.fromArray(T[] array)`

---

### 2. **Flux from Iterable (List / Set)**

Example:

```java
List<String> list = List.of("A", "B", "C");

Flux.fromIterable(list)
    .subscribe(Util.subscriber());
```

**Output:**

```
[A]
[B]
[C]
Completed
```

🔑 **Note:** Any `Iterable` works → `List`, `Set`, `Queue`, etc.

---

### 3. **Flux from Array**

Example:

```java
Integer[] numbers = {1, 2, 3, 4, 5, 6};

Flux.fromArray(numbers)
    .subscribe(Util.subscriber());
```

**Output:**

```
[1]
[2]
[3]
[4]
[5]
[6]
Completed
```

---

### 4. **Why is this useful?**

* Quickly adapt **in-memory data structures** into reactive streams.
* Ideal when transitioning existing **imperative code** into **reactive pipelines**.
* Makes it easier to use operators like `filter`, `map`, `flatMap`, etc., on pre-existing data.

---

# **Flux from Java Stream**

### 1. **Overview**

* Just like `Iterable` and `Array`, you can also create a `Flux` from a **Java Stream**.
* Reactor provides the factory method:

  ```java
  Flux.fromStream(Stream<T> stream)
  ```
* ⚠️ But there’s a **trick**: Java streams are **single-use** (consumed once).
  If you try to reuse them, you’ll get an **error**.

---

### 2. **Basic Example**

```java
List<Integer> list = List.of(1, 2, 3, 4);

// Create a Java Stream
Stream<Integer> stream = list.stream();

// Create a Flux from the Stream
Flux<Integer> flux = Flux.fromStream(stream);

// First subscriber works fine
flux.subscribe(Util.subscriber("Sub1"));

// Second subscriber -> ❌ Error
flux.subscribe(Util.subscriber("Sub2"));
```

**Error:**

```
java.lang.IllegalStateException: stream has already been operated upon or closed
```

---

### 3. **Why does this happen?**

* This issue is **not about Flux**, but about **Java Stream** itself.
* Example without Flux:

```java
Stream<Integer> stream = List.of(1, 2, 3, 4).stream();

// First time → works
stream.forEach(System.out::println);

// Second time → ❌ error
stream.forEach(System.out::println);
```

* Streams in Java are **consumable once**. After consumption, they are **closed** and cannot be reused.

---

### 4. **The Fix – Using a Supplier of Stream**

* If you want multiple subscribers, you must provide a **supplier**, i.e., a way to create a *new* stream for every subscription.

```java
List<Integer> list = List.of(1, 2, 3, 4);

// Supplier provides a fresh Stream for each subscriber
Flux<Integer> flux = Flux.fromStream(list::stream);

flux.subscribe(Util.subscriber("Sub1"));
flux.subscribe(Util.subscriber("Sub2"));
```

**Output:**

```
[Sub1] onNext: 1
[Sub1] onNext: 2
[Sub1] onNext: 3
[Sub1] onNext: 4
[Sub1] onComplete

[Sub2] onNext: 1
[Sub2] onNext: 2
[Sub2] onNext: 3
[Sub2] onNext: 4
[Sub2] onComplete
```

✅ Both subscribers now receive the complete data stream, because each time a **new Stream is opened**.

---

### 5. **Important Points to Ponder**

* **Streams are one-time use only**:

    * After a terminal operation (`forEach`, `collect`, etc.), the stream is closed.
* If you need **only one subscriber**, `Flux.fromStream(stream)` is fine.
* If you need **multiple subscribers**, always use a **supplier**:

  ```java
  Flux.fromStream(() -> list.stream())
  ```
* Reactor itself is not at fault; this limitation comes directly from the Java Stream API.
* Using `fromIterable` or `fromArray` is often simpler and avoids this issue when data is already available in memory.

---

✅ **Takeaway:**

* Use `Flux.fromStream(stream)` only for **single-subscriber** use cases.
* For **multi-subscriber** scenarios, always use a **supplier** (`Flux.fromStream(list::stream)`) to ensure a new Stream is created for each subscription.

---
# **Flux.range()**

### 1. **Overview**

* `Flux.range(int start, int count)`
* Emits a **sequence of integers**, beginning at `start`, and producing `count` consecutive numbers.
* This can be thought of as a **reactive for-loop**.

---

### 2. **Basic Example**

```java
Flux.range(1, 10)
    .subscribe(Util.subscriber("Sub1"));
```

**Output:**

```
[Sub1] onNext: 1
[Sub1] onNext: 2
[Sub1] onNext: 3
...
[Sub1] onNext: 10
[Sub1] onComplete
```

---

### 3. **Clarifying Parameters**

* **First parameter** → Starting number.
* **Second parameter** → How many numbers to emit.

Example:

```java
Flux.range(3, 10)
    .subscribe(Util.subscriber("Sub2"));
```

**Output:**

```
[Sub2] onNext: 3
[Sub2] onNext: 4
[Sub2] onNext: 5
...
[Sub2] onNext: 12
[Sub2] onComplete
```

> ✅ Starts at 3 and emits **10 numbers** (`3 → 12`).

---

### 4. **Using Flux.range as a Reactive For-Loop**

* Instead of:

```java
for (int i = 1; i <= 10; i++) {
    System.out.println(i);
}
```

* You can write:

```java
Flux.range(1, 10)
    .subscribe(System.out::println);
```
---

# **Flux `log()` Operator**

### 1. **Overview**

* `log()` is a **debugging operator** in Project Reactor.
* It sits **between producer and subscriber**, acting like a *middleman*.
* It logs all the **signals** exchanged:

    * `onSubscribe`
    * `request(n)`
    * `onNext`
    * `onComplete`
    * `onError`

✅ useful when debugging reactive pipelines.

---

### 2. **Basic Example**

```java
Flux.range(1, 5)
    .log()
    .subscribe(Util.subscriber("Sub1"));
```

**Sample Output:**

```
[ INFO] (main) | onSubscribe([Synchronous Fuseable] FluxRange.RangeSubscription)
[ INFO] (main) | request(unbounded)
[ INFO] (main) | onNext(1)
[ INFO] (main) | onNext(2)
[ INFO] (main) | onNext(3)
[ INFO] (main) | onNext(4)
[ INFO] (main) | onNext(5)
[ INFO] (main) | onComplete()
[Sub1] onNext: 1
...
[Sub1] onComplete
```

---

### 3. **How `log()` Works**

* Acts like both **subscriber** and **publisher**.
* Whatever the subscriber requests → `log()` forwards to producer.
* Whatever the producer emits → `log()` logs, then forwards to subscriber.
* So you can see **exactly what signals are flowing** in the pipeline.

---

### 4. **Partial Request Example**

```java
Flux.range(1, 5)
    .log()
    .subscribe(new BaseSubscriber<Integer>() {
        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(3); // request only 3 items
        }

        @Override
        protected void hookOnNext(Integer value) {
            System.out.println("Received: " + value);
        }
    });
```

**Sample Output:**

```
| onSubscribe(...)
| request(3)
| onNext(1)
| onNext(2)
| onNext(3)
Received: 1
Received: 2
Received: 3
```

* Producer **did not emit 4 & 5**, nor complete, since subscriber asked for only 3.

---

### 5. **Chaining with Other Operators**

Consider transformation with `map()`:

```java
Flux.range(1, 3)
    .log("Between Range and Map")
    .map(i -> Util.faker().name().firstName())
    .log("After Map")
    .subscribe(Util.subscriber("Names"));
```

**Key Observations:**

* First `log` only sees raw integers (`onNext(1), onNext(2), ...`).
* Second `log` sees transformed names (`onNext(Alice), onNext(Ravi), ...`).
* That’s because **each operator subscribes to the previous one**, and `log()` shows only what passes *through that stage*.

---

### 6. **Points to Ponder**

* `log()` is **non-intrusive** — it only observes and prints events.
* Very helpful for **debugging missing signals** (e.g., producer emitted, but subscriber didn’t receive).
* Add `log()` at **different points** in the chain to see transformations stage by stage.
* In production, be cautious: excessive logging can **slow down** performance.
* Use **category names** to differentiate multiple `log()`s in a pipeline.

---

# **Flux vs List – Traditional vs Reactive Approach**

### 1. **Business Requirement**

* Requirement: Generate random **first names** for users.
* We will implement this requirement in **two styles**:

    1. **Traditional Style** → Returns a `List<String>`.
    2. **Reactive Style** → Returns a `Flux<String>`.

---

### 2. **Helper Class – `NameGenerator`**

```java
package section03.helper;

import com.github.javafaker.Faker;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.IntStream;

public class NameGenerator {

    private static final Faker faker = new Faker();

    // Simulates a time-consuming operation (~1s per name)
    private static String generateName() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return faker.name().firstName();
    }

    // Traditional Style: Generate names as List
    public static List<String> getNamesList(int count) {
        return IntStream.rangeClosed(1, count)
                        .mapToObj(i -> generateName())
                        .toList();
    }

    // Reactive Style: Generate names as Flux
    public static Flux<String> getNamesFlux(int count) {
        return Flux.range(1, count)
                   .map(i -> generateName());
    }
}
```

---

### 3. **Driver Class – `FluxVsListDemo`**

```java
public class FluxVsListDemo {
    public static void main(String[] args) {
        // --- Traditional Style ---
        System.out.println("Generating names using List...");
        List<String> names = NameGenerator.getNamesList(10);
        System.out.println("Names (after 10s): " + names);

        // --- Reactive Style ---
        System.out.println("\nGenerating names using Flux...");
        NameGenerator.getNamesFlux(10)
                     .subscribe(Util.subscriber("Names"));
    }
}
```

---

### 4. **Observed Behavior**

#### **Traditional List Approach**

* Nothing is printed until **all 10 names are generated** (≈10s wait).
* After that, the list is printed in one go.
* **Blocking behavior** – User waits with no feedback.

```
Generating names using List...
(10 seconds pause...)
Names (after 10s): [Alice, John, Maria, ...]
```

#### **Reactive Flux Approach**

* Names are generated **one by one**.
* Each name is **pushed immediately** to the subscriber.
* The subscriber sees intermediate results and can **react early**.

```
Generating names using Flux...
[Names] onNext: Alice
[Names] onNext: John
[Names] onNext: Maria
...
[Names] onComplete
```

---

### 5. **Advanced Demo – Backpressure & Early Cancellation**

```java
SubscriberImpl<String> subscriber = new SubscriberImpl<>();

Flux<String> flux = NameGenerator.getNamesFlux(10);
flux.subscribe(subscriber);

// Request only 3 names
subscriber.getSubscription().request(3);

// After receiving 3, cancel further requests
subscriber.getSubscription().cancel();
```

**Output:**

```
onSubscribe
onNext: Felicia
onNext: Robert
onNext: David
-- Cancelled early --
```

✅ The subscriber **reacts immediately** and can **stop early** (cancel subscription) if satisfied.

---

### 6. **Key Differences**

| Aspect                  | Traditional (`List`)                | Reactive (`Flux`)                      |
| ----------------------- | ----------------------------------- | -------------------------------------- |
| **Execution style**     | Blocking (waits until all complete) | Non-blocking (emits as data is ready)  |
| **User feedback**       | None until the end                  | Immediate feedback (streaming)         |
| **Partial consumption** | Not possible (all-or-nothing)       | Possible (request `n`, cancel anytime) |
| **Responsiveness**      | Poor (long wait times)              | High (progress visible instantly)      |

---

# **List vs Flux Analogy – Gemini vs ChatGPT**

### 1. **Experiment Setup**

* Ask the same question to **Gemini** and **ChatGPT**:

  > *“Create a function in Java to generate the first 1000 prime numbers.”*
* We’re not focusing on the correctness of the answer, but on the **behavior of the response**.

---

### 2. **Observed Behaviors**

#### **Gemini (List-like Behavior)**

* Appears to *do some heavy work first* (prepares the full answer internally).
* Keeps the user waiting until it has the **entire response ready**.
* Finally sends the completed answer **all at once**.
* **User experience:** no visibility into progress, no chance to stop early.

👉 This is like the **traditional List approach**:

* Producer generates *all items first*.
* Consumer (user) sees the result only **after everything is ready**.

---

#### **ChatGPT (Flux-like Behavior)**

* Starts streaming tokens (partial answer) **immediately**.
* User can **see progress live** as the answer is generated.
* User has the ability to **stop early** if:

    * The answer is going in the wrong direction.
    * They realize the question wasn’t phrased properly.
* **User experience:** highly responsive, interactive, saves time.

👉 This is like the **Flux approach**:

* Producer emits items **as soon as they’re ready**.
* Subscriber (user) consumes them incrementally.
* Subscriber can cancel early, avoiding wasted computation.

---

### 3. **Why Responsiveness Matters**

* In real-world applications:

    * Users may not always phrase their request correctly.
    * Immediate feedback helps them **adjust quickly**.
    * Streaming results = faster iteration, less wasted effort.

✅ With Flux (or reactive streams):

* The user **is never left in the dark**.
* They can **observe, react, and control** the flow of data.

❌ With List (or traditional blocking):

* The user **waits blindly**.
* Only after the process ends do they discover mistakes.

---

### 4. **Analogy Recap**

| Behavior           | Gemini (List-like)           | ChatGPT (Flux-like)            |
| ------------------ | ---------------------------- | ------------------------------ |
| **Execution**      | Prepares full response first | Streams response incrementally |
| **Responsiveness** | Low (wait until done)        | High (live feedback)           |
| **User Control**   | None (must wait till end)    | Can stop anytime               |
| **Correction**     | After full answer is shown   | Mid-way, adjust immediately    |

---

# **Mono & Flux vs Java Collections**

### 1. **Traditional Data Structures (List, Set, Map)**

* Collections like `List`, `Set`, and `Map` are **data storage containers**.
* They **store elements in memory** → finite capacity, bounded by system resources.
* Example:

  ```java
  List<String> names = List.of("A", "B", "C");
  ```

    * Data is fully loaded in memory.
    * You can access elements randomly at any time.

---

### 2. **Mono & Flux**

* `Mono<T>` and `Flux<T>` are **not storage containers**.
* They represent a **pipeline (tunnel/pipe)** for data transfer.
* Think of them as a **flow of messages/events over time**.
* They can represent:

    * **Finite streams** (like a list of items).
    * **Infinite streams** (e.g., live data feed, event bus).

---

### 3. **Analogy: Container vs Pipe**

* **List/Set** → Like a **water container** (bucket, tank).

    * Can hold water (data) but only **finite amount**.
    * Capacity limited.
* **Mono/Flux** → Like a **pipe**.

    * Not a container itself.
    * **Transfers fluid (data)** from one place to another.
    * Can carry **finite or infinite flow** of data.

👉 So asking “How many elements does a Flux hold?” is like asking “How much water is stored in a pipe?” → Wrong question. Flux is about *flow*, not *storage*.

---

### 4. **Microservices Example**

* In distributed systems:

    * **App1** = Publisher → keeps pushing data (via TCP, Kafka, WebSocket, etc.).
    * **App2** = Subscriber → receives data as soon as App1 pushes it.
* No storage in the middle → Flux/Mono are just **channels for data transfer**.
* This makes it possible to handle **infinite streams** efficiently.

---

### 5. **Key Differences**

| Aspect        | Collections (List/Set/Map) | Mono/Flux                  |
| ------------- | -------------------------- | -------------------------- |
| Nature        | **Storage**                | **Pipeline / Stream**      |
| Data Handling | Stored in memory           | Flows through as signals   |
| Size          | Finite (bounded by memory) | Finite or Infinite         |
| Access        | Random access              | Sequential event-driven    |
| Use Case      | In-memory processing       | Asynchronous data transfer |

---

### 6. **Takeaway**

* Collections = *storage containers*.
* Mono/Flux = *pipes/tunnels for streaming data*.
* Collections are bounded; Flux/Mono can represent **unbounded / infinite streams**.
* This is why **Reactive Programming** is essential in modern architectures (microservices, event-driven systems, real-time apps).

---

# **Consuming Stream of Messages over HTTP**

### 1. **Context**

* In real applications, data often flows as a **continuous stream** instead of a one-time response.
* Example:

    * A service exposes an endpoint `/demo02/namestream`.
    * Every **500ms**, it emits a random first name.
    * Total: 10 names → then completes.

This is an HTTP endpoint returning a **Flux of messages**.

---

### 2. **Swagger vs Browser vs Reactor Netty**

* **Swagger UI**:

    * Not designed for streaming → may hang or fail to show incremental data.
* **Browser**:

    * Depending on support, you might see streaming messages appear live.
* **Reactor Netty Client**:

    * Fully supports **consuming Flux streams** over HTTP.
    * Ideal for reactive pipelines.

---

### 3. **Client Implementation**

```java
// ExternalServiceClient.java
public class ExternalServiceClient {

    private final WebClient client = WebClient.create("http://localhost:7070");

    // Endpoint: /demo02/namestream
    public Flux<String> getNames() {
        return client.get()
                     .uri("/demo02/namestream")
                     .retrieve()
                     .bodyToFlux(String.class);
    }
}
```

---

### 4. **Consuming the Stream**

```java
public class NonBlockingStreamingMessages {
    public static void main(String[] args) throws InterruptedException {
        ExternalServiceClient client = new ExternalServiceClient();

        client.getNames()
              .subscribe(Util.subscriber("Sub1"));

        // Block main thread to let Flux emit (simulate server running for 6s)
        Thread.sleep(6000);
    }
}
```

**Sample Output:**

```
[Sub1] onNext: Alice
[Sub1] onNext: John
[Sub1] onNext: Ravi
...
[Sub1] onComplete
```

---

### 5. **Multiple Subscribers**

```java
client.getNames().subscribe(Util.subscriber("Sub1"));
client.getNames().subscribe(Util.subscriber("Sub2"));

Thread.sleep(6000);
```

**Output:**

```
[Sub1] onNext: Maria
[Sub1] onNext: David
...
[Sub1] onComplete

[Sub2] onNext: Maria
[Sub2] onNext: David
...
[Sub2] onComplete
```

* Each subscriber gets its **own stream of 10 names**.
* Both are handled **on the same thread**, showing how efficiently messages are transferred.

---

### 6. **Important Observations**

* The stream is **non-blocking**:

    * Names arrive every 500ms, main thread is not stuck waiting.
* Each subscriber is **independent**:

    * They receive full streams separately.
* Flux is **generic**:

    * Here we used `Flux<String>`.
    * Could just as easily be `Flux<Customer>` or `Flux<Order>`.
    * With JSON, deserialization is handled automatically (e.g., by Spring WebFlux).

---

# **Flux.interval()**

### 1. **Overview**

* So far, we created `Flux` from:

    * Static data (`just`, `fromIterable`, `fromArray`, `fromStream`)
    * Generated ranges (`range`)
* Sometimes, we need a **time-based Flux**:

    * Emit an item **every fixed duration**.
    * Example: Every `1s`, every `500ms`, etc.

👉 For this, Reactor provides `Flux.interval`.

---

### 2. **Syntax**

```java
Flux.interval(Duration.ofMillis(500))
```

* Starts emitting from `0`.
* Then `1, 2, 3, ...` sequentially.
* Keeps emitting at the specified interval **indefinitely** until cancelled.

---

### 3. **Basic Example**

```java
Flux.interval(Duration.ofMillis(500))
    .subscribe(Util.subscriber("Sub1"));

Thread.sleep(5000); // Block main thread to observe
```

**Output (sample):**

```
[Sub1] onNext: 0
[Sub1] onNext: 1
[Sub1] onNext: 2
[Sub1] onNext: 3
[Sub1] onNext: 4
```

* Main thread exits after 5s, so the stream stops.
* If not blocked, the program would finish instantly (since Flux emits asynchronously on a separate thread).

---

### 4. **Stopping the Stream**

* By default, `Flux.interval` is **infinite**.
* To stop:

    1. Cancel via **subscription**.
    2. Use operators like `.take(n)` to limit items.

**Example – Cancel after 3 items:**

```java
Flux.interval(Duration.ofMillis(500))
    .take(3)
    .subscribe(Util.subscriber("Sub2"));
```

**Output:**

```
[Sub2] onNext: 0
[Sub2] onNext: 1
[Sub2] onNext: 2
[Sub2] onComplete
```

---

### 5. **Generating Data Instead of Numbers**

* Often, we don’t care about the sequence (`0,1,2...`).
* Use `.map()` to transform emitted values.

**Example – Generate names every 500ms:**

```java
Flux.interval(Duration.ofMillis(500))
    .map(i -> Util.faker().name().firstName())
    .subscribe(Util.subscriber("Names"));

Thread.sleep(5000);
```

**Output (sample):**

```
[Names] onNext: Alice
[Names] onNext: Ravi
[Names] onNext: Maria
[Names] onNext: John
...
```

---

### 6. **Points to Ponder**

* `Flux.interval` is **asynchronous**: runs on a **separate scheduler thread**.
* Runs **forever** unless:

    * You cancel the subscription.
    * You use a limiting operator (`take`, `takeWhile`, etc.).
* Perfect for:

    * Periodic tasks.
    * Polling APIs at intervals.
    * Emitting **heartbeat** or **status updates**.

---

# **Flux.empty() and Flux.error()**

### 1. **Background**

* A `Flux` does **not always emit data**.
* It can:

    * Emit items → then `onComplete()`.
    * Emit **nothing at all** → directly `onComplete()`.
    * Emit an **error signal** instead of data.

👉 `Flux.empty()` and `Flux.error()` are the factory methods to represent these cases.

---

### 2. **Flux.empty()**

* Represents a `Flux` with **no items**, only a **completion signal**.

**Example:**

```java
Flux.empty()
    .subscribe(Util.subscriber("Empty"));
```

**Output:**

```
[Empty] onComplete
```

✅ Use when:

* You have nothing to emit (e.g., no results found).
* But you still want to **gracefully complete**.

---

### 3. **Flux.error()**

* Represents a `Flux` that terminates with an **error signal**.
* Takes a `Throwable` as input.

**Example:**

```java
Flux.error(new RuntimeException("Oops! Something went wrong"))
    .subscribe(Util.subscriber("Error"));
```

**Output:**

```
[Error] onError: java.lang.RuntimeException: Oops! Something went wrong
```

✅ Use when:

* You need to **signal failure** to the subscriber.
* Example: invalid input, service unavailable, etc.

---

### 4. **Comparison**

| Factory Method | Behavior                        | Signals sent         |
| -------------- | ------------------------------- | -------------------- |
| `Flux.empty()` | No items → immediate completion | `onComplete()`       |
| `Flux.error()` | No items → immediate failure    | `onError(Throwable)` |

---

### 5. **Key Takeaway**

* `Flux.empty()` → *no data, just completes*.
* `Flux.error()` → *no data, ends with error*.
* Both are **useful shortcuts** when designing reactive APIs to handle **edge cases**.
---

# **Flux.defer()**

### 1. **Background**

* With `Mono`, we already saw methods like:

    * `fromSupplier()`
    * `fromCallable()`
      → These allow **delayed execution**: values are created *only when subscribed*.
* For `Flux`, the equivalent mechanism is **`Flux.defer()`**.

---

### 2. **Why `defer`?**

* By default, methods like `Flux.fromIterable()` or `Flux.just()` are **eager**:

    * They capture data **immediately** at the time of Flux creation.
* Sometimes, you want a **fresh Flux every time someone subscribes**.
* This ensures:

    * New data source (e.g., a new list, random values, timestamps).
    * Avoids reusing stale data.

---

### 3. **Syntax**

```java
Flux.defer(() -> Flux.fromIterable(generateNewList()))
```

* `defer` accepts a \*\*Supplier\<Publisher<T>>\`.
* Each new subscriber → supplier is re-executed → new Flux is created.

---

### 4. **Example Without Defer**

```java
List<String> names = new ArrayList<>();
names.add("Alice");

Flux<String> flux = Flux.fromIterable(names);

names.add("Bob");

flux.subscribe(Util.subscriber("Sub1"));  // Output: Alice, Bob
flux.subscribe(Util.subscriber("Sub2"));  // Output: Alice, Bob (same data reused)
```

👉 Same list is reused for every subscriber.

---

### 5. **Example With Defer**

```java
Supplier<List<String>> generateList = () -> {
    System.out.println("Generating a new list...");
    return List.of("Name-" + System.currentTimeMillis());
};

Flux<String> flux = Flux.defer(() -> Flux.fromIterable(generateList.get()));

flux.subscribe(Util.subscriber("Sub1"));
Thread.sleep(1000);
flux.subscribe(Util.subscriber("Sub2"));
```

**Sample Output:**

```
Generating a new list...
[Sub1] onNext: Name-1695400000000
[Sub1] onComplete

Generating a new list...
[Sub2] onNext: Name-1695400001000
[Sub2] onComplete
```

👉 Each subscriber gets a **fresh Flux** with new data.

---

### 6. **Use Cases**

* When you want **dynamic data generation** for each subscription.
* When you need to **avoid caching** or reusing stale data.
* Examples:

    * Generate a fresh timestamp on each subscription.
    * Call an API every time a subscriber connects.
    * Provide a new collection for every request.

---

### 7. **Key Takeaway**

* `Flux.defer()` = **lazy Flux creation**.
* Each subscriber triggers a **new Flux pipeline**.
* Use it when your data source should be created **at subscription time**, not at declaration time.

---

# Converting between `Mono` and `Flux`

## 1) When and why

* Sometimes third-party APIs or library methods expect a different reactive type than you have:

    * e.g. you have a `Mono<String>` but a method expects `Flux<String>`, or vice-versa.
* Conversions are straightforward but the semantics differ (a `Mono` holds 0..1 item; a `Flux` 0..N items). Be mindful how extra/absent items are treated.

---

## 2) **Mono → Flux**

**How:** `Flux.from(mono)` or `mono.flux()` (both create a `Flux` that will emit the Mono’s item, or complete/error accordingly).

**Semantics:**

* `Mono.just("x")` → `Flux.from(mono)` emits `"x"` then completes.
* `Mono.empty()` → `Flux.from(mono)` is an empty Flux that completes immediately.
* `Mono.error(e)` → `Flux.from(mono)` is a Flux that terminates with the same error.

**Example:**

```java
Mono<String> mono = Mono.just("alice");
Flux<String> flux = Flux.from(mono); // or mono.flux()

flux.subscribe(
  v -> System.out.println("onNext: " + v),
  e -> System.err.println("onError: " + e),
  () -> System.out.println("onComplete")
);
```

**Use case:** pass a single-item result into code that expects a Flux stream.

---

## 3) **Flux → Mono**

There are a few ways; pick depending on the desired semantics.

### a) `flux.next()`

* Returns a `Mono<T>` of the **first** element emitted by the Flux (or `Mono.empty()` if the Flux completes without emitting).
* It **does not** error if Flux has more elements — it simply takes the first and cancels the rest.

```java
Flux<Integer> flux = Flux.range(1, 10); // emits 1..10
Mono<Integer> first = flux.next();      // Mono emitting 1
```

### b) `Mono.from(flux)`

* Equivalent to `.next()` semantics: returns a `Mono` that emits the first element (if any).

```java
Mono<Integer> m = Mono.from(Flux.range(1, 10)); // m emits 1
```

### c) `flux.single()` or `flux.singleOrEmpty()`

* Use when you **expect exactly one** element (or at most one):

    * `single()` — emits the single element, but throws `IndexOutOfBoundsException`/`IllegalStateException` if flux emits 0 or >1 (depending on overload/implementation).
    * `singleOrEmpty()` — emits the single element or completes empty if none; errors if more than one element.

```java
Flux<Integer> one = Flux.just(42);
Mono<Integer> m1 = one.single();            // emits 42

Flux<Integer> many = Flux.range(1, 3);
Mono<Integer> m2 = many.single();           // will signal error because >1 elements

Flux<Integer> none = Flux.empty();
Mono<Integer> m3 = none.singleOrEmpty();    // completes empty
```

**Important:** choose `next()` when you want the *first* element; choose `single()` when you expect exactly one (and want an error if not).

---

## 4) Behavior for empty & error cases (summary)

| Source              | Conversion                               | Result                               |
| ------------------- | ---------------------------------------- | ------------------------------------ |
| `Mono.empty()`      | `Flux.from(mono)`                        | `Flux.empty()` (onComplete only)     |
| `Mono.error(e)`     | `Flux.from(mono)`                        | `Flux.error(e)`                      |
| `Flux.empty()`      | `Mono.from(flux)` / `flux.next()`        | `Mono.empty()` (onComplete)          |
| `Flux.error(e)`     | `Mono.from(flux)` / `flux.next()`        | `Mono.error(e)`                      |
| `Flux` with ≥1 item | `mono = Flux.next()` / `Mono.from(flux)` | `Mono` emits first item              |
| `Flux` with >1 item | `mon = flux.single()`                    | **error** (if >1 and using `single`) |

---

## 5) Quick recipes

* **Pass a `Mono<T>` to an API that expects `Flux<T>`:**

  ```java
  Flux.from(mono) // or mono.flux()
  ```

* **Get the first element of a `Flux<T>` as `Mono<T>`:**

  ```java
  Mono<T> first = flux.next();   // or Mono.from(flux)
  ```

* **Convert a `Flux<T>` that should contain exactly one value into a `Mono<T>` (and fail otherwise):**

  ```java
  Mono<T> single = flux.single();          // throws if not exactly 1
  Mono<T> singleOrEmpty = flux.singleOrEmpty(); // empty if none, error if >1
  ```

---

## 6) Practical tips / gotchas

* Don’t use `single()` on a multi-element Flux unless you intentionally want an error when there are >1 items.
* `flux.next()` cancels the rest of the stream after capturing the first item — that is usually what you want when converting a multi-element stream to a single value.
* Converting `Mono → Flux` is cheap and preserves the Mono’s terminal signal (complete or error).
* Conversions don’t do background buffering or caching unless you explicitly add operators (`cache()`, `replay()`, etc.). Each subscribe behaves according to the operator semantics.

---

# **Section Summary – Flux Basics**

### 1. **Flux Recap**

* `Flux<T>` = A **Publisher** that can emit:

    * `0` items
    * `1` item
    * `n` items (finite stream)
    * Infinite stream (never-ending)
* Always ends with:

    * `onComplete()` or
    * `onError(Throwable)`

---

### 2. **Factory Methods (Basic)**

These methods help create a `Flux` easily when data is **already in memory** or **generated simply**.

1. **From Arbitrary Items**

   ```java
   Flux.just("A", "B", "C");
   ```

2. **From Collections**

   ```java
   Flux.fromIterable(List.of("A", "B", "C"));
   ```

3. **From Arrays**

   ```java
   Flux.fromArray(new Integer[]{1, 2, 3});
   ```

4. **From Streams**

   ```java
   Flux.fromStream(list::stream); // Supplier ensures fresh stream per subscriber
   ```

5. **Range (like a loop)**

   ```java
   Flux.range(1, 10); // emits 1..10
   ```

6. **Interval (time-based)**

   ```java
   Flux.interval(Duration.ofMillis(500));
   // emits 0,1,2,... every 500ms (infinite until cancelled)
   ```

---

### 3. **Conversion Between Mono & Flux**

* **Mono → Flux**

  ```java
  Mono<String> mono = Mono.just("Alex");
  Flux<String> flux = Flux.from(mono); // or mono.flux()
  ```

* **Flux → Mono**

  ```java
  Flux<Integer> flux = Flux.range(1, 10);
  Mono<Integer> first = flux.next();       // takes first item
  Mono<Integer> single = flux.single();    // expects exactly 1 item
  Mono<Integer> singleOrEmpty = flux.singleOrEmpty(); // 0 or 1 allowed
  ```

---

### 4. **Lazy Creation**

* Use `Flux.defer()` when you want to **delay execution** and create a new Flux **for each subscriber**.

```java
Flux<String> flux = Flux.defer(() -> Flux.fromIterable(generateNewList()));
```

---

### 5. **Key Takeaways**

* **Flux is not a collection**; it’s a *stream of data* (possibly infinite).
* Factory methods let you create Flux easily from existing data sources.
* `interval` and `range` help generate data without storing it.
* `defer` gives you **lazy instantiation** for fresh data at subscription time.
* Conversions (`Mono ↔ Flux`) are useful for API compatibility.

---

# **Flux Factory Methods – Quick Reference**

| **Method**              | **Purpose**                                | **Example**                                           | **Output**                                                     |
| ----------------------- | ------------------------------------------ | ----------------------------------------------------- | -------------------------------------------------------------- |
| `Flux.just(T...)`       | Emit arbitrary items                       | `Flux.just("A","B","C")`                              | `A, B, C, onComplete`                                          |
| `Flux.fromIterable()`   | Create from `List`, `Set`, etc.            | `Flux.fromIterable(List.of("X","Y"))`                 | `X, Y, onComplete`                                             |
| `Flux.fromArray()`      | Create from array                          | `Flux.fromArray(new Integer[]{1,2,3})`                | `1, 2, 3, onComplete`                                          |
| `Flux.fromStream()`     | Create from Java Stream                    | `Flux.fromStream(list::stream)`                       | emits stream elements; reopens per subscriber if supplier used |
| `Flux.range(start, n)`  | Emit sequential integers                   | `Flux.range(3, 5)`                                    | `3, 4, 5, 6, 7, onComplete`                                    |
| `Flux.interval()`       | Emit numbers at fixed interval             | `Flux.interval(Duration.ofMillis(500))`               | `0, 1, 2, 3...` (infinite until cancelled)                     |
| `Flux.empty()`          | Emit nothing, just complete                | `Flux.empty()`                                        | `onComplete` only                                              |
| `Flux.error(Throwable)` | Immediately signal error                   | `Flux.error(new RuntimeException("fail"))`            | `onError(RuntimeException)`                                    |
| `Flux.defer()`          | Lazily create a Flux per subscription      | `Flux.defer(() -> Flux.fromIterable(generateList()))` | new list generated for each subscriber                         |
| `Flux.from(Publisher)`  | Convert any Publisher (incl. Mono) to Flux | `Flux.from(Mono.just("Alex"))`                        | `Alex, onComplete`                                             |

---

# **Mono ↔ Flux Conversions**

| **Conversion**         | **Method**                        | **Behavior**                                 |
| ---------------------- | --------------------------------- | -------------------------------------------- |
| `Mono → Flux`          | `Flux.from(mono)` / `mono.flux()` | Flux with 0 or 1 item (or error/empty)       |
| `Flux → Mono (first)`  | `flux.next()` / `Mono.from(flux)` | Mono with first item only                    |
| `Flux → Mono (single)` | `flux.single()`                   | Mono with exactly one item; error if 0 or >1 |
| `Flux → Mono (0 or 1)` | `flux.singleOrEmpty()`            | Mono with 1 item or empty; error if >1       |

---


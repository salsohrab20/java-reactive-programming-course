## 1. Reactive Streams and Reactor

* **Reactive Streams**: A _specification_ for **asynchronous** stream processing with **non-blocking** **backpressure**.
* **Reactor**: A library implementing the Reactive Streams specification.

    * Comparable to how **Hibernate implements JPA**.
    * Provides concrete implementations of the `Publisher` interface defined in Reactive Streams.

---

### 2. The Publisher Interface

* A **Publisher** is a provider of a potentially unbounded number of sequenced elements, publishing them according to demand from a `Subscriber`.
* In Reactor, the two main `Publisher` implementations are:

    * **Mono**
    * **Flux**

* A `Mono<T>` can emit:

    * **0 or 1 item**
    * Followed by either:

        * **onComplete()** (successful completion)
        * **onError(Throwable)** (error signal)

#### Behavior:

* **One item scenario**:

    * If the `Mono` can emit exactly one item, it will pass that item downstream and then call `onComplete()`.
    * Example: Querying a database by primary key and finding a matching record.
* **No item scenario**:

    * If no data is available, `Mono` completes without emitting any item (`onComplete()` directly).
    * Example: Database query returns no rows.
* **Error scenario**:

    * If an exception occurs during data retrieval or processing, `Mono` signals `onError(Throwable)`.
* **Backpressure**:

    * Even if the subscriber requests many items (e.g., 5 or 10), `Mono` emits at most **1 item**, then completes.

---

* A `Flux<T>` can emit:

    * **0 to N items**
    * Followed by either:

        * **onComplete()** (when stream finishes)
        * **onError(Throwable)** (when an error occurs)

#### Behavior:

* **Finite streams**:

    * Flux can represent a finite dataset, e.g., rows from a database query.
    * Once all items are emitted, it calls `onComplete()`.
* **Infinite streams**:

    * Flux can also represent infinite or unbounded streams.
    * Example: Streaming live Bitcoin price updates every second.
* **Demand-driven (backpressure aware)**:

    * Subscriber explicitly requests how many items it wants (`request(n)`), and Flux delivers exactly that many (if available).
    * Example:

        * Subscriber requests 3 → Flux emits 3 items.
        * Subscriber later requests 5 → Flux emits 5 more items.
* **Completion or cancellation**:

    * The stream ends either when:

        * Publisher calls `onComplete()` after emitting all items.
        * Subscriber cancels subscription when it no longer needs data.
* **Error scenario**:

    * If an exception occurs, Flux signals it via `onError(Throwable)` and the stream is terminated.

---

### Summary – Mono vs Flux

| Feature           | Mono                                 | Flux                                              |
| ----------------- | ------------------------------------ | ------------------------------------------------- |
| Number of items   | 0 or 1                               | 0 to N (finite or infinite)                       |
| Completion signal | `onComplete()` or `onError`          | `onComplete()` or `onError`                       |
| Example use cases | Fetch single record, async response  | Streaming prices, multiple DB rows, event streams |
| Backpressure      | At most 1 item, regardless of demand | Emits as many as subscriber requests              |

---

👉 In essence:

* **Mono** = a promise of a single result (or empty).
* **Flux** = a stream of multiple results, possibly unbounded.

---

# Why Do We Need **Mono** When We Already Have **Flux**?

### 1. Core Idea

* At first glance, it may look like **Flux** (which can emit 0…N items) is enough to cover all use cases, including single-item scenarios.
* However, **Mono** exists for **convenience, clarity, and simplicity** when you know the publisher will emit at most **one item**.

---

### 2. Real-World Analogy (Spring Data JPA Example)

* **Query by Primary Key (ID)**:

    * By definition, a primary key uniquely identifies a record.
    * Expected results: either **one record** (if ID exists) or **zero records** (if ID doesn’t exist).
    * In traditional Spring Data JPA, the method would return `Optional<Customer>`.
    * In **Spring Reactive Repository**, the return type is naturally `Mono<Customer>`.

* **Query by First Name**:

    * First name is usually not unique.
    * Expected results: zero, one, or multiple records.
    * In JPA, the method would return `List<Customer>` or `Stream<Customer>`.
    * In **Spring Reactive Repository**, the return type is `Flux<Customer>`.

---

### 3. Functional Differences Between Mono and Flux

| Aspect            | Mono                         | Flux                                         |
| ----------------- | ---------------------------- | -------------------------------------------- |
| Number of items   | 0 or 1                       | 0…N (finite or infinite)                     |
| Stream processing | Not applicable (no stream)   | Has rich operators for handling data streams |
| Backpressure      | Not relevant (only one item) | Important (producer may overwhelm consumer)  |
| Use case          | Simple request-response      | Continuous or batch data streaming           |
| Complexity        | Lightweight, straightforward | More powerful but requires careful handling  |

* **Clarity of intent**:

    * When you see `Mono<Customer>`, *at most one customer*.
    * When you see `Flux<Customer>`, *zero, one, or many customers*.
* **Simplicity**:

    * Mono avoids unnecessary stream-handling overhead (no need for backpressure, buffering, etc.).
* **Consistency with traditional programming models**:

    * `Mono` is analogous to `Optional<T>` or `Future<T>`.
    * `Flux` is analogous to `List<T>`, `Stream<T>`, or an event queue.
* **Cleaner API usage**:

    * Reactor provides dedicated `Mono` methods like `.map()`, `.flatMap()`, `.switchIfEmpty()`, `.defaultIfEmpty()`, which fit perfectly for single-response workflows.

---

### 5. Backpressure Note

* **Backpressure** = occurs when the producer emits data faster than the consumer can process.
* Since **Mono** emits only one item, backpressure is not a concern.
* Since **Flux** can emit many items, it comes with additional operators (`limitRate()`, `onBackpressureBuffer()`, etc.) to handle producer-consumer balance.


👉 Both are **asynchronous, non-blocking, and Reactive**, but **Mono is simpler and more lightweight** for single-item use cases.

---

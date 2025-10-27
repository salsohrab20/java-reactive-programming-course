## 🧠 **Reactor Operator: `merge()`**

---

### **1. Introduction**

In this lecture, we’ll talk about another important combining operator — **`merge()`**.

So far, we’ve learned:

* `startWith()` → executes publishers **in reverse order** (prepends values).
* `concatWith()` → executes publishers **sequentially**, one after another.

Now, let’s see how **`merge()`** differs — it subscribes to **all publishers at the same time**.

---

### **2. Concept of `merge()`**

Let’s imagine we have three publishers:

```java
producer1, producer2, producer3
```

If we use `merge()`, instead of subscribing to these one by one,
we can merge them into **one combined publisher**.

When the subscriber subscribes to the merged publisher:

* All three producers start **simultaneously**.
* Emissions are interleaved — there is **no guaranteed order**.
* Whoever emits first, the subscriber receives first.
* If the subscriber cancels, all upstream producers are canceled at once.

---

### **3. Example Setup**

Create a class called **`MergeExample`** (under section09).
Add the logger and define three producers:

```java
private static Flux<Integer> producer1() {
    return Flux.just(1, 2, 3)
        .delayElements(Duration.ofMillis(50))
        .transform(Util.fluxLogger("Producer1"));
}

private static Flux<Integer> producer2() {
    return Flux.just(51, 52, 53)
        .delayElements(Duration.ofMillis(30))
        .transform(Util.fluxLogger("Producer2"));
}

private static Flux<Integer> producer3() {
    return Flux.just(111, 112, 113)
        .delayElements(Duration.ofMillis(40))
        .transform(Util.fluxLogger("Producer3"));
}
```

Here each producer emits values at different intervals.
The `transform(Util.fluxLogger(...))` utility prints logs for subscription, cancellation, and completion.

---

### **4. Merging Multiple Publishers**

```java
public static void demo1() throws InterruptedException {
    Flux<Integer> mergedFlux = Flux.merge(producer1(), producer2(), producer3());

    mergedFlux.subscribe(Util.subscriber("MergedSubscriber"));

    Util.sleepSeconds(3);
}
```

✅ **Key Point:**

* `Flux.merge()` subscribes to **all publishers concurrently**.
* The output order depends on which producer emits data faster.

---

### **5. Example Output**

Example output (will vary each run):

```
Subscribing to Producer1
Subscribing to Producer2
Subscribing to Producer3

Producer2 → 51
Producer3 → 111
Producer1 → 1
Producer3 → 112
Producer2 → 52
Producer1 → 2
Producer3 → 113
Producer2 → 53
Producer1 → 3

Completed Producer1
Completed Producer2
Completed Producer3
```

✅ **Observation:**

* All producers start **simultaneously**.
* The subscriber receives items in **unpredictable order**.
* Order depends on emission timing (thread scheduling, delay, etc.).

---

### **6. Behavior with `take()` Operator**

Let’s say the subscriber is interested in **only 2 items**:

```java
Flux.merge(producer1(), producer2(), producer3())
    .take(2)
    .subscribe(Util.subscriber("MergedSubscriber"));
```

**Output:**

```
Subscribing to Producer1
Subscribing to Producer2
Subscribing to Producer3

Received 51
Received 111
Complete

Canceling Producer1
Canceling Producer2
Canceling Producer3
```

✅ **Explanation:**

* As soon as 2 items are received, `take(2)` sends a **cancel signal** to all producers.
* All active publishers are stopped immediately.
* This demonstrates **coordinated cancellation** across merged streams.

---

### **7. Alternative API: `mergeWith()`**

Instead of `Flux.merge()`, we can chain merges:

```java
producer2()
    .mergeWith(producer1())
    .mergeWith(producer3())
    .subscribe(Util.subscriber("MergedSubscriber"));
```

Behavior is the same — all publishers run concurrently,
and item order is still **non-deterministic**.

---

### **8. `merge()` vs `concat()` vs `startWith()`**

| Operator       | Subscription Behavior | Execution Order                | Output Order      | Use Case                            |
| -------------- | --------------------- | ------------------------------ | ----------------- | ----------------------------------- |
| `startWith()`  | Sequential            | Prepends additional data first | Deterministic     | Add defaults or cached data         |
| `concatWith()` | Sequential            | One after another              | Deterministic     | Ordered data combination            |
| `merge()`      | Concurrent            | All together                   | Non-deterministic | Combine live or independent streams |

---

### **9. Real-Life Use Case of `merge()`**

| Scenario                   | Example                                                   |
| -------------------------- | --------------------------------------------------------- |
| **Multiple data sources**  | Combine data from cache, database, and API simultaneously |
| **Real-time streams**      | Merge multiple live event feeds (e.g., chat, sensor data) |
| **Parallel I/O calls**     | Fetch independent data asynchronously for faster response |
| **Reactive microservices** | Merge service responses when order does not matter        |

**Example:**

```java
Flux.merge(
    getProductDetails(),
    getRecommendations(),
    getShippingInfo()
).subscribe(...);
```

All services are invoked in parallel, and results are processed as soon as they arrive.

---

### **10. Key Takeaways**

* `merge()` subscribes to **all publishers concurrently**.
* Emissions are **interleaved and unordered**.
* Great for **independent, parallel data sources**.
* Combine with `take()` for bounded consumption.
* Cancellation and completion are propagated to all active streams.
* Use `mergeWith()` for chaining and `Flux.merge()` for combining multiple publishers.

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                     | Key Points                                               |
| ----- | ------------------------- | -------------------------------------------------------- |
| 1     | **Intro: `merge()`**      | Merges multiple publishers into one                      |
| 2     | **Concept**               | Subscribes to all sources concurrently                   |
| 3     | **Setup**                 | Example with 3 producers emitting at different rates     |
| 4     | **Demo 1: Basic Merge**   | `Flux.merge(p1, p2, p3)`                                 |
| 5     | **Behavior**              | Output order not guaranteed — depends on emission speed  |
| 6     | **Demo 2: With `take()`** | Early cancellation stops all producers                   |
| 7     | **Alternate Syntax**      | Use `mergeWith()` for chaining                           |
| 8     | **Comparison Table**      | StartWith vs ConcatWith vs Merge                         |
| 9     | **Real-Life Use Cases**   | Combine APIs, parallel data fetch, real-time feeds       |
| 10    | **Takeaway**              | `merge()` = parallelism and speed, but unordered results |

---
## 🧠 **Real-World Use Case for `merge()` — Flight Booking Example**

---

### **1. Introduction**

In the previous lecture, we discussed how the **`merge()`** operator works:
it subscribes to multiple publishers **simultaneously**, emitting items as soon as any of them produces data.

Now, let’s explore a **real-world scenario** where this concurrent behavior is extremely useful.

---

### **2. The Scenario – Flight Search Applications**

Think of popular **flight search and booking platforms** like:
✈️ *Kayak*, *Skyscanner*, or *Google Flights.*

When a user searches for flights from *New York → Paris* on a specific date:

* The system must contact **multiple airline APIs** (e.g., Emirates, Qatar Airways, American Airlines, etc.)
* Each airline acts as a **separate data producer**
* You cannot afford to call each airline **one after another** (sequentially)
* You must call **all of them simultaneously**

Each airline will respond at different speeds depending on its API latency.
So we’ll take whatever results come back **first** and present them to the user.

✅ Therefore, the **`merge()` operator** is perfect for this situation.

---

### **3. Implementation Plan**

We’ll simulate this in our reactive example:

1. **Create a `Flight` record** – to represent flight information.
2. **Create multiple airline classes** (Emirates, Qatar, AmericanAirlines) — each acting as a producer.
3. **Create a `KayakService`** — merges all airline publishers and enforces a time limit.
4. **Create a `MergeUseCase` client** — subscribes to the service and prints results.

---

### **4. Step 1: Flight Record**

```java
public record Flight(String airline, int price) { }
```

A simple data model containing:

* `airline`: the carrier name
* `price`: a randomly generated ticket price

---

### **5. Step 2: Airline Producers**

Each airline will emit random flight prices with random delays (simulating different response times).

#### ✈ Emirates.java

```java
public class Emirates {

    private static final String AIRLINE = "Emirates";

    public static Flux<Flight> getFlights() {
        return Flux.range(1, Util.faker().random().nextInt(2, 10))
            .delayElements(Duration.ofMillis(Util.faker().random().nextInt(200, 1000)))
            .map(i -> new Flight(AIRLINE, Util.faker().random().nextInt(300, 2000)))
            .transform(Util.fluxLogger(AIRLINE));
    }
}
```

#### ✈ Qatar.java

```java
public class Qatar {

    private static final String AIRLINE = "Qatar";

    public static Flux<Flight> getFlights() {
        return Flux.range(1, Util.faker().random().nextInt(3, 5))
            .delayElements(Duration.ofMillis(Util.faker().random().nextInt(300, 800)))
            .map(i -> new Flight(AIRLINE, Util.faker().random().nextInt(400, 900)))
            .transform(Util.fluxLogger(AIRLINE));
    }
}
```

#### ✈ AmericanAirlines.java

```java
public class AmericanAirlines {

    private static final String AIRLINE = "American Airlines";

    public static Flux<Flight> getFlights() {
        return Flux.range(1, Util.faker().random().nextInt(5, 10))
            .delayElements(Duration.ofMillis(Util.faker().random().nextInt(200, 1200)))
            .map(i -> new Flight(AIRLINE, Util.faker().random().nextInt(300, 1200)))
            .transform(Util.fluxLogger(AIRLINE));
    }
}
```

Each airline emits flights asynchronously — at different speeds — just like real APIs.

---

### **6. Step 3: KayakService (Aggregator)**

This class represents the **business logic** that merges all airline publishers
and applies a **time constraint** to avoid long waits.

```java
public class KayakService {

    public Flux<Flight> getFlights() {
        return Flux.merge(
                AmericanAirlines.getFlights(),
                Emirates.getFlights(),
                Qatar.getFlights()
            )
            .take(Duration.ofSeconds(2)); // Wait max 2 seconds
    }
}
```

✅ **Explanation:**

* `Flux.merge()` → Calls all airlines **at the same time**
* `take(Duration.ofSeconds(2))` → Collect results for **only 2 seconds**
* After 2 seconds → Automatically **cancel remaining calls**

So, whoever returns flight data within that window — we’ll show those results to the user.

---

### **7. Step 4: MergeUseCase (Client)**

```java
public class MergeUseCase {

    public static void main(String[] args) throws InterruptedException {
        KayakService service = new KayakService();

        service.getFlights()
            .subscribe(Util.subscriber("User"));

        Util.sleepSeconds(3); // block to see output
    }
}
```

---

### **8. Sample Output**

```
Subscribing to Emirates
Subscribing to Qatar
Subscribing to American Airlines

Emirates -> Flight{airline='Emirates', price=1200}
Qatar -> Flight{airline='Qatar', price=650}
American Airlines -> Flight{airline='American Airlines', price=970}
Qatar -> Flight{airline='Qatar', price=550}
Emirates -> Flight{airline='Emirates', price=900}

[2 seconds later...]
Completed: Timed out waiting for flights
Cancelling all active publishers
```

✅ The system:

* Subscribed to all airlines at once
* Received responses **in random order** (depending on who responded first)
* Stopped after 2 seconds and canceled all remaining publishers

---

### **9. Why `merge()` is Perfect Here**

| Requirement                           | Solution with `merge()`                      |
| ------------------------------------- | -------------------------------------------- |
| Call multiple airlines simultaneously | `Flux.merge()` subscribes concurrently       |
| Unpredictable response order          | Emits items as they arrive                   |
| Don’t block user forever              | Use `take(Duration)` to limit waiting time   |
| Combine all responses in one stream   | Merged publisher aggregates all results      |
| Reactive cancellation                 | After timeout, cancels all active publishers |

---

### **10. Key Takeaways**

* `merge()` is ideal for **parallel I/O calls** where order doesn’t matter.
* Each publisher (airline) operates independently.
* Results are **emitted as soon as available**.
* `take(Duration)` enforces a timeout — great for **responsive UX**.
* This pattern models **aggregator microservices** (like flight or hotel search engines).

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                                  | Key Points                                       |
| ----- | -------------------------------------- | ------------------------------------------------ |
| 1     | **Real-World Use Case: Flight Search** | Multiple airlines queried in parallel            |
| 2     | **Problem**                            | Sequential calls are too slow                    |
| 3     | **Solution**                           | Use `Flux.merge()` to call all simultaneously    |
| 4     | **Airline Producers**                  | Each emits random data with delays               |
| 5     | **KayakService**                       | Merges all publishers, limits time window        |
| 6     | **`take(Duration)`**                   | Cancels remaining publishers after timeout       |
| 7     | **Output**                             | Concurrent results, random order                 |
| 8     | **Benefits**                           | Fast, non-blocking, real-time aggregation        |
| 9     | **Reactive Pattern**                   | Aggregator microservice design                   |
| 10    | **Takeaway**                           | `merge()` = parallel efficiency + responsiveness |

---




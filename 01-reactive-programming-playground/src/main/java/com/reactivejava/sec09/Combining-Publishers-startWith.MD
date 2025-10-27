## 🧩 **Combining Multiple Publishers**

---

### **Introduction**

**Topic:** How to combine multiple publishers in Project Reactor
**Why important:**

* Real-world microservice calls rarely rely on a single data source
* You’ll often need to aggregate responses from multiple services

---

### ** The Real-World Scenario**

**Imagine an e-commerce application (e.g., Amazon):**
When a user views a product:

* The frontend sends **one request**
* Backend must call **multiple services** to prepare the full response

---

### **Data Sources for Product View**

| Data Type       | Source                 | Reactive Type          |
| --------------- | ---------------------- | ---------------------- |
| Product details | Product Service        | `Mono<Product>`        |
| Recommendations | Recommendation Service | `Flux<Recommendation>` |
| Shipping Info   | Shipping Service       | `Mono<ShippingInfo>`   |
| Reviews         | Review Service         | `Flux<Review>`         |

➡️ Each is a **separate reactive stream** that must be combined into one response.

---

### **Slide 4: Reactive Composition Example**

```java
Mono<Product> productMono = productService.getProduct(id);
Mono<ShippingInfo> shippingMono = shippingService.getShippingInfo(id);
Flux<Recommendation> recFlux = recommendationService.getRecommendations(id);

Mono<ProductViewResponse> combined =
    Mono.zip(productMono, shippingMono, recFlux.collectList())
        .map(tuple -> new ProductViewResponse(
            tuple.getT1(), tuple.getT2(), tuple.getT3()
        ));
```

✅ Combines multiple async calls
✅ Waits for all responses
✅ Emits one aggregated result

---

### **Slide 5: Why Combine Publishers?**

* To **aggregate** results from multiple sources
* To **coordinate asynchronous calls**
* To **maintain non-blocking flow**
* To **simplify complex business workflows**

---

### **Slide 6: Reactor Operators for Combination**

| Operator          | Description                              | Use Case                  |
| ----------------- | ---------------------------------------- | ------------------------- |
| `zip()`           | Combine results when all publishers emit | Aggregate responses       |
| `merge()`         | Interleave emissions as they arrive      | Combine fast streams      |
| `concat()`        | Sequentially combine publishers          | Ordered processing        |
| `combineLatest()` | Emit when any publisher updates          | Live dashboards / streams |
| `flatMap()`       | Flatten and transform async results      | Dependent API calls       |

---

### **Slide 7: Sequence vs Parallel**

* **Sequential (concat)** → one after another
* **Parallel (merge/zip)** → simultaneous execution
* Choose based on **dependency** and **data consistency**

---

### **Slide 8: Key Takeaways**

* Real-world microservices involve **multiple reactive sources**
* Reactor provides operators to **combine, merge, or orchestrate** them
* Use appropriate combination based on:

    * Business logic
    * Data dependency
    * Performance goals

---

## 🧠 **Reactor Operator: `startWith()`**

---

### **1. Introduction**

In this section, we’ll explore the **`startWith()` operator** in Project Reactor.

This operator allows us to **prepend data or publishers** before the main reactive stream begins emitting items.
It’s particularly useful when you want your stream to start with some default or initial values before the actual data source emits its items.

---

### **2. Concept and Behavior**

Let’s imagine we have two publishers — both returning a `Flux<T>`, where `T` could be any type (e.g., `Integer`, `Customer`, etc.).

```java
Flux<Integer> publisher1;
Flux<Integer> publisher2;
```

Using the `startWith()` operator, we can connect these publishers so that they act as **one single publisher** to the subscriber.

From the **subscriber’s perspective**, there is only **one publisher** emitting data of type `T`.

---

### **3. How `startWith()` Works**

When a subscriber subscribes to this combined publisher:

1. The subscriber first starts receiving items **from the publisher passed into `startWith()`**.
2. Once that publisher completes, the subscription moves **upstream** to the next publisher.
3. This continues until all publishers have emitted their items or the subscriber cancels.

👉 If the subscriber requests only a limited number of items (`take(n)`),
the upstream publishers will **stop producing** once that demand is met — preventing unnecessary work.

---

### **4. Example 1 – Simple Numbers**

```java
private static Flux<Integer> producer1() {
    return Flux.just(1, 2, 3)
        .doOnSubscribe(s -> System.out.println("Subscribing to Producer 1"))
        .delayElements(Duration.ofMillis(10));
}

public static void main(String[] args) throws InterruptedException {
    producer1()
        .subscribe(System.out::println);
    Thread.sleep(3000);
}
```

**Output:**

```
Subscribing to Producer 1
1
2
3
Complete
```

---

### **5. Example 2 – Using `startWith()` to Prepend Values**

Now, suppose we want to **emit `-1` and `0` first** before the producer emits its own items.

```java
producer1()
    .startWith(-1, 0)
    .subscribe(System.out::println);
```

**Output:**

```
-1
0
Subscribing to Producer 1
1
2
3
Complete
```

✅ `startWith()` first emits hardcoded values
✅ Then the main `producer1` is subscribed and emits its items

---

### **6. Behavior with `take()` Operator**

Let’s add a `take(2)` operator to limit consumption:

```java
producer1()
    .startWith(-1, 0)
    .take(2)
    .subscribe(System.out::println);
```

**Output:**

```
-1
0
Complete
```

👉 The subscriber received only two items.
👉 Notice that `producer1()` was **never subscribed** — since demand was satisfied early.
This shows how **lazy evaluation** works in Reactor.

---

### **7. Example 3 – `startWith()` Using Iterable**

You can also prepend an **iterable collection**:

```java
List<Integer> numbers = List.of(-2, -1, 0);
producer1()
    .startWith(numbers)
    .subscribe(System.out::println);
```

**Output:**

```
-2
-1
0
1
2
3
Complete
```

---

### **8. Example 4 – `startWith()` Another Publisher**

Let’s create another publisher:

```java
private static Flux<Integer> producer2() {
    return Flux.just(51, 52, 53)
        .doOnSubscribe(s -> System.out.println("Subscribing to Producer 2"));
}
```

Now we can prepend `producer2` before `producer1`:

```java
producer1()
    .startWith(producer2())
    .subscribe(System.out::println);
```

**Output:**

```
Subscribing to Producer 2
51
52
53
Subscribing to Producer 1
1
2
3
Complete
```

🔹 The request flow moves **from bottom to top**.
`startWith()` executes the lower (inner) publisher first, then moves upstream.

---

### **9. Example 5 – Multiple `startWith()` Calls**

You can chain multiple `startWith()` calls:

```java
producer1()
    .startWith(producer2())
    .startWith(1000)
    .subscribe(System.out::println);
```

**Output:**

```
1000
Subscribing to Producer 2
51
52
53
Subscribing to Producer 1
1
2
3
Complete
```

Order of execution:

1. Emit `1000`
2. Emit values from `producer2`
3. Emit values from `producer1`
---

## ✅ **Key Takeaways**

* `startWith()` **prepends** data or another publisher before the main stream.
* Execution order flows **from bottom to top** (last `startWith()` executes first).
* Works well with:

    * **Static values** (`startWith(value1, value2)`)
    * **Collections** (`startWith(Iterable)`)
    * **Other publishers** (`startWith(Publisher)`)
* When combined with **`take()`**, it avoids unnecessary subscriptions (lazy behavior).
* Useful for:

    * Adding default or seed data
    * Combining fallback or cached publishers
    * Maintaining predictable sequence of emissions

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                      | Key Points                                               |
| ----- | -------------------------- | -------------------------------------------------------- |
| 1     | **Intro: `startWith()`**   | Prepends items/publishers before main Flux begins        |
| 2     | **Basic Behavior**         | Subscriber sees one combined publisher                   |
| 3     | **Simple Example**         | `Flux.just(1,2,3).startWith(-1,0)` emits `-1,0,1,2,3`    |
| 4     | **Lazy Subscription**      | With `take(2)`, upstream not invoked                     |
| 5     | **Iterable Support**       | Can prepend a list of items                              |
| 6     | **Publisher Composition**  | `startWith(producer2)` runs 2 → 1                        |
| 7     | **Multiple `startWith()`** | Executes bottom to top                                   |
| 8     | **Assignment Example**     | Build sequence 49–53 → 0–3                               |
| 9     | **Use Cases**              | Add default data, cached publishers, or fallback streams |
| 10    | **Takeaway**               | Simple but powerful tool for controlled emission order   |

---
### **2. Scenario Setup: Name Generator**

We’ll create a helper class named `NameGenerator` (under `section09.helper`) to simulate a **time-consuming data generation process**, such as fetching or computing data from an external source.

```java
public class NameGenerator {

    private static final Logger log = LoggerFactory.getLogger(NameGenerator.class);

    public Flux<String> generateNames() {
        return Flux.generate(sink -> {
            log.info("Generating name...");
            Util.sleepSeconds(1); // simulate heavy computation
            String name = Util.faker().name().firstName();
            sink.next(name);
        });
    }
}
```

✅ This `Flux` emits a random name after simulating a one-second delay.
It represents a **CPU or I/O-heavy operation**, such as calling an API or computing data.

---

### **3. Multiple Subscribers Example**

Now let’s create another class called `StartWithUseCase`.

Here, we’ll have **multiple subscribers** (Sam, Mike, and Jake) interested in getting names from our generator.

```java
public class StartWithUseCase {

    public static void main(String[] args) {
        NameGenerator generator = new NameGenerator();

        // Sam wants 2 names
        generator.generateNames()
            .take(2)
            .subscribe(Util.subscriber("Sam"));

        // Mike wants 2 names
        generator.generateNames()
            .take(2)
            .subscribe(Util.subscriber("Mike"));

        // Jake wants 3 names
        generator.generateNames()
            .take(3)
            .subscribe(Util.subscriber("Jake"));
    }
}
```

🧩 **Problem:**
Each subscriber triggers the **entire name generation process** independently.
That means:

* The `generate()` function runs multiple times.
* Each subscription causes a new one-second delay per name.
* Total execution time increases drastically.

In real applications, this is **inefficient**, especially when:

* The same data is reused often, and
* The generation or retrieval is expensive (e.g., database call, remote API, complex computation).

---

### **4. Real-World Analogy**

Imagine you’re fetching product details from a third-party API or generating a report.
Each call takes time, but the results don’t change frequently.

👉 Ideally, you’d want to **cache the results** and serve them directly to new subscribers without recalculating every time.

---

### **5. Introducing Cache with `startWith()`**

We can simulate a cache using an in-memory `List<String>` (in real projects, this could be Redis, Memcached, or a local in-memory store).

Here’s the updated `NameGenerator`:

```java
public class NameGenerator {

    private static final Logger log = LoggerFactory.getLogger(NameGenerator.class);
    private final List<String> redisCache = new ArrayList<>(); // simulated cache

    public Flux<String> generateNames() {
        return Flux.generate(sink -> {
            log.info("Generating new name...");
            Util.sleepSeconds(1);
            String name = Util.faker().name().firstName();
            redisCache.add(name); // store in cache
            sink.next(name);
        })
        .startWith(redisCache); // emit cached names first
    }
}
```

### **6. Explanation**

* The **cache (`redisCache`)** stores previously generated names.
* `startWith(redisCache)` ensures that before generating new names,
  the publisher **first emits all names currently in cache**.
* If the subscriber still requests more data (i.e., `request(n)` > cache size),
  the **`generate()` function** will start producing new names.
* Newly generated names are added back into the cache.

This gives us the best of both worlds:
✅ Fast emission from cache
✅ Automatic fallback to computation when cache is insufficient

---

### **7. Example Execution Flow**

Let’s use the same subscribers again:

```java
NameGenerator generator = new NameGenerator();

generator.generateNames()
    .take(2)
    .subscribe(Util.subscriber("Sam"));

generator.generateNames()
    .take(2)
    .subscribe(Util.subscriber("Mike"));

generator.generateNames()
    .take(3)
    .subscribe(Util.subscriber("Jake"));
```

**Execution (Step-by-Step):**

1. **Sam subscribes** → Cache is empty → Generates 2 names → Stores them.
2. **Mike subscribes** → Reads both names from cache → No new generation needed.
3. **Jake subscribes** → Cache has 2 names → Emits both → Still needs one more → Generates 1 new name → Adds it to cache.

---

### **8. Sample Output (Simplified)**

```
Sam: Generating new name...
Sam received: Jeffrey
Sam: Generating new name...
Sam received: Michael
Sam complete

Mike received: Jeffrey
Mike received: Michael
Mike complete

Jake received: Jeffrey
Jake received: Michael
Jake: Generating new name...
Jake received: Robert
Jake complete
```

✅ Total time significantly reduced
✅ Cached names reused
✅ New computation only when necessary

---

### **9. Real-Life Applications of `startWith()`**

| Use Case                    | Description                                                      |
| --------------------------- | ---------------------------------------------------------------- |
| **Caching layer**           | Emit cached results first, then compute new values if needed     |
| **Offline-first apps**      | Serve locally stored data before syncing with backend            |
| **Preload defaults**        | Emit default data while waiting for live data                    |
| **Fallback strategy**       | Emit static/fallback data if network call is delayed or fails    |
| **UI Loading optimization** | Display last known data immediately, then refresh asynchronously |

---

### **10. Key Takeaways**

* `startWith()` can prepend **any data source** — hardcoded, cached, or another publisher.
* Helps implement **read-through cache** patterns in reactive pipelines.
* Reduces redundant computations and improves response time.
* Cache can be local or remote (e.g., Redis, database, memory).
* Combines **data reuse** with **reactive non-blocking flow**.

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                     | Key Points                                                 |
| ----- | ------------------------- | ---------------------------------------------------------- |
| 1     | **Why a Real Use Case?**  | Reuse heavy computations, avoid recomputation              |
| 2     | **Setup: Name Generator** | Simulate a slow data source using `Flux.generate()`        |
| 3     | **Problem**               | Each subscriber triggers recomputation                     |
| 4     | **Solution: Add Cache**   | Use `startWith()` to emit cached items before regeneration |
| 5     | **How It Works**          | Cache first → Generate new only if needed                  |
| 6     | **Code Demo**             | `Flux.generate().startWith(cache)`                         |
| 7     | **Results**               | First subscriber computes → others read from cache         |
| 8     | **Benefits**              | Faster, efficient, reusable                                |
| 9     | **Real-Life Scenarios**   | Offline-first, fallback data, Redis caching, UI preload    |
| 10    | **Takeaway**              | `startWith()` = smart reactive caching pattern             |

---

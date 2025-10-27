## 🧠 **Reactor Operator: `concatWith()`**

---

### **1. Introduction**

In this lecture, let’s explore another combining operator called **`concatWith()`**.

Conceptually, `concatWith()` is **the opposite of `startWith()`** —
it runs the **first publisher completely**, and **only after it completes**, the second publisher begins.

---

### **2. Basic Concept**

Here’s the difference at a glance:

| Operator       | Order of Execution                                                     |
| -------------- | ---------------------------------------------------------------------- |
| `startWith()`  | Executes **the added publisher first**, then the main publisher        |
| `concatWith()` | Executes **the main publisher first**, then the concatenated publisher |

In simple terms:

* **`startWith()`** → “Begin with this first, then continue.”
* **`concatWith()`** → “Finish this first, then move to the next.”

So the flow for `concatWith()` is:

```
Producer1 → (onComplete) → Producer2
```

---

### **3. Project Setup**

Create a new class called **`ConcatWithExample`** (or `ConcatenateWith`) under the respective package.
Add a logger for better visibility of execution flow.

This example will be similar to the previous `startWith()` demo —
so we can reuse the same producers.

---

### **4. Example: Two Simple Producers**

```java
private static Flux<Integer> producer1() {
    return Flux.just(1, 2, 3)
        .delayElements(Duration.ofMillis(10))
        .doOnSubscribe(s -> log.info("Subscribing to Producer 1"));
}

private static Flux<Integer> producer2() {
    return Flux.just(51, 52, 53)
        .delayElements(Duration.ofMillis(10))
        .doOnSubscribe(s -> log.info("Subscribing to Producer 2"));
}
```

---

### **5. Example 1 – Using `concatWithValues()`**

Let’s concatenate **arbitrary values** after a Flux.

```java
public static void demo1() {
    producer1()
        .concatWithValues(100, 200, 300)
        .subscribe(Util.subscriber());
}
```

**Output:**

```
Subscribing to Producer 1
1
2
3
100
200
300
Complete
```

✅ First, all items from `producer1()` are emitted.
✅ Then, once completed, the values passed in `concatWithValues()` are emitted.

⚠️ **Note:** Project Reactor’s API naming is slightly inconsistent here —
instead of `concatWith()`, it’s called `concatWithValues()` when using direct values.

---

### **6. Example 2 – Using `concatWith()` Another Publisher**

You can concatenate another publisher instead of direct values.

```java
public static void demo2() {
    producer1()
        .concatWith(producer2())
        .subscribe(Util.subscriber());
}
```

**Output:**

```
Subscribing to Producer 1
1
2
3
Subscribing to Producer 2
51
52
53
Complete
```

✅ The order is sequential —
Producer 1 emits its items → completes → Producer 2 starts emitting.

---

### **7. Behavior with `take()` Operator**

If we use `take(2)`, only part of the first stream is needed —
so the second publisher is **never subscribed**.

```java
public static void demo2_takeExample() {
    producer1()
        .concatWith(producer2())
        .take(2)
        .subscribe(Util.subscriber());
}
```

**Output:**

```
Subscribing to Producer 1
1
2
Complete
```

✅ The downstream request was satisfied early,
so Producer 2 was **not triggered at all**.
This demonstrates **lazy subscription** — a key feature in Reactor.

---

### **8. Example 3 – Using `Flux.concat()` Factory Method**

Reactor also provides a **factory method** for concatenating multiple publishers:

```java
public static void demo3() {
    Flux.concat(producer1(), producer2())
        .subscribe(Util.subscriber());
}
```

**Output:**

```
Subscribing to Producer 1
1
2
3
Subscribing to Producer 2
51
52
53
Complete
```

✅ Same behavior — executes publishers in the given order
✅ Automatically subscribes one after the previous completes

You can also concatenate **more than two publishers**:

```java
Flux.concat(producer1(), producer2(), producer3());
```

---

### **9. Key Differences Between `startWith()` and `concatWith()`**

| Aspect              | `startWith()`                                           | `concatWith()`                                        |
| ------------------- | ------------------------------------------------------- | ----------------------------------------------------- |
| Order               | Emits prepended values/publisher **before** main stream | Emits appended values/publisher **after** main stream |
| Execution Flow      | New → Original                                          | Original → New                                        |
| Typical Use         | Emit cached or default data first                       | Append follow-up or dependent stream                  |
| Iterable Support    | Supports `Iterable` and direct values                   | Only supports direct values or other publishers       |
| Factory Alternative | –                                                       | `Flux.concat()`                                       |

---

### **10. Real-World Use Cases**

| Use Case                       | Description                                             |
| ------------------------------ | ------------------------------------------------------- |
| **Chaining dependent streams** | Execute one stream, then another sequentially           |
| **Stepwise workflows**         | Complete step 1 before triggering step 2                |
| **Data enrichment**            | Fetch primary data first, then additional related data  |
| **Batch processing**           | Concatenate results from multiple sources sequentially  |
| **Controlled sequencing**      | Maintain strict execution order without merging streams |

---

### **11. Key Takeaways**

* `concatWith()` ensures **sequential, ordered execution** of publishers.
* It **waits for completion** of one before starting the next.
* Ideal when you need **strict control over execution order**.
* Behaves **opposite to `startWith()`** in emission order.
* Supports **lazy subscription** — downstream demand controls activation.

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                                   | Key Points                                                                  |
| ----- | --------------------------------------- | --------------------------------------------------------------------------- |
| 1     | **Intro: `concatWith()`**               | Opposite of `startWith()` — execute first → then next                       |
| 2     | **Basic Flow**                          | `Producer1 → (onComplete) → Producer2`                                      |
| 3     | **API Note**                            | Use `concatWithValues()` for direct values                                  |
| 4     | **Example 1**                           | Emits producer1 values, then appended values                                |
| 5     | **Example 2**                           | Sequentially combines two publishers                                        |
| 6     | **Behavior with `take()`**              | Lazy execution — second publisher skipped if demand met                     |
| 7     | **Factory Method**                      | `Flux.concat(p1, p2, p3)` for multiple publishers                           |
| 8     | **Comparison: startWith vs concatWith** | Prepend vs Append logic                                                     |
| 9     | **Real Use Cases**                      | Chained workflows, ordered operations                                       |
| 10    | **Takeaway**                            | Use `concatWith()` when order matters; use `merge` when parallelism is okay |

---

## 🧠 **Reactor Operator: `concatWith()` – Error Handling Behavior**


### **1. Basic Error Scenario**

We’ll start with three publishers:

```java
private static Flux<Integer> producer1() {
    return Flux.just(1, 2, 3)
        .delayElements(Duration.ofMillis(10))
        .doOnSubscribe(s -> log.info("Subscribing to Producer 1"));
}

private static Flux<Integer> producer2() {
    return Flux.just(51, 52, 53)
        .delayElements(Duration.ofMillis(10))
        .doOnSubscribe(s -> log.info("Subscribing to Producer 2"));
}

// Producer that emits an error immediately
private static Flux<Integer> producer3() {
    return Flux.error(new RuntimeException("Oops! Something went wrong in Producer 3"));
}
```

---

### **3. Example 1 – Regular `concatWith()`**

Now, let’s concatenate these three publishers:

```java
public static void demo1() {
    producer1()
        .concatWith(producer3())  // this one throws error
        .concatWith(producer2())  // will this run?
        .subscribe(Util.subscriber());
}
```

---

### **4. Output & Explanation**

**Output:**

```
Subscribing to Producer 1
1
2
3
Subscribing to Producer 3
Error: Oops! Something went wrong in Producer 3
```

✅ **Observation:**

* Once an **error signal** is emitted by any publisher, the concatenation **stops immediately**.
* `producer2()` is **never invoked**.

In reactive streams, the **error signal is terminal**, just like the **complete signal** —
once sent downstream, no further data or signals can flow.

So:

```
onError() → stops all subsequent publishers
```

---

### **5. Example 2 – Handling Error with `concatDelayError()`**

Reactor provides a variation of concatenation that allows us to **delay error propagation** until all publishers have completed —
called **`Flux.concatDelayError()`**.

Let’s rewrite the same logic:

```java
public static void demo2() {
    Flux.concatDelayError(producer1(), producer3(), producer2())
        .subscribe(Util.subscriber());
}
```

---

### **6. Output & Explanation**

**Output:**

```
Subscribing to Producer 1
1
2
3
Subscribing to Producer 3
Subscribing to Producer 2
51
52
53
Error: Oops! Something went wrong in Producer 3
```

✅ **What happens here:**

* Reactor **subscribes sequentially**: Producer1 → Producer3 → Producer2
* When `producer3` emits an error, Reactor **records the error** but **continues** executing the next publisher (Producer2).
* Only **after all publishers complete**, Reactor finally sends the **error signal downstream**.

This behavior ensures that:

* All available data is emitted.
* The subscriber still receives the error — but **after** consuming all data.

---

### **7. Key Behavioral Differences**

| Operator             | Behavior on Error                                              | Error Timing |
| -------------------- | -------------------------------------------------------------- | ------------ |
| `concatWith()`       | Stops immediately; no further publishers are invoked           | Immediate    |
| `concatDelayError()` | Continues with next publishers, emits error after all complete | Delayed      |

---

### **8. Real-World Use Case**

Imagine you are concatenating multiple data sources — for example:

* `Flux.concat(apiCall1, apiCall2, apiCall3)`
  If one of these sources fails, but others can still produce data,
  `concatDelayError()` ensures that the subscriber still receives **all possible results** before being notified of the failure.

📘 **Example scenario:**

```java
Flux.concatDelayError(
    getPrimaryData(),
    getBackupData(),
    getAuditTrail()
)
.subscribe(...);
```

Even if `getBackupData()` fails,
the system still receives data from `getPrimaryData()` and `getAuditTrail()`
before the error is reported.

---

### **9. Visual Comparison**

```
concatWith() flow:
Producer1 → Producer3(error) 🚫 STOP

concatDelayError() flow:
Producer1 → Producer3(error recorded) → Producer2 → emit error finally
```

---

### **10. Key Takeaways**

* In Reactor, **error is a terminal signal** — it stops further emissions by default.
* `concatWith()` stops the sequence immediately when an error occurs.
* `concatDelayError()` **delays the error propagation**, allowing other publishers to continue.
* Choose `concatDelayError()` when:

    * Data from other sources is still valuable even after one fails.
    * You want complete processing before notifying the subscriber of failure.

---

## 🎞️ **Slide-Friendly Summary**

| Slide | Topic                                | Key Points                                                          |
| ----- | ------------------------------------ | ------------------------------------------------------------------- |
| 1     | **Error Handling in `concatWith()`** | What happens when one publisher emits an error?                     |
| 2     | **Setup**                            | Three publishers – two normal, one error-emitting                   |
| 3     | **Behavior of `concatWith()`**       | Stops immediately after first error                                 |
| 4     | **Demo Output**                      | Error signal halts all further publishers                           |
| 5     | **Introducing `concatDelayError()`** | Continues emitting remaining publishers before sending error        |
| 6     | **Demo Output**                      | Emits all data, then error                                          |
| 7     | **Comparison Table**                 | Immediate stop vs Delayed error                                     |
| 8     | **Real-Life Use Case**               | Fetch data from multiple APIs or files even if one fails            |
| 9     | **Visual Flow**                      | Sequential vs delayed error propagation                             |
| 10    | **Takeaway**                         | Use `concatDelayError()` for graceful recovery & full data delivery |

---


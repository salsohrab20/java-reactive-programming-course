# 🧩 Lecture Notes: Understanding the `then()` Operator in Reactor

---

## 🎯 Learning Objective

To understand:

* What the `then()` operator does,
* Why it’s useful when you only care about **completion**, not emitted data, and
* How to chain publishers **sequentially** using `then()`.

---

## 🧠 Concept Overview

At first glance, `then()` might look like a useless operator —
but it’s actually quite **handy** in specific cases.

Let’s start with the **core idea**:

> The `then()` operator **ignores all onNext values** and waits only for the **completion signal** (or error) from the upstream publisher.

That means:

* You won’t get any of the intermediate results (the emitted items),
* You only know **whether it completed successfully or failed**.

---

## 🧩 Real-World Scenario

Let’s imagine a **database insertion** use case:

* You have a list of records to insert.
* The database driver returns a **Flux<String>**.
* Each `onNext()` emission might say:
  `"Record A saved"`, `"Record B saved"`, etc.

But — in this case, you’re **not interested** in each “saved” message.
You just want to know **when all records are saved successfully**,
or if something **failed**.

That’s exactly what the `then()` operator is made for.

---

## ⚙️ Example Code

```java
public class Lecture15_ThenExample {

    private static final Logger log = LoggerFactory.getLogger(Lecture15_ThenExample.class);

    private Flux<String> saveRecords(List<String> records) {
        return Flux.fromIterable(records)
                   .delayElements(Duration.ofMillis(300))
                   .map(record -> "Saved record: " + record);
    }

    private Mono<Void> sendNotification() {
        return Mono.fromRunnable(() ->
            log.info("All records saved successfully. Sending notification email...")
        );
    }

    public void demo() {
        List<String> records = List.of("A", "B", "C");

        saveRecords(records)
            .then(sendNotification()) // Wait for saveRecords() to complete
            .subscribe(
                null,
                err -> log.error("Error: {}", err.getMessage()),
                () -> log.info("Process completed successfully")
            );

        Util.sleepSeconds(3); // Keep main thread alive
    }
}
```

---

## 🧠 Step-by-Step Explanation

1. **`saveRecords()`** →
   A `Flux<String>` that emits messages like
   `"Saved record A"`, `"Saved record B"`, etc.

2. Normally, if you subscribe directly:
   You’ll receive all these messages.

3. But if you apply `.then(...)` →
   You’re telling Reactor:

   > “Ignore all intermediate emissions,
   > I only care if the stream completes successfully.”

4. Once the `Flux` completes:

    * It triggers the **`Mono`** passed to `then()` (in this case, `sendNotification()`).
    * If there’s an **error**, it skips the next step and directly propagates the error.

---

## ⚙️ Type Transformation

| Input Type | Operator         | Output Type  | Behavior                             |
| ---------- | ---------------- | ------------ | ------------------------------------ |
| `Flux<T>`  | `.then()`        | `Mono<Void>` | Ignores values, waits for completion |
| `Flux<T>`  | `.then(Mono<U>)` | `Mono<U>`    | Triggers next Mono on completion     |

---

## 🧩 Visual Flow

```
[saveRecords()] ──► emits → "Saved A", "Saved B", "Saved C"
                          │
                          │ (then ignores all)
                          ▼
[then(sendNotification())] ──► emits only after completion
```

✅ Output Sequence:

```
INFO  - Saved record: A
INFO  - Saved record: B
INFO  - Saved record: C
INFO  - All records saved successfully. Sending notification email...
INFO  - Process completed successfully
```

---

## ⚠️ What Happens on Error?

Let’s modify the example slightly:

```java
Flux<String> faultySave = Flux.just("A", "B", "C")
    .flatMap(record -> {
        if (record.equals("B")) return Flux.error(new RuntimeException("Failed to save B"));
        return Flux.just("Saved record: " + record);
    });

faultySave
    .then(sendNotification())
    .subscribe(
        null,
        err -> log.error("Error occurred: {}", err.getMessage()),
        () -> log.info("Completed successfully")
    );
```

### 🖥 Output:

```
INFO  - Saved record: A
ERROR - Error occurred: Failed to save B
```

👉 Notice that:

* The **`sendNotification()`** Mono is **never invoked**.
* The error is directly propagated downstream.
* `then()` only proceeds if the upstream **completes successfully**.

---

## 🧩 Why Not Just Chain with `subscribe()`?

You might think:

```java
saveRecords(records).subscribe();
sendNotification().subscribe();
```

But that runs **both publishers concurrently**, not sequentially.
You might end up sending a notification **before** all records are actually saved!

That’s why `then()` is important — it creates a **dependent chain**:

* Do Task A (wait for completion)
* **Then** do Task B.

---

## ⚙️ Additional Use Case — Sequential Task Execution

You can use `then()` to **chain multiple publishers** in order:

```java
Flux<String> step1 = taskService.runStep1();
Flux<String> step2 = taskService.runStep2();
Mono<Void> step3 = taskService.finalizeProcess();

step1
    .then(step2)    // Wait for step1 to complete
    .then(step3)    // Then execute step3
    .subscribe();
```

This ensures each step starts **only after** the previous one **completes**.

---

## 🧭 Summary

| Concept            | Description                                                                |
| ------------------ | -------------------------------------------------------------------------- |
| **Operator**       | `then()`                                                                   |
| **Purpose**        | Waits for completion of a publisher, ignoring its emitted items            |
| **Return Type**    | `Mono<Void>` or `Mono<U>` (if another Mono is passed)                      |
| **Behavior**       | Triggers only after upstream completes successfully                        |
| **Error Handling** | Skips next stage if error occurs                                           |
| **Use Case**       | When you only need to know completion status or chain actions sequentially |

---

## 🧠 Quick Comparison

| Scenario                         | Operator                 | Behavior                          |
| -------------------------------- | ------------------------ | --------------------------------- |
| You care about the emitted items | Use `map()`, `flatMap()` | Process all onNext values         |
| You care only about completion   | Use `then()`             | Wait for complete/error           |
| You want sequential execution    | Use `then()`             | Run next after previous completes |

---

## 🧩 Final Takeaway

* `then()` is **not about transformation**, it’s about **completion**.
* It’s **non-blocking**, and it’s perfect for chaining actions that must happen **after another publisher completes**.
* Think of it like saying:

  > “Do this... and *then* do that — but only if the first one finished successfully.”

---


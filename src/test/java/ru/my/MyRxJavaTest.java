package ru.my;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.my.core.Disposable;
import ru.my.core.Observable;
import ru.my.schedulers.ComputationScheduler;
import ru.my.schedulers.IOScheduler;
import ru.my.schedulers.SingleScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MyRxJavaTest {
    @Test
    @DisplayName("Тест 1: Создание Observable и подписка")
    void testCreateAndSubscribe() {
        List<Integer> received = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable<Integer> observable = Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        });

        Disposable disposable = observable.subscribe(
                received::add,
                error::set,
                () -> completed.set(true)
        );

        assertEquals(3, received.size());
        assertEquals(1, received.get(0));
        assertEquals(2, received.get(1));
        assertEquals(3, received.get(2));
        assertTrue(completed.get());
        assertNull(error.get());
        assertTrue(disposable.isDisposed());
    }

    @Test
    @DisplayName("Тест 2: Оператор map")
    void testMapOperator() {
        List<String> result = new ArrayList<>();

        Observable.just(1, 2, 3, 4, 5)
                .map(n -> n * 2)
                .map(n -> "Число: " + n)
                .subscribe(result::add);

        assertEquals(5, result.size());
        assertEquals("Число: 2", result.get(0));
        assertEquals("Число: 4", result.get(1));
        assertEquals("Число: 6", result.get(2));
        assertEquals("Число: 8", result.get(3));
        assertEquals("Число: 10", result.get(4));
    }

    @Test
    @DisplayName("Тест 3: Оператор filter")
    void testFilterOperator() {
        List<Integer> result = new ArrayList<>();

        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .filter(n -> n % 2 == 0)
                .filter(n -> n > 4)
                .subscribe(result::add);

        assertEquals(3, result.size());
        assertEquals(6, result.get(0));
        assertEquals(8, result.get(1));
        assertEquals(10, result.get(2));
    }

    @Test
    @DisplayName("Тест 4: Оператор flatMap")
    void testFlatMapOperator() {
        List<String> result = new ArrayList<>();

        Observable.just("Hello World", "RxJava Rules")
                .flatMap(line -> {
                    String[] words = line.split(" ");
                    return Observable.<String>create(emitter -> {
                        for (String word : words) {
                            emitter.onNext(word);
                        }
                        emitter.onComplete();
                    });
                })
                .map(String::toUpperCase)
                .subscribe(result::add);

        assertEquals(4, result.size());
        assertTrue(result.contains("HELLO"));
        assertTrue(result.contains("WORLD"));
        assertTrue(result.contains("RXJAVA"));
        assertTrue(result.contains("RULES"));
    }

    @Test
    @DisplayName("Тест 5: Комбинация map и filter")
    void testMapAndFilterCombination() {
        List<Integer> result = new ArrayList<>();

        Observable.just(1, 2, 3, 4, 5, 6)
                .map(n -> n * n)
                .filter(square -> square > 10)
                .subscribe(result::add);

        assertEquals(3, result.size());
        assertEquals(16, result.get(0));
        assertEquals(25, result.get(1));
    }

    @Test
    @DisplayName("Тест 6: Обработка ошибок")
    void testErrorHandling() {
        List<String> result = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<String>create(emitter -> {
                    emitter.onNext("Первый");
                    emitter.onError(new RuntimeException("Тестовая ошибка"));
                    emitter.onNext("Второй");
                })
                .subscribe(
                        result::add,
                        error::set,
                        () -> completed.set(true)
                );

        assertEquals(1, result.size());
        assertEquals("Первый", result.get(0));
        assertNotNull(error.get());
        assertEquals("Тестовая ошибка", error.get().getMessage());
        assertFalse(completed.get());
    }

    @Test
    @DisplayName("Тест 7: Восстановление после ошибки")
    void testOnErrorResumeNext() {
        List<String> result = new ArrayList<>();

        Observable.<String>create(emitter -> {
                    emitter.onNext("Начало");
                    emitter.onError(new RuntimeException("Ошибка"));
                })
                .onErrorResumeNext(error -> Observable.just("Fallback 1", "Fallback 2"))
                .subscribe(result::add);

        assertEquals(3, result.size());
        assertEquals("Начало", result.get(0));
        assertEquals("Fallback 1", result.get(1));
        assertEquals("Fallback 2", result.get(2));
    }

    @Test
    @DisplayName("Тест 8: Отмена подписки")
    void testDisposable() throws InterruptedException {
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicBoolean completed = new AtomicBoolean(false);

        Disposable disposable = Observable.create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        if (emitter.isDisposed()) {
                            return;
                        }
                        emitter.onNext(i);
                        Thread.sleep(50);
                    }
                    emitter.onComplete();
                })
                .subscribe(
                        value -> receivedCount.incrementAndGet(),
                        Throwable::printStackTrace,
                        () -> completed.set(true)
                );

        Thread.sleep(150);
        disposable.dispose();
        Thread.sleep(200);

        assertTrue(disposable.isDisposed());
    }

    @Test
    @DisplayName("Тест 9: subscribeOn с разными Scheduler")
    void testSubscribeOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable.create(emitter -> {
                    threadName.set(Thread.currentThread().getName());
                    emitter.onNext("data");
                    emitter.onComplete();
                })
                .subscribeOn(new IOScheduler())
                .subscribe(
                        data -> {},
                        Throwable::printStackTrace,
                        latch::countDown
                );

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(threadName.get().contains("io-thread"));
    }

    @Test
    @DisplayName("Тест 10: observeOn с разными Scheduler")
    void testObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();

        Observable.just("test")
                .observeOn(new ComputationScheduler())
                .subscribe(
                        data -> threadName.set(Thread.currentThread().getName()),
                        Throwable::printStackTrace,
                        latch::countDown
                );

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(threadName.get().contains("computation-thread"));
    }

    @Test
    @DisplayName("Тест 11: Комбинация subscribeOn и observeOn")
    void testSubscribeOnAndObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        AtomicReference<String> observeThread = new AtomicReference<>();

        Observable.create(emitter -> {
                    subscribeThread.set(Thread.currentThread().getName());
                    emitter.onNext("data");
                    emitter.onComplete();
                })
                .subscribeOn(new IOScheduler())
                .observeOn(new SingleScheduler())
                .subscribe(
                        data -> observeThread.set(Thread.currentThread().getName()),
                        Throwable::printStackTrace,
                        latch::countDown
                );

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertTrue(subscribeThread.get().contains("io-thread"));
        assertTrue(observeThread.get().contains("single-thread"));
        assertNotEquals(subscribeThread.get(), observeThread.get());
    }

    @Test
    @DisplayName("Тест 12: Observable.just с несколькими элементами")
    void testJust() {
        List<Integer> result = new ArrayList<>();

        Observable.just(10, 20, 30, 40)
                .subscribe(result::add);

        assertEquals(4, result.size());
        assertEquals(10, result.get(0));
        assertEquals(20, result.get(1));
        assertEquals(30, result.get(2));
        assertEquals(40, result.get(3));
    }

    @Test
    @DisplayName("Тест 13: Пустой Observable")
    void testEmptyObservable() {
        List<Object> result = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.create(emitter -> {
                    emitter.onComplete();
                })
                .subscribe(
                        result::add,
                        Throwable::printStackTrace,
                        () -> completed.set(true)
                );

        assertEquals(0, result.size());
        assertTrue(completed.get());
    }

    @Test
    @DisplayName("Тест 14: Обработка исключений в map")
    void testExceptionInMap() {
        List<Integer> result = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        Observable.just(1, 2, 3)
                .map(n -> {
                    if (n == 2) {
                        throw new RuntimeException("Ошибка в map");
                    }
                    return n * 10;
                })
                .subscribe(
                        result::add,
                        error::set,
                        () -> {}
                );

        assertEquals(1, result.size());
        assertEquals(10, result.get(0));
        assertNotNull(error.get());
        assertEquals("Ошибка в map", error.get().getMessage());
    }

    @Test
    @DisplayName("Тест 15: Многопоточная обработка")
    void testMultithreading() throws InterruptedException {
        int itemCount = 50;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicInteger sentCount = new AtomicInteger(0);

        Observable.<Integer>create(emitter -> {
                    for (int i = 0; i < itemCount; i++) {
                        sentCount.incrementAndGet();
                        emitter.onNext(i);
                        Thread.sleep(1);
                    }
                    emitter.onComplete();
                })
                .subscribeOn(new ComputationScheduler())
                .observeOn(new IOScheduler())
                .map(n -> {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return n * 2;
                })
                .subscribe(
                        value -> receivedCount.incrementAndGet(),
                        Throwable::printStackTrace,
                        latch::countDown
                );

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(completed);
        Thread.sleep(200);
        assertEquals(sentCount.get(), receivedCount.get());
    }
}

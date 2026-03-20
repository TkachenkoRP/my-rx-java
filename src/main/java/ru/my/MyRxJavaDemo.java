package ru.my;

import ru.my.core.Disposable;
import ru.my.core.Observable;
import ru.my.schedulers.ComputationScheduler;
import ru.my.schedulers.IOScheduler;
import ru.my.schedulers.SingleScheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MyRxJavaDemo {
    static void main() throws InterruptedException {

        System.out.println("Демонстрация реактивного программирования MyRxJava");

        demonstrateCreateOperator();
        demonstrateMapOperator();
        demonstrateFilterOperator();
        demonstrateFlatMapOperator();
        demonstrateSubscribeOn();
        demonstrateObserveOn();
        demonstrateErrorHandling();
        demonstrateDisposable();

        System.out.println("\nВсе демонстрации завершены!");
    }

    private static void demonstrateCreateOperator() {
        System.out.println("1. ДЕМОНСТРАЦИЯ OPERATOR CREATE");

        Observable<Integer> observable = Observable.create(emitter -> {
            System.out.println("Начало генерации данных");
            for (int i = 1; i <= 5; i++) {
                if (emitter.isDisposed()) {
                    System.out.println("Подписка отменена");
                    return;
                }
                System.out.println("Отправка: " + i);
                emitter.onNext(i);
            }
            emitter.onComplete();
            System.out.println("Завершение генерации");
        });

        System.out.println("Подписка на Observable...");
        observable.subscribe(
                value -> System.out.println("Получено: " + value),
                error -> System.err.println("Ошибка: " + error.getMessage()),
                () -> System.out.println("Поток завершен\n")
        );
    }

    private static void demonstrateMapOperator() {
        System.out.println("2. ДЕМОНСТРАЦИЯ OPERATOR MAP");

        System.out.println("Исходные данные: [1, 2, 3, 4, 5]");
        System.out.println("Применяем map: число → квадрат числа → строка 'Квадрат: X'");

        Observable.just(1, 2, 3, 4, 5)
                .map(n -> n * n)
                .map(square -> "Квадрат: " + square)
                .subscribe(
                        result -> System.out.println("Результат map: " + result),
                        Throwable::printStackTrace,
                        () -> System.out.println("Map операция завершена\n")
                );
    }

    private static void demonstrateFilterOperator() {
        System.out.println("3. ДЕМОНСТРАЦИЯ OPERATOR FILTER");

        System.out.println("Исходные данные: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
        System.out.println("Фильтр: оставляем только четные числа");

        Observable.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .filter(n -> n % 2 == 0)
                .subscribe(
                        value -> System.out.println("Четное число: " + value),
                        Throwable::printStackTrace,
                        () -> System.out.println("Filter операция завершена\n")
                );
    }

    private static void demonstrateFlatMapOperator() {
        System.out.println("4. ДЕМОНСТРАЦИЯ OPERATOR FLATMAP");

        System.out.println("Исходные данные: ['Hello World', 'RxJava Rules']");
        System.out.println("FlatMap: разбиваем строки на слова");

        Observable.just("Hello World", "RxJava Rules")
                .flatMap(line -> {
                    String[] words = line.split(" ");
                    return fromArray(words);
                })
                .map(String::toUpperCase)
                .subscribe(
                        word -> System.out.println("Слово: " + word),
                        Throwable::printStackTrace,
                        () -> System.out.println("FlatMap операция завершена\n")
                );
    }

    private static <T> Observable<T> fromArray(T[] array) {
        return Observable.create(emitter -> {
            for (T item : array) {
                if (emitter.isDisposed()) return;
                emitter.onNext(item);
            }
            emitter.onComplete();
        });
    }

    private static void demonstrateSubscribeOn() throws InterruptedException {
        System.out.println("5. ДЕМОНСТРАЦИЯ SUBSCRIBEON");

        CountDownLatch latch = new CountDownLatch(1);

        System.out.println("Используем IOScheduler для подписки");

        Observable.create(emitter -> {
                    System.out.println("Генерация данных в потоке: " + Thread.currentThread().getName());
                    emitter.onNext("Данные 1");
                    emitter.onNext("Данные 2");
                    emitter.onComplete();
                })
                .subscribeOn(new IOScheduler())
                .subscribe(
                        data -> System.out.println("Получено: " + data + " в потоке: " + Thread.currentThread().getName()),
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("SubscribeOn демонстрация завершена\n");
                            latch.countDown();
                        }
                );

        latch.await(2, TimeUnit.SECONDS);
    }

    private static void demonstrateObserveOn() throws InterruptedException {
        System.out.println("6. ДЕМОНСТРАЦИЯ OBSERVEON");

        CountDownLatch latch = new CountDownLatch(1);

        System.out.println("Генерация в текущем потоке, обработка в ComputationScheduler");

        Observable.just(1, 2, 3, 4, 5)
                .map(n -> {
                    System.out.println("Map в потоке: " + Thread.currentThread().getName() + ", значение: " + n);
                    return n * 10;
                })
                .observeOn(new ComputationScheduler())
                .subscribe(
                        value -> System.out.println("Получено: " + value + " в потоке: " + Thread.currentThread().getName()),
                        Throwable::printStackTrace,
                        () -> {
                            System.out.println("ObserveOn демонстрация завершена\n");
                            latch.countDown();
                        }
                );

        latch.await(2, TimeUnit.SECONDS);
    }

    private static void demonstrateErrorHandling() {
        System.out.println("7. ДЕМОНСТРАЦИЯ ОБРАБОТКИ ОШИБОК");

        System.out.println("Создаем Observable, который генерирует ошибку");

        Observable.create(emitter -> {
                    emitter.onNext("Начало");
                    emitter.onNext("Середина");
                    emitter.onError(new RuntimeException("Произошла ошибка в потоке!"));
                    emitter.onNext("Это не будет отправлено");
                })
                .onErrorResumeNext(error -> {
                    System.out.println("Восстановление после ошибки: " + error.getMessage());
                    return Observable.just("Fallback 1", "Fallback 2", "Fallback 3");
                })
                .subscribe(
                        value -> System.out.println("Получено: " + value),
                        error -> System.err.println("Ошибка (не должна появиться): " + error.getMessage()),
                        () -> System.out.println("Успешное завершение после восстановления\n")
                );

        System.out.println("Демонстрация ошибки без восстановления:");
        Observable.create(emitter -> emitter.onError(new IllegalArgumentException("Критическая ошибка")))
                .subscribe(
                        value -> System.out.println("  " + value),
                        error -> System.out.println("Ошибка перехвачена в onError: " + error.getMessage()),
                        () -> System.out.println("  Завершено")
                );
        System.out.println();
    }

    private static void demonstrateDisposable() throws InterruptedException {
        System.out.println("8. ДЕМОНСТРАЦИЯ DISPOSABLE (ОТМЕНА ПОДПИСКИ)");

        System.out.println("Создаем долгий поток данных...");

        Disposable disposable = Observable.create(emitter -> {
                    for (int i = 1; i <= 10; i++) {
                        if (emitter.isDisposed()) {
                            System.out.println("Отмена подписки на элементе " + i);
                            return;
                        }
                        emitter.onNext(i);
                        Thread.sleep(200);
                    }
                    emitter.onComplete();
                })
                .subscribeOn(new IOScheduler())
                .observeOn(new SingleScheduler())
                .subscribe(
                        value -> System.out.println("Получено значение: " + value),
                        Throwable::printStackTrace,
                        () -> System.out.println("Поток завершен полностью")
                );

        System.out.println("Ожидание 500ms...");
        Thread.sleep(500);

        System.out.println("Вызов dispose()");
        disposable.dispose();

        System.out.println("Проверка состояния:");
        System.out.println("isDisposed() = " + disposable.isDisposed());

        Thread.sleep(1000);
        System.out.println("Disposable демонстрация завершена\n");
    }
}

package ru.my.core;

import ru.my.operators.FilterOperator;
import ru.my.operators.FlatMapOperator;
import ru.my.operators.MapOperator;
import ru.my.schedulers.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Основной класс реактивного потока
 *
 * @param <T> тип данных в потоке
 */
public class Observable<T> {
    private final ObservableOnSubscribe<T> source;

    private Observable(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    /**
     * Создает новый Observable с заданным источником
     */
    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<>(source);
    }

    /**
     * Создает Observable из элементов
     */
    @SafeVarargs
    public static <T> Observable<T> just(T... items) {
        return create(emitter -> {
            try {
                for (T item : items) {
                    if (emitter.isDisposed()) return;
                    emitter.onNext(item);
                }
                if (!emitter.isDisposed()) {
                    emitter.onComplete();
                }
            } catch (Exception e) {
                emitter.onError(e);
            }
        });
    }

    /**
     * Подписывает наблюдателя на поток
     */
    public Disposable subscribe(Observer<T> observer) {
        AtomicBoolean disposed = new AtomicBoolean(false);

        Emitter<T> emitter = new Emitter<T>() {
            @Override
            public void onNext(T item) {
                if (!disposed.get()) {
                    try {
                        observer.onNext(item);
                    } catch (Exception e) {
                        onError(e);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!disposed.get()) {
                    try {
                        observer.onError(t);
                    } finally {
                        disposed.set(true);
                    }
                }
            }

            @Override
            public void onComplete() {
                if (!disposed.get()) {
                    try {
                        observer.onComplete();
                    } finally {
                        disposed.set(true);
                    }
                }
            }

            @Override
            public boolean isDisposed() {
                return disposed.get();
            }
        };

        try {
            source.subscribe(emitter);
        } catch (Exception e) {
            emitter.onError(e);
        }

        return new Disposable() {
            @Override
            public void dispose() {
                disposed.set(true);
            }

            @Override
            public boolean isDisposed() {
                return disposed.get();
            }
        };
    }

    /**
     * Упрощенная подписка с лямбда-выражениями
     */
    public Disposable subscribe(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onComplete) {
        return subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                onNext.accept(item);
            }

            @Override
            public void onError(Throwable t) {
                onError.accept(t);
            }

            @Override
            public void onComplete() {
                onComplete.run();
            }
        });
    }

    public Disposable subscribe(Consumer<T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace, () -> {
        });
    }

    /**
     * Оператор преобразования элементов
     */
    public <R> Observable<R> map(Function<T, R> mapper) {
        return new Observable<>(new MapOperator<>(this, mapper));
    }

    /**
     * Оператор фильтрации элементов
     */
    public Observable<T> filter(Predicate<T> predicate) {
        return new Observable<>(new FilterOperator<>(this, predicate));
    }

    /**
     * Оператор flatMap - преобразование в новые Observable
     */
    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return new Observable<>(new FlatMapOperator<>(this, mapper));
    }

    /**
     * Указывает, в каком потоке выполнять подписку
     */
    public Observable<T> subscribeOn(Scheduler scheduler) {
        return create(emitter ->
                scheduler.execute(() ->
                        this.subscribe(new Observer<T>() {
                            @Override
                            public void onNext(T item) {
                                emitter.onNext(item);
                            }

                            @Override
                            public void onError(Throwable t) {
                                emitter.onError(t);
                            }

                            @Override
                            public void onComplete() {
                                emitter.onComplete();
                            }
                        })));
    }

    /**
     * Указывает, в каком потоке обрабатывать элементы
     */
    public Observable<T> observeOn(Scheduler scheduler) {
        return create(emitter -> this.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (!emitter.isDisposed()) {
                    scheduler.execute(() -> {
                        if (!emitter.isDisposed()) {
                            emitter.onNext(item);
                        }
                    });
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!emitter.isDisposed()) {
                    scheduler.execute(() -> {
                        if (!emitter.isDisposed()) {
                            emitter.onError(t);
                        }
                    });
                }
            }

            @Override
            public void onComplete() {
                if (!emitter.isDisposed()) {
                    scheduler.execute(() -> {
                        if (!emitter.isDisposed()) {
                            emitter.onComplete();
                        }
                    });
                }
            }
        }));
    }

    /**
     * Обработка ошибок с восстановлением
     */
    public Observable<T> onErrorResumeNext(Function<Throwable, Observable<T>> resumeFunction) {
        return create(emitter -> this.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (!emitter.isDisposed()) {
                    emitter.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!emitter.isDisposed()) {
                    try {
                        resumeFunction.apply(t).subscribe(new Observer<T>() {
                            @Override
                            public void onNext(T item) {
                                emitter.onNext(item);
                            }

                            @Override
                            public void onError(Throwable error) {
                                emitter.onError(error);
                            }

                            @Override
                            public void onComplete() {
                                emitter.onComplete();
                            }
                        });
                    } catch (Exception e) {
                        emitter.onError(e);
                    }
                }
            }

            @Override
            public void onComplete() {
                if (!emitter.isDisposed()) {
                    emitter.onComplete();
                }
            }
        }));
    }
}
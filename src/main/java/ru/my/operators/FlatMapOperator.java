package ru.my.operators;

import ru.my.core.Emitter;
import ru.my.core.Observable;
import ru.my.core.ObservableOnSubscribe;
import ru.my.core.Observer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Оператор flatMap - преобразование в новые Observable
 */
public class FlatMapOperator<T, R> implements ObservableOnSubscribe<R> {
    private final Observable<T> source;
    private final Function<T, Observable<R>> mapper;

    public FlatMapOperator(Observable<T> source, Function<T, Observable<R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public void subscribe(Emitter<R> emitter) {
        AtomicInteger activeSubscriptions = new AtomicInteger(1);
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean errored = new AtomicBoolean(false);

        source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (errored.get() || emitter.isDisposed()) return;

                activeSubscriptions.incrementAndGet();

                try {
                    Observable<R> innerObservable = mapper.apply(item);

                    innerObservable.subscribe(new Observer<R>() {
                        @Override
                        public void onNext(R innerItem) {
                            if (!errored.get() && !emitter.isDisposed()) {
                                emitter.onNext(innerItem);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            if (errored.compareAndSet(false, true)) {
                                emitter.onError(t);
                            }
                        }

                        @Override
                        public void onComplete() {
                            if (activeSubscriptions.decrementAndGet() == 0 && completed.get()) {
                                emitter.onComplete();
                            }
                        }
                    });
                } catch (Exception e) {
                    if (errored.compareAndSet(false, true)) {
                        emitter.onError(e);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (errored.compareAndSet(false, true)) {
                    emitter.onError(t);
                }
            }

            @Override
            public void onComplete() {
                completed.set(true);
                if (activeSubscriptions.decrementAndGet() == 0) {
                    emitter.onComplete();
                }
            }
        });
    }
}

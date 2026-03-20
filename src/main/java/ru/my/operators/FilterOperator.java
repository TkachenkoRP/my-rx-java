package ru.my.operators;

import ru.my.core.Emitter;
import ru.my.core.Observable;
import ru.my.core.ObservableOnSubscribe;
import ru.my.core.Observer;

import java.util.function.Predicate;

/**
 * Оператор фильтрации элементов
 */
public class FilterOperator<T> implements ObservableOnSubscribe<T> {
    private final Observable<T> source;
    private final Predicate<T> predicate;

    public FilterOperator(Observable<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public void subscribe(Emitter<T> emitter) {
        source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    if (!emitter.isDisposed() && predicate.test(item)) {
                        emitter.onNext(item);
                    }
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (!emitter.isDisposed()) {
                    emitter.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (!emitter.isDisposed()) {
                    emitter.onComplete();
                }
            }
        });
    }
}

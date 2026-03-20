package ru.my.operators;

import ru.my.core.Emitter;
import ru.my.core.Observable;
import ru.my.core.ObservableOnSubscribe;
import ru.my.core.Observer;

import java.util.function.Function;

/**
 * Оператор преобразования элементов
 */
public class MapOperator<T, R> implements ObservableOnSubscribe<R> {
    private final Observable<T> source;
    private final Function<T, R> mapper;

    public MapOperator(Observable<T> source, Function<T, R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public void subscribe(Emitter<R> emitter) {
        source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    if (!emitter.isDisposed()) {
                        R result = mapper.apply(item);
                        emitter.onNext(result);
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

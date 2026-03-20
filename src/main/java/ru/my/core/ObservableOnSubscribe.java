package ru.my.core;

/**
 * Функциональный интерфейс для создания Observable
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {
    void subscribe(Emitter<T> emitter) throws InterruptedException;
}

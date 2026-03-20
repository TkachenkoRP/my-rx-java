package ru.my.core;

/**
 * Эмиттер для отправки событий в Observable
 *
 * @param <T> тип отправляемых данных
 */
public interface Emitter<T> {
    /**
     * Отправляет новый элемент
     *
     * @param item элемент для отправки
     */
    void onNext(T item);

    /**
     * Отправляет ошибку
     *
     * @param t ошибка
     */
    void onError(Throwable t);

    /**
     * Завершает поток
     */
    void onComplete();

    /**
     * Проверяет, отменена ли подписка
     *
     * @return true если подписка отменена
     */
    boolean isDisposed();
}

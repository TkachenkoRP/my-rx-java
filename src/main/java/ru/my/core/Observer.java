package ru.my.core;

/**
 * Интерфейс наблюдателя, получающего события от Observable
 *
 * @param <T> тип получаемых данных
 */
public interface Observer<T> {
    /**
     * Вызывается при получении нового элемента
     *
     * @param item полученный элемент
     */
    void onNext(T item);

    /**
     * Вызывается при возникновении ошибки
     *
     * @param t ошибка
     */
    void onError(Throwable t);

    /**
     * Вызывается при завершении потока
     */
    void onComplete();
}

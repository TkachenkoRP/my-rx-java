package ru.my.core;

/**
 * Интерфейс для управления подпиской
 */
public interface Disposable {
    /**
     * Отменяет подписку
     */
    void dispose();

    /**
     * Проверяет, отменена ли подписка
     *
     * @return true если подписка отменена
     */
    boolean isDisposed();
}

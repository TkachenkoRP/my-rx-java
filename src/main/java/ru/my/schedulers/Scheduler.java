package ru.my.schedulers;

/**
 * Интерфейс планировщика потоков
 */
public interface Scheduler {
    /**
     * Выполняет задачу в соответствующем потоке
     */
    void execute(Runnable task);

    /**
     * Создает рабочий поток для выполнения задач
     */
    Worker createWorker();

    /**
     * Интерфейс рабочего потока
     */
    interface Worker {
        void execute(Runnable task);
        void dispose();
    }
}

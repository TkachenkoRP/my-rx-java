package ru.my.schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Планировщик с одним потоком
 * Использует single thread executor
 */
public class SingleScheduler implements Scheduler {
    private final ExecutorService executor;

    public SingleScheduler() {
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "single-thread");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public Worker createWorker() {
        return new SingleWorker();
    }

    private class SingleWorker implements Worker {
        private final AtomicBoolean disposed = new AtomicBoolean(false);

        @Override
        public void execute(Runnable task) {
            if (!disposed.get()) {
                executor.execute(() -> {
                    if (!disposed.get()) {
                        task.run();
                    }
                });
            }
        }

        @Override
        public void dispose() {
            disposed.set(true);
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
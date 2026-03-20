package ru.my.schedulers;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Планировщик для вычислительных задач
 * Использует фиксированный пул потоков (количество = числу ядер)
 */
public class ComputationScheduler implements Scheduler {
    private final ExecutorService executor;
    private final int threadCount;

    public ComputationScheduler() {
        this.threadCount = Runtime.getRuntime().availableProcessors();
        this.executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "computation-thread-" + UUID.randomUUID());
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
        return new ComputationWorker();
    }

    private class ComputationWorker implements Worker {
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

    public int getThreadCount() {
        return threadCount;
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
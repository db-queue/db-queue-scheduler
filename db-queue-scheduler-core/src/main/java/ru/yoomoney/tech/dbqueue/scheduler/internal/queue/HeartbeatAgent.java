package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of a heartbeat agent
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.11.2021
 */
class HeartbeatAgent {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatAgent.class);

    private static final long ALLOWABLE_WAITING_ERROR_IN_MILLS = 50L;

    private final String name;
    private final Duration heartbeatInterval;
    private final Runnable heartbeatAction;
    private final Object mutex;
    private volatile boolean isTaskRunning;
    private volatile boolean isTaskStopped;

    HeartbeatAgent(@Nonnull String name,
                   @Nonnull Duration heartbeatInterval,
                   @Nonnull Runnable heartbeatAction) {
        this.name = requireNonNull(name, "name");
        this.heartbeatInterval = requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatAction = requireNonNull(heartbeatAction, "heartbeatAction");
        this.mutex = new Object();
        this.isTaskRunning = false;
    }

    /**
     * Starts heart beating
     */
    public void start() {
        if (isTaskRunning) {
            throw new RuntimeException("unexpected agent state. the previous execution must be finished: name=" + name);
        }
        isTaskRunning = true;
        // tasks are rarely executed
        Thread thread = new Thread(this::run);
        thread.setName("heartbeat-agent-" + name);
        thread.start();
    }

    private void run() {
        try {
            doHeartbeats();
        } finally {
            synchronized (mutex) {
                isTaskStopped = true;
                mutex.notifyAll();
            }
        }
    }

    private void doHeartbeats() {
        while (isTaskRunning) {
            try {
                heartbeatAction.run();
            } catch (RuntimeException ex) {
                log.warn("failed to run heartbeat action. that might lead to race conditions: name={}", name, ex);
            }
            try {
                synchronized (mutex) {
                    long remainingMills = heartbeatInterval.toMillis();
                    while (isTaskRunning && remainingMills > ALLOWABLE_WAITING_ERROR_IN_MILLS) {
                        long start = System.currentTimeMillis();
                        mutex.wait(remainingMills);
                        remainingMills -= System.currentTimeMillis() - start;
                    }
                }
            } catch (InterruptedException ex) {
                log.info("agent thread interrupted: name={}", name, ex);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Stop heart beating
     */
    public void stop() {
        synchronized (mutex) {
            isTaskRunning = false;
            mutex.notifyAll();
        }
    }

    /**
     * Block until the heartbeat agent have been finished, or the timeout occurs,
     * or the current thread is interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @return true if this executor terminated and false if the timeout elapsed before termination
     */
    public boolean awaitTermination(@Nonnull Duration timeout) {
        requireNonNull(timeout, "timeout");
        try {
            synchronized (mutex) {
                long remainingMills = timeout.toMillis();
                while (!isTaskStopped) {
                    if (remainingMills <= 0L) {
                        return false;
                    }
                    long start = System.currentTimeMillis();
                    mutex.wait(remainingMills);
                    remainingMills -= System.currentTimeMillis() - start;
                }
            }
        } catch (InterruptedException ex) {
            log.info("termination awaiting interrupted: name={}", name, ex);
            Thread.currentThread().interrupt();
        }
        return true;
    }
}

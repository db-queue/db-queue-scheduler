package ru.yoomoney.tech.dbqueue.scheduler.internal.queue;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.11.2021
 */
class HeartbeatAgentTest {

    @Test
    void should_do_heartbeat() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMillis(100L), () -> {
            counter.incrementAndGet();
            throw new RuntimeException("fail");
        });

        Thread.sleep(200L);
        assertThat(counter.get(), equalTo(0));

        heartbeatAgent.start();
        Thread.sleep(500L);
        heartbeatAgent.stop();
        heartbeatAgent.awaitTermination(Duration.ofSeconds(5L));

        assertThat(counter.get(), greaterThanOrEqualTo(4));
        assertThat(counter.get(), lessThanOrEqualTo(6));
    }

    @Test
    void should_release_thread() throws InterruptedException {
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMinutes(10L), () ->
                threadRef.set(Thread.currentThread()));

        heartbeatAgent.start();
        Thread.sleep(100L);
        heartbeatAgent.stop();
        heartbeatAgent.awaitTermination(Duration.ofSeconds(5L));

        assertThat(threadRef.get().isAlive(), equalTo(false));
    }

    @Test
    void should_await_for_termination_and_return_true() throws InterruptedException {
        AtomicReference<Thread> threadRef = new AtomicReference<>();
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMinutes(10L), () ->
                threadRef.set(Thread.currentThread()));

        heartbeatAgent.start();
        Thread.sleep(100L);
        heartbeatAgent.stop();

        assertThat(heartbeatAgent.awaitTermination(Duration.ofSeconds(5L)), equalTo(true));
        assertThat(threadRef.get().isAlive(), equalTo(false));
    }

    @Test
    void should_return_false_if_await_termination_timeout_occurs() throws InterruptedException {
        HeartbeatAgent heartbeatAgent = new HeartbeatAgent("name", Duration.ofMinutes(10L), () ->{});

        try {
            heartbeatAgent.start();
            assertThat(heartbeatAgent.awaitTermination(Duration.ofSeconds(1L)), equalTo(false));
        } finally {
            heartbeatAgent.stop();
        }
    }
}
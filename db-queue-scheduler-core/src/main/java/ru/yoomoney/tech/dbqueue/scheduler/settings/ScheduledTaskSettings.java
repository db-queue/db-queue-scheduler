package ru.yoomoney.tech.dbqueue.scheduler.settings;

import ru.yoomoney.tech.dbqueue.scheduler.models.ScheduledTaskIdentity;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Scheduled task settings
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 20.10.2021
 */
public class ScheduledTaskSettings {
    /**
     * Unique identity of a scheduled task
     */
    @Nonnull
    private final ScheduledTaskIdentity identity;

    /**
     * Max interval during which task is not executed again unless task is rescheduled or the interval exceeded.
     *
     * <p>Normally, the second condition happens - tasks rescheduled according to its execution result. The interval
     * prevents unexpected conditions - an application crashed, some dead-lock happened, etc.
     *
     * <p>Pay attention, small interval may lead to a simultaneous execution of the same task by different nodes.
     */
    @Nonnull
    private final Duration maxExecutionLockInterval;

    /**
     * Scheduled settings
     */
    @Nonnull
    private final ScheduleSettings scheduleSettings;

    private ScheduledTaskSettings(@Nonnull ScheduledTaskIdentity identity,
                                  @Nonnull Duration executionLock,
                                  @Nonnull ScheduleSettings scheduleSettings) {
        this.identity = requireNonNull(identity, "identity");
        this.maxExecutionLockInterval = requireNonNull(executionLock, "executionLock");
        this.scheduleSettings = requireNonNull(scheduleSettings, "scheduleSettings");
    }

    /**
     * Creates an object builder
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public ScheduledTaskIdentity getIdentity() {
        return identity;
    }

    @Nonnull
    public Duration getMaxExecutionLockInterval() {
        return maxExecutionLockInterval;
    }

    @Nonnull
    public ScheduleSettings getScheduleSettings() {
        return scheduleSettings;
    }

    @Override
    public String toString() {
        return "ScheduledTaskSettings{" +
                "identity=" + identity +
                ", maxExecutionLockInterval=" + maxExecutionLockInterval +
                ", scheduleSettings=" + scheduleSettings +
                '}';
    }

    /**
     * Builder for {@link ScheduledTaskSettings}
     */
    public static final class Builder {
        private ScheduledTaskIdentity identity;
        private Duration maxExecutionLockInterval;
        private ScheduleSettings scheduleSettings;

        private Builder() {
        }

        public Builder withIdentity(@Nonnull ScheduledTaskIdentity identity) {
            this.identity = identity;
            return this;
        }

        public Builder withMaxExecutionLockInterval(@Nonnull Duration maxExecutionLockInterval) {
            this.maxExecutionLockInterval = maxExecutionLockInterval;
            return this;
        }

        public Builder withScheduleSettings(@Nonnull ScheduleSettings scheduleSettings) {
            this.scheduleSettings = scheduleSettings;
            return this;
        }

        /**
         * Creates an object
         */
        @Nonnull
        public ScheduledTaskSettings build() {
            return new ScheduledTaskSettings(identity, maxExecutionLockInterval, scheduleSettings);
        }
    }
}

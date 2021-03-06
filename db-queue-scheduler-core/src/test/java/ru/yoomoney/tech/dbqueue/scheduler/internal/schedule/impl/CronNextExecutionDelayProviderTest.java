package ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.NextExecutionDelayProvider;
import ru.yoomoney.tech.dbqueue.scheduler.internal.schedule.ScheduledTaskExecutionContext;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 26.10.2021
 */
class CronNextExecutionDelayProviderTest {

    private static final ZoneId ZONE_ID =  ZoneId.of("Europe/Moscow");
    private static final ZonedDateTime TODAY = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZONE_ID)
            .with(TemporalAdjusters.next(DayOfWeek.MONDAY));

    private static Stream<Arguments> cronSampleStream() {
        return Stream.of(
                Arguments.of("50 * * * * *", TODAY.withSecond(50)),
                Arguments.of("0 20 * * * *", TODAY.withMinute(20)),
                Arguments.of("0 0 3 * * *", TODAY.withHour(3)),
                Arguments.of("0 0 0 31 * *", TODAY.withDayOfMonth(31)),
                Arguments.of("0 0 0 1 2 *", TODAY.withMonth(Month.FEBRUARY.getValue()).withDayOfMonth(1)),
                Arguments.of("0 * * * * FRI", TODAY.with(TemporalAdjusters.next(DayOfWeek.FRIDAY))),
                Arguments.of("0 * * * * 5", TODAY.with(TemporalAdjusters.next(DayOfWeek.FRIDAY)))
        );
    }

    @ParameterizedTest
    @MethodSource("cronSampleStream")
    void should_resolve_next_time_based_on_configured_zoneId(String cron, ZonedDateTime expectedDateTime) {
        // given
        NextExecutionDelayProvider cronNextExecutionDelayProvider = new CronNextExecutionDelayProvider(cron, ZONE_ID);
        ScheduledTaskExecutionContext context = new ScheduledTaskExecutionContext();
        context.setExecutionStartTime(TODAY.toInstant());

        // when
        Duration nextExecutionDelay = cronNextExecutionDelayProvider.getNextExecutionDelay(context);

        // then
        assertThat(nextExecutionDelay, equalTo(Duration.between(TODAY, expectedDateTime)));
    }

    @Test
    void should_throw_exception_when_cron_expression_not_valid() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CronNextExecutionDelayProvider("not valid", ZoneId.of("Europe/Moscow"))
        );
    }
}
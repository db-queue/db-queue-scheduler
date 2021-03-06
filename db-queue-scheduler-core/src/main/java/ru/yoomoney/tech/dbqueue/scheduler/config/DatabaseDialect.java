package ru.yoomoney.tech.dbqueue.scheduler.config;

/**
 * Supported database type (dialect)
 *
 * @author Petr Zinin pgzinin@yoomoney.ru
 * @since 25.10.2021
 */
public enum DatabaseDialect {
    /**
     * PostgreSQL (version equals or higher than 9.5).
     */
    POSTGRESQL,
    /**
     * Microsoft SQL Server
     */
    MSSQL,
    /**
     * Oracle 11g
     *
     * <p> This version doesn't have automatically incremented primary keys,
     * so you must specify sequence name in a scheduler configurator.
     */
    ORACLE_11G,
    /**
     * H2 in-memory database
     */
    H2
}

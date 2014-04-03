package com.facebook.presto.plugin.jdbc;

public class MysqlJdbcPlugin
        extends JdbcPlugin
{
    public MysqlJdbcPlugin()
    {
        super("mysql", new MysqlJdbcClientModule());
    }
}

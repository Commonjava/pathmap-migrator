package org.commonjava.migrate.pathmap;

public interface Command
{
    void run(MigrateOptions options) throws MigrateException;
}

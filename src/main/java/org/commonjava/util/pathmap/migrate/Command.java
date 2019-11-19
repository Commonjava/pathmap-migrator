package org.commonjava.util.pathmap.migrate;

public interface Command
{
    void run(MigrateOptions options) throws MigrateException;
}

/**
 * Copyright (C) 2013~2019 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.migrate.pathmap;

import java.lang.reflect.InvocationTargetException;

import static org.commonjava.migrate.pathmap.Util.CMD_MIGRATE;
import static org.commonjava.migrate.pathmap.Util.CMD_SCAN;

public class Main
{
    public static void main( String[] args )
    {
        Thread.currentThread().setUncaughtExceptionHandler( ( thread, error ) -> {
            if ( error instanceof InvocationTargetException )
            {
                final InvocationTargetException ite = (InvocationTargetException) error;
                System.err.println(
                        "In: " + thread.getName() + "(" + thread.getId() + "), caught InvocationTargetException:" );
                ite.getTargetException().printStackTrace();

                System.err.println( "...via:" );
                error.printStackTrace();
            }
            else
            {
                System.err.println( "In: " + thread.getName() + "(" + thread.getId() + ") Uncaught error:" );
                error.printStackTrace();
            }
        } );

        final MigrateOptions options = new MigrateOptions();
        try
        {
            if ( options.parseArgs( args ) )
            {
                Command cmd = decideCommand( options );
                if ( cmd != null )
                {
                    cmd.run( options );
                }
            }
        }
        catch ( final IllegalArgumentException | MigrateException e )
        {
            System.err.printf( "ERROR: %s", e.getMessage() );
            System.exit( 1 );
        }
    }

    private static Command decideCommand( MigrateOptions options )
    {
        switch ( options.getCommand().toLowerCase().trim() )
        {
            case CMD_SCAN:
                return new ScanCmd();
            case CMD_MIGRATE:
                return new MigrateCmd();
        }
        return null;
    }
}

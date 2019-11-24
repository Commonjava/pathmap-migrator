/**
 * Copyright (C) 2013 Red Hat, Inc.
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

import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.commonjava.migrate.pathmap.Util.FAILED_PATHS_FILE;
import static org.commonjava.migrate.pathmap.Util.PROGRESS_FILE;
import static org.commonjava.migrate.pathmap.Util.STATUS_FILE;
import static org.commonjava.migrate.pathmap.Util.TODO_FILES_DIR;

public class MigrateCmd
        implements Command
{
    private CassandraMigrator migrator;

    private AtomicInteger processedCount = new AtomicInteger( 0 );

    private AtomicInteger failedCount =  new AtomicInteger( 0 );

    private long startFromScratch;

    static final Predicate<Path> WORKING_FILES_FILTER =
            p -> Files.isRegularFile( p ) && p.getFileName().toString().startsWith( TODO_FILES_DIR );

    @Override
    public void run( final MigrateOptions options )
            throws MigrateException
    {
        init( options );
        migrator = options.getMigrator();

        try
        {
            final List<Path> todoPaths = new ArrayList<>(  );
            Files.walk( Paths.get( options.getToDoDir() ), 1 ).filter( WORKING_FILES_FILTER ).forEach( todoPaths::add );
            if ( options.getMigrateThreads() <= 1 )
            {
                processBatch( todoPaths, options );
            }
            else
            {
                final Map<Integer, List<Path>> batchTodoPaths = calculateTodoPathBatches( todoPaths, options );
                final CountDownLatch latch = new CountDownLatch( options.getMigrateThreads() );
                final ExecutorService service = Executors.newFixedThreadPool( options.getMigrateThreads() );
                for ( int i = 0; i < options.getMigrateThreads(); i++ )
                {
                    List<Path> paths = batchTodoPaths.get( i );
                    service.execute( () -> {
                        try
                        {
                            processBatch( paths, options );
                        }
                        finally
                        {
                            latch.countDown();
                        }
                    } );

                }
                latch.await();
                service.shutdownNow();
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            stop( options );
            throw new MigrateException( "Error: Some error happened!", e );
        }

        final long end = System.currentTimeMillis();

        System.out.println( "\n\n" );
        System.out.println( String.format( "Migrate: total processed paths: %s", processedCount ) );
        System.out.println( String.format( "Migrate: total failed paths: %s", processedCount ) );
        System.out.println( String.format( "Migrate: total spent time: %s seconds", ( end - startFromScratch ) / 1000 ) );

        stop( options );
    }

    private Map<Integer, List<Path>> calculateTodoPathBatches( final List<Path> paths, final MigrateOptions options )
    {
        final int threads = options.getMigrateThreads();
        Map<Integer, List<Path>> batchMap = new HashMap<>( threads );
        for ( int i = 0; i < paths.size(); i++ )
        {
            Integer batch = i % threads;
            final List<Path> batchList = batchMap.computeIfAbsent( batch, ArrayList::new );
            batchList.add( paths.get( i ) );
        }
        return batchMap;
    }

    private void processBatch( final List<Path> todoPaths, final MigrateOptions options )
    {
        final List<String> failedPaths = new ArrayList<>();

        Consumer<Path> handler = p -> {
				    System.out.println( String.format( "Start to process files in %s ", p ) );
            List<String> paths = null;
            try (InputStream is = new FileInputStream( p.toFile() ))
            {
                paths = IOUtils.readLines( is );
                final Path processedPath = Paths.get( options.getProcessedDir(), p.getFileName().toString() );
                Files.move( p, processedPath );
            }
            catch ( IOException e )
            {
                //FIXME: how to handle this exception?
                e.printStackTrace();
            }
            if ( paths != null && !paths.isEmpty() )
            {
                paths.forEach( path -> {
                    try
                    {
                        migrator.migrate( path );
                        processedCount.getAndIncrement();
                    }
                    catch ( MigrateException e )
                    {
                        System.out.println( String.format( "Error: %s in %s failed to migrate. Error is: %s", path, p,
                                                           e.getMessage() ) );
                        failedPaths.add( path );

                        if ( failedPaths.size() > Util.DEFAULT_BATCH_SIZE )
                        {
                            storeFailedPaths( options, failedPaths );
                            failedCount.addAndGet( failedPaths.size() );
                            failedPaths.clear();
                        }
                    }
                } );
                paths = null; // for gc
                System.out.println( String.format( "%s finished processing and moved to processed folder", p ) );
            }
        };

        try
        {
            todoPaths.forEach( handler );
        }
        finally
        {
            if ( !failedPaths.isEmpty() )
            {
                storeFailedPaths( options, failedPaths );
                failedCount.addAndGet( failedPaths.size() );
                failedPaths.clear();
            }
        }
    }

    private Timer progressTimer = new Timer();

    private void init( MigrateOptions options )
    {
        // Reload last processed paths count
        Path progressFilePath = Paths.get( options.getWorkDir(), PROGRESS_FILE );
        File progressFile = progressFilePath.toFile();
        if ( progressFile.exists() )
        {
            try (BufferedReader reader = new BufferedReader( new FileReader( progressFile ) ))
            {
                String line = reader.readLine();
                while ( line != null )
                {
                    if ( line.trim().startsWith( "Processed" ) )
                    {
                        this.processedCount.set( Integer.parseInt( line.split( ":" )[1].trim() ) );
                        break;
                    }
                    line = reader.readLine();
                }
            }
            catch ( IOException | NumberFormatException e )
            {
                e.printStackTrace();
            }
        }

        startFromScratch = System.currentTimeMillis();
        final long period = 15000L;
        // Trigger progress update task.
        progressTimer.schedule( new UpdateProgressTask( options ), period, period );
    }

    private void stop( MigrateOptions options )
    {
        new UpdateProgressTask( options ).run(); // last run
        progressTimer.cancel();
        migrator.shutdown();
    }

    private synchronized void storeFailedPaths( MigrateOptions options, List<String> failedPaths )
    {
        File failedFile = Paths.get( options.getWorkDir(), FAILED_PATHS_FILE ).toFile();
        try
        {
            if ( !failedFile.exists() )
            {
                failedFile.createNewFile();
            }
            try (OutputStream os = new FileOutputStream( failedFile ))
            {
                IOUtils.writeLines( failedPaths, null, os );
            }
        }
        catch ( IOException e )
        {
            //FIXME: how to handle this?
            e.printStackTrace();
        }
    }

    private class UpdateProgressTask
            extends TimerTask
    {
        private final MigrateOptions options;

        UpdateProgressTask( final MigrateOptions options )
        {
            this.options = options;
        }

        @Override
        public void run()
        {
            final File statusFile = Paths.get( options.getWorkDir(), STATUS_FILE ).toFile();
            int totalCnt = 0;
            if ( statusFile.exists() )
            {
                try (BufferedReader reader = new BufferedReader( new FileReader( statusFile ) ))
                {
                    String line = reader.readLine();
                    while ( line != null )
                    {
                        if ( line.trim().startsWith( "Total" ) )
                        {
                            totalCnt = Integer.parseInt( line.split( ":" )[1].trim() );
                            break;
                        }
                        line = reader.readLine();
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }

            final int currentProcessedCnt = MigrateCmd.this.processedCount.get();
            double progress = (double) currentProcessedCnt / (double) totalCnt;
            String progressString = new DecimalFormat( "##.##" ).format( progress * 100 );
            final int currentTimeConsumedSeconds = (int) ( ( System.currentTimeMillis() - startFromScratch ) / 1000 );
            final Path progressFilePath = Paths.get( options.getWorkDir(), PROGRESS_FILE );
            try
            {
                Files.deleteIfExists( progressFilePath );
                final File progressFile = progressFilePath.toFile();
                try (BufferedWriter writer = new BufferedWriter( new FileWriter( progressFile ) ))
                {
                    writer.write( String.format( "Total:%s", totalCnt ) );
                    writer.newLine();
                    writer.write( String.format( "Processed:%s", currentProcessedCnt ) );
                    writer.newLine();
                    writer.write( String.format( "Failed:%s", failedCount.get() ) );
                    writer.newLine();
                    writer.write( String.format( "Progress:%s", progressString ) + "%" );
                    writer.newLine();
                    writer.write( String.format( "Time spent: %s", currentTimeConsumedSeconds ) );
                }
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }

        }
    }
}

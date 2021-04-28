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

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.nio.charset.Charset.defaultCharset;
import static java.nio.file.Files.readAttributes;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.commonjava.migrate.pathmap.MigrateOptions.FILE_DATE_FORMAT;
import static org.commonjava.migrate.pathmap.Util.TODO_FILES_DIR;
import static org.commonjava.migrate.pathmap.Util.newLine;
import static org.commonjava.migrate.pathmap.Util.newLines;
import static org.commonjava.migrate.pathmap.Util.prepareWorkingDir;
import static org.commonjava.migrate.pathmap.Util.printInfo;
import static org.commonjava.migrate.pathmap.Util.slicePathsByMod;

public class ScanCmd
        implements Command
{

    private void init( MigrateOptions options )
            throws MigrateException
    {
        try
        {
            prepareWorkingDir( options.getWorkDir() );
        }
        catch ( IOException e )
        {
            throw new MigrateException( String.format( "Error: can not prepare work dir. Error: %s", e.getMessage() ),
                                        e );
        }
    }

    public void run( MigrateOptions options )
            throws MigrateException
    {
        init( options );

        final long start = System.currentTimeMillis();
        final List<String> pkgFolderPaths;
        pkgFolderPaths = listValidPkgFolders( options.getBaseDir() );

        final int total;
        if ( options.getThreads() <= 1 )
        {
            total = noScanReposRun( pkgFolderPaths, options );
        }
        else
        {
            total = scanReposRun( pkgFolderPaths, options );
        }

        final long end = System.currentTimeMillis();
        newLines( 2 );
        printInfo( String.format( "File Scan completed, there are %s files need to migrate.", total ) );
        printInfo( String.format( "Time consumed: %s seconds", ( end - start ) / 1000 ) );
        printInfo( String.format( "Global last modified time: %s\n", globalLastModifiedTime ) );
        newLine();

        try
        {
            storeTotal( total, options );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private Integer noScanReposRun( final List<String> pkgFolderPaths, final MigrateOptions options )
    {
        final AtomicInteger total = new AtomicInteger( 0 );

        final ExecutorService executor = Executors.newFixedThreadPool( pkgFolderPaths.size() );
        final CountDownLatch latch = new CountDownLatch( pkgFolderPaths.size() );
        pkgFolderPaths.forEach( p -> executor.execute( () -> {
            try
            {
                int totalForPkg = listPkgFiles( p, options );
                total.addAndGet( totalForPkg );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
            finally
            {
                printInfo( String.format( "%s: %s scan finished", Thread.currentThread().getName(), p ) );
                latch.countDown();
            }
        } ) );
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        executor.shutdownNow();
        return total.get();
    }

    private Integer scanReposRun( final List<String> pkgFolderPaths, final MigrateOptions options )
    {
        final AtomicInteger total = new AtomicInteger( 0 );
        final Map<String, Map<Integer, List<Path>>> pkgAllPathsSlices = new HashMap<>( 3 );
        pkgFolderPaths.forEach( pkg -> {
            try
            {
                final List<Path> repos = listReposForPkg( pkg );
                final Map<Integer, List<Path>> slices = slicePathsByMod( repos, options.getThreads() );
                pkgAllPathsSlices.put( pkg, slices );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        } );

        final CountDownLatch pkgLatch = new CountDownLatch( pkgAllPathsSlices.size() );

        pkgAllPathsSlices.forEach( ( k, v ) -> new Thread( () -> {
            try
            {
                final String pkg = k;
                final Map<Integer, List<Path>> slices = v;
                printInfo( String.format( "Scanning for package %s start", pkg ) );
                final ExecutorService service = Executors.newFixedThreadPool( slices.size() );
                final CountDownLatch sliceLatch = new CountDownLatch( slices.size() );
                final AtomicInteger totalForPkg = new AtomicInteger( 0 );
                final AtomicInteger batchNum = new AtomicInteger( 0 );
                for ( int i = 0; i < slices.size(); i++ )
                {
                    final int sliceNum = i;
                    final List<Path> repoSlice = slices.get( sliceNum );
                    printInfo(
                            String.format( "Slice %s for pkg %s scan start, there are %s repos in this slice", sliceNum,
                                           pkg, repoSlice.size() ) );
                    service.execute( () -> {
                        try
                        {
                            int totalForSlice = listReposFiles( pkg, repoSlice, options, batchNum );
                            totalForPkg.addAndGet( totalForSlice );
                        }
                        finally
                        {
                            printInfo( String.format( "slice %s for pkg %s scan finished,", sliceNum, pkg ) );
                            sliceLatch.countDown();
                        }
                    } );
                }
                try
                {
                    sliceLatch.await();
                    service.shutdownNow();
                    total.addAndGet( totalForPkg.get() );
                    printInfo( String.format( "Package %s scan finished. There are %s files for the pkg", pkg,
                                              totalForPkg.get() ) );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }
            finally
            {
                pkgLatch.countDown();
            }
        } ).start() );

        try
        {
            pkgLatch.await();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        return total.get();
    }

    private List<Path> listReposForPkg( final String pkgDir )
            throws IOException
    {
        printInfo( String.format( "Start to scan package %s for repos", pkgDir ) );
        final List<Path> repos = new ArrayList<>();
        final Path pkgPath = Paths.get( pkgDir );
        Files.walk( pkgPath, 1 ).filter( p -> Files.isDirectory( p ) && !p.equals( pkgPath ) ).forEach( repos::add );
        printInfo(
                String.format( "Repos scan for package %s finished, there are %s repos in it", pkgDir, repos.size() ) );
        return repos;
    }

    private static final String PKG_TYPE_GENERIC_HTTP = "generic-http";
    private static final String PKG_TYPE_MAVEN = "maven";
    private static final String PKG_TYPE_NPM = "npm";

    private List<String> listValidPkgFolders( final String baseDir )
    {
        final List<String> pkgPaths = new ArrayList<>( 3 );
        for ( String pkg : Arrays.asList( PKG_TYPE_GENERIC_HTTP, PKG_TYPE_MAVEN, PKG_TYPE_NPM ) )
        {
            if ( Files.isDirectory( Paths.get( baseDir, pkg ) ) )
            {
                pkgPaths.add( Paths.get( baseDir, pkg ).toString() );
            }
        }
        return pkgPaths;
    }

    private int listPkgFiles( final String pkgDir, final MigrateOptions options )
            throws IOException
    {
        final String todoPrefix = getTodoPrefixForPkg( pkgDir );
        printInfo( String.format( "Start to scan package %s for files", pkgDir ) );
        final List<String> filePaths = new ArrayList<>( options.getBatchSize() );
        final AtomicInteger batchNum = new AtomicInteger( 0 );
        final AtomicInteger totalFileNum = new AtomicInteger( 0 );
        final Predicate<Path> fileFilter = getFileFilter( options );
        Files.walk( Paths.get( pkgDir ), Integer.MAX_VALUE ).filter( fileFilter ).forEach( p -> {
            filePaths.add( p.toString() );
            if ( filePaths.size() >= options.getBatchSize() )
            {
                storeBatchToFile( filePaths, options.getToDoDir(), todoPrefix, batchNum.getAndIncrement() );
                totalFileNum.addAndGet( filePaths.size() );
                filePaths.clear();

            }
        } );

        if ( !filePaths.isEmpty() )
        {
            storeBatchToFile( filePaths, options.getToDoDir(), todoPrefix, batchNum.get() );
            totalFileNum.addAndGet( filePaths.size() );
            filePaths.clear();
        }
        printInfo( String.format( "There are %s files in package path %s to migrate", totalFileNum.get(), pkgDir ) );
        return totalFileNum.get();
    }

    private int listReposFiles( final String pkg, final List<Path> repos, final MigrateOptions options,
                                final AtomicInteger batchNum )
    {
        final String todoPrefix = getTodoPrefixForPkg( pkg );
        final List<String> filePaths = new ArrayList<>( options.getBatchSize() );
        final AtomicInteger totalFileNum = new AtomicInteger( 0 );
        final Predicate<Path> fileFilter = getFileFilter( options );
        repos.forEach( repo -> {
            try
            {
                Files.walk( repo, Integer.MAX_VALUE ).filter( fileFilter ).forEach( p -> {
                    filePaths.add( p.toString() );
                    if ( filePaths.size() >= options.getBatchSize() )
                    {
                        storeBatchToFile( filePaths, options.getToDoDir(), todoPrefix, batchNum.getAndIncrement() );
                        totalFileNum.addAndGet( filePaths.size() );
                        filePaths.clear();
                    }
                } );
            }
            catch ( IOException e )
            {
                printInfo( String.format( "Error: something wrong happened during scanning repo %s. Error is: %s", repo,
                                          e.getMessage() ) );
            }
        } );
        if ( !filePaths.isEmpty() )
        {
            storeBatchToFile( filePaths, options.getToDoDir(), todoPrefix, batchNum.getAndIncrement() );
            totalFileNum.addAndGet( filePaths.size() );
            filePaths.clear();
        }
        return totalFileNum.get();
    }

    private Predicate<Path> getFileFilter( final MigrateOptions options )
    {
        final FileTime fileTime = options.getParsedFileTime();

        if ( isNotBlank( options.getFilterPattern() ) )
        {
            final Pattern pattern = Pattern.compile( options.getFilterPattern() );
            return p -> {
                boolean notNeed = pattern.matcher( p.getFileName().toString() ).matches();
                return !notNeed && ( fileTime == null ?
                                isRegularFile( p ) :
                                isRegularFileAfterTime( p, fileTime ) );
            };
        }
        else
        {
            return p -> fileTime == null ? isRegularFile( p ) : isRegularFileAfterTime( p, fileTime );
        }
    }

    private FileTime globalLastModifiedTime = FileTime.fromMillis( 0 );

    private synchronized void updateGlobalLastModifiedTime( FileTime t )
    {
        if ( t.compareTo( globalLastModifiedTime ) > 0 )
        {
            globalLastModifiedTime = t;
        }
    }

    public boolean isRegularFile( Path path, LinkOption... options )
    {
        BasicFileAttributes attr;
        try
        {
            attr = readAttributes( path, BasicFileAttributes.class, options );
        }
        catch ( IOException e )
        {
            printInfo( "Read file attribute failed, " + e );
            return false;
        }
        boolean isRegularFile = attr.isRegularFile();
        if ( isRegularFile )
        {
            updateGlobalLastModifiedTime( attr.lastModifiedTime() );
        }
        return isRegularFile;
    }

    public boolean isRegularFileAfterTime( Path path, FileTime other, LinkOption... options )
    {
        BasicFileAttributes attr;
        try
        {
            attr = readAttributes( path, BasicFileAttributes.class, options );
        }
        catch ( IOException e )
        {
            printInfo( "Read file attribute failed, " + e );
            return false;
        }

        boolean isRegularFile = attr.isRegularFile();
        if ( isRegularFile )
        {
            FileTime t = attr.lastModifiedTime();
            updateGlobalLastModifiedTime( t );
            return t.compareTo( other ) > 0; // compareTo return a value greater than 0 if this FileTime is after other
        }

        return false;
    }

    private String getTodoPrefixForPkg( final String pkgPathString )
    {
        final Path pkgPath = Paths.get( pkgPathString );
        final String pkgName = pkgPath.getName( pkgPath.getNameCount() - 1 ).toString();
        return TODO_FILES_DIR + "-" + pkgName;
    }

    private void storeBatchToFile( final List<String> filePaths, final String todoDir, final String prefix,
                                   final int batch )
    {
        final String batchFileName = prefix + "-" + "batch-" + batch + ".txt";
        final Path batchFilePath = Paths.get( todoDir, batchFileName );
        printInfo( String.format( "Start to store paths for batch %s to file %s", batch,
                                  Paths.get( todoDir, batchFileName ) ) );
        try (OutputStream os = new FileOutputStream( batchFilePath.toFile() ))
        {
            IOUtils.writeLines( filePaths, null, os );
        }
        catch ( IOException e )
        {
            printInfo( String.format( "Error: Cannot write paths to files for batch %s", batchFileName ) );
        }
        printInfo( String.format( "Batch %s to file %s finished", batch, Paths.get( todoDir, batchFileName ) ) );
    }

    private void storeTotal( final int totalNum, final MigrateOptions options )
            throws IOException
    {
        final File f = options.getStatusFile();
        try (FileOutputStream os = new FileOutputStream( f ))
        {
            IOUtils.write( String.format( "Total:%s\n", totalNum ), os );
            IOUtils.write( String.format( "Global last modified time: %s\n",
                                  FILE_DATE_FORMAT.format( new Date( globalLastModifiedTime.toMillis() ) ) ), os );
        }
    }

}

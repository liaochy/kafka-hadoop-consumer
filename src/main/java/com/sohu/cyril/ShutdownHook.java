package com.sohu.cyril;

import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.sohu.cyril.tools.Threads;

/**
 *
 * @see #install(Configuration, FileSystem, Stoppable, Thread)
 */
class ShutdownHook {
    private static final Log LOG = LogFactory.getLog(ShutdownHook.class);
    private static final String CLIENT_FINALIZER_DATA_METHOD = "clientFinalizer";

    /**
     * Key for boolean configuration whose default is true.
     */
    public static final String RUN_SHUTDOWN_HOOK = "picstore.shutdown.hook";

    /**
     * Key for a long configuration on how much time to wait on the fs shutdown
     * hook. Default is 30 seconds.
     */
    public static final String FS_SHUTDOWN_HOOK_WAIT = "picstore.fs.shutdown.hook.wait";

    /**
     * Install a shutdown hook that calls stop on the passed Stoppable
     * and then thread joins against the passed <code>threadToJoin</code>.
     * When this thread completes, it then runs the hdfs thread (This install
     * removes the hdfs shutdown hook keeping a handle on it to run it after
     * <code>threadToJoin</code> has stopped).
     * <p/>
     * <p>To suppress all shutdown hook  handling -- both the running of the
     * regionserver hook and of the hdfs hook code -- set
     * {@link ShutdownHook#RUN_SHUTDOWN_HOOK} in {@link Configuration} to
     * <code>false</code>.
     * This configuration value is checked when the hook code runs.
     *
     * @param conf
     * @param fs           Instance of Filesystem used by the RegionServer
     * @param stop         Installed shutdown hook will call stop against this passed
     *                     <code>Stoppable</code> instance.
     * @param threadToJoin After calling stop on <code>stop</code> will then
     *                     join this thread.
     */
    static void install(final Configuration conf, final FileSystem fs,
                        final Stoppable stop, final Thread threadToJoin) {
        Thread fsShutdownHook = suppressHdfsShutdownHook(fs);
        Thread t = new ShutdownHookThread(conf, stop, threadToJoin, fsShutdownHook);
        Runtime.getRuntime().addShutdownHook(t);
        LOG.info("Installed shutdown hook thread: " + t.getName());
    }

    /*
    * Thread run by shutdown hook.
    */
    private static class ShutdownHookThread extends Thread {
        private final Stoppable stop;
        private final Thread threadToJoin;
        private final Thread fsShutdownHook;
        private final Configuration conf;

        ShutdownHookThread(final Configuration conf, final Stoppable stop,
                           final Thread threadToJoin, final Thread fsShutdownHook) {
            super("Shutdownhook:" + threadToJoin.getName());
            this.stop = stop;
            this.threadToJoin = threadToJoin;
            this.conf = conf;
            this.fsShutdownHook = fsShutdownHook;
        }

        @Override
        public void run() {
            boolean b = this.conf.getBoolean(RUN_SHUTDOWN_HOOK, true);
            LOG.info("Shutdown hook starting; " + RUN_SHUTDOWN_HOOK + "=" + b +
                    "; fsShutdownHook=" + this.fsShutdownHook);
            if (b) {
                this.stop.stop("Shutdown hook");
                Threads.shutdown(this.threadToJoin);
                if (this.fsShutdownHook != null) {
                    LOG.info("Starting fs shutdown hook thread.");
                    this.fsShutdownHook.start();
                    Threads.shutdown(this.fsShutdownHook,
                            this.conf.getLong(FS_SHUTDOWN_HOOK_WAIT, 30000));
                }
            }
            LOG.info("Shutdown hook finished.");
        }
    }

    /*
    * So, HDFS keeps a static map of all FS instances. In order to make sure
    * things are cleaned up on our way out, it also creates a shutdown hook
    * so that all filesystems can be closed when the process is terminated; it
    * calls FileSystem.closeAll. This inconveniently runs concurrently with our
    * own shutdown handler, and therefore causes all the filesystems to be closed
    * before the server can do all its necessary cleanup.
    *
    * <p>The dirty reflection in this method sneaks into the FileSystem class
    * and grabs the shutdown hook, removes it from the list of active shutdown
    * hooks, and returns the hook for the caller to run at its convenience.
    *
    * <p>This seems quite fragile and susceptible to breaking if Hadoop changes
    * anything about the way this cleanup is managed. Keep an eye on things.
    * @return The fs shutdown hook
    * @throws RuntimeException if we fail to find or grap the shutdown hook.
    */
    private static Thread suppressHdfsShutdownHook(final FileSystem fs) {
        try {
            // This introspection has been updated to work for hadoop 0.20, 0.21 and for
            // cloudera 0.20.  0.21 and cloudera 0.20 both have hadoop-4829.  With the
            // latter in place, things are a little messy in that there are now two
            // instances of the data member clientFinalizer; an uninstalled one in
            // FileSystem and one in the innner class named Cache that actually gets
            // registered as a shutdown hook.  If the latter is present, then we are
            // on 0.21 or cloudera patched 0.20.
            Thread hdfsClientFinalizer = null;
            // Look into the FileSystem#Cache class for clientFinalizer
            Class<?>[] classes = FileSystem.class.getDeclaredClasses();
            Class<?> cache = null;
            for (Class<?> c : classes) {
                if (c.getSimpleName().equals("Cache")) {
                    cache = c;
                    break;
                }
            }
            Field field = null;
            try {
                field = cache.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
            } catch (NoSuchFieldException e) {
                // We can get here if the Cache class does not have a clientFinalizer
                // instance: i.e. we're running on straight 0.20 w/o hadoop-4829.
            }
            if (field != null) {
                field.setAccessible(true);
                Field cacheField = FileSystem.class.getDeclaredField("CACHE");
                cacheField.setAccessible(true);
                Object cacheInstance = cacheField.get(fs);
                hdfsClientFinalizer = (Thread) field.get(cacheInstance);
            } else {
                // Then we didnt' find clientFinalizer in Cache.  Presume clean 0.20 hadoop.
                field = FileSystem.class.getDeclaredField(CLIENT_FINALIZER_DATA_METHOD);
                field.setAccessible(true);
                hdfsClientFinalizer = (Thread) field.get(null);
            }
            if (hdfsClientFinalizer == null) {
                throw new RuntimeException("Client finalizer is null, can't suppress!");
            }
            if (!Runtime.getRuntime().removeShutdownHook(hdfsClientFinalizer)) {
                throw new RuntimeException("Failed suppression of fs shutdown hook: " +
                        hdfsClientFinalizer);
            }
            return hdfsClientFinalizer;
        } catch (NoSuchFieldException nsfe) {
            LOG.fatal("Couldn't find field 'clientFinalizer' in FileSystem!", nsfe);
            throw new RuntimeException("Failed to suppress HDFS shutdown hook");
        } catch (IllegalAccessException iae) {
            LOG.fatal("Couldn't access field 'clientFinalizer' in FileSystem!", iae);
            throw new RuntimeException("Failed to suppress HDFS shutdown hook");
        }
    }

    // Thread that does nothing. Used in below main testing.
    static class DoNothingThread extends Thread {
        DoNothingThread() {
            super("donothing");
        }

        @Override
        public void run() {
            super.run();
        }
    }

    // Stoppable with nothing to stop.  Used below in main testing.
    static class DoNothingStoppable implements Stoppable {
        public boolean isStopped() {
            return false;
        }

        public void stop(String why) {
            // TODO Auto-generated method stub
        }
    }

}
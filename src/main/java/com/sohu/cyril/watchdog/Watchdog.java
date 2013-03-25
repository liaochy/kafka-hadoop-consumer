package com.sohu.cyril.watchdog;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.cyril.tools.InputStreamPipe;
import com.sohu.cyril.tools.PropertiesLoader;

public class Watchdog {
	static final Logger LOG = LoggerFactory.getLogger(Watchdog.class);

	String[] args;

	// intilized by laucnh
	Selector selector;
	Runtime rt;
	Process proc;
	InputStreamPipe outPipe, errPipe, watchdogPipe;
	PrintStream procIn;
	SelectableChannel stdout, stderr, stdin;
	boolean interactive;

	public Watchdog(String[] args, boolean interactive) {
		this.args = args.clone();
		this.interactive = interactive;
	}

	public Watchdog(String[] args) {
		this(args, false);
	}

	/**
	 * This starts the different pumper threads, and opens all the piping
	 * connections.
	 * 
	 * @throws IOException
	 */
	void startup() throws IOException {
		selector = Selector.open();

		rt = Runtime.getRuntime();
		proc = rt.exec(args);
		outPipe = new InputStreamPipe(proc.getInputStream());
		errPipe = new InputStreamPipe(proc.getErrorStream());
		if (interactive) {
			watchdogPipe = new InputStreamPipe(System.in);
			procIn = new PrintStream(proc.getOutputStream());
		}

		stdout = outPipe.getChannel();
		stderr = errPipe.getChannel();
		if (interactive) {
			stdin = watchdogPipe.getChannel();
		}

		stdout.register(selector, SelectionKey.OP_READ);
		stderr.register(selector, SelectionKey.OP_READ);
		if (interactive) {
			stdin.register(selector, SelectionKey.OP_READ);
		}

		outPipe.start();
		errPipe.start();
		if (interactive) {
			watchdogPipe.start();
		}
		// if watchdog gets shutdown, make sure the subprocess is closed as well
		rt.addShutdownHook(new Thread() {
			public void run() {
				synchronized (Watchdog.this) {
					LOG.info("Watchdog shutdown hook");
					if (proc != null) {
						proc.destroy();
					}
				}
			}
		});
	}

	// This prevents eventual exhaustion by watch dog due to file handle
	// exhaustion and thread frame exhaustion.
	// # of open handles can be checked by doing an "lsof | grep <pid> | wc"
	void shutdown() throws IOException {
		if (proc == null)
			return;

		if (interactive) {
			watchdogPipe.shutdown();
			watchdogPipe = null;
		}

		outPipe.shutdown(); // +1 thread
		outPipe = null;
		errPipe.shutdown(); // +1 thread
		errPipe = null;

		if (interactive) {
			procIn.close();
			procIn = null;
		}
		proc.getOutputStream().close(); // +1 file handle
		proc.getInputStream().close(); // +1 file handle
		proc.getErrorStream().close(); // +1 file handle
		proc.destroy();
		proc = null;

		if (interactive) {
			stdin.close();
			stdin = null;
		}
		stdout.close(); // +1 file handle
		stdout = null;
		stderr.close(); // +0 but never used in test case
		stderr = null;
		selector.close(); // +5 handles!
		selector = null;
	}

	/**
	 * This version uses NIO to do unix style select input output redirection of
	 * the child process. If the i/o streams are closed this likely means our
	 * process is exiting, so exit and wait for process death.
	 * 
	 * This all happens in a single thread but each pipe has its own thread for
	 * moving data between the inputstream and ouptutstream
	 * 
	 */
	public int launchAgent() throws IOException, InterruptedException {
		startup();
		ByteBuffer buffer = ByteBuffer.allocate(32);

		while (true) {
			selector.select(2000);
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();
			if (!it.hasNext()) {
				// nothing to do, loop again.
				continue;
			}

			SelectionKey key = it.next();
			it.remove();
			buffer.clear();

			ReadableByteChannel channel = (ReadableByteChannel) key.channel();
			int count = channel.read(buffer);

			if (count < 0) {
				// EOF
				channel.close();
				break;
			}

			buffer.flip();

			while (buffer.hasRemaining()) {
				if (key.channel() == stdout) {
					System.out.print((char) buffer.get());
				} else if (key.channel() == stderr) {
					System.err.print((char) buffer.get());
				} else if (key.channel() == stdin) {
					procIn.print((char) buffer.get());
					procIn.flush();
				}
			}
		}

		int retval = proc.waitFor();

		shutdown();
		LOG.info("Subprocess exited with value " + retval);
		return retval;
	}

	public void run(int maxTriesPerMin) {
		ArrayList<Date> times = new ArrayList<Date>();

		while (true) {

			Date now = new Date();

			if (times.size() > maxTriesPerMin) {
				// drop entries that are too old to count against us
				ArrayList<Date> newTimes = new ArrayList<Date>();
				for (Date t : times) {
					Calendar c = Calendar.getInstance();
					c.setTime(t);
					c.add(Calendar.MINUTE, 1);
					Date t_plus1 = c.getTime();
					if (t_plus1.getTime() - now.getTime() > 0) {
						newTimes.add(t);
					}
				}

				// if it still to many recent tries, get the oldest and wait
				// until it
				// should expire.
				times = newTimes;
				if (newTimes.size() > maxTriesPerMin) {
					try {
						Calendar c = Calendar.getInstance();
						c.setTime(times.get(0));
						c.add(Calendar.MINUTE, 1);
						Date old_plus1 = c.getTime();

						long delta = old_plus1.getTime() - now.getTime();
						LOG.warn("too many attempts failed per minute -- waiting for "
								+ (delta / 1000) + "s");
						Thread.sleep(delta);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			Date d = now;
			LOG.info("Restarting process @ " + d);
			times.add(d);
			try {
				int ret = launchAgent();
				if (ret == 0) {
					LOG.info("Subprocess exited cleanly, closing watchdog");
					break;
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] argv) {
		if (argv.length == 0) {
			System.out.println("need to specify watched command as arguments");
			System.exit(-1);
		}

		String[] args = argv;

		PropertiesLoader loader = new PropertiesLoader("etl.properties");
		int maxTriesPerMin = loader.getInteger("watchdog.restarts.max", 5);
		Watchdog watchdog = new Watchdog(args);
		watchdog.run(maxTriesPerMin);
	}
}

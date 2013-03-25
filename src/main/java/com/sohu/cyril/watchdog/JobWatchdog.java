package com.sohu.cyril.watchdog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.cyril.tools.PropertiesLoader;

public class JobWatchdog {

	static final Logger LOG = LoggerFactory.getLogger(JobWatchdog.class);

	public static void main(String[] argv) {
		if (argv.length == 0) {
			System.out.println("need to specify watched command as arguments");
			System.exit(-1);
		}

		// drop a pid file if these options are set.
		String pid = System.getProperty("pid");
		String pidfile = System.getProperty("pidfile");
		if (pidfile != null && pid != null) {
			File f = new File(pidfile);
			f.deleteOnExit();

			try {
				FileWriter fw = new FileWriter(f);
				fw.write(pid);
				fw.close();
			} catch (IOException e) {
				LOG.error("failed to drop a pid file", e);
				System.exit(-1);
			}
			LOG.info("Dropped a pidfile='" + pidfile + "' with pid=" + pid);
		} else {
			LOG.warn("No pid or pidfile system property specified.");
		}

		String interactiveprop = System.getProperty("fwdstdin");
		boolean interactive = (interactiveprop != null);

		String[] args = argv;

		PropertiesLoader loader = new PropertiesLoader("etl.properties");
		int maxTriesPerMin = loader.getInteger("watchdog.restarts.max", 5);
		Watchdog watchdog = new Watchdog(args, interactive);
		watchdog.run(maxTriesPerMin);
	}
}

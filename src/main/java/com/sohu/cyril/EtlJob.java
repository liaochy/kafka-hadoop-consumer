package com.sohu.cyril;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.sohu.cyril.listener.BlockBasedRotateListener;
import com.sohu.cyril.listener.TimeBasedRotateListener;
import com.sohu.cyril.tools.EtlUtils;
import com.sohu.cyril.tools.EtlZkClient;
import com.sohu.cyril.tools.PropertiesLoader;
import com.sohu.cyril.tools.Threads;

public class EtlJob {

	public static final Logger log = Logger.getLogger(EtlJob.class);

	public static void main(String[] args) throws IOException {
		PropertiesLoader loader = new PropertiesLoader("etl.properties");

		FileSystem fs = FileSystem.get(JobConfiguration.create());
		Path execBasePath = new Path(EtlUtils.getDestinationPath(loader));
		if (!fs.exists(execBasePath)) {
			log.info("The execution base path does not exist. Creating the directory");
			fs.mkdirs(execBasePath);
		}

		String zkHosts = EtlUtils.getZkHosts(loader);
		String group = loader
				.getProperty("kafka.groupid", "EtlJobDefaultGroup");
		EtlZkClient zkClient = new EtlZkClient(zkHosts, group);
		log.info("zkClient.toString(): " + zkClient.toString());

		List<String> topicList = null;
		Set<String> whiteListTopics = new HashSet<String>(
				Arrays.asList(EtlUtils.getKafkaWhitelistTopic(loader)));

		log.info("whiteListTopics: " + whiteListTopics);

		Set<String> blackListTopics = new HashSet<String>(
				Arrays.asList(EtlUtils.getKafkaBlacklistTopic(loader)));

		log.info("blackListTopics: " + blackListTopics);

		if (whiteListTopics.isEmpty()) {
			topicList = zkClient.getTopics(blackListTopics);
		} else {
			topicList = zkClient.getTopics(whiteListTopics, blackListTopics);
		}

		List<RotateListener> listeners = new ArrayList<RotateListener>();
		listeners.add(new TimeBasedRotateListener(loader));
		listeners.add(new BlockBasedRotateListener(loader));

		boolean restOffset = false;
		if (Arrays.asList(args).contains("init")) {
			restOffset = true;
		}
		ExecutorService executor = Executors.newFixedThreadPool(topicList
				.size());
		ConsumerFactory factory = new ConsumerFactory(restOffset, executor,
				zkClient, listeners, loader);
		for (String valid : topicList) {
			MessageConsumer consumer = factory.createConsumer(valid,false);
			consumer.addObserver(factory);
			executor.submit(consumer);
		}
		Threads.setDaemonThreadRunning(new Thread(factory), "Consumner-Monitor");
	}
}

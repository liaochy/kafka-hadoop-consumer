package com.sohu.cyril;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sohu.cyril.io.EtlFile;
import com.sohu.cyril.tools.EtlUtils;
import com.sohu.cyril.tools.EtlZkClient;
import com.sohu.cyril.tools.PropertiesLoader;
import com.sohu.cyril.tools.Sleeper;

public class ConsumerFactory implements Runnable, Stoppable, Observer {

	public static Log logger = LogFactory.getLog(ConsumerFactory.class);

	private volatile boolean stopped = false;
	private final EtlZkClient zkClient;
	private final List<RotateListener> listeners;
	private final PropertiesLoader loader;
	private final ExecutorService executor;

	private final Sleeper sleeper;

	public ConsumerFactory(boolean restOffset, ExecutorService executor,
			EtlZkClient zkClient, List<RotateListener> listeners,
			PropertiesLoader loader) {
		this.executor = executor;
		this.zkClient = zkClient;
		this.listeners = listeners;
		this.loader = loader;
		int msgInterval = JobConfiguration.create().getInt(
				"job.server.interval", 3 * 1000);
		sleeper = new Sleeper(msgInterval, this);
		if (restOffset) {
			tryCleanupZookeeper(loader.getProperty("kafka.groupid"));
		}
	}

	private Properties createConsumerProperties(String topic) {
		Properties props = new Properties();

		props.put("groupid", loader.getProperty("kafka.groupid"));
		props.put("socket.buffersize",
				loader.getProperty("kafka.client.buffer.size"));
		props.put("fetch.size", loader.getProperty("kafka.client.buffer.size"));
		props.put("auto.commit", "true");
		props.put("autocommit.interval.ms",
				loader.getProperty("autocommit.interval.ms"));
		props.put("zk.connect", EtlUtils.getZkHosts(loader));
		props.put("zk.sessiontimeout.ms",
				String.valueOf(EtlUtils.getZkSessionTimeout(loader)));
		// 从最大位置或者从上次消费位置读取数据
		props.put("autooffset.reset", "largest");
		return props;
	}

	private void tryCleanupZookeeper(String groupId) {
		try {
			String dir = "/consumers/" + groupId;
			logger.info("Cleaning up temporary zookeeper data under " + dir
					+ ".");
			ZkClient zk = new ZkClient(EtlUtils.getZkHosts(loader), 30 * 1000,
					30 * 1000, new BytesPushThroughSerializer());
			zk.deleteRecursive(dir);
			zk.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public MessageConsumer createConsumer(String topic, boolean restart)
			throws IOException {
		Properties props = createConsumerProperties(topic);
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 1);
		Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector
				.createMessageStreams(topicMap);
		List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
		KafkaStream<Message> stream = streams.get(0);
		EtlFile file = new EtlFile(loader, topic, listeners);
		MessageConsumer consumer = new MessageConsumer(stream, file, topic);
		return consumer;
	}

	public void run() {
		for (; !this.stopped;) {
			long now = System.currentTimeMillis();
			sleeper.sleep(now);
		}
		logger.info(Thread.currentThread().getName() + " exiting");
	}

	public void update(Observable o, Object arg) {
		logger.info("Observer get changed notify ,args :" + arg);
		try {
			String topic = arg.toString();
			MessageConsumer consumer;
			consumer = this.createConsumer(topic, true);
			consumer.addObserver(this);
			executor.submit(consumer);
		} catch (IOException e) {
			logger.error("recreate consumer thread error !", e);
		}

	}

	public void stop(String msg) {
		this.stopped = true;
		logger.info("STOPPED: " + msg);
	}

	public boolean isStopped() {
		return this.stopped;
	}

}

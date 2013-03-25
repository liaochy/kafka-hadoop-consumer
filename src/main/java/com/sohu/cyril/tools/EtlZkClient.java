package com.sohu.cyril.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class EtlZkClient {
	private Log logger = LogFactory.getLog(EtlZkClient.class);

	public static final int DEFAULT_ZOOKEEPER_TIMEOUT = 30000;
	private static final String DEFAULT_ZOOKEEPER_TOPIC_PATH = "/brokers/topics";
	private static final String DEFAULT_ZOOKEEPER_BROKER_PATH = "/brokers/ids";
	private static final String DEFAULT_ZOOKEEPER_CONSUMER_PATH = "/consumers";
	private final String group;

	private final String zkTopicPath;
	private ZkClient zkClient;
	// key=topicName,value=whether consumer offset exits
	private Map<String, Boolean> topics;

	public EtlZkClient(String zkHosts, String group) throws IOException {
		this(zkHosts, group, DEFAULT_ZOOKEEPER_TIMEOUT,
				DEFAULT_ZOOKEEPER_TIMEOUT);
	}

	public EtlZkClient(String zkHosts, String group, int zkSessionTimeout,
			int zkConnectionTimeout) throws IOException {
		this(zkHosts, group, zkSessionTimeout, zkConnectionTimeout,
				DEFAULT_ZOOKEEPER_TOPIC_PATH, DEFAULT_ZOOKEEPER_BROKER_PATH);
	}

	public EtlZkClient(String zkHosts, String group, int zkSessionTimeout,
			int zkConnectionTimeout, String zkTopicPath, String zkBrokerPath)
			throws IOException {
		this.zkTopicPath = zkTopicPath;
		this.group = group;
		this.topics = new HashMap<String, Boolean>();
		try {
			zkClient = new ZkClient(zkHosts, zkSessionTimeout,
					zkConnectionTimeout, new BytesPushThroughSerializer());
			loadKafkaTopic();
		} catch (Exception e) {
			logger.error(e);
			System.exit(-1);
		} finally {
			zkClient.close();
		}
	}

	/**
	 * Loading kafka topics
	 * 
	 * @throws IOException
	 */
	private void loadKafkaTopic() throws IOException {
		String groupPath = DEFAULT_ZOOKEEPER_CONSUMER_PATH + "/" + group
				+ "/ids";
		if (zkClient.exists(DEFAULT_ZOOKEEPER_CONSUMER_PATH + "/" + group)
				&& zkClient.getChildren(groupPath).size() > 0) {
			throw new IOException("current consumer group is used by other instance.");
		}
		List<String> _topics = zkClient.getChildren(zkTopicPath);
		for (String topic : _topics) {
			String topicPath = zkTopicPath + "/" + topic;
			List<String> nodeIds = zkClient.getChildren(topicPath);
			int partition = 0;
			for (String nodeId : nodeIds) {
				String nodePath = topicPath + "/" + nodeId;
				String numPartitions = getZKString(nodePath);
				if (numPartitions == null) {
					System.err.println("Error on Topic: " + topic
							+ ", Cannot find partitions in " + nodePath);
					continue;
				}
				partition += Integer.parseInt(numPartitions);
			}
			if (partition > 0) {
				String offsetPath = DEFAULT_ZOOKEEPER_CONSUMER_PATH + "/"
						+ group + "/offsets/" + topic;
				if (zkClient.exists(offsetPath)) {
					topics.put(topic, true);
				} else {
					topics.put(topic, false);
				}
			}
		}
		logger.info("kafka topics are : "
				+ StringUtils.join(topics.entrySet(), ","));
	}

	/**
	 * Returns the topics in the zookeeper that aren't in the blacklist.
	 * 
	 * @param blacklist
	 * @return
	 */
	public List<String> getTopics(Set<String> blacklist) {
		ArrayList<String> topics = new ArrayList<String>();
		for (String topic : this.topics.keySet()) {
			if (!matchesPattern(blacklist, topic)) {
				topics.add(topic);
			}
		}

		return topics;
	}

	public boolean isOffsetExists(String topic) {
		return this.topics.get(topic);
	}

	public List<String> getTopics(Set<String> whitelist, Set<String> blacklist) {
		ArrayList<String> topics = new ArrayList<String>();
		for (String topic : this.topics.keySet()) {
			if (!matchesPattern(blacklist, topic)
					&& matchesPattern(whitelist, topic)) {
				topics.add(topic);
			}
		}

		return topics;
	}

	private boolean matchesPattern(Set<String> list, String compare) {
		for (String pattern : list) {
			if (Pattern.matches(pattern, compare)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get String data from zookeeper
	 * 
	 * @param path
	 * @return
	 */
	private String getZKString(String path) {
		byte[] bytes = zkClient.readData(path);
		if (bytes == null) {
			return null;
		}
		String nodeData = new String(bytes);

		return nodeData;
	}
}

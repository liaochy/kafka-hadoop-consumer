package com.sohu.cyril.tools;

import java.io.File;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

public class EtlUtils {

	public static final String ZK_HOSTS = "zookeeper.hosts";
	public static final String ZK_TOPIC_PATH = "zookeeper.broker.topics";
	public static final String ZK_BROKER_PATH = "zookeeper.broker.nodes";
	public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
	public static final String ZK_CONNECTION_TIMEOUT = "zookeeper.connection.timeout";

	public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
	public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";
	public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

	public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
	public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

	public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
	public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

	public static final String OUTPUT_PATH_DATE_FORMAT = "output.path.date.format";

	public static String[] getKafkaBlacklistTopic(PropertiesLoader loader) {
		return loader.getStrings(KAFKA_BLACKLIST_TOPIC);
	}

	public static String[] getKafkaWhitelistTopic(PropertiesLoader loader) {
		return loader.getStrings(KAFKA_WHITELIST_TOPIC);
	}

	public static String getZkHosts(PropertiesLoader loader) {
		return loader.getProperty(ZK_HOSTS);
	}

	public static String getZkTopicPath(PropertiesLoader loader) {
		return loader.getProperty(ZK_TOPIC_PATH);
	}

	public static String getZkBrokerPath(PropertiesLoader loader) {
		return loader.getProperty(ZK_BROKER_PATH);
	}

	public static int getZkSessionTimeout(PropertiesLoader loader) {
		return loader.getInteger(ZK_SESSION_TIMEOUT,
				EtlZkClient.DEFAULT_ZOOKEEPER_TIMEOUT);
	}

	public static int getKafkaClientTimeout(PropertiesLoader loader) {
		return loader.getInteger(KAFKA_CLIENT_SO_TIMEOUT, 60000);
	}

	public static int getZkConnectionTimeout(PropertiesLoader loader) {
		return loader.getInteger(ZK_CONNECTION_TIMEOUT,
				EtlZkClient.DEFAULT_ZOOKEEPER_TIMEOUT);
	}

	public static String generateFileName(PropertiesLoader loader, String topic) {
		StringBuffer sb = new StringBuffer();
		String parent = loader.getProperty("etl.output.file.dir");
		sb.append(parent);
		sb.append(File.separator);
		sb.append(topic);
		sb.append(File.separator);
		DateTime dateTime = new DateTime();
		DateTimeFormatter hourFmt = DateUtils.getDateTimeFormatter(
				"YYYYMMddHH", DateUtils.PST);
		DateTimeFormatter minuteFmt = DateUtils.getDateTimeFormatter(
				"YYYYMMddHHmm", DateUtils.PST);
		String hour = dateTime.toString(hourFmt);
		sb.append(hour);
		sb.append(File.separator);
		String minute = DateUtils.getLastTenMinutes(dateTime).toString(
				minuteFmt);
		sb.append(minute);
		return sb.toString();
	}

	public static String getDestinationPath(PropertiesLoader loader) {
		return loader.getProperty("etl.output.file.dir");
	}

}

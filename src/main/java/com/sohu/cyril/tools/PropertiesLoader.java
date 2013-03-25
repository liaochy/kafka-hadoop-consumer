package com.sohu.cyril.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader {

	private static Logger logger = LoggerFactory
			.getLogger(PropertiesLoader.class);

	private final Properties properties;

	public PropertiesLoader(String... resourcesPaths) {
		properties = loadProperties(resourcesPaths);
	}

	public Properties getProperties() {
		return properties;
	}

	private String getValue(String key) {
		String systemProperty = System.getProperty(key);
		if (systemProperty != null) {
			return systemProperty;
		}
		return properties.getProperty(key);
	}

	public String getProperty(String key) {
		String value = getValue(key);
		if (value == null) {
			throw new NoSuchElementException();
		}
		return value;
	}

	public String getProperty(String key, String defaultValue) {
		String value = getValue(key);
		return value != null ? value : defaultValue;
	}

	public String[] getStrings(String key) {
		String value = getProperty(key);
		return value != null ? StringUtils.split(value, ",") : new String[] {};
	}

	public Integer getInteger(String key) {
		String value = getValue(key);
		if (value == null) {
			throw new NoSuchElementException();
		}
		return Integer.valueOf(value);
	}

	public Integer getInteger(String key, Integer defaultValue) {
		String value = getValue(key);
		return value != null ? Integer.valueOf(value) : defaultValue;
	}

	public Double getDouble(String key) {
		String value = getValue(key);
		if (value == null) {
			throw new NoSuchElementException();
		}
		return Double.valueOf(value);
	}

	public Double getDouble(String key, Integer defaultValue) {
		String value = getValue(key);
		return value != null ? Double.valueOf(value) : defaultValue;
	}

	public Boolean getBoolean(String key) {
		String value = getValue(key);
		if (value == null) {
			throw new NoSuchElementException();
		}
		return Boolean.valueOf(value);
	}

	public Boolean getBoolean(String key, boolean defaultValue) {
		String value = getValue(key);
		return value != null ? Boolean.valueOf(value) : defaultValue;
	}

	private Properties loadProperties(String... resourcesPaths) {
		Properties props = new Properties();

		for (String location : resourcesPaths) {

			logger.debug("Loading properties file from path:{}", location);

			InputStream is = null;
			try {

				is = PropertiesLoader.class.getClassLoader()
						.getResourceAsStream(location);
				props.load(is);
			} catch (IOException ex) {
				logger.info("Could not load properties from path:{}, {} ",
						location, ex.getMessage());
			} finally {
				IOUtils.closeQuietly(is);
			}
		}
		return props;
	}

}

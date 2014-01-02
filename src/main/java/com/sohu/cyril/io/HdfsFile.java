package com.sohu.cyril.io;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class HdfsFile implements Closeable {

	private static final String utf8 = "UTF-8";
	private static final byte[] newline;

	static {
		try {
			newline = "\n".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}

	static final Log LOG = LogFactory.getLog(HdfsFile.class);

	private DataOutputStream outputStream;
	private Path path;

	public HdfsFile(FileSystem fs, Path path, Configuration conf)
			throws IOException {
		checkArgument(fs, path);
		this.path = path;
		boolean isCompressed = conf.getBoolean("hdfs.output.compress", false);
		if (!isCompressed) {
			if (fs.exists(path)) {
				fs.rename(path, path.suffix("." + System.currentTimeMillis()));
			}
			this.outputStream = fs.create(path);
		} else {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					conf, GzipCodec.class);
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass,
					conf);
			Path fileName = path.suffix(codec.getDefaultExtension());
			if (fs.exists(fileName)) {
				fs.rename(
						fileName,
						path.suffix("." + System.currentTimeMillis()).suffix(
								codec.getDefaultExtension()));
			}
			CompressionOutputStream compressedOut = codec.createOutputStream(fs
					.create(fileName));
			this.outputStream = new FSDataOutputStream(compressedOut, null);
		}
	}

	public static Class<? extends CompressionCodec> getOutputCompressorClass(
			Configuration conf, Class<? extends CompressionCodec> defaultValue) {
		Class<? extends CompressionCodec> codecClass = defaultValue;
		String name = conf.get("hdfs.output.compression.codec");
		if (name != null) {
			try {
				codecClass = conf.getClassByName(name).asSubclass(
						CompressionCodec.class);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException("Compression codec " + name
						+ " was not found.", e);
			}
		}
		return codecClass;
	}

	public long append(final byte[] array) throws IOException {
		this.outputStream.write(array);
		this.outputStream.write(newline);
		long curPos = this.outputStream.size();
		return curPos;
	}


	private void checkArgument(FileSystem fs, Path path) {
		if (fs == null) {
			throw new IllegalArgumentException(
					"The file system must not be null value");
		}
		if (path == null) {
			throw new IllegalArgumentException(
					"The file path must not be null value");
		}
	}

	public void close() throws IOException {
		if (this.outputStream == null) {
			return;
		}
		this.outputStream.flush();
		this.outputStream.close();
		this.outputStream = null;
	}

	public Path getPath() {
		return path;
	}
}

package com.sohu.cyril.listener;

import java.io.File;

import org.joda.time.format.DateTimeFormatter;

import com.sohu.cyril.RotateListener;
import com.sohu.cyril.tools.DateUtils;
import com.sohu.cyril.tools.PropertiesLoader;

public class TimeBasedRotateListener implements RotateListener {

	PropertiesLoader loader;
	private long rotateTimes;
	DateTimeFormatter minuteFmt = DateUtils.getDateTimeFormatter(
			"YYYYMMddHHmm", DateUtils.PST);

	public TimeBasedRotateListener(PropertiesLoader _loader) {
		this.loader = _loader;
		this.rotateTimes = this.loader
				.getInteger("etl.output.file.time.partition.mins") * 60 * 1000;
	}

	public boolean rotateNeeded(long pos, String fileName) {
		String[] path = fileName.split(File.separator);
		String time = path[path.length - 1];
		long start = minuteFmt.parseDateTime(time).getMillis();
		return System.currentTimeMillis() - start >= rotateTimes;
	}

}

package com.sohu.cyril.listener;

import com.sohu.cyril.RotateListener;
import com.sohu.cyril.tools.PropertiesLoader;

public class BlockBasedRotateListener implements RotateListener {

	PropertiesLoader loader;
	private long maxBlockSize;

	public BlockBasedRotateListener(PropertiesLoader _loader) {
		this.loader = _loader;
		this.maxBlockSize = this.loader.getInteger("hdfs.file.rotate.size") * 1024 * 1024;
	}

	public boolean rotateNeeded(long pos, String fileName) {
		return pos >= this.maxBlockSize;
	}

}

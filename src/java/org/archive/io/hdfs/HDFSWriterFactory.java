package org.archive.io.hdfs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

public class HDFSWriterFactory extends BasePoolableObjectFactory {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private HDFSParameters _parameters;

	public HDFSWriterFactory(HDFSParameters parameters) {
		_parameters = parameters;
	}

	@Override
	public Object makeObject() throws Exception {
		return new HDFSWriter(_parameters);
	}

	@Override
	public void destroyObject(Object obj) throws Exception {
		try {
			if (obj instanceof Closeable)
				((Closeable)obj).close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}

		super.destroyObject(obj);
	}
}

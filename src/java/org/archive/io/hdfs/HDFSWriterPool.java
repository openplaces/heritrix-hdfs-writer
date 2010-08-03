package org.archive.io.hdfs;

import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.DefaultWriterPoolSettings;
import org.archive.io.WriterPool;

/**
 * @author greglu
 */
public class HDFSWriterPool extends WriterPool {

	/**
	 * Create a pool of HDFSWriter objects.
	 *
	 * @param parameters the {@link org.archive.io.hdfs.HDFSParameters} object containing your settings
	 * @param poolMaximumActive the maximum number of writers in the writer pool.
	 * @param poolMaximumWait the maximum waittime for all writers in the pool.
	 */
	public HDFSWriterPool(final HDFSParameters parameters, final int poolMaximumActive,
			final int poolMaximumWait) {
		super(
			new AtomicInteger(),
			new HDFSWriterFactory(parameters),
			new DefaultWriterPoolSettings(),
			poolMaximumActive,
			poolMaximumWait);
	}
}

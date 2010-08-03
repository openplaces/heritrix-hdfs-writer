package org.archive.modules.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.archive.io.hdfs.HDFSWriterPool;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;

public class HDFSWriterProcessor extends WriterPoolProcessor {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private static final long serialVersionUID = -177504411709375639L;

	@Override
	long getDefaultMaxFileSize() {
        return (20 * 1024 * 1024);
	}

	@Override
	List<String> getDefaultStorePaths() {
        return new ArrayList<String>();
	}

	@Override
	protected List<String> getMetadata() {
        return new ArrayList<String>();
	}
	
	@Override
	protected void setupPool(AtomicInteger arg0) {
		setPool(new HDFSWriterPool(getHDFSParameters(), getPoolMaxActive(), getPoolMaxWaitMs()));
	}

	@Override
	protected ProcessResult innerProcessResult(CrawlURI arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	

}

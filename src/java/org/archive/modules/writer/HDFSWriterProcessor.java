package org.archive.modules.writer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.io.hdfs.HDFSParameters;
import org.archive.io.hdfs.HDFSWriter;
import org.archive.io.hdfs.HDFSWriterPool;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.net.UURI;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.ANVLRecord;


/**
 * A <a href="http://crawler.archive.org">Heritrix 3</a> processor that writes
 * to <a href="http://hadoop.apache.org/">Hadoop HDFS</a>.
 *
 * The following example shows how to configure the crawl job configuration.
 *
 * <pre>
 * {@code
 * <!-- DISPOSITION CHAIN -->
 * <bean id="hdfsParameters" class="org.archive.io.hdfs.HDFSParameters">
 * </bean>
 *
 * <bean id="hdfsWriterProcessor" class="org.archive.modules.writer.HDFSWriterProcessor">
 *   <property name="hdfsParameters">
 *     <bean ref="hdfsParameters" />
 *   </property>
 * </bean>
 *
 * <bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
 *   <property name="processors">
 *     <list>
 *     <!-- write to aggregate archival files... -->
 *     <ref bean="hdfsWriterProcessor"/>
 *     <!-- other references -->
 *     </list>
 *   </property>
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.io.hdfs.HDFSParameters {@link org.archive.io.hdfs.HDFSParameters}
 *  for defining hdfsParameters
 *
 * @author greg
 */
public class HDFSWriterProcessor extends WriterPoolProcessor {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private static final long serialVersionUID = -177504411709375639L;

	/**
	 * @see org.archive.io.hdfs.HDFSParameters
	 */
	HDFSParameters hdfsParameters;

	public HDFSParameters getHdfsParameters() {
		return hdfsParameters;
	}

	public void setHdfsParameters(HDFSParameters hdfsParameters) {
		this.hdfsParameters = hdfsParameters;
	}

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
		setPool(new HDFSWriterPool(getHdfsParameters(), getPoolMaxActive(), getPoolMaxWaitMs()));
	}

	@Override
	protected ProcessResult innerProcessResult(CrawlURI uri) {
		CrawlURI curi = uri;
		long recordLength = getRecordedSize(curi);
		ReplayInputStream ris = null;
		try {
			if (shouldWrite(curi)) {
				ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
				return write(curi, recordLength, ris);
			}
			LOG.info("Does not write " + curi.toString());
		} catch (IOException e) {
			curi.getNonFatalFailures().add(e);
			LOG.error("Failed write of Record: " + curi.toString(), e);
		} finally {
			ArchiveUtils.closeQuietly(ris);
		}
		return ProcessResult.PROCEED;
	}

	/**
	 * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.ProcessorURI)
	 */
	@Override
	protected boolean shouldProcess(CrawlURI curi) {
		// The old method is still checked, but only continue with the next
		// checks if it returns true.
		if (!super.shouldProcess(curi))
			return false;

		// If we make it here, then we passed all our checks and we can assume
		// we should write the record.
		return true;
	}

	/**
	 * Whether the given CrawlURI should be written to archive files.
	 * Annotates CrawlURI with a reason for any negative answer.
	 *
	 * @param curi CrawlURI
	 *
	 * @return true if URI should be written; false otherwise
	 */
	protected boolean shouldWrite(CrawlURI curi) {
		// The old method is still checked, but only continue with the next
		// checks if it returns true.
		if (!super.shouldWrite(curi))
			return false;

		// If the content exceeds the maxContentSize, then dont write.
		if (curi.getContentSize() > getMaxFileSizeBytes()) {
			// content size is too large
			curi.getAnnotations().add(ANNOTATION_UNWRITTEN + ":size");

			LOG.warn("Content size for " + curi.getUURI() + " is too large ("
					+ curi.getContentSize() + ") - maximum content size is: "
					+ getMaxFileSizeBytes());

			return false;
		}

		// all tests pass, return true to write the content locally.
		return true;
	}

	/**
	 * Write to HDFS.
	 *
	 * @param curi
	 * @param recordLength
	 * @param in
	 *
	 * @return the process result
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected ProcessResult write(final CrawlURI curi, long recordLength, InputStream in) throws IOException {
		WriterPoolMember writerPoolMember = getPool().borrowFile();

		long writerPoolMemberPosition = writerPoolMember.getPosition();

		// See if we need to open a new file because we've exceeed maxBytes.
		// Call to checkFileSize will open new file if we're at maximum for
		// current file.
		writerPoolMember.checkSize();

		if (writerPoolMember.getPosition() != writerPoolMemberPosition) {
			// We just closed the file because it was larger than maxBytes.
			// Add to the totalBytesWritten the size of the first record
			// in the file, if any.
			setTotalBytesWritten(getTotalBytesWritten() +
					(writerPoolMember.getPosition() - writerPoolMemberPosition));
			writerPoolMemberPosition = writerPoolMember.getPosition();
		}

		UURI via = curi.getVia();

		CrawlURI seed = curi.getFullVia();
		String seedUrl = "";

		// Set a limit on the number of times to recurse
		int count = 50;

		if (seed != null) {
			while (!seed.isSeed()) {
				if (count == 0)
					break;

				seed = seed.getFullVia();

				count--;
			}

			seedUrl = seed.toString();
		}

		ANVLRecord record = new ANVLRecord();
		record.addLabelValue(getHdfsParameters().getUrlFieldName(), curi.toString());
		record.addLabelValue(getHdfsParameters().getIpFieldName(), getHostAddress(curi));
		record.addLabelValue(getHdfsParameters().getCrawlTimeFieldName(),
				ArchiveUtils.get14DigitDate(curi.getFetchBeginTime()));
		record.addLabelValue(getHdfsParameters().getIsSeedFieldName(), Boolean.toString(curi.isSeed()));
		record.addLabelValue(getHdfsParameters().getSeedUrlFieldName(), seedUrl);

		if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0)
			record.addLabelValue(getHdfsParameters().getPathFromSeedFieldName(), curi.getPathFromSeed());

		if (via != null && via.toString().trim().length() > 0)
			record.addLabelValue(getHdfsParameters().getViaFieldName(), via.toString());

		final byte [] namedFieldsBlock = record.getUTF8Bytes();

		HDFSWriter writer = (HDFSWriter)writerPoolMember;
		try {
			writer.write(curi, namedFieldsBlock, curi.getRecorder().getRecordedOutput(),	
					curi.getRecorder().getRecordedInput());	    

			LOG.info("HDFSWriterProcessor: Successfully written url " + curi.getBaseURI());
		} catch (IOException e) {
			// Invalidate this file (It gets a '.invalid' suffix).
			getPool().invalidateFile(writerPoolMember);

			// Set the writer to null otherwise the pool accounting
			// of how many active writers gets skewed if we subsequently
			// do a returnWriter call on this object in the finally block.
			writerPoolMember = null;

			LOG.error("Error encountered while processing: " + curi.toString());
			throw e;
		} finally {
			if (writerPoolMember != null) {
				setTotalBytesWritten(getTotalBytesWritten() +
						(writerPoolMember.getPosition() - writerPoolMemberPosition));

				getPool().returnFile(writerPoolMember);
			}
		}

		return checkBytesWritten();
	}

}

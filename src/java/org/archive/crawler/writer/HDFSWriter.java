/*
 * HDFSWriter
 *
 * $Id$
 *
 * Created on January 20th, 2007
 *
 * Copyright (C) 2007 Zvents
 *
 * This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 * Heritrix is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * any later version.
 *
 * Heritrix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser Public License for more details.
 *
 * You should have received a copy of the GNU Lesser Public License
 * along with Heritrix; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.archive.crawler.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.ArchiveFileConstants;
import org.archive.util.DevUtils;


/**
 * Write Crawl output to HDFS.
 *
 * Assumption is that the caller is managing access to this HDFSWriter ensuring
 * only one thread of control accessing this HDFS file instance at any one time.
 *
 * <p>While being written, HDFS files have a '.open' suffix appended.
 *
 * @author Doug Judd
 */
public class HDFSWriter extends WriterPoolMemberHdfs implements ArchiveFileConstants {
    //private static final Logger logger =
      //  Logger.getLogger(HDFSWriter.class.getName());

    public String HDFSWRITER_ID = "HDFSWriter/0.2";

    private int mCaptureStreamCapacity = 262144;
    private ByteArrayOutputStream mCaptureStream = new ByteArrayOutputStream(262144);

    /**
     * Constructor.
     *
     * @param serialNo  used to generate unique file name sequences
     * @param dirs Where to drop the ARC files.
     * @param prefix ARC file prefix to use.  If null, we use
     * DEFAULT_ARC_FILE_PREFIX.
     * @param cmprs Compress the ARC files written.  The compression is done
     * by individually gzipping each record added to the ARC file: i.e. the
     * ARC file is a bunch of gzipped records concatenated together.
     * @param maxSize Maximum size for ARC files written.
     * @param hdfsReplication Replication factor for HDFS files
     * @param hdfsCompressionType Type of SequenceFile compression to use 
     * @param hdfsOutputPath Directory with HDFS where job content files
     *     will get written
     * @param hdfsFsDefaultName fs.default.name Hadoop property
     */
    public HDFSWriter(final AtomicInteger serialNo, final String jobDir,
		     final String prefix, final boolean cmprs,
		     final int maxSize, final int hdfsReplication,
                     final String hdfsCompressionType,
		     final String hdfsOutputPath,
		     final String hdfsFsDefaultName) throws IOException {
        this(serialNo, jobDir, prefix, "", cmprs, maxSize, hdfsReplication,
	     hdfsCompressionType, hdfsOutputPath, hdfsFsDefaultName);
    }
            
    /**
     * Constructor.
     *
     * @param serialNo  used to generate unique file name sequences
     * @param dirs Where to drop files.
     * @param prefix File prefix to use.
     * @param suffix File tail to use.  If null, unused.
     * @param cmprs Compress the records written. 
     * @param maxSize Maximum size for ARC files written.
     * @param hdfsReplication Replication factor for HDFS files
     * @param hdfsCompressionType Type of SequenceFile compression to use 
     * @param hdfsOutputPath Directory with HDFS where job content files
     *     will get written
     * @param hdfsFsDefaultName fs.default.name Hadoop property
     */
    public HDFSWriter(final AtomicInteger serialNo, final String jobDir,
    		final String prefix, final String suffix, final boolean cmprs,
	        final long maxSize, final int hdfsReplication,
		final String hdfsCompressionType, final String hdfsOutputPath,
	        final String hdfsFsDefaultName) throws IOException {
        super(serialNo, jobDir, prefix, suffix, cmprs, maxSize,
	      hdfsReplication, hdfsCompressionType, hdfsOutputPath,
	      hdfsFsDefaultName);
    }

    protected String createFile()
    throws IOException {
        return super.createFile();
    }

    /**
     * write method
     *
     * @param uri URI of crawled document
     * @param fieldBytes block of fields to write to output after header line
     * @param ros recording output stream that captured the GET request (for http*)
     * @param ris recording input stream that captured the response
     */
    public void write(String uri, byte [] fieldBytes, RecordingOutputStream ros,
		      RecordingInputStream ris) throws IOException {
	ReplayInputStream replayStream = null;
        preWriteRecordTasks();
        try {
            try {

		int recordLength = 256 + fieldBytes.length + (int)ros.getSize() + (int)ris.getSize();

		if (mCaptureStreamCapacity < recordLength) {
		    mCaptureStreamCapacity = recordLength + 8192;
		    mCaptureStream = 
			new ByteArrayOutputStream(mCaptureStreamCapacity);
		}
		else
		    mCaptureStream.reset();
		
		byte [] CRLF_BYTES = CRLF.getBytes();

		// write header line
		mCaptureStream.write(HDFSWRITER_ID.getBytes());
		mCaptureStream.write(CRLF_BYTES);

		// write fields
		mCaptureStream.write(fieldBytes);

		// write request
		char [] uriChars = uri.toCharArray();
		if ((uriChars[0] == 'h' || uriChars[0] == 'H') &&
		    (uriChars[1] == 't' || uriChars[1] == 'T') &&
		    (uriChars[2] == 't' || uriChars[2] == 'T') &&
		    (uriChars[3] == 'p' || uriChars[3] == 'P')) {
		    replayStream = ros.getReplayInputStream();
		    replayStream.readFullyTo(mCaptureStream);
		    replayStream.close();
		}

		// write response
		replayStream = ris.getReplayInputStream();
                replayStream.readFullyTo(mCaptureStream);
		write(mCaptureStream.toByteArray());
		
                long remaining = replayStream.remaining();
                // Should be zero at this stage.  If not, something is
                // wrong.
                if (remaining != 0) {
                    String message = "Gap between expected and actual: " +
                        remaining + "\n" + DevUtils.extraInfo() +
                        " writing arc " + this.getFilename();
                    DevUtils.warnHandle(new Throwable(message), message);
                    throw new IOException(message);
                }
            } finally {
		if (replayStream != null)
		    replayStream.close();
            } 
            
        } finally {
            postWriteRecordTasks(uri);
        }
    }
}

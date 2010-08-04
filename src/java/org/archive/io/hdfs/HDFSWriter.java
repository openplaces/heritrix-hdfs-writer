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
package org.archive.io.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.modules.CrawlURI;
import org.archive.util.DevUtils;



public class HDFSWriter extends HDFSWriterPoolMember {

	@SuppressWarnings("unused")
	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	public String HDFSWRITER_ID = "HDFSWriter/0.3";

	@SuppressWarnings("unused")
	private HDFSParameters _parameters;

	public HDFSWriter(HDFSParameters parameters) throws IOException {
		super(parameters);
		this._parameters = parameters;
	}

    private int mCaptureStreamCapacity = 262144;
    private ByteArrayOutputStream mCaptureStream = new ByteArrayOutputStream(mCaptureStreamCapacity);


    /**
     * Write
     *
     * @param uri URI of crawled document
     * @param fieldBytes block of fields to write to output after header line
     * @param ros recording output stream that captured the GET request (for http*)
     * @param ris recording input stream that captured the response
     */
    public void write(final CrawlURI curi, byte [] fieldBytes, RecordingOutputStream ros,
    		RecordingInputStream ris) throws IOException {
    	String uri = curi.toString();
    	ReplayInputStream replayStream = null;

    	preWriteRecordTasks();

    	try {
    		try {

    			int recordLength = 256 + fieldBytes.length + (int)ros.getSize() + (int)ris.getSize();

    			if (mCaptureStreamCapacity < recordLength) {
    				mCaptureStreamCapacity = recordLength + 8192;
    				mCaptureStream = new ByteArrayOutputStream(mCaptureStreamCapacity);
    			} else {
    				mCaptureStream.reset();
    			}

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
    					remaining + "\n" + DevUtils.extraInfo() + "writing arc ";

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

/* WriterPoolProcessorHdfs
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.modules.writer.WriterPoolProcessor;


/**
 * Abstract implementation of an HDFS file pool processor.
 * @author Doug Judd
 */
public abstract class WriterPoolProcessorHdfs extends WriterPoolProcessor {
    
    
    /**
     * @param name Name of this processor.
     */
    public WriterPoolProcessorHdfs(String name) {
    	this(name, "Pool of files processor");
    }
    	
    /**
     * @param name Name of this processor.
     * @param description Description for this processor.
     */
    public WriterPoolProcessorHdfs(final String name,
    		final String description) {
    	super();
    }

    public void crawlCheckpoint(File checkpointDir) throws IOException {
        int serial = getSerialNo().get();
        if (getPool().getNumActive() > 0) {
            // If we have open active Archive files, up the serial number
            // so after checkpoint, we start at one past current number and
            // so the number we serialize, is one past current serialNo.
            // All this serial number manipulation should be fine in here since
            // we're paused checkpointing (Revisit if this assumption changes).
            serial = getSerialNo().incrementAndGet();
        }
        
        //saveCheckpointSerialNumber(checkpointDir, serial);
        // Close all ARCs on checkpoint.
        try {
	    getPool().close();
        } finally {
            // Reopen on checkpoint.
            setupPool(new AtomicInteger(serial));
        }
    }

}

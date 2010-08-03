/*
 * HDFSWriterProcessor
 *
 * $Id: HDFSWriterProcessor.java,v 1.54 2006/09/01 00:55:51 paul_jack Exp $
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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringEscapeUtils;
import org.archive.crawler.datamodel.CoreAttributeConstants;
import org.archive.crawler.datamodel.CrawlURI;
import org.archive.crawler.framework.EngineConfig;
import static org.archive.modules.fetcher.FetchStatusCodes.*;

import org.archive.modules.writer.ARCWriterProcessor;
import org.archive.io.DefaultWriterPoolSettings;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolSettings;
import org.archive.io.arc.ARCConstants;
import org.archive.modules.ProcessResult;
import org.archive.modules.ProcessorURI;
import org.archive.modules.writer.MetadataProvider;
import org.archive.net.UURI;
import org.archive.state.Key;
import org.archive.state.KeyManager;
import org.archive.state.StateProvider;
import org.archive.util.anvl.ANVLRecord;
import org.archive.util.ArchiveUtils;
import org.archive.util.IoUtils;




/**
 * Processor module for writing the results of successful fetches (and
 * perhaps someday, certain kinds of network failures) to HDFS.
 *
 * Assumption is that there is only one of these HDFSWriterProcessors per
 * Heritrix instance.
 *
 * @author Doug Judd
 */
public class HDFSWriterProcessor extends WriterPoolProcessorHdfs 
//implements CoreAttributeConstants, ARCConstants, CrawlStatusListener,
implements CoreAttributeConstants, ARCConstants ,WriterPoolSettingsHdfs {
	//FetchStatusCodes {  //prats

	private static final long serialVersionUID = 5L;
	
	final static private String TEMPLATE = readTemplate();
	
    private static final Logger logger = Logger.getLogger(HDFSWriterProcessor.class.getName());

    public static final String NAMED_FIELD_CRAWL_TIME     = "Crawl-Time";
    public static final String NAMED_FIELD_IP_LABEL       = "Ip-Address";
    public static final String NAMED_FIELD_PATH_FROM_SEED = "Path-From-Seed";
    public static final String NAMED_FIELD_SEED           = "Is-Seed";
    public static final String NAMED_FIELD_URL            = "URL";
    public static final String NAMED_FIELD_VIA            = "Via";
    
    private static DefaultWriterPoolSettings defwpsettings;
    
    private static String attr_hdfs_fs_deafult_name;
    private static String attr_hdfs_output_path;
    private static String attr_hdfs_compression_type;
    private static int attr_hdfs_replication;    
    

    /*Keys added to this class by Pratyush in order to suit HDFSWriter with Heritrix2.0*/
    
    /***
     * HDFS fs.default.name
    */
    final public static Key<String> ATTR_HDFS_FS_DEFAULT_NAME = Key.make("hdfs://<host:port>/");
    
    /***
     * Key for the HDFS base output directory
     */
    final public static Key<String> ATTR_HDFS_OUTPUT_PATH = Key.make("hdfs-output-path");
    //public static final String ATTR_HDFS_OUTPUT_PATH = "hdfs-output-path";

    /***
     * Key for the HDFS replication count
     */
     final public static Key<String> ATTR_HDFS_COMPRESSION_TYPE = Key.make("DEFAULT");
     
    /***
     * Replication factor for HDFS files
     */
    final public static Key<Integer> ATTR_HDFS_REPLICATION = Key.make(2);
    
    final public static Key<List<String>> PATH = Key.makeSimpleList(String.class, "hdfs");
    
    static {
        KeyManager.addKeys(HDFSWriterProcessor.class);
      }
    /**
     * Calculate metadata once only.
     */
    transient private List<String> cachedMetadata = null;

    /**
     * Default Constructor, prats 
     *
     */
    
    public HDFSWriterProcessor() {
        this("HDFSWriterProcessor");
      }
    
    /**
     * @param name Name of this writer.
     */
    
    public HDFSWriterProcessor(String name) {
    	super(name, "HDFSWriter processor");
    }
    
    public synchronized void initialTasks(StateProvider global) {
     super.initialTasks(global);
     attr_hdfs_fs_deafult_name = global.get(this, ATTR_HDFS_FS_DEFAULT_NAME);
     attr_hdfs_output_path = global.get(this, ATTR_HDFS_OUTPUT_PATH);
     attr_hdfs_compression_type = global.get(this, ATTR_HDFS_COMPRESSION_TYPE);
     attr_hdfs_replication = global.get(this, ATTR_HDFS_REPLICATION);
     System.out.println("attr_hdfs_fs_deafult_name = " + attr_hdfs_fs_deafult_name);
     System.out.println("attr_hdfs_output_path = " + attr_hdfs_output_path);
     System.out.println("attr_hdfs_compression_type = " + attr_hdfs_compression_type);
     System.out.println("attr_hdfs_replication = " + attr_hdfs_replication);
    }
    
    protected void setupPool(final AtomicInteger serialNo) {
    	WriterPoolSettings hwps= getWriterPoolSettings();
		setPool(new HDFSWriterPool(serialNo, hwps, getMaxActive(),
            getMaxWait()));

    }
    
    /**
     * Writes a CrawlURI and its associated data to store file.
     *
     * Currently this method understands the following uri types: dns, http, 
     * and https.
     *
     * @param curi CrawlURI to process.
     */
    protected void innerProcess(CrawlURI curi) {
        // If failure, or we haven't fetched the resource yet, return
        if (curi.getFetchStatus() <= 0) {
            return;
        }
        
        // If no content, don't write record.
        int recordLength = (int)curi.getContentSize();
        if (recordLength <= 0) {
        	// Write nothing.
        	return;
        }
        
        String scheme = curi.getUURI().getScheme().toLowerCase();
        try {
            // TODO: Since we made FetchDNS work like FetchHTTP, IF we
            // move test for success of different schemes -- DNS, HTTP(S) and 
            // soon FTP -- up into CrawlURI#isSuccess (Have it read list of
            // supported schemes from heritrix.properties and cater to each's
            // notions of 'success' appropriately), then we can collapse this
            // if/else into a lone if (curi.isSuccess).  See WARCWriter for
            // an example.
            if ((scheme.equals("dns") &&
            		curi.getFetchStatus() == S_DNS_SUCCESS)) {
                write(curi);
            } else if ((scheme.equals("http") || scheme.equals("https")) &&
            		curi.getFetchStatus() > 0 && curi.isHttpTransaction()) {
                write(curi);
            } else if (scheme.equals("ftp") && (curi.getFetchStatus() == 200)) {
                write(curi);
            } else {
                logger.info("This writer does not write out scheme " + scheme +
                    " content");
            }
        } catch (IOException e) {
            curi.setError("WriteRecord: " + curi.toString() + e.getLocalizedMessage());
            logger.log(Level.SEVERE, "Failed write of Record: " +
                curi.toString(), e);
        }
    }
    
    protected void write(CrawlURI curi) throws IOException {
        WriterPoolMemberHdfs writer = (WriterPoolMemberHdfs)getPool().borrowFile();
        long position = writer.getPosition();
        // See if we need to open a new file because we've exceeed maxBytes.
        // Call to checkFileSize will open new file if we're at maximum for
        // current file.
        writer.checkSize();
        if (writer.getPosition() != position) {
            // We just closed the file because it was larger than maxBytes.
            // Add to the totalBytesWritten the size of the first record
            // in the file, if any.
            setTotalBytesWritten(getTotalBytesWritten() +
            	(writer.getPosition() - position));
            position = writer.getPosition();
        }

	ANVLRecord r = new ANVLRecord();
	r.addLabelValue(NAMED_FIELD_URL, curi.toString());
	r.addLabelValue(NAMED_FIELD_IP_LABEL, getHostAddress(curi));
	//r.addLabelValue(NAMED_FIELD_CRAWL_TIME, ArchiveUtils.get14DigitDate(curi.getLong(A_FETCH_BEGAN_TIME)));
	r.addLabelValue(NAMED_FIELD_CRAWL_TIME, ArchiveUtils.get14DigitDate(curi.getFetchBeginTime()));  //prats
	r.addLabelValue(NAMED_FIELD_SEED, Boolean.toString(curi.isSeed()));
	if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0)
	    r.addLabelValue(NAMED_FIELD_PATH_FROM_SEED, curi.getPathFromSeed());
	UURI via = curi.getVia();
	if (via != null && via.toString().trim().length() > 0)
	    r.addLabelValue(NAMED_FIELD_VIA, via.toString());
	final byte [] namedFieldsBlock = r.getUTF8Bytes();

        HDFSWriter w = (HDFSWriter)writer;
        try {
	    w.write(curi.toString(), namedFieldsBlock,
		    //curi.getHttpRecorder().getRecordedOutput(),   //prats
	    	curi.getRecorder().getRecordedOutput(),	
		    //curi.getHttpRecorder().getRecordedInput());  //prats
	    	curi.getRecorder().getRecordedInput());
	    System.out.println("HDFSWriterProcessor: Successfully written url " + 
	    		curi.getBaseURI());
        } catch (IOException e) {
            // Invalidate this file (It gets a '.invalid' suffix).
            getPool().invalidateFile(writer);
            // Set the writer to null otherwise the pool accounting
            // of how many active writers gets skewed if we subsequently
            // do a returnWriter call on this object in the finally block.
            writer = null;
            throw e;
        } finally {
            if (writer != null) {
            	setTotalBytesWritten(getTotalBytesWritten() +
            	     (writer.getPosition() - position));
                getPool().returnFile(writer);
            }
        }
        checkBytesWritten(curi);
    }

    /**
     * Return list of metadatas to add to first arc file metadata record.
     *
     * Get xml files from settingshandle.  Currently order file is the
     * only xml file.  We're NOT adding seeds to meta data. function modified by
     *	Prats tyo suit HEritrix 2.0.0
     * @return List of strings and/or files to add to arc file as metadata or
     * null.
     */
    public synchronized List<String> getMetadata(StateProvider global) {
    	if (TEMPLATE == null) {
            return null;
        }
        
        if (cachedMetadata != null) {
            return cachedMetadata;
        }
        
        MetadataProvider provider = global.get(this, METADATA_PROVIDER);
        
        String meta = TEMPLATE;
        meta = replace(meta, "${VERSION}", ArchiveUtils.VERSION);
        meta = replace(meta, "${HOST}", getHostName());
        meta = replace(meta, "${IP}", getHostAddress());
        
        if (provider != null) {
            meta = replace(meta, "${JOB_NAME}", provider.getJobName());
            meta = replace(meta, "${DESCRIPTION}", provider.getJobDescription());
            meta = replace(meta, "${OPERATOR}", provider.getJobOperator());
            // TODO: fix this to match job-start-date (from UI or operator setting)
            // in the meantime, don't include a slightly-off date
            // meta = replace(meta, "${DATE}", GMT());
            meta = replace(meta, "${USER_AGENT}", provider.getUserAgent());
            meta = replace(meta, "${FROM}", provider.getFrom());
            meta = replace(meta, "${ROBOTS}", provider.getRobotsPolicy());
        }

        this.cachedMetadata = Collections.singletonList(meta);
        return this.cachedMetadata;
       
    }
    
    private static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.log(Level.SEVERE, "Could not get local host name.", e);
            return "localhost";
        }
    }
    
    
    private static String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.log(Level.SEVERE, "Could not get local host address.", e);
            return "localhost";
        }        
    }


    private static String replace(String meta, String find, String replace) {
        replace = StringEscapeUtils.escapeXml(replace);
        return meta.replace(find, replace);
    }
    
    
    
    protected ProcessResult innerProcessResult(ProcessorURI puri) {
        CrawlURI curi = (CrawlURI)puri;
        
        //long recordLength = getRecordedSize(curi);
        
        ReplayInputStream ris = null;
        try {
            if (shouldWrite(curi)) {
                //ris = curi.getRecorder().getRecordedInput()
                  //      .getReplayInputStream();
                 write(curi);
                 return ProcessResult.PROCEED;
                 
            } else {
                logger.info("does not write " + curi.toString());
            }
         } catch (IOException e) {
            curi.getNonFatalFailures().add(e);
            logger.log(Level.SEVERE, "Failed write of Record: " +
                curi.toString(), e);
        } finally {
            IoUtils.close(ris);
        }
        return ProcessResult.PROCEED;
    }
    
    
    private static String readTemplate() {
        InputStream input = ARCWriterProcessor.class.getResourceAsStream(
                "arc_metadata_template.xml");
        if (input == null) {
            logger.severe("No metadata template.");
            return null;
        }
        try {
            return IoUtils.readFullyAsString(input);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            IoUtils.close(input);
        }
    }

    public String getSuffix() {
    	return defwpsettings.getSuffix();
    }
    
    public boolean isCompressed() {
        return defwpsettings.isCompressed();
    }
    
    public long getMaxSize() {
        return defwpsettings.getMaxSize();
    }
    
    public List getMetadata() {
        return defwpsettings.getMetadata();
    }
    
    public List<File> getOutputDirs() {
        return defwpsettings.getOutputDirs();
    }
    
    public String getPrefix() {
        return defwpsettings.getPrefix();
    }
    
    public String getJobDir() {
        EngineConfig conf = new EngineConfig();	
    	return conf.getJobsDirectory();
        }

    public String getHdfsFsDefaultName() {
    	return attr_hdfs_fs_deafult_name;
    }

    public String getHdfsBaseDir() {
    	return attr_hdfs_output_path;
    }


     public String getHdfsCompressionType() {
    	 return attr_hdfs_compression_type;
     }

     public int getHdfsReplication() {
    	 return attr_hdfs_replication;
     }
     
     public Key<List<String>> getPathKey() {
    	 return PATH;
     }
}

/* $Id$
 *
 * Created on January 20th, 2007
 *
 * Copyright (C) 2007  Zvents
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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.archive.io.ArchiveFileConstants;
import org.archive.io.WriterPoolMember;
import org.archive.util.ArchiveUtils;
import org.archive.util.TimestampSerialno;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;


/**
 * Member of {@link WriterPool}.
 * Implements WriterPoolMember for HDFS files.  Creates a job subdirectory of the same name as the
 * job subdirectory in the Heritrix installation and deposits files there.  It gives some guarantee
 *  of uniqueness, and position in file.
 * @author Doug Judd
 * @version $Date: 2006/09/26 22:51:28 $ $Revision: 1.14.2.1 $
 */
public abstract class WriterPoolMemberHdfs extends WriterPoolMember implements ArchiveFileConstants {
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    
    /**
     * Default file prefix.
     * 
     * Stands for Internet Archive Heritrix.
     */
    public static final String DEFAULT_PREFIX = "IAH";
    
    /**
     * Value to interpolate with actual hostname.
     */
    public static final String HOSTNAME_VARIABLE = "${HOSTNAME}";
    
    /**
     * Default for file suffix.
     */
    public static final String DEFAULT_SUFFIX = HOSTNAME_VARIABLE;

    /**
     * Reference to file Path object we're currently writing.
     */
    private Path fpath = null;

    /**
     * Reference to file name string since it's used often
     */
    private String fstr = null;

    /**
     *  Output stream for file.
     */
    private OutputStream out = null;
    
    /**
     * File output stream.

    private FSDataOutputStream fsdOut;
     */

    /**
     * SequenceFile writer
     * This is used to write the output in SequenceFile format
     */
    private SequenceFile.Writer sfWriter = null;

    /**
     * HDFS FileSystem object
     */
    private FileSystem fs = null;
    
    private final boolean compressed;
    private String jobDir = null;
    private String prefix = DEFAULT_PREFIX;
    private String suffix = DEFAULT_SUFFIX;
    private final long maxSize;   //pratyush
    private String hdfsOutputPath = null;
    private String hdfsFsDefaultName = "local";
    private String hdfsCompressionType = "Record";
    private Configuration hdfsConf = null;
    private int hdfsReplication = 3;

    /**
     * Accumulator to hold record contents
     */
    private byte [] accumBuffer = new byte [ 262144 ];
    private int accumOffset = 4;

    /**
     * Creation date for the current file.
     * Set by {@link #createFile()}.
     */
	private String createTimestamp = "UNSET!!!";
    
    /**
     * A running sequence used making unique file names.
     */
    final private AtomicInteger serialNo;
    
    /**
     * Directories round-robin index.
     */
    private static int roundRobinIndex = 0;

    /**
     * NumberFormat instance for formatting serial number.
     *
     * Pads serial number with zeros.
     */
    private static NumberFormat serialNoFormatter = new DecimalFormat("00000");
    
    /**
     * Constructor.
     *
     * @param serialNo  used to create unique filename sequences
     * @param jobDir Job directory
     * @param prefix File prefix to use.
     * @param cmprs Compress the records written. 
     * @param maxSize Maximum size for ARC files written.
     * @param hdfsReplication Replication factor for HDFS files
     * @param hdfsCompressionType Type of SequenceFile compression to use 
     * @param hdfsOutputPath Directory with HDFS where job content files
     *     will get written
     * @param hdfsFsDefaultName fs.default.name Hadoop property
     * @exception IOException
     */
    public WriterPoolMemberHdfs(AtomicInteger serialNo, 
            final String jobDir, final String prefix, 
            final boolean cmprs, final long maxSize,  //prats
	    final int hdfsReplication, final String hdfsCompressionType,
	    final String hdfsOutputPath, final String hdfsFsDefaultName)
	throws IOException {
        this(serialNo, jobDir, prefix, "", cmprs, maxSize,
	     hdfsReplication, hdfsCompressionType, hdfsOutputPath,
	     hdfsFsDefaultName);
    }
            
    /**
     * Constructor.
     *
     * @param serialNo  used to create unique filename sequences
     * @param jobDir Job directory
     * @param prefix File prefix to use.
     * @param suffix File tail to use.  If null, unused.
     * @param cmprs Compress the records written. 
     * @param maxSize Maximum size for ARC files written.
     * @param hdfsReplication Replication factor for HDFS files
     * @param hdfsCompressionType Type of SequenceFile compression to use 
     * @param hdfsOutputPath Directory with HDFS where job content files
     *     will get written
     * @param hdfsFsDefaultName fs.default.name Hadoop property
     * @exception IOException
     */
    public WriterPoolMemberHdfs(AtomicInteger serialNo,
            final String jobDir, final String prefix, 
            final String suffix, final boolean cmprs,
	    final long maxSize, final int hdfsReplication,  //pratyush
	    final String hdfsCompressionType, final String hdfsOutputPath,
	    final String hdfsFsDefaultName)
	throws IOException {
	super(serialNo, null, prefix, suffix, cmprs, maxSize, null);
        this.suffix = suffix;
        this.prefix = prefix;
        this.maxSize = maxSize;
	int lastSlash = jobDir.lastIndexOf("/");
	this.jobDir = (lastSlash == -1) ? jobDir : jobDir.substring(lastSlash+1);
        this.compressed = cmprs;
        this.serialNo = serialNo;
	this.hdfsReplication = hdfsReplication;
	if (hdfsOutputPath.endsWith("/"))
	    this.hdfsOutputPath =
		hdfsOutputPath.substring(0, hdfsOutputPath.length()-1);
	else
	    this.hdfsOutputPath = hdfsOutputPath;
	this.hdfsFsDefaultName = hdfsFsDefaultName;
	this.hdfsCompressionType = hdfsCompressionType;

	hdfsConf = new Configuration();
	hdfsConf.set("fs.default.name", this.hdfsFsDefaultName);
	System.out.println("fs.default.name="+this.hdfsFsDefaultName);

	fs = FileSystem.get(hdfsConf);

	// make sure the output directory exists
	Path outputDir = new Path(this.hdfsOutputPath + "/" + this.jobDir);
	fs.mkdirs(outputDir);
    }

    /**
     * Call this method just before/after any significant write.
     *
     * Call at the end of the writing of a record or just before we start
     * writing a new record.  Will close current file and open a new file
     * if file size has passed out maxSize.
     * 
     * <p>Creates and opens a file if none already open.
     *
     * @exception IOException
     */
    public void checkSize() throws IOException {
	if (sfWriter == null ||
	    (this.maxSize != -1 && (this.sfWriter.getLength() > this.maxSize))) {
            createFile();
	}
    }

    /**
     * Create a new file.
     * 
     * The resulting name looks something like this:  
     * IAH-20070111024623-00000-judd.dnsalias.org.open
     * The .gz extension is not included since compression is
     * handled in a different way within HDFS.
     * Usually called from {@link #checkSize()}.
     * @return Name of file created.
     * @throws IOException
     */
    protected String createFile() throws IOException {
        TimestampSerialno tsn = getTimestampSerialNo();
        String name = this.prefix + '-' + getUniqueBasename(tsn) +
            ((this.suffix == null || this.suffix.length() <= 0)?
                "": "-" + this.suffix) + OCCUPIED_SUFFIX;

	// close existing file
	close();

        this.createTimestamp = tsn.getTimestamp();
	fstr  = hdfsOutputPath + "/" + jobDir + "/" + name;
	this.fpath = new Path(fstr);

	// Determine SequenceFile compression type
	SequenceFile.CompressionType compType;
	if (hdfsCompressionType.equals("DEFAULT")) {
	    String zname = hdfsConf.get("io.seqfile.compression.type");
	    compType = (zname == null) ? SequenceFile.CompressionType.RECORD : 
		SequenceFile.CompressionType.valueOf(zname);
	}
	else
	    compType = SequenceFile.CompressionType.valueOf(hdfsCompressionType);

	int origRep = hdfsConf.getInt("dfs.replication", -1);
	hdfsConf.setInt("dfs.replication", hdfsReplication);

	sfWriter = SequenceFile.createWriter(this.fs, hdfsConf, this.fpath,
					     Text.class, Text.class, compType);

	hdfsConf.setInt("dfs.replication", origRep);

        logger.info("Opened " + this.fpath.toString());
	return this.fpath.toString();
    }
    
    protected synchronized TimestampSerialno getTimestampSerialNo() {
        return getTimestampSerialNo(null);
    }
    
    /**
     * Do static synchronization around getting of counter and timestamp so
     * no chance of a thread getting in between the getting of timestamp and
     * allocation of serial number throwing the two out of alignment.
     * 
     * @param timestamp If non-null, use passed timestamp (must be 14 digit
     * ARC format), else if null, timestamp with now.
     * @return Instance of data structure that has timestamp and serial no.
     */
    protected synchronized TimestampSerialno
            getTimestampSerialNo(final String timestamp) {
        return new TimestampSerialno((timestamp != null)?
                timestamp: ArchiveUtils.get14DigitDate(),
                serialNo.getAndIncrement());
    }

    /**
     * Return a unique basename.
     *
     * Name is timestamp + an every increasing sequence number.
     *
     * @param tsn Structure with timestamp and serial number.
     *
     * @return Unique basename.
     */
    private String getUniqueBasename(TimestampSerialno tsn) {
        return tsn.getTimestamp() + "-" +
           WriterPoolMemberHdfs.serialNoFormatter.format(tsn.getSerialNumber());
    }

    /**
     * Class for serializing an integer to a byte buffer
     */
    private static class IntegerSerializer extends ByteArrayOutputStream {
	DataOutputStream dos = null;
	public IntegerSerializer() {
	    super(4);
	    dos = new DataOutputStream(this);
	}
	public void write(int val, byte [] dst, int offset) throws IOException {
	    reset();
	    dos.writeInt(val);
	    if (count != 4)
		throw new AssertionError(count != 4);
	    System.arraycopy(buf, 0, dst, offset, 4);
	}
    }

    private IntegerSerializer iser = new IntegerSerializer();

    /**
     * Post write tasks.
     * 
     * Has side effects.  Will open new file if we're at the upperbound.
     *
     * @exception IOException
     */
    protected void preWriteRecordTasks()
    throws IOException {
        checkSize();
    }

    /**
     * Post file write tasks.
     *
     * @exception IOException
     */
    protected void postWriteRecordTasks(String uri)
    throws IOException {
	Text key = new Text(uri);
	Text value = new Text();
	iser.write(accumOffset-4, accumBuffer, 0);
	value.set(accumBuffer, 0, accumOffset);
	sfWriter.append(key, value);
	accumOffset = 4;
	if (accumBuffer.length > 1048576)
	    accumBuffer = new byte [ 262144 ];
    }

    /**
     * Postion in current physical file.
     * Used making accounting of bytes written.
     * @return Position in underlying file.  Call before or after writing
     * records *only* to be safe.
     * @throws IOException
     */
    public long getPosition() throws IOException {
        long position = 0;
        if (this.sfWriter != null) {
            // Call flush on underlying file though probably not needed assuming
            // above this.out.flush called through to this.fos.
            position = this.sfWriter.getLength() + accumOffset;
        }
        return position;
    }

    public boolean isCompressed() {
        return compressed;
    }
    
    protected void write(final byte [] b) throws IOException {
	if (accumBuffer.length - accumOffset < b.length)
	    growAccumBuffer(b.length-(accumBuffer.length-accumOffset));
	System.arraycopy(b, 0, accumBuffer, accumOffset, b.length);
	accumOffset += b.length;
    }
    
    protected void write(byte[] b, int off, int len) throws IOException {
	if (accumBuffer.length - accumOffset < len)
	    growAccumBuffer(len-(accumBuffer.length-accumOffset));
	System.arraycopy(b, off, accumBuffer, accumOffset, len);
	accumOffset += len;
    }

    protected void write(int b) throws IOException {
	if (accumBuffer.length - accumOffset < 1)
	    growAccumBuffer(1);
	accumBuffer[accumOffset] = (byte)b;
	accumOffset++;
    }
	
    protected void readFullyFrom(final InputStream is, final long recordLength)
	throws IOException {
        int total = 0;
	int remain = accumBuffer.length - accumOffset;

	if (remain < recordLength)
	    growAccumBuffer((int)recordLength-remain);

	total = is.read(accumBuffer, accumOffset, (int)recordLength);
	accumOffset += total;

        if (total != recordLength) {
	    throw new IOException("Read " + total + " but expected " +
				  recordLength);
        }
    }

    private void growAccumBuffer(int needed) {
	byte [] newBuf = new byte [ accumBuffer.length + needed + 8192 ];
	System.arraycopy(accumBuffer, 0, newBuf, 0, accumOffset);
	accumBuffer = newBuf;
    }
	
    public void close() throws IOException {
        if (this.sfWriter == null) {
            return;
        }
        this.sfWriter.close();
        this.out = null;
        if (this.fpath != null && this.fs.exists(fpath)) {
            String path = this.fpath.toString();
            if (path.endsWith(OCCUPIED_SUFFIX)) {
		fstr = path.substring(0, path.length() - OCCUPIED_SUFFIX.length());
		Path finalPath = new Path(fstr);
		if (!this.fs.rename(fpath, finalPath)) {
                    logger.warning("Failed rename of " + path);
                }
		this.fpath = new Path(fstr);
            }
            // not getting size here because it adds more dependency on HDFS
            logger.info("Closed " + this.fpath.toString() + ", size ");
        }
    }
    
    protected String getCreateTimestamp() {
	return createTimestamp;
    }

    public String getFilename() { return fstr; }
}

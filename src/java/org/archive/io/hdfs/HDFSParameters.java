package org.archive.io.hdfs;



/**
 * Configures the values of the field names used to save data from
 * the crawl. Also contains a full set of default values.
 *
 * Meant to be configured within the Spring framework either inline of
 * {@link org.archive.modules.writer.HDFSWriterProcessor} or as a named bean and referenced later on.
 *
 * <pre>
 * {@code
 * <bean id="hdfsParameters" class="org.archive.io.hdfs.HDFSParameters">
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.modules.writer.HDFSWriterProcessor
 *  {@link org.archive.modules.writer.HDFSWriterProcessor} for a full example
 *
 * @author greglu
 */
public class HDFSParameters {

	/** DEFAULT FIELD NAMES **/

	public static final String NAMED_FIELD_CRAWL_TIME		= "Crawl-Time";
    public static final String NAMED_FIELD_IP				= "Ip-Address";
    public static final String NAMED_FIELD_PATH_FROM_SEED	= "Path-From-Seed";
    public static final String NAMED_FIELD_IS_SEED			= "Is-Seed";
    public static final String NAMED_FIELD_URL				= "URL";
    public static final String NAMED_FIELD_VIA				= "Via";
    public static final String NAMED_FIELD_SEED_URL			= "Seed-Url";
    public static final String NAMED_FIELD_REQUEST			= "Request";
    public static final String NAMED_FIELD_RESPONSE			= "Response";

	private String urlFieldName				= NAMED_FIELD_URL;
    private String crawlTimeFieldName		= NAMED_FIELD_CRAWL_TIME;
	private String ipFieldName				= NAMED_FIELD_IP;
	private String pathFromSeedFieldName	= NAMED_FIELD_PATH_FROM_SEED;
	private String isSeedFieldName			= NAMED_FIELD_IS_SEED;
	private String viaFieldName				= NAMED_FIELD_VIA;
	private String seedUrlFieldName			= NAMED_FIELD_SEED_URL;
	private String requestFieldName			= NAMED_FIELD_REQUEST;
	private String responseFieldName		= NAMED_FIELD_RESPONSE;

	public String getCrawlTimeFieldName() {
		return crawlTimeFieldName;
	}
	public void setCrawlTimeFieldName(String crawlTimeFieldName) {
		this.crawlTimeFieldName = crawlTimeFieldName;
	}
	public String getIpFieldName() {
		return ipFieldName;
	}
	public void setIpFieldName(String ipFieldName) {
		this.ipFieldName = ipFieldName;
	}
	public String getPathFromSeedFieldName() {
		return pathFromSeedFieldName;
	}
	public void setPathFromSeedFieldName(String pathFromSeedFieldName) {
		this.pathFromSeedFieldName = pathFromSeedFieldName;
	}
	public String getIsSeedFieldName() {
		return isSeedFieldName;
	}
	public void setIsSeedFieldName(String isSeedFieldName) {
		this.isSeedFieldName = isSeedFieldName;
	}
	public String getViaFieldName() {
		return viaFieldName;
	}
	public void setViaFieldName(String viaFieldName) {
		this.viaFieldName = viaFieldName;
	}
	public void setSeedUrlFieldName(String seedUrlName) {
		this.seedUrlFieldName = seedUrlName;
	}
	public String getSeedUrlFieldName() {
		return seedUrlFieldName;
	}
	public String getUrlFieldName() {
		return urlFieldName;
	}
	public void setUrlFieldName(String urlFieldName) {
		this.urlFieldName = urlFieldName;
	}
	public String getRequestFieldName() {
		return requestFieldName;
	}
	public void setRequestFieldName(String requestFieldName) {
		this.requestFieldName = requestFieldName;
	}
	public String getResponseFieldName() {
		return responseFieldName;
	}
	public void setResponseFieldName(String responseFieldName) {
		this.responseFieldName = responseFieldName;
	}
	

	private String prefix				= "";
	private String suffix				= "";
	private boolean compression			= false;
	private long maxSize				= 63*1024*1024;
	private int hdfsReplication			= 3;
	private String hdfsCompressionType	= "DEFAULT";
	private String hdfsOutputPath		= "/crawl";
    private String hdfsFsDefaultName	= "hdfs://localhost:9000";

	public String getPrefix() {
		if (prefix.isEmpty())
			throw new RuntimeException("A filename prefix was never set for this object. " +
			"Define one before trying to access it.");

		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String getSuffix() {
		return suffix;
	}
	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}
	public boolean isCompression() {
		return compression;
	}
	public void setCompression(boolean compression) {
		this.compression = compression;
	}
	public long getMaxSize() {
		if (maxSize == 0L)
			throw new RuntimeException("A max size was never set for this object. " +
			"Define one before trying to access it.");

		return maxSize;
	}
	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}
	public int getHdfsReplication() {
		return hdfsReplication;
	}
	public void setHdfsReplication(int hdfsReplication) {
		this.hdfsReplication = hdfsReplication;
	}
	public String getHdfsCompressionType() {
		return hdfsCompressionType;
	}
	public void setHdfsCompressionType(String hdfsCompressionType) {
		this.hdfsCompressionType = hdfsCompressionType;
	}
	public String getHdfsOutputPath() {
		return hdfsOutputPath;
	}
	public void setHdfsOutputPath(String hdfsOutputPath) {
		this.hdfsOutputPath = hdfsOutputPath;
	}
	public String getHdfsFsDefaultName() {
		return hdfsFsDefaultName;
	}
	public void setHdfsFsDefaultName(String hdfsFsDefaultName) {
		this.hdfsFsDefaultName = hdfsFsDefaultName;
	}

}

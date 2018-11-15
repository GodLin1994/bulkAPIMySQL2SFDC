package com.cg.zcript;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;

import com.cg.zcript.StreamUtil;
import com.cg.zcript.CSVHelper;

import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class BulkApiHelper {

	public static final Logger log = Logger.getLogger(BulkApiHelper.class);
	
	public static int totalRecordCount;
	public static int successRecordCount;
	
	public static final String DEFAULT_TEST_ENDPOINT = "https://login.salesforce.com/services/Soap/u/37.0";
	
	private String endpoint;
	private String username;
	private String password;
	
    public static void main(String[] args) throws AsyncApiException, ConnectionException, IOException, InterruptedException {
//        BulkApiHelper helper = new BulkApiHelper();
//		String csvFilePath = "./src/com/capgemini/csv/20170928145400.csv";
//		try (InputStream is = new FileInputStream(csvFilePath)){
//	        helper.invoke("Price__c", "shu.chen@capgemini.com.kionchinademo", "passwordtoken", is, OperationEnum.insert);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} 
    	BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, Hardcode.username, Hardcode.password);
		String soql = "SELECT Id,Name,Phone from Account";
		List<List<String>> lists = bah.query("Account", soql);//lists whose first line as header and the rest as data 
		System.out.println(lists);
		
		
    }
    
    /**
     * Creates a Bulk API job and uploads batches for a CSV file.
     * @author chenshu
     */
    @Deprecated
    public void invoke(String sobjectType, String userName, String password, InputStream inputStream, OperationEnum operation)
            throws AsyncApiException, ConnectionException, IOException {
        BulkConnection connection = getBulkConnection(userName, password);
        JobInfo job = createJob(sobjectType, connection, operation);
        List<BatchInfo> batchInfoList = createBatchesFromCSVFile(connection, job, inputStream);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
    }

    /**
     * Creates a Bulk API job and uploads insert batches for lists.
     * The method is synchronized.
     * Preferable to invoke if insert.
     */
    public void insert(String sobjectType, List<List<String>> lists)
            throws AsyncApiException, ConnectionException, IOException {
        BulkConnection connection = getBulkConnection(username, password);
        JobInfo job = createJob(sobjectType, connection, OperationEnum.insert);
        List<BatchInfo> batchInfoList = createBatchesFromLists(connection, job, lists);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
    }

    /**
     * Creates a Bulk API job and uploads update batches for lists.
     * The method is synchronized.
     * @param sObjectName
     * @param lists the first line as header and the rest as data. The list must contains Id col.
     */
    public void update(String sObjectName, List<List<String>> lists)
            throws AsyncApiException, IOException, ConnectionException {
        BulkConnection connection = getBulkConnection(username, password);
        JobInfo job = createJob(sObjectName, connection, OperationEnum.update);
        List<BatchInfo> batchInfoList = createBatchesFromLists(connection, job, lists);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
    }

    /**
     * Creates a Bulk API job and uploads upsert batches for lists.
     * The method is synchronized.
     * @param sObjectName
     * @param lists the first line as header and the rest as data 
     * @param externalIdFieldName the logic PK to upsert
     */
    public void upsert(String sObjectName, List<List<String>> lists, String externalIdFieldName)
            throws AsyncApiException, IOException, ConnectionException {
        BulkConnection connection = getBulkConnection(username, password);
        JobInfo job = createJob(sObjectName, connection, OperationEnum.upsert, externalIdFieldName);
        List<BatchInfo> batchInfoList = createBatchesFromLists(connection, job, lists);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
    }

    /**
     * Creates a Bulk API job and uploads upsert batches for lists and delete the records which are not upserted
     * The method is synchronized.
     * @param sObjectName
     * @param lists the first line as header and the rest as data 
     * @param externalIdFieldName the logic PK to upsert
     */
    public void merge(String sObjectName, List<List<String>> lists, String externalIdFieldName)
            throws AsyncApiException, IOException, ConnectionException, InterruptedException {
    	//upsert
    	log.info("upsert");
    	Date upsertStart = new Date();
    	upsert(sObjectName, lists, externalIdFieldName);
    	//delete
		SimpleDateFormat sdfGMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdfGMT.setTimeZone(TimeZone.getTimeZone("GMT"));
    	String selectSOQL = "select Id from "+sObjectName+" where LastModifiedDate<"+sdfGMT.format(upsertStart);
    	log.info("delete: "+selectSOQL);
    	delete(sObjectName, selectSOQL);
    }
    
    /**
     * Gets the results of the operation and checks for errors.
     */
    private void checkResults(BulkConnection connection, JobInfo job,
              List<BatchInfo> batchInfoList)
            throws AsyncApiException, IOException {
        // batchInfoList was populated when batches were created and submitted
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr =
              new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();

            List<String> row;
//            boolean printed = false;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
//                if (!printed){
//                    log.info(resultInfo);
//                    printed = true;
//                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                boolean created = Boolean.valueOf(resultInfo.get("Created"));
                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created) {
                    log.debug("Created row with id " + id);
                    successRecordCount++;
                } else if (success) {
                    log.debug("Success row with id: " + id);
                    successRecordCount++;
                } else if (!success) {
                	if (error!=null&&!error.toString().startsWith("INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST:Amount Type")){
                        log.error("Failed with error: " + error);
                	}
                }
                totalRecordCount++;
            }
//            log.info(String.format("success/total: %s/%s", successRecordCount, recordCount));
        }
    }



    private void closeJob(BulkConnection connection, String jobId)
          throws AsyncApiException {
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }



    /**
     * Wait for a job to complete by polling the Bulk API.
     * 
     * @param connection
     *            BulkConnection used to check results.
     * @param job
     *            The job awaiting completion.
     * @param batchInfoList
     *            List of batches for this job.
     * @throws AsyncApiException
     */
    private void awaitCompletion(BulkConnection connection, JobInfo job,
          List<BatchInfo> batchInfoList)
            throws AsyncApiException {
        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            log.info("Awaiting results..." + incomplete.size());
            sleepTime = 10000L;
            BatchInfo[] statusList =
              connection.getBatchInfoList(job.getId()).getBatchInfo();
            for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed
                  || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        log.info("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }



    /**
     * Create a new job using the Bulk API.
     * 
     * @param sobjectType
     *            The object type being loaded, such as "Account"
     * @param connection
     *            BulkConnection used to create the new job.
     * @return The JobInfo for the new job.
     * @throws AsyncApiException
     */
    private JobInfo createJob(String sobjectType, BulkConnection connection, OperationEnum operation)
            throws AsyncApiException {
          return createJob(sobjectType, connection, operation, null);
      }

    private JobInfo createJob(String sobjectType, BulkConnection connection, OperationEnum operation, String externalIdFieldName)
            throws AsyncApiException {
          JobInfo job = new JobInfo();
          job.setObject(sobjectType);
          job.setOperation(operation);
          job.setContentType(ContentType.CSV);
          if (externalIdFieldName!=null){
              job.setExternalIdFieldName(externalIdFieldName);
          }
          job = connection.createJob(job); 
          log.debug(job);
          return job;
      }

    

    /**
     * Create the BulkConnection used to call Bulk API operations.
     */
    private BulkConnection getBulkConnection(String endpoint, String userName, String password)
          throws ConnectionException, AsyncApiException {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(endpoint);
//        partnerConfig.setTraceMessage(true);
//        partnerConfig.setPrettyPrintXml(true);
        // Creating the connection automatically handles login and stores
        // the session in partnerConfig
        new PartnerConnection(partnerConfig);
        // When PartnerConnection is instantiated, a login is implicitly
        // executed and, if successful,
        // a valid session is stored in the ConnectorConfig instance.
        // Use this key to initialize a BulkConnection:
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        // The endpoint for the Bulk API service is the same as for the normal
        // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "37.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
            + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        // This should only be false when doing debugging.
        config.setCompression(true);
        // Set this to true to see HTTP requests and responses on stdout
//        config.setTraceMessage(true);
//        config.setPrettyPrintXml(true);
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }

    /**
     * Create the BulkConnection used to call Bulk API operations.
     */
    private BulkConnection getBulkConnection(String username, String password)
    	throws ConnectionException, AsyncApiException {
    	return getBulkConnection(endpoint, username, password);
    }


    /**
     * Create and upload batches using a CSV file.
     * The file into the appropriate size batch files.
     * 
     * @param connection
     *            Connection to use for creating batches
     * @param jobInfo
     *            Job associated with new batches
     * @param csvFileName
     *            The source file for batch data
     */
    private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection,
          JobInfo jobInfo, InputStream csvFileName)
            throws IOException, AsyncApiException {
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        BufferedReader rdr = new BufferedReader(
            new InputStreamReader(csvFileName)
        );
        // read the CSV header row
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

        // Split the CSV file into multiple batches
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000; // 10 million bytes per batch
            int maxRowsPerBatch = 10000; // 10 thousand rows per batch
            int currentBytes = 0;
            int currentLines = 0;
            String nextLine;
            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
                // Create a new batch when our batch size limit is reached
                if (currentBytes + bytes.length > maxBytesPerBatch
                  || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }
                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }
            // Finished processing all rows
            // Create a final batch for any remaining data
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
        }
        return batchInfos;
    }

    /**
     * Create and upload batches using List<List<String>>.
     * The List<List<String>> into the appropriate size batch files.
     * The method is always preferable to createBatchesFromCSVFile
     * 
     * @author chenshu
     * 
     * @param connection
     *            Connection to use for creating batches
     * @param jobInfo
     *            Job associated with new batches
     * @param csvFileName
     *            The source file for batch data
     */
    private List<BatchInfo> createBatchesFromLists(BulkConnection connection, JobInfo jobInfo, List<List<String>> lists)
            throws IOException, AsyncApiException {
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
//        BufferedReader rdr = new BufferedReader(
//            new InputStreamReader(csvFileName)
//        );
        // read the CSV header row
        if (lists==null||lists.isEmpty()){
        	return batchInfos;
        }
        byte[] headerBytes = CSVHelper.convertListToCSVBytes(lists.get(0));
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

        // Split the CSV file into multiple batches
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000; // 10 million bytes per batch
            int maxRowsPerBatch = 10000; // 10 thousand rows per batch
            int currentBytes = 0;
            int currentLines = 0;
//            while ((nextLine = rdr.readLine()) != null) {
            for (int i=1;i<lists.size();i++){
                byte[] bytes = CSVHelper.convertListToCSVBytes(lists.get(i));
                // Create a new batch when our batch size limit is reached
                if (currentBytes + bytes.length > maxBytesPerBatch
                  || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }
                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }
            // Finished processing all rows
            // Create a final batch for any remaining data
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
        }
        return batchInfos;
    }

    /**
     * Create a batch by uploading the contents of the file.
     * This closes the output stream.
     * 
     * @param tmpOut
     *            The output stream used to write the CSV data for a single batch.
     * @param tmpFile
     *            The file associated with the above stream.
     * @param batchInfos
     *            The batch info for the newly created batch is added to this list.
     * @param connection
     *            The BulkConnection used to create the new batch.
     * @param jobInfo
     *            The JobInfo associated with the new batch.
     */
    private void createBatch(FileOutputStream tmpOut, File tmpFile,
      List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo)
              throws IOException, AsyncApiException {
        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);
        try {
            BatchInfo batchInfo =
              connection.createBatchFromStream(jobInfo, tmpInputStream);
            log.debug(batchInfo);
            batchInfos.add(batchInfo);

        } finally {
            tmpInputStream.close();
        }
    }

    /**
     * 
     * @param object
     * @param query
     * @return 返回查询结果，CSV格式的String
     * @throws ConnectionException
     * @throws AsyncApiException
     * @throws InterruptedException
     */
	private String doBulkQuery(String object, String query) throws ConnectionException, AsyncApiException, InterruptedException {
		BulkConnection bulkConnection = getBulkConnection(username, password);
		JobInfo job = new JobInfo();
		job.setObject(object);

		job.setOperation(OperationEnum.query);
		job.setConcurrencyMode(ConcurrencyMode.Parallel);
		job.setContentType(ContentType.CSV);

		job = bulkConnection.createJob(job);
		assert job.getId() != null;

		job = bulkConnection.getJobStatus(job.getId());

//		long start = System.currentTimeMillis();

		BatchInfo info = null;
		ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes());
		info = bulkConnection.createBatchFromStream(job, bout);

		String[] queryResults = null;
		final int POLLING_COUNT = 500;//40min
		for (int i = 0; i < POLLING_COUNT; i++) {
			Thread.sleep(5000L); 
			info = bulkConnection.getBatchInfo(job.getId(), info.getId());

			if (info.getState() == BatchStateEnum.Completed) {
				QueryResultList list = bulkConnection.getQueryResultList(job.getId(), info.getId());
				queryResults = list.getResult();
				break;
			} else if (info.getState() == BatchStateEnum.Failed) {
				log.info("-------------- failed ----------" + info);
				break;
			} else {
				log.info("-------------- waiting ----------" + info);
			}
		}
		
		StringBuffer ret = new StringBuffer();
		if (queryResults==null){
			log.info("queryResults.length:"+null);
			throw new AsyncApiException("Timeout: queryResults is null after ms: "+5000L*POLLING_COUNT, AsyncExceptionCode.Timeout);
		}else{
			log.info("queryResults.length:"+queryResults.length);
		}
		if (queryResults != null) {
			for (String resultId : queryResults) {
				InputStream queryResultStream = bulkConnection.getQueryResultStream(job.getId(), info.getId(), resultId);
				String queryResult = StreamUtil.convertInputStreamToString(queryResultStream);
				ret.append(queryResult);
			}
		}
		return ret.toString();
	}

	/**
     * Creates a Bulk API job and to query by sObjectName and soql
     * The method is synchronized.
	 * @param sObjectName
	 * @param soql
	 * @return lists whose first line as header and the rest as data 
	 */
	public List<List<String>> query(String sObjectName, String soql) throws ConnectionException, AsyncApiException, InterruptedException {
		String resultStr = doBulkQuery(sObjectName, soql);
		InputStream is = StreamUtil.convertStringToInputStream(resultStr);
		return CSVHelper.readListsFromCSV(is);
	}
	
	/**
	 * 把soql得出结果的Ids全都软删除
	 * @param sObjectName
	 * @param soql 只能以select Id from开头
	 */
	public void delete(String sObjectName, String soql) throws ConnectionException, AsyncApiException, InterruptedException, IOException{
		List<List<String>> lists = query(sObjectName, soql);
        BulkConnection connection = getBulkConnection(username, password);
        JobInfo job = createJob(sObjectName, connection, OperationEnum.delete);
        List<BatchInfo> batchInfoList = createBatchesFromLists(connection, job, lists);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
	}

	/**
	 * 把soql得出结果的Ids全都硬删除
	 * @param sObjectName
	 * @param soql 只能以select Id from开头
	 */
	public void hardDelete(String sObjectName, String soql) throws ConnectionException, AsyncApiException, InterruptedException, IOException{
		List<List<String>> lists = query(sObjectName, soql);
        BulkConnection connection = getBulkConnection(username, password);
        JobInfo job = createJob(sObjectName, connection, OperationEnum.hardDelete);
        List<BatchInfo> batchInfoList = createBatchesFromLists(connection, job, lists);
        closeJob(connection, job.getId());
        awaitCompletion(connection, job, batchInfoList);
        successRecordCount = totalRecordCount = 0;
        checkResults(connection, job, batchInfoList);
        log.info(String.format("Overall success/total: %s/%s", successRecordCount, totalRecordCount));
	}

	public BulkApiHelper(String endpoint, String username, String password) {
		super();
		this.endpoint = endpoint;
		this.username = username;
		this.password = password;
	}

	
}





































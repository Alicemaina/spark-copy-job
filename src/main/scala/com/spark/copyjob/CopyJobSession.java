package com.spark.copyjob;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.RateLimiter;


public class CopyJobSession {

	public static Logger logger = Logger.getLogger(CopyJobSession.class);
	private static CopyJobSession copyJobSession;
	private final Session session;

	private PreparedStatement insertStatement;
	private PreparedStatement selectStatement;

	// Read/Write Rate limiter
	// Determine the total throughput for the entire cluster in terms of wries/sec, reads/sec
	// then do the following to set the values as they are only applicable per JVM (hence spark Executor)...
	//  Rate = Total Throughput (write/read per sec) / Total Executors
	private RateLimiter readLimiter = RateLimiter.create(20000);
	private RateLimiter writeLimiter = RateLimiter.create(40000);

	public static CopyJobSession getInstance(String hosts) {

		if (copyJobSession == null) {
			synchronized (CopyJobSession.class) {
				if (copyJobSession == null) {
					copyJobSession = new CopyJobSession(hosts);
				}
			}
		}
		return copyJobSession;
	}

	private CopyJobSession(String hosts) {

		PoolingOptions options = new PoolingOptions();
		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setFetchSize(1000);

		// its using local quorum default with ExponentialRetryPolicy. The
		// default fetch size in the query options is set to 1000
		queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		Cluster cluster = Cluster.builder().withQueryOptions(queryOptions).withPoolingOptions(options)
				//.withRetryPolicy(new ExponentialRetryPolicy()) -- Can plug in retry policy
				.addContactPoints(hosts).build();


		session = cluster.connect();

	
		insertStatement = session.prepare(
				"insert into keyspace.table (col1,col2,col3) values (?,?,?)");

		selectStatement = session.prepare(
				"select col1, col2, col3 from keyspace.table where token(parition_key) >= ? and token(partition_key) <= ? and clustering_key=? ALLOW FILTERING");
	}

	public void getDataAndInsert(Long min, Long max) {
		try {

			Future<ResultSet> resultSet = session.executeAsync(selectStatement.bind(min, max));
			Collection<Future<ResultSet>> writeResults = new ArrayList<Future<ResultSet>>();
			
			ResultSet rs = resultSet.get();
			for (Row row : rs) {
				try {
					readLimiter.acquire();
					if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
						rs.fetchMoreResults(); // this is asynchronous
					}

					writeLimiter.acquire();
					
					//Sample insert query, fill it in with own details
					Future<ResultSet> writeResultSet = session.executeAsync(insertStatement.bind(row.getString(0)));
					writeResults.add(writeResultSet);
					
				} catch (Exception e) {
					logger.error("Error occurred" ,e);
				}
			}
			
			
			for(Future<ResultSet> writeResult: writeResults){
				//wait for the writes to complete for the batch. The Retry policy, if defined,  should retry the write on timeouts.
				writeResult.get();
			}
			
		} catch (Exception e) {
			System.out.println("Error occurred");
			logger.error("Error occurred" ,e);
		}

		
	}

}

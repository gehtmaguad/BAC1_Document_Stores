package couchbase;

import helper.Result;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.clustermanager.BucketType;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

public class Couchbase {

	// Result Object
	private Result result;

	// Node and Port Config
	private String node1 = "192.168.122.21";
	private String node2 = "192.168.122.147";
	private String node3 = "192.168.122.148";
	private String port = "8091";

	// Bucket
	private String bucket_name = "mybucket22";
	private String bucket_pwd = "";

	// Cluster Manager
	ClusterManager manager = null;
	private String cm_user = "Administrator";
	private String cm_pwd = "password";

	// Misc
	private CouchbaseClient client = null;
	private String unique = "A";

	// Testkonfig
	public Integer inserts = 10000;
	public Integer runs = 5;
	public PersistTo persist = null;
	public ReplicateTo replicate = null;
	public Boolean async = false;

	public Couchbase() {

	}

	public Couchbase(Integer inserts, Integer runs, PersistTo persist,
			ReplicateTo replicate, Boolean async) {
		this.inserts = inserts;
		this.runs = runs;
		this.persist = persist;
		this.replicate = replicate;
		this.async = async;
	}

	public Integer getInserts() {
		return inserts;
	}

	public void setInserts(Integer inserts) {
		this.inserts = inserts;
	}

	public Integer getRuns() {
		return runs;
	}

	public void setRuns(Integer runs) {
		this.runs = runs;
	}

	public PersistTo getPersist() {
		return persist;
	}

	public void setPersist(PersistTo persist) {
		this.persist = persist;
	}

	public ReplicateTo getReplicate() {
		return replicate;
	}

	public void setReplicate(ReplicateTo replicate) {
		this.replicate = replicate;
	}

	public Boolean getAsync() {
		return async;
	}

	public void setAsync(Boolean async) {
		this.async = async;
	}

	private ArrayList<URI> getHosts() throws URISyntaxException {

		// (Subset) of nodes in the cluster to establish a connection
		ArrayList<URI> hosts = new ArrayList<URI>();
		hosts.add(URI.create("http://" + this.node1 + ":" + this.port
				+ "/pools"));
		hosts.add(URI.create("http://" + this.node2 + ":" + this.port
				+ "/pools"));
		hosts.add(URI.create("http://" + this.node3 + ":" + this.port
				+ "/pools"));

		return hosts;
	}

	public void performWriteTest() throws Exception {

		// Intialize Variables
		this.result = new Result();

		// Setup Cluster Manager
		List<URI> hosts = getHosts();
		this.manager = new ClusterManager(hosts, this.cm_user, this.cm_pwd);

		// Create Test Environment
		createTestEnvironment();

		// Execute Runs
		for (Integer i = 0; i < this.runs; i++) {

			// Record Start Time
			long startTime = System.nanoTime();

			// Insert Documents
			insertDocuments(i);
			System.out.println("Finished insert");

			// Print Count
			 printCount();

			// Record End Time and calculate Run Time
			long estimatedTime = System.nanoTime() - startTime;
			double seconds = (double) estimatedTime / 1000000000.0;

			result.addMeasureResult("Run" + (i + 1), seconds, this.inserts);
		}

		// Delete Test Environment
		deleteTestEnvironment();

		this.result.printMeasureResult();
	}

	private void createTestEnvironment() throws URISyntaxException,
			InterruptedException {

//		// Create Bucket
		createBucket(this.manager);

		// Connect Client
		connectClient();

//	    // Create View
		createView();

	}

	private void deleteTestEnvironment() {

		// // Delete Bucket
		 deleteBucket(this.manager);

		// Disconnect Client
		disconnectClient();

		// Close Cluster Manager
		this.manager.shutdown();
	}

	private void connectClient() throws URISyntaxException {

		// Get Cluster URI
		List<URI> hosts = getHosts();

		// Setup ConnectionFactory
		CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
		cfb.setObsPollInterval(100);

		// Connect to the Cluster
		try {
			this.client = new CouchbaseClient(cfb.buildCouchbaseConnection(
					hosts, this.bucket_name, this.bucket_pwd));
		} catch (Exception e) {
			System.err.println("Error connecting to Couchbase: "
					+ e.getMessage());
			System.exit(1);
		}
	}

	private void disconnectClient() {
		this.client.shutdown();
	}

	private void createBucket(ClusterManager manager)
			throws InterruptedException {

		// Create Bucket
		manager.createNamedBucket(BucketType.COUCHBASE, this.bucket_name, 100,
				2, this.bucket_pwd, false);

		// Sleep x seconds, so replication between nodes is initialized when
		// returning
		Thread.sleep(10000);
	}

	private void deleteBucket(ClusterManager manager) {

		// Delete Bucket
		manager.deleteBucket(this.bucket_name);
	}

	private void insertDocuments(Integer run) throws IOException,
			InterruptedException, ExecutionException, TimeoutException,
			URISyntaxException {

		// Insert Documents Asynchronous

		// Declare future Object
		OperationFuture<Boolean> future;
		String key;
		String value = "{ \"test\": \"test\" }";

		if (this.async == true) {

			final CountDownLatch latch = new CountDownLatch(this.inserts - 1);

			for (int i = 0; i < this.inserts; i++) {
				key = String.valueOf(run) + this.unique + String.valueOf(i);
				future = this.client.set(key, value);
				future.addListener(new OperationCompletionListener() {
					@Override
					public void onComplete(OperationFuture<?> future2)
							throws Exception {
						latch.countDown();
					}
				});
			}

			latch.await();
		
		}

		// Insert Documents Synchronous
		if (this.async == false) {

			for (int i = 0; i < this.inserts; i++) {
				
				key = String.valueOf(run) + this.unique + String.valueOf(i);
				try {
					if (this.replicate != null && this.persist != null) {
						this.client.set(key, value, this.persist,
								this.replicate).get();
					} else if (this.replicate != null && this.persist == null) {
						this.client.set(key, 0, value, this.replicate).get();
					} else if (this.replicate == null && this.persist != null) {
						this.client.set(key, 0, value, this.persist).get();
					} else {
						this.client.set(key, 0, value).get();
					}
				} catch (Exception e) {
					System.out.println("Error on inserting element: " + key);
				}
			}

		}

	}

	private void printCount() throws URISyntaxException {

		System.setProperty("viewmode", "production");
		View view = this.client.getView("documents", "itemCount");
		Query query = new Query();
		query.setIncludeDocs(true);
		query.setStale(Stale.FALSE); // Read actual data
		ViewResponse result = this.client.query(view, query);

		for (ViewRow row : result) {
			System.out.println("Rowcount: " + row.getValue());
		}

	}

	private void createView() throws URISyntaxException, InterruptedException {

		DesignDocument designDoc = new DesignDocument("dev_documents");
		String viewName = "itemCount";
		String mapFunction = "function (doc, meta) {\n"
				+ "emit(meta.id, null)\n" + "}";
		String reduceFunction = "_count";
		ViewDesign viewDesign = new ViewDesign(viewName, mapFunction,
				reduceFunction);
		designDoc.getViews().add(viewDesign);
		this.client.createDesignDoc(designDoc);

		Thread.sleep(30000);

	}

}
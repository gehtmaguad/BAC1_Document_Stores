package couchdb;

import helper.Result;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.BufferingResponseListener;

public class CouchDB {

	// Result Object
	private Result result;

	// Node and Port Config
	private String node1 = "192.168.122.21";
	private String node2 = "192.168.122.167";
	private String node3 = "192.168.122.194";
	private String port = "5984";

	// DB
	private String db_name = "hello-world";

	// Run Configuration
	public Integer inserts = null;
	public Integer runs = null;
	public OperationType op_type = null;

	// Operationen auch serverseitig mit delayed_commits beeinflussbar
	// (default.ini)

	public CouchDB() {

	}

	public CouchDB(Integer inserts, Integer runs, OperationType op_type) {
		this.inserts = inserts;
		this.runs = runs;
		this.op_type = op_type;
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

	public OperationType getOperationType() {
		return op_type;
	}

	public void setOperationType(OperationType op_type) {
		this.op_type = op_type;
	}

	public void performWriteTest() throws Exception {

		// Intialize Variables
		this.result = new Result();

		// Create Source and Destination DB and create Replication between them
		createDB("node1");
		createDB("node2");
		createDB("node3");

		// executeWriteTest
		executeWriteRun();

		// // DeleteDB
		deleteDB("node1");
		deleteDB("node2");
		deleteDB("node3");

		this.result.printMeasureResult();
	}

	private void executeWriteRun() throws Exception {

		// Execute Runs
		for (Integer i = 0; i < this.runs; i++) {

			// Record Start Time
			long startTime = System.nanoTime();

			// Insert Documents
			insertDocuments();

			// Record End Time and calculate Run Time
			long estimatedTime = System.nanoTime() - startTime;
			double seconds = (double) estimatedTime / 1000000000.0;

			System.out.println("Run" + String.valueOf((i + 1)) + String.valueOf(seconds) + String.valueOf(this.inserts));
			result.addMeasureResult("Run" + (i + 1), seconds, this.inserts);
		}
	}

	private URI getUri(String node) throws Exception {

		String concatenate_url = null;

		if (node == "node1") {
			concatenate_url = "http://" + this.node1 + ":" + this.port + "/"
					+ this.db_name;
		} else if (node == "node2") {
			concatenate_url = "http://" + this.node2 + ":" + this.port + "/"
					+ this.db_name;
		} else if (node == "node3") {
			concatenate_url = "http://" + this.node3 + ":" + this.port + "/"
					+ this.db_name;
		} else {
			throw new Exception();
		}

		URI uri = new URI(concatenate_url);

		return uri;
	}

	private URI getUriBatchProcess() throws URISyntaxException {

		String concatenate_url = "http://" + this.node1 + ":" + this.port + "/"
				+ this.db_name + "?batch=ok";

		URI uri = new URI(concatenate_url);

		return uri;
	}

	private void createDB(String node) throws Exception {

		URI uri = getUri(node);
		HttpURLConnection conn = (HttpURLConnection) uri.toURL()
				.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("PUT");
		System.out
				.println("Return Code of createDB: " + conn.getResponseCode());

	}

	private void deleteDB(String node) throws Exception {

		URI uri = getUri(node);
		HttpURLConnection conn = (HttpURLConnection) uri.toURL()
				.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("DELETE");
		System.out
				.println("Return Code of deleteDB: " + conn.getResponseCode());

	}

	private void replicate(Boolean continuous) throws Exception {

		// Define Source and Destination
		String concatenate_url = "http://" + this.node1 + ":" + this.port + "/"
				+ "_replicate";

		URI uri = new URI(concatenate_url);

		String source = "http://" + this.node1 + ":" + this.port + "/"
				+ this.db_name + "/";

		String target1 = "http://" + this.node2 + ":" + this.port + "/"
				+ this.db_name + "/";
		
		String target2 = "http://" + this.node3 + ":" + this.port + "/"
				+ this.db_name + "/";		

		// SetUp Replication
		HttpURLConnection conn = (HttpURLConnection) uri.toURL()
				.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.addRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");

		try {

			OutputStreamWriter out = new OutputStreamWriter(
					conn.getOutputStream());

			if (continuous == true) {
				out.write("{" + "\"source\": " + source + "," + "\"target\": "
						+ target1 + "," + "\"continuous\": true" + "}");
				out.write("{" + "\"source\": " + source + "," + "\"target\": "
						+ target2 + "," + "\"continuous\": true" + "}");				
			} else {
				out.write("{" + "\"source\": " + source + "," + "\"target\": "
						+ target1 + "}");
				out.write("{" + "\"source\": " + source + "," + "\"target\": "
						+ target2 + "}");				
			}
			out.flush();
			out.close();

		} catch (java.net.NoRouteToHostException e) {
			System.out.println("NoRouteToHostException");
		} 

		System.out.println("Return Code of replicate: "
				+ conn.getResponseCode());

		conn.getResponseCode();
		conn.disconnect();
	}

	private void insertDocuments() throws Exception {

		URI uri = null;

		if (OperationType.ASYNC == this.op_type) {
			uri = getUri("node1");
			insertAsyncMode(uri);
		} else if (OperationType.BATCH == this.op_type) {
			uri = getUriBatchProcess(); // assumes node1 is master
			insertMode(uri, false);
		} else if (OperationType.BULK == this.op_type) {
			uri = getUri("node1");
			insertBulkMode(uri);
		} else if (OperationType.REPLICA_ACK == this.op_type) {
			insertMode(uri, true);
		}

	}

	private void insertMode(URI uri, Boolean replica_ack) throws Exception {

		for (int i = 0; i < this.inserts; i++) {
			HttpURLConnection conn = (HttpURLConnection) uri.toURL()
					.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.addRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			try {
				OutputStreamWriter out = new OutputStreamWriter(
						conn.getOutputStream());

				out.write("{\"" + String.valueOf(i) + "\": \"test\"}");

				out.flush();
				out.close();
			} catch (java.net.NoRouteToHostException e) {
				// DEBUG INFO
				System.out.println("NoRouteToHostException: " + i);
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}

			conn.getResponseCode();
			conn.disconnect();

			// SYNC AFTER EVERY INSERT
			if (replica_ack == true) {
				replicate(false);
			}
		}

	}

	private void insertBulkMode(URI uri) throws MalformedURLException,
			IOException {

		HttpURLConnection conn = (HttpURLConnection) uri.toURL()
				.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.addRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		try {
			OutputStreamWriter out = new OutputStreamWriter(
					conn.getOutputStream());

			out.write("{\"docs\":[");
			for (int i = 0; i < (this.inserts - 1); i++) {
				out.write("{\"" + String.valueOf(i) + "\": \"test\"},");
				out.flush();
			}
			out.write("{\"" + String.valueOf(this.inserts) + "\": \"test\"}");
			out.write("]}");

			out.flush();
			out.close();
		} catch (java.net.NoRouteToHostException e) {
			// DEBUG INFO
			System.out.println("NoRouteToHostException");
		} 

		conn.getResponseCode();
		conn.disconnect();
	}

	private void insertAsyncMode(URI uri) throws Exception {

		HttpClient client = new HttpClient();
		client.setMaxConnectionsPerDestination(this.inserts);
		client.setMaxRequestsQueuedPerDestination(this.inserts);
		final CountDownLatch countDown = new CountDownLatch(this.inserts);
		client.start();

		for (int i = 0; i < this.inserts; i++) {

			client.POST(uri).header("Content-Type", "application/json")
					.file(Paths.get("/home/markus/json.json"))
					.send(new BufferingResponseListener() {

						@Override
						public void onComplete(
								org.eclipse.jetty.client.api.Result res) {
							countDown.countDown();
						}

						@Override
						public void onFailure(Response response,
								Throwable failure) {
							countDown.countDown();
						}
					});

		}
		countDown.await();
		Thread.sleep(60000);
		client.stop();

	}

}
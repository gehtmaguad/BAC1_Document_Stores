package mongodb;

import helper.Result;

import java.net.UnknownHostException;
import java.util.ArrayList;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongoDB {

	// Result Object
	private Result result;

	// Node and Port Config
	private String node1 = "192.168.122.21";
	private String node2 = "192.168.122.147";
	private String node3 = "192.168.122.148";
	private Integer port = 27017;

	// Host
	private MongoClient mongoClient;

	// Collection
	private String collection_name = "test_collection";
	private DBCollection collection = null;

	// DB
	private String db_name = "test_db";
	private DB db = null;

	// Testkonfig
	public Integer inserts = 100000;
	public Integer runs = 5;
	public WriteConcern write_concern = null;

	public MongoDB() {

	}

	public MongoDB(Integer inserts, Integer runs, WriteConcern write_concern) {
		this.inserts = inserts;
		this.runs = runs;
		this.write_concern = write_concern;
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

	public WriteConcern getWrite_concern() {
		return write_concern;
	}

	public void setWrite_concern(WriteConcern write_concern) {
		this.write_concern = write_concern;
	}

	private ArrayList<ServerAddress> getHosts() throws UnknownHostException {
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
		addrs.add(new ServerAddress(this.node1, this.port));
		addrs.add(new ServerAddress(this.node2, this.port));
		addrs.add(new ServerAddress(this.node3, this.port));
		return addrs;
	}

	public Result performWriteTest() throws Exception {

		// Intialize Variables
		this.result = new Result();

		// Create Test Environment
		createTestEnvironment();

		// Execute Runs
		for (Integer i = 0; i < this.runs; i++) {

			// Record Start Time
			long startTime = System.nanoTime();

			// Insert Documents
			insertDocuments();

			// Print Count
			 printCount();

			// Record End Time and calculate Run Time
			long estimatedTime = System.nanoTime() - startTime;
			double seconds = (double) estimatedTime / 1000000000.0;

			result.addMeasureResult("Run" + (i), seconds, this.inserts);
			System.out.println("Run" + (i) + " finished");

		}

		// Delete Test Environment
		 deleteTestEnvironment();

		// Print Result
		return this.result;

	}

	private void createTestEnvironment() throws Exception {

		// Get Cluster URI
		ArrayList<ServerAddress> addrs = getHosts();

		// Connect to MongoDB Server
		this.mongoClient = new MongoClient(addrs);
		
		// Configure Write Concern
		this.mongoClient.setWriteConcern(write_concern);
		

		// Set Read Preference
		this.mongoClient.setReadPreference(ReadPreference.secondary());

		// Connect to Database (Creates the DB if it does not exist)
		this.db = this.mongoClient.getDB(this.db_name);

		// Create and Connect to Collection
		this.db.createCollection(this.collection_name, null);
		this.collection = this.db.getCollection(this.collection_name);

	}

	private void deleteTestEnvironment() {

		// Delete Connection
		this.db.getCollection(this.collection_name).drop();

	}

	private void insertDocuments() {
		for (int i = 0; i < this.inserts; i++) {
			try {
				this.collection.insert(new BasicDBObject(String.valueOf(i),
						"test"));
			} catch (Exception e) {
				System.out.println("Error on inserting element: " + i);
				e.printStackTrace();
			}
		}
	}

	private void printCount() {
		System.out.println(this.collection.find().count());
	}

}

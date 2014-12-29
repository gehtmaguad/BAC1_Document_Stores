package main;

import mongodb.MongoDB;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import com.mongodb.WriteConcern;

import couchbase.Couchbase;
import couchdb.CouchDB;
import couchdb.OperationType;

public class Tester {
	
	public static void main(String[] args) {
		
		// Couchbase
		Couchbase couchbase = new Couchbase(10000, 1000, PersistTo.ZERO, ReplicateTo.ZERO, false);

		try {
			couchbase.performWriteTest();
		} catch (Exception e) {
			System.out.println("Test Failed");
			e.printStackTrace();
		}
		
		// CouchDB
		OperationType op_type = OperationType.BATCH;
		CouchDB couchdb = new CouchDB(10000, 3, op_type);
		try {
			couchdb.performWriteTest();
		} catch (Exception e) {
			System.out.println("Test Failed");
			e.printStackTrace();
		}
		
//		 MongoDB
		WriteConcern write_concern = WriteConcern.ACKNOWLEDGED;
		MongoDB mongodb = new MongoDB(100000, 1, write_concern);
		try {
			mongodb.performWriteTest().printMeasureResult();
		} catch (Exception e) {
			System.out.println("Test Failed");
			e.printStackTrace();
		}
		
	}
}

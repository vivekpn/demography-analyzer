package com.se.db;

/**
 * Created by Yathish on 3/2/17.
 */
import java.util.Collection;
import java.util.List;

import org.bson.BsonSerializationException;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;

import com.mongodb.MongoClient;
import com.se.data.DemographyEntry;

public class DatabaseUtil {
	private static final String DATABASE_NAME = "DemographyAnalysis";
	private final Datastore datastore;

	public DatabaseUtil() {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		Morphia morphia = new Morphia();

		morphia.mapPackage("com.se.file");
		morphia.mapPackage("com.se.data");

		datastore = morphia.createDatastore(mongoClient, DATABASE_NAME);
		datastore.ensureIndexes();
	}

	public void insert(Collection<Object> objects) {
		datastore.save(objects);
	}

	public <T> T searchById(Class<T> cls, Object term) {
		return datastore.get(cls, term);
	}

	public DemographyEntry searchInvertedIndex(String term) {
		return searchById(DemographyEntry.class, term);
	}

	public void insert(Object object) {
		try {
			datastore.save(object);
		} catch (BsonSerializationException exception) {
			System.err.print("Error while inserting to MongoDB");
			System.err.println(exception);
		}
	}

	public <T> List<T> search(Class<T> tClass, String key, Object value) {
		Query<T> query = datastore.createQuery(tClass);
		query.field(key).equals(value);
		return query.asList();
	}
	

}

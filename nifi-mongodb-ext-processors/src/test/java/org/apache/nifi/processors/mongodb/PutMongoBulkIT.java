package org.apache.nifi.processors.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class PutMongoBulkIT { // (a PR against NiFi would get this processor in nifi-mongodb-bundle and we can use MongoWriteTestBase)
    static final String MONGO_URI = "mongodb://localhost";
    static final String COLLECTION_NAME = "test";
    static final String DATABASE_NAME = PutMongoBulk.class.getSimpleName();

    static final List<Document> DOCUMENTS = Arrays.asList(
            new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
            new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4),
            new Document("_id", "doc_3").append("a", 1).append("b", 3)
    );

    protected MongoClient mongoClient;
    protected MongoCollection<Document> collection;

    @Before
    public void setup() {
        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
        collection = mongoClient.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME);
    }

    @After
    public void teardown() {
        mongoClient.getDatabase(DATABASE_NAME).drop();
    }

    TestRunner init() {
        TestRunner runner = TestRunners.newTestRunner(PutMongoBulk.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DATABASE_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        return runner;
    }

    @Test
    public void testBulkWriteInsert() {
        TestRunner runner = init();

        StringBuffer doc = new StringBuffer();
        doc.append("[");
        for (int i = 0; i < DOCUMENTS.size(); i++) {
            if (i > 0) {
                doc.append(", ");
            }
            doc.append("{\"insertOne\": {\"document\": ");
            doc.append(DOCUMENTS.get(i).toJson());
            doc.append("}}");
        }
        doc.append("]");
        runner.enqueue(doc.toString());
        runner.run();
        runner.assertTransferCount(PutMongo.REL_FAILURE, 0);
        runner.assertTransferCount(PutMongo.REL_SUCCESS, 1);

        assertEquals(3, collection.countDocuments());
        Document doc1 = collection.find(new Document().append("_id", "doc_1")).first();
        assertNotNull(doc1);
        assertEquals(3, doc1.getInteger("c", 0));
    }

    // (TODO: remaining - each type + maybe transaction)
}

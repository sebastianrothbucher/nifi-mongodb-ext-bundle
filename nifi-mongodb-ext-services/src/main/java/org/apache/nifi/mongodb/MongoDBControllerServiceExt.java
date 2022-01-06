package org.apache.nifi.mongodb;

import org.bson.BsonDocument;

public class MongoDBControllerServiceExt extends MongoDBControllerService {

    public BsonDocument toBSON(String json) { // just expose the method, that is all we need!
        return BsonDocument.parse(json);
    }
}

package org.apache.nifi.mongodb;

import org.bson.BsonDocument;

public interface MongoDBClientServiceExt {

    public BsonDocument toBSON(String json);
}

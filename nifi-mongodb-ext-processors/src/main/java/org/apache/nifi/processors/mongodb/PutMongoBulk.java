package org.apache.nifi.processors.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.util.JSON;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.nio.charset.Charset;
import java.util.*;

@EventDriven
@Tags({ "mongodb", "insert", "update", "write", "put", "bulk" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB as a mongo bulk-update")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutMongoBulk extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the data is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(WRITE_CONCERN);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final WriteConcern writeConcern = getWriteConcern(context);

        try {
            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, content, true));

            // parse
            final Object doc = JSON.parse(new String(content, charset));

            BasicDBList updateItems = (BasicDBList)doc;
            List updateModels = new ArrayList();
            for (Object item : updateItems) {
                final BasicDBObject updateItem = (BasicDBObject) item;
                if (updateItem.keySet().size() != 1) {
                    logger.error("Invalid bulk-update: more than one type given {}", new Object[] { String.join(", ", updateItem.keySet()) });
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                    return;
                }
                // TODO: here: does mongo-shell not have sth here??
                final String updateType = updateItem.keySet().iterator().next();
                if ("insertOne".equals(updateType)) {
                    final InsertOneModel insertOneModel = new InsertOneModel(updateItem.get("document"));
                    updateModels.add(insertOneModel);
                } else if ("updateOne".equals(updateType)) {
                    final UpdateOptions options = parseUpdateOptions(updateItem);
                    final UpdateOneModel updateOneModel = new UpdateOneModel((BasicDBObject) updateItem.get("filter"), (BasicDBObject) updateItem.get("update"), options);
                    updateModels.add(updateOneModel);
                } else if ("updateMany".equals(updateType)) {
                    final UpdateOptions options = parseUpdateOptions(updateItem);
                    final UpdateManyModel updateManyModel = new UpdateManyModel((BasicDBObject) updateItem.get("filter"), (BasicDBObject) updateItem.get("update"), options);
                    updateModels.add(updateManyModel);
                } else if ("replaceOne".equals(updateType)) {
                    final ReplaceOptions options = parseReplaceOptions(updateItem);
                    final ReplaceOneModel replaceOneModel = new ReplaceOneModel((BasicDBObject) updateItem.get("filter"), updateItem.get("replacement"), options);
                    updateModels.add(replaceOneModel);
                } else if ("deleteOne".equals(updateType)) {
                    final DeleteOptions options = parseDeleteOptions(updateItem);
                    final DeleteOneModel deleteOneModel = new DeleteOneModel((BasicDBObject) updateItem.get("filter"), options);
                    updateModels.add(deleteOneModel);
                } else  if ("deleteMany".equals(updateType)) {
                    final DeleteOptions options = parseDeleteOptions(updateItem);
                    final DeleteManyModel deleteManyModel = new DeleteManyModel((BasicDBObject) updateItem.get("filter"), options);
                    updateModels.add(deleteManyModel);
                } else {
                    logger.error("Invalid bulk-update: invalid update type {}", new Object[] { updateType });
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                    return;
                }
            }

            collection.bulkWrite(updateModels);
            logger.info("bulk-updated {} into MongoDB", new Object[] { flowFile });

            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to bulk-update {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected UpdateOptions parseUpdateOptions(BasicDBObject updateItem) {
        final UpdateOptions options = new UpdateOptions();
        if (updateItem.containsField("upsert")) {
            options.upsert((boolean) updateItem.get("upsert"));
        }
        if (updateItem.containsField("arrayFilters")) {
            options.arrayFilters((List<? extends Bson>) updateItem.get("arrayFilters"));
        }
        if (updateItem.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateItem.get("collation")));
        }
        return options;
    }

    protected ReplaceOptions parseReplaceOptions(BasicDBObject updateItem) {
        final ReplaceOptions options = new ReplaceOptions();
        if (updateItem.containsField("upsert")) {
            options.upsert((boolean) updateItem.get("upsert"));
        }
        if (updateItem.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateItem.get("collation")));
        }
        return options;
    }

    protected DeleteOptions parseDeleteOptions(BasicDBObject updateItem) {
        final DeleteOptions options = new DeleteOptions();
        if (updateItem.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateItem.get("collation")));
        }
        return options;
    }

    protected Collation parseCollation(BasicDBObject updateItem) {
        final Collation collation = Collation.builder().build();
        // TODO
        return collation;
    }

    protected WriteConcern getWriteConcern(final ProcessContext context) {
        final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
        WriteConcern writeConcern = null;
        switch (writeConcernProperty) {
        case WRITE_CONCERN_ACKNOWLEDGED:
            writeConcern = WriteConcern.ACKNOWLEDGED;
            break;
        case WRITE_CONCERN_UNACKNOWLEDGED:
            writeConcern = WriteConcern.UNACKNOWLEDGED;
            break;
        case WRITE_CONCERN_FSYNCED:
            writeConcern = WriteConcern.FSYNCED;
            break;
        case WRITE_CONCERN_JOURNALED:
            writeConcern = WriteConcern.JOURNALED;
            break;
        case WRITE_CONCERN_REPLICA_ACKNOWLEDGED:
            writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            break;
        case WRITE_CONCERN_MAJORITY:
            writeConcern = WriteConcern.MAJORITY;
            break;
        default:
            writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }
}

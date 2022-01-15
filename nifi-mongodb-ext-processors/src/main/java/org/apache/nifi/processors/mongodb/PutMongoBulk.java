package org.apache.nifi.processors.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
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

import java.lang.reflect.Field;
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

    static final PropertyDescriptor ORDERED = new PropertyDescriptor.Builder()
            .name("Ordered")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("Ordered execution of bulk-writes - otherwise arbitrary order")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor USE_TRANSACTION = new PropertyDescriptor.Builder()
            .name("Use transaction")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("Run all actions in one MongoDB transaction - does NOT work with a client service so far")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

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
        // force package scope for inherited fields
        // (this is not super-nice, but not unheard of in NiFi source - plus: a PR against NiFi would get this processor in nifi-mongodb-bundle and we can use the attributes w/out reflection)
        try {
            Field descriptorsField = AbstractMongoProcessor.class.getDeclaredField("descriptors");
            descriptorsField.setAccessible(true);
            Field writeConcernField = AbstractMongoProcessor.class.getDeclaredField("WRITE_CONCERN");
            writeConcernField.setAccessible(true);
            _propertyDescriptors.addAll((List<PropertyDescriptor>) descriptorsField.get(null));
            _propertyDescriptors.add((PropertyDescriptor) writeConcernField.get(null));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        _propertyDescriptors.add(ORDERED);
        _propertyDescriptors.add(USE_TRANSACTION);
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

        ClientSession clientSession = null;
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
                final String updateType = updateItem.keySet().iterator().next();
                final BasicDBObject updateSpec = (BasicDBObject) updateItem.get(updateType);
                if ("insertOne".equals(updateType)) {
                    final InsertOneModel insertOneModel = new InsertOneModel(updateSpec.get("document"));
                    updateModels.add(insertOneModel);
                } else if ("updateOne".equals(updateType)) {
                    final UpdateOptions options = parseUpdateOptions(updateSpec);
                    final UpdateOneModel updateOneModel = new UpdateOneModel((BasicDBObject) updateSpec.get("filter"), (BasicDBObject) updateSpec.get("update"), options);
                    updateModels.add(updateOneModel);
                } else if ("updateMany".equals(updateType)) {
                    final UpdateOptions options = parseUpdateOptions(updateSpec);
                    final UpdateManyModel updateManyModel = new UpdateManyModel((BasicDBObject) updateSpec.get("filter"), (BasicDBObject) updateSpec.get("update"), options);
                    updateModels.add(updateManyModel);
                } else if ("replaceOne".equals(updateType)) {
                    final ReplaceOptions options = parseReplaceOptions(updateSpec);
                    final ReplaceOneModel replaceOneModel = new ReplaceOneModel((BasicDBObject) updateSpec.get("filter"), updateSpec.get("replacement"), options);
                    updateModels.add(replaceOneModel);
                } else if ("deleteOne".equals(updateType)) {
                    final DeleteOptions options = parseDeleteOptions(updateSpec);
                    final DeleteOneModel deleteOneModel = new DeleteOneModel((BasicDBObject) updateSpec.get("filter"), options);
                    updateModels.add(deleteOneModel);
                } else  if ("deleteMany".equals(updateType)) {
                    final DeleteOptions options = parseDeleteOptions(updateSpec);
                    final DeleteManyModel deleteManyModel = new DeleteManyModel((BasicDBObject) updateSpec.get("filter"), options);
                    updateModels.add(deleteManyModel);
                } else {
                    logger.error("Invalid bulk-update: invalid update type {}", new Object[] { updateType });
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                    return;
                }
            }

            if (context.getProperty(USE_TRANSACTION).asBoolean()) {
                MongoClient currentMongoClient;
                if (clientService != null) {
                    // would need four additional projects to extend interface, etc.
                    // (this is not super-nice - but: a PR against NiFi would get this processor in nifi-mongodb-bundle and we can just add getMongoClient() or startClientSession() to the controller service itself)
                    throw new RuntimeException("Cannot have transactions and use a client service - so far");
                } else {
                    currentMongoClient = this.mongoClient;
                }
                clientSession = currentMongoClient.startSession();
                clientSession.startTransaction();
                // now run this w/in a transaction
                collection.bulkWrite(clientSession, updateModels, (new BulkWriteOptions().ordered(context.getProperty(ORDERED).asBoolean())));
            } else {
                collection.bulkWrite(updateModels, (new BulkWriteOptions().ordered(context.getProperty(ORDERED).asBoolean())));
            }
            logger.info("bulk-updated {} into MongoDB", new Object[] { flowFile });
            // (could also return the result again as JSON - mostly not needed afaik)

            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);

            if (clientSession != null) {
                if (clientSession.hasActiveTransaction()) {
                    clientSession.commitTransaction();
                }
                clientSession.close();
            }
        } catch (Exception e) {
            logger.error("Failed to bulk-update {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
            try {
                if (clientSession != null) {
                    if (clientSession.hasActiveTransaction()) {
                        clientSession.abortTransaction();
                    }
                    clientSession.close();
                }
            } catch (Exception ee) {
                logger.error("Cannot rollback client session due to {}", new Object[]{ee}, ee); // (but no further action)
            }
        }
    }

    protected UpdateOptions parseUpdateOptions(BasicDBObject updateSpec) {
        final UpdateOptions options = new UpdateOptions();
        if (updateSpec.containsField("upsert")) {
            options.upsert((boolean) updateSpec.get("upsert"));
        }
        if (updateSpec.containsField("arrayFilters")) {
            options.arrayFilters((List<? extends Bson>) updateSpec.get("arrayFilters"));
        }
        if (updateSpec.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateSpec.get("collation")));
        }
        return options;
    }

    protected ReplaceOptions parseReplaceOptions(BasicDBObject updateSpec) {
        final ReplaceOptions options = new ReplaceOptions();
        if (updateSpec.containsField("upsert")) {
            options.upsert((boolean) updateSpec.get("upsert"));
        }
        if (updateSpec.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateSpec.get("collation")));
        }
        return options;
    }

    protected DeleteOptions parseDeleteOptions(BasicDBObject updateSpec) {
        final DeleteOptions options = new DeleteOptions();
        if (updateSpec.containsField("collation")) {
            options.collation(parseCollation((BasicDBObject) updateSpec.get("collation")));
        }
        return options;
    }

    protected Collation parseCollation(BasicDBObject updateSpec) {
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

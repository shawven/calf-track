package com.github.shawven.calf.track.datasource.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author xw
 * @date 2023-01-05
 */
public class OplogClient implements AutoCloseable {

    public static Logger logger = LoggerFactory.getLogger(OplogClient.class);

    private final MongoClient mongoClient;

    private BsonTimestamp ts;

    public OplogClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public Observable<Document> oplog = Observable.<Document>create(emitter -> {
        Disposable disposable = Schedulers.io().scheduleDirect(() -> doWork(emitter));
        emitter.setDisposable(disposable);
    }).share();


    private void doWork(ObservableEmitter<Document> emitter) {
        logger.info("oplog watch starting");
        try {
            MongoCollection<Document> collection = null;
            while( !emitter.isDisposed() && collection == null ) {
                try {
                    collection = mongoClient.getDatabase("local")
                            .getCollection("oplog.rs");
                } catch( MongoInterruptedException ie ) {
                    throw ie;
                } catch( Exception e ) {
                    logger.warn("could not get oplog collection.", e);
                    TimeUnit.SECONDS.wait(200);
                }
            }
            while( !emitter.isDisposed() && ts == null ) {
                // find the first oplog entry.
                try {
                    FindIterable<Document> tsCursor = collection
                            .find()
                            .sort(new BasicDBObject("$natural", -1))
                            .limit(1);
                    Document tsDoc = tsCursor.first();
                    if( tsDoc == null ) {
                        logger.warn("could not find latest oplog document");
                        TimeUnit.SECONDS.sleep(200);
                        continue;
                    }
                    ts = (BsonTimestamp) tsDoc.get("ts");
                    // if this document matches, emit it?
                } catch( MongoInterruptedException ie ) {
                    throw ie;
                } catch( Exception e ) {
                    logger.warn("could not find latest oplog document", e);
                    TimeUnit.SECONDS.sleep(200);
                    continue;
                }
            }

            logger.info("latest timestamp is "+ts);

            // watch the oplog, while we can.
            while( !emitter.isDisposed() ) {
                logger.debug("finding more oplog entries");

                // build the query
                BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", ts));

                if( logger.isDebugEnabled() ) {
                    logger.debug("Oplog query:"+query.toJson());
                }

                try (
                        com.mongodb.client.MongoCursor<Document> docCursor = collection
                                .find(query)
                                .cursorType(CursorType.TailableAwait)
                                .noCursorTimeout(true)
                                .oplogReplay(true)
                                .maxAwaitTime(1, TimeUnit.SECONDS)
                                .iterator()) {

                    while( !emitter.isDisposed() && docCursor.hasNext() ) {
                        Document document = docCursor.next();
                        emitter.onNext(document);
                        ts = (BsonTimestamp)document.get("ts");
                    }

                    logger.debug("oplog cursor out of results");
                } catch( MongoInterruptedException ie ) {
                    throw ie;
                }
                catch( Exception e ) {
                    logger.warn("oplog cursor threw an exception", e);
                    TimeUnit.SECONDS.sleep(200);
                }
            }

            emitter.onComplete();
            logger.info("oplog watch terminating");
        }
        catch( MongoInterruptedException | InterruptedException ie ) {
            Thread.currentThread().interrupt();
            if( !emitter.isDisposed() ) {
                logger.debug("oplog watch interrupted", ie);
            }
            emitter.onComplete();
        } catch( Exception e ) {
            logger.warn("oplog watch terminating due to exception", e);
            emitter.onError(e);
        }
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    public void setTs(BsonTimestamp ts) {
        this.ts = ts;
    }

    public Observable<Document> getOplog() {
        return oplog;
    }
}

package cosmos;

import com.azure.data.cosmos.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * In this sample, block() is used but should not be used in production code.
 */

public class App {
    final static String ENDPOINT = "URL";
    final static String KEY = "KEY";
    final static String DATABASE = "test_db";
    final static String CONTAINER = "test_container";
    final static String LEASE_CONTAINER = "lease";
    static Logger logger;

    public static void main(String... args) {
        logger = Logger.getLogger(App.class.getName());
        App app = new App();
        app.createSample();
        app.querySample1();
        app.querySample2();
        app.changeFeedSample();
    }

    void createSample() {

        // Add new records
        People p1 = new People("111", "Test1", "test description1", "US");
        People p2 = new People("222", "Test2", "test description2", "CA");
        People p3 = new People("333", "Test3", "test description3", "JP");


        // Connect to Cosmos DB
        CosmosClient client = CosmosClient.builder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .build();

        // Create Database and Container if none of them exists
        logger.info("[Create] Create Database if it does not exist");

        CosmosDatabase cosmosDatabase = client.createDatabaseIfNotExists(DATABASE)
                .doOnError(throwable -> logger.info("[Create] Unable to create Database: " + throwable.getMessage()))
                .doOnSuccess(cosmosDatabaseResponse -> logger.info("[Create] Database created " + cosmosDatabaseResponse.database().id()))
                .flatMap(cosmosDatabaseResponse -> Mono.just(cosmosDatabaseResponse.database()))
                .publishOn(Schedulers.elastic())
                .block();

        if (!Optional.of(cosmosDatabase).isPresent()) {
            return;
        }

        CosmosContainer container1 = cosmosDatabase.createContainerIfNotExists(new CosmosContainerProperties(CONTAINER, "/country"))
//        CosmosContainer container1 = cosmosDatabase.createContainer(new CosmosContainerProperties(CONTAINER, "/country"))
                .doOnError(throwable -> logger.info("[Create] Unable to create Container " + throwable.getMessage()))
                .doOnSuccess(cosmosContainerResponse -> logger.info("[Create] Container created " + cosmosContainerResponse.container().id()))
                .flatMap(cosmosContainerResponse -> Mono.just(cosmosContainerResponse.container()))
                .publishOn(Schedulers.elastic())
                .block();

        if (!Optional.of(container1).isPresent()) {
            return;
        }

        // Insert data
        // Wait for completion of last loading
        CosmosItemResponse response1 = container1.upsertItem(p1)
                .doOnError(throwable -> logger.info("[Create] Unable to load p1 " + throwable.getMessage()))
                .mergeWith(container1.upsertItem(p2))
                .doOnError(throwable -> logger.info("[Create] Unable to load p2 " + throwable.getMessage()))
                .mergeWith(container1.upsertItem(p3))
                .doOnError(throwable -> logger.info("[Create] Unable to load p3 " + throwable.getMessage()))
                .doOnComplete(() -> logger.info("[Create] Loading completed"))
                .publishOn(Schedulers.elastic())
                .blockLast();

        if (!Optional.of(response1).isPresent()) return;

        // Read All items
        List<FeedResponse<CosmosItemProperties>> cosmosItemProperties = container1.readAllItems(new FeedOptions().enableCrossPartitionQuery(true).maxDegreeOfParallelism(2))
                .doOnError(throwable -> logger.info("[Create] Unable to read data " + throwable.getMessage()))
                .collectSortedList()
                .flatMap(feedResponses -> {
                    feedResponses.stream().forEach(f -> logger.info("[Create] f.toString() " + f.toString()));
                    return Mono.just(feedResponses);
                })
                .publishOn(Schedulers.elastic())
                .block();

        for (FeedResponse<CosmosItemProperties> f : cosmosItemProperties) {
            for (CosmosItemProperties c : f.results()) {
                logger.info("[Create] c.toJson()" + c.toJson(SerializationFormattingPolicy.INDENTED));
            }
            logger.info("[Create] f.results().toString() " + f.results().toString());
        }

        // Get a specified item to modify and update it
        CosmosItem cosmosItem1 = container1.getItem("111", "US");
        p1.setDescription("変更するよ");
        cosmosItem1.replace(p1)
                .doOnError(throwable -> logger.info("[Create] Unable to replace p1 " + throwable.getMessage()))
                .publishOn(Schedulers.immediate())
                .block();

        // Delete item
        CosmosItem cosmosItem2 = container1.getItem("222", "CA");
        cosmosItem2.delete().publishOn(Schedulers.immediate()).block();

        // Query item(s)
        FeedResponse<CosmosItemProperties> feedResponse =
                container1.queryItems("select c.id, c.name, c.country, c.description from " + CONTAINER + " c", new FeedOptions().enableCrossPartitionQuery(true).maxDegreeOfParallelism(2))
                        .doOnError(throwable -> logger.info("[Create] Unable to query: " + throwable.getMessage()))
                        .publishOn(Schedulers.immediate()).blockLast();

        feedResponse.results().stream().forEach(f -> logger.info("[Create] Current stored data " + f.toJson(SerializationFormattingPolicy.INDENTED)));

        //Close client
        client.close();
        logger.info("[Create] Completed");
    }

    void querySample1() {

        // Connect to Cosmos DB
        CosmosClient client = CosmosClient.builder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .build();

        // Create Database and Container if none of them exists
        logger.info("[querySample1] start");

        CosmosDatabase cosmosDatabase = client.getDatabase(DATABASE);
        CosmosContainer cosmosContainer = cosmosDatabase.getContainer(CONTAINER);
        FeedResponse<CosmosItemProperties> feedResponse = cosmosContainer.queryItems("select c.id, c.name, c.country, c.description from " + CONTAINER + " c", new FeedOptions().enableCrossPartitionQuery(true).maxDegreeOfParallelism(2))
                .doOnError(throwable -> logger.info("[querySample1] Unable to create Database: " + throwable.getMessage()))
                .publishOn(Schedulers.elastic())
                .blockLast();

        if (!Optional.of(feedResponse).isPresent()) {
            return;
        }

        feedResponse.results().stream().forEach(f -> logger.info("[querySample1] Current stored data \n" + f.toJson(SerializationFormattingPolicy.INDENTED)));

        List<People> peopleList = new ArrayList<>();
        Jsonb jsonb = JsonbBuilder.create();
        feedResponse.results().forEach(f -> {
            peopleList.add(jsonb.fromJson(f.toJson(), People.class));
        });
        for (People p : peopleList) {
            logger.info("[querySample1] " + jsonb.toJson(p));
        }

        //Close client
        client.close();
        logger.info("[querySample1] Completed");
    }

    void querySample2() {
        logger.info("[querySample2] start");

        // Connect to Cosmos DB
        CosmosClient client = CosmosClient.builder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .build();

        List<People> peopleList = new ArrayList<>();
        Jsonb jsonb = JsonbBuilder.create();

        client.getDatabase(DATABASE)
                .getContainer(CONTAINER)
                .queryItems("select c.id, c.name, c.country, c.description from " + CONTAINER + " c", new FeedOptions().enableCrossPartitionQuery(true).maxDegreeOfParallelism(2))
                .doOnError(throwable -> logger.info("[querySample2] Unable to get Database: " + throwable.getMessage()))
                .toStream()
                .forEach(cosmosItemPropertiesFeedResponse -> cosmosItemPropertiesFeedResponse.results()
                        .stream()
                        .forEach(f -> peopleList.add(jsonb.fromJson(f.toJson(), People.class))));

        peopleList.stream().forEach(d -> logger.info("[querySample2]" + jsonb.toJson(d)));

        client.close();
        logger.info("[querySample2] Completed");
    }

    void changeFeedSample() {

        logger.info("[ChangeFeedSample] START!");
        // Connect to Cosmos DB
        CosmosClient client = CosmosClient.builder()
        .endpoint(ENDPOINT)
        .key(KEY)
        .build();

        CosmosContainer feedContainer = client.getDatabase(DATABASE)
            .createContainerIfNotExists(CONTAINER, "/country")
            .flatMap(f -> Mono.just(f.container()))
            .publishOn(Schedulers.elastic())
            .block();
        CosmosContainer leaseContainer = client.getDatabase(DATABASE)
            .createContainerIfNotExists(LEASE_CONTAINER, "/id")
            .flatMap(f -> Mono.just(f.container()))
            .publishOn(Schedulers.elastic())
            .block();

        ChangeFeedProcessor.Builder()
            .hostName(ENDPOINT)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .handleChanges(docs -> {
                logger.info("[ChangeFeedSample] handleChanges START!");
                for (CosmosItemProperties cosmosItemProperties : docs) {
                    logger.info("[ChangeFeedSample] ChangeFeed Received " + cosmosItemProperties.toJson(SerializationFormattingPolicy.INDENTED));
                }
                logger.info("[ChangeFeedSample] handleChanges End!");
            })
            .options(new ChangeFeedProcessorOptions().feedPollDelay(Duration.ofSeconds(3)))
            .build()
            .start()
            .subscribe();
    }
}

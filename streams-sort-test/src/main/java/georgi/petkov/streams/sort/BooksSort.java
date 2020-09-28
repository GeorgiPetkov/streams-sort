package georgi.petkov.streams.sort;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static georgi.petkov.streams.sort.Constants.*;
import static georgi.petkov.streams.sort.Serdes.DURATION_SERDE;
import static georgi.petkov.streams.sort.Serdes.MESSAGE_SERDE;
import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.time.Instant.now;
import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.UUID;
import static org.apache.kafka.streams.KafkaStreams.State.REBALANCING;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.kstream.Consumed.with;
import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;
import static org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;

@Slf4j
class BooksSort {

    private static final Object ALL_PARTITIONS_CONSUMED_MONITOR = new Object();
    private static final JoinWindows WINDOWS = JoinWindows.of(Duration.ofDays(1)).grace(Duration.ofDays(1));

    private static final SortAlgorithm SORT_ALGORITHM = SortAlgorithm.valueOf(getenv(SORT_ALGORITHM_VARIABLE));
    private static final int PARTITIONS = parseInt(getenv(PARTITIONS_VARIABLE));
    private static final int INSTANCES = parseInt(getenv(INSTANCES_VARIABLE));

    public static void main(String[] args) throws InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<UUID, Message> input = builder.stream(INPUT_TOPIC, with(UUID(), MESSAGE_SERDE));

        KStream<UUID, Message> sortedInput = switch (SORT_ALGORITHM) {
            case LOGICAL -> LogicallyPartitionedStreamTopologicalSorter.sort(
                    input,
                    builder,
                    UUID(),
                    MESSAGE_SERDE,
                    "",
                    Message::getId,
                    Message::getPartOfId,
                    WINDOWS,
                    Stores::persistentWindowStore);
            case RELATIONAL -> RelationallyPartitionedStreamTopologicalSorter.sort(
                    input,
                    builder,
                    UUID(),
                    MESSAGE_SERDE,
                    "",
                    Message::getId,
                    Message::getPartOfId,
                    WINDOWS,
                    Stores::persistentWindowStore);
        };

        StoreBuilder<KeyValueStore<Integer, Duration>> timesStore =
                keyValueStoreBuilder(inMemoryKeyValueStore("times"), Integer(), DURATION_SERDE);
        builder.addGlobalStore(timesStore, TIMES_TOPIC, Consumed.with(Integer(), DURATION_SERDE), () -> new Processor<>() {

            private KeyValueStore<Integer, Duration> partitionsProcessingTimes;

            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                partitionsProcessingTimes = (KeyValueStore<Integer, Duration>) context.getStateStore(timesStore.name());
            }

            @Override
            public void process(Integer partition, Duration processingTime) {
                log.info("Received processing time {} for partition {}", processingTime, partition);
                partitionsProcessingTimes.putIfAbsent(partition, processingTime);
                if (partitionsProcessingTimes.approximateNumEntries() == PARTITIONS) {
                    log.info("Notifying all partitions consumed.");
                    notifyAllPartitionsConsumed();
                }
            }

            @Override
            public void close() {
            }
        });

        AtomicReference<Instant> start = new AtomicReference<>();
        StateListener stateChangeListener = (newState, oldState) -> {
            if (newState == RUNNING && oldState == REBALANCING) {
                start.compareAndSet(null, now());
            }
        };

        sortedInput
                .transform(() -> new Transformer<UUID, Message, KeyValue<Integer, Duration>>() {

                    private final SortedMap<Integer, Book> books = new TreeMap<>();
                    private final Map<UUID, Object> objectById = new HashMap<>();
                    private ProcessorContext context;
                    private int partition;
                    private Instant lastMessageArrival;
                    private Cancellable schedule;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        Duration scheduleInterval = Duration.ofSeconds(10);
                        log.info("Starting processing for task {}...", context.taskId());
                        this.schedule = context.schedule(scheduleInterval, WALL_CLOCK_TIME, unused -> {
                            log.info("Last message arrival: {}", lastMessageArrival);
                            if (lastMessageArrival == null || lastMessageArrival.isBefore(now().minus(scheduleInterval))) {
                                schedule.cancel();
                                validateResults();
                                context.forward(
                                        partition,
                                        lastMessageArrival == null
                                                ? Duration.ZERO
                                                : Duration.between(start.get(), lastMessageArrival));
                                log.info("Finished processing for task {}.", context.taskId());
                            }
                        });
                    }

                    @Override
                    public KeyValue<Integer, Duration> transform(UUID key, Message message) {
                        partition = context.partition();
                        lastMessageArrival = now();

                        switch (message.getType()) {
                            case BOOK -> {
                                Book book = new Book(message.getContent());
                                putUnique(objectById, message.getId(), book);
                                putUnique(books, message.getNumber(), book);
                            }
                            case CHAPTER -> {
                                Chapter chapter = new Chapter(message.getContent());
                                putUnique(objectById, message.getId(), chapter);
                                putUnique(
                                        ((Book) objectById.get(message.getPartOfId())).getChapters(),
                                        message.getNumber(),
                                        chapter);
                            }
                            case PARAGRAPH -> {
                                Paragraph paragraph = new Paragraph();
                                putUnique(objectById, message.getId(), paragraph);
                                putUnique(
                                        ((Chapter) objectById.get(message.getPartOfId())).getParagraphs(),
                                        message.getNumber(),
                                        paragraph);
                            }
                            case WORD -> putUnique(
                                    ((Paragraph) objectById.get(message.getPartOfId())).getWords(),
                                    message.getNumber(),
                                    message.getContent());
                            default -> throw new IllegalStateException(message.getType().name());
                        }

                        return null;
                    }

                    private <K, V> void putUnique(Map<K, V> map, K key, V value) {
                        if (map.putIfAbsent(key, value) != null) {
                            throw new IllegalStateException("No duplicates expected.");
                        }
                    }

                    private void validateResults() {
                        if (books.isEmpty()) {
                            throw new RuntimeException("Each partition should have had at least one book.");
                        }

                        for (Book book : books.values()) {
                            assertAllMessagesReceived(book.getChapters().size(), parseInt(getenv(CHAPTERS_VARIABLE)));
                            for (Chapter chapter : book.getChapters().values()) {
                                assertAllMessagesReceived(chapter.getParagraphs().size(), parseInt(getenv(PARAGRAPHS_VARIABLE)));
                                for (Paragraph paragraph : chapter.getParagraphs().values()) {
                                    assertAllMessagesReceived(paragraph.getWords().size(), parseInt(getenv(WORDS_VARIABLE)));
                                }
                            }
                        }
                    }

                    private void assertAllMessagesReceived(int actualCount, int expectedCount) {
                        if (actualCount != expectedCount) {
                            throw new RuntimeException(format("Expected count %d, but was %d.", expectedCount, actualCount));
                        }
                    }

                    @Override
                    public void close() {
                    }
                })
                .to(TIMES_TOPIC, Produced.with(Integer(), DURATION_SERDE));

        Properties topologyProperties = new Properties();
        topologyProperties.put(TOPOLOGY_OPTIMIZATION, OPTIMIZE);
        Properties streamsProperties = new Properties();
        streamsProperties.put(APPLICATION_ID_CONFIG, "books-app");
        streamsProperties.put(BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        streamsProperties.put(NUM_STREAM_THREADS_CONFIG, PARTITIONS / INSTANCES);

        try (KafkaStreams streams = new KafkaStreams(builder.build(topologyProperties), streamsProperties)) {
            streams.setStateListener(stateChangeListener);
            log.info("Starting Kafka streams...");
            streams.start();
            log.info("Kafka streams started.");
            getRuntime().addShutdownHook(new Thread(streams::close));

            log.info("Waiting for all partitions to be consumed...");
            waitAllPartitionsConsumed();
            log.info("All partitions consumed. Exiting...");
        }
    }

    private static void waitAllPartitionsConsumed() throws InterruptedException {
        synchronized (ALL_PARTITIONS_CONSUMED_MONITOR) {
            ALL_PARTITIONS_CONSUMED_MONITOR.wait();
        }
    }

    private static void notifyAllPartitionsConsumed() {
        synchronized (ALL_PARTITIONS_CONSUMED_MONITOR) {
            ALL_PARTITIONS_CONSUMED_MONITOR.notify();
        }
    }

    @Value
    private static class Book {

        String title;
        SortedMap<Integer, Chapter> chapters = new TreeMap<>();
    }

    @Value
    private static class Chapter {

        String title;
        SortedMap<Integer, Paragraph> paragraphs = new TreeMap<>();
    }

    @Value
    private static class Paragraph {

        SortedMap<Integer, String> words = new TreeMap<>();
    }
}

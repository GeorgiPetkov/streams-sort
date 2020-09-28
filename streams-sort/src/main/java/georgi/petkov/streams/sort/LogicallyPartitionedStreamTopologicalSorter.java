package georgi.petkov.streams.sort;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

import static org.apache.kafka.common.serialization.Serdes.ByteArray;
import static org.apache.kafka.common.utils.Bytes.EMPTY;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.kstream.internals.KStreamHacks.*;
import static org.apache.kafka.streams.state.Stores.windowStoreBuilder;

public final class LogicallyPartitionedStreamTopologicalSorter<CorrelationId, Id, Event> extends StreamTopologicalSorter<CorrelationId, Id, Event> {

    /**
     * Sorts the events and automatically filters the duplicate events (based on the {@code eventIdExtractor}).
     * <p>
     * The {@code input} should be already partitioned so that each partition's data is self-contained.
     * <p>
     * Keys and values remain unchanged.
     * <p>
     * No repartitioning is done by the sorter (or required after it). The sorter will use 2 state stores to buffer waiting events.
     * <p>
     * There are no constraints on the keys, they are ignored. The values must not be null.
     * <p>
     * The ID of an event should never be null. The dependency ID should be null iff the event is independent.
     * <p>
     * The sort is "stable" - events sharing the same dependency will be propagated in their original relative order.
     *
     * @param <Id> should have proper implementation of {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    public static <CorrelationId, Id, Event> KStream<CorrelationId, Event> sort(
            KStream<CorrelationId, Event> input,
            StreamsBuilder builder,
            Serde<Id> idSerde,
            Serde<Event> eventSerde,
            String topologyPrefix,
            Function<Event, Id> eventIdExtractor,
            Function<Event, Id> dependencyEventIdExtractor,
            JoinWindows windows,
            WindowBytesStoreSupplierSupplier storeSupplierSupplier) {
        return new LogicallyPartitionedStreamTopologicalSorter<>(input, builder, idSerde, eventSerde, topologyPrefix, eventIdExtractor, dependencyEventIdExtractor, windows, storeSupplierSupplier)
                .sort();
    }

    private KStream<CorrelationId, Event> sort() {
        Serde<CorrelationId> correlationIdSerde = getKeySerde(input);

        StoreBuilder<WindowStore<Id, byte[]>> resolvedEventsStore = windowStoreBuilder(
                storeSupplierSupplier.supply(
                        topologyPrefix + "resolved-events",
                        Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                        Duration.ofMillis(windows.size()),
                        false),
                idSerde,
                ByteArray())
                .withCachingEnabled();
        StoreBuilder<WindowStore<Id, KeyValue<CorrelationId, Event>>> waitingEventsStore = windowStoreBuilder(
                storeSupplierSupplier.supply(
                        topologyPrefix + "waiting-events",
                        Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                        Duration.ofMillis(windows.size()),
                        true),
                idSerde,
                new KeyValueSerde<>(correlationIdSerde, eventSerde))
                .withCachingDisabled(); // not supported when using retainDuplicates = true

        builder.addStateStore(resolvedEventsStore);
        builder.addStateStore(waitingEventsStore);

        KStream<CorrelationId, Event> output = input
                .flatTransform(() -> new Transformer<>() {

                            private ProcessorContext context;
                            private WindowStore<Id, byte[]> resolvedEventIds;
                            private WindowStore<Id, KeyValue<CorrelationId, Event>> waitingEventsByDependencyId;

                            @Override
                            @SuppressWarnings("unchecked")
                            public void init(ProcessorContext context) {
                                this.context = context;
                                this.resolvedEventIds = (WindowStore<Id, byte[]>) context.getStateStore(resolvedEventsStore.name());
                                this.waitingEventsByDependencyId = (WindowStore<Id, KeyValue<CorrelationId, Event>>) context.getStateStore(waitingEventsStore.name());
                            }

                            @Override
                            public Iterable<KeyValue<CorrelationId, Event>> transform(CorrelationId key, Event event) {
                                Id dependencyId = dependencyEventIdExtractor.apply(event);
                                if (dependencyId == null || isResolved(dependencyId)) {
                                    return getResolvedEvents(key, event);
                                }

                                waitingEventsByDependencyId.put(dependencyId, pair(key, event), context.timestamp());
                                return null;
                            }

                            private Iterable<KeyValue<CorrelationId, Event>> getResolvedEvents(CorrelationId key, Event event) {
                                if (isResolved(eventIdExtractor.apply(event))) {
                                    return null;
                                }

                                List<KeyValue<CorrelationId, Event>> result = new ArrayList<>();
                                Queue<KeyValue<Long, KeyValue<CorrelationId, Event>>> waitingEvents = new LinkedList<>();
                                waitingEvents.add(pair(context.timestamp(), pair(key, event)));

                                while (!waitingEvents.isEmpty()) {
                                    KeyValue<Long, KeyValue<CorrelationId, Event>> waitingPair = waitingEvents.remove();
                                    Long timestamp = waitingPair.key;
                                    KeyValue<CorrelationId, Event> waitingEvent = waitingPair.value;
                                    Id id = eventIdExtractor.apply(waitingEvent.value);

                                    result.add(waitingEvent);
                                    resolvedEventIds.put(id, EMPTY, timestamp);
                                    waitingEvents.addAll(fetchUniqueDependentEvents(
                                            waitingEventsByDependencyId,
                                            id,
                                            timestamp,
                                            pair -> eventIdExtractor.apply(pair.value)));
                                }

                                return result;
                            }

                            private boolean isResolved(Id id) {
                                return containsKey(resolvedEventIds, id, context.timestamp());
                            }

                            @Override
                            public void close() {
                            }
                        },
                        Named.as(topologyPrefix + "flat-map-values-to-resolved-events"),
                        resolvedEventsStore.name(),
                        waitingEventsStore.name());

        return markNotRequiringRepartition(withSerdes(output, correlationIdSerde, eventSerde));
    }

    private LogicallyPartitionedStreamTopologicalSorter(KStream<CorrelationId, Event> input, StreamsBuilder builder, Serde<Id> idSerde, Serde<Event> eventSerde, String topologyPrefix, Function<Event, Id> eventIdExtractor, Function<Event, Id> dependencyEventIdExtractor, JoinWindows windows, WindowBytesStoreSupplierSupplier storeSupplierSupplier) {
        super(input, builder, idSerde, eventSerde, topologyPrefix, eventIdExtractor, dependencyEventIdExtractor, windows, storeSupplierSupplier);
    }

    private static class KeyValueSerde<K, V> implements Serde<KeyValue<K, V>> {

        private static final int INT_SIZE = 4;

        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        KeyValueSerde(Serde<K> keySerde, Serde<V> valueSerde) {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public Serializer<KeyValue<K, V>> serializer() {
            return ((topic, pair) -> {
                byte[] keyBytes = keySerde.serializer().serialize(topic, pair.key);
                byte[] valueBytes = valueSerde.serializer().serialize(topic, pair.value);

                return ByteBuffer.allocate(INT_SIZE + keyBytes.length + valueBytes.length)
                        .putInt(keyBytes.length)
                        .put(keyBytes)
                        .put(valueBytes)
                        .array();
            });
        }

        @Override
        public Deserializer<KeyValue<K, V>> deserializer() {
            return ((topic, bytes) -> {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                int keyLength = buffer.getInt();
                byte[] keyBytes = new byte[keyLength];
                buffer.get(keyBytes);
                byte[] valueBytes = new byte[buffer.remaining()];
                buffer.get(valueBytes);

                return pair(
                        keySerde.deserializer().deserialize(topic, keyBytes),
                        valueSerde.deserializer().deserialize(topic, valueBytes));
            });
        }
    }
}

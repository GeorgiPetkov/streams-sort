package georgi.petkov.streams.sort;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.apache.kafka.streams.kstream.internals.KStreamHacks.requiresRepartition;

abstract class StreamTopologicalSorter<InputKey, Id, Event> {

    final KStream<InputKey, Event> input;
    final StreamsBuilder builder;
    final Serde<Id> idSerde;
    final Serde<Event> eventSerde;
    final String topologyPrefix;
    final Function<Event, Id> eventIdExtractor;
    final Function<Event, Id> dependencyEventIdExtractor;
    final JoinWindows windows;
    final WindowBytesStoreSupplierSupplier storeSupplierSupplier;

    StreamTopologicalSorter(
            KStream<InputKey, Event> input,
            StreamsBuilder builder,
            Serde<Id> idSerde,
            Serde<Event> eventSerde,
            String topologyPrefix,
            Function<Event, Id> eventIdExtractor,
            Function<Event, Id> dependencyEventIdExtractor,
            JoinWindows windows,
            WindowBytesStoreSupplierSupplier storeSupplierSupplier) {
        if (requiresRepartition(input)) {
            throw new IllegalArgumentException("The provided stream is not repartitioned yet. Stream: " + input);
        }

        this.input = input;
        this.builder = builder;
        this.idSerde = idSerde;
        this.eventSerde = eventSerde;
        this.topologyPrefix = topologyPrefix;
        this.eventIdExtractor = eventIdExtractor;
        this.dependencyEventIdExtractor = dependencyEventIdExtractor;
        this.windows = windows;
        this.storeSupplierSupplier = storeSupplierSupplier;
    }

    final <V> Collection<KeyValue<Long, V>> fetchUniqueDependentEvents(
            WindowStore<Id, V> waitingEventsByDependencyId,
            Id id,
            Long timestamp,
            Function<V, Id> eventIdExtractor) {
        return fetch(waitingEventsByDependencyId, id, timestamp, iterator ->
                toStream(iterator)
                        .collect(toMap(
                                pair -> eventIdExtractor.apply(pair.value),
                                identity(),
                                (pair1, pair2) -> pair1,
                                LinkedHashMap::new))
                        .values());
    }

    final <K> boolean containsKey(WindowStore<K, ?> windowStore, K key, long timestamp) {
        return fetch(windowStore, key, timestamp, Iterator::hasNext);
    }

    final <K, V, R> R fetch(WindowStore<K, V> windowStore, K key, long timestamp, Function<WindowStoreIterator<V>, R> mapper) {
        try (WindowStoreIterator<V> iterator = windowStore.fetch(
                key, timestamp - windows.beforeMs, timestamp + windows.afterMs)) {
            return mapper.apply(iterator);
        }
    }

    static <V> Stream<KeyValue<Long, V>> toStream(WindowStoreIterator<V> iterator) {
        return stream(((Iterable<KeyValue<Long, V>>) () -> iterator).spliterator(), false);
    }
}

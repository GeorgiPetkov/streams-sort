package georgi.petkov.streams.sort;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.apache.kafka.streams.state.Stores.windowStoreBuilder;

public final class RelationallyPartitionedStreamTopologicalSorter<Id, Event> extends StreamTopologicalSorter<Id, Id, Event> {

    public static <Id, Event> KeyValueMapper<?, Event, Id> keySelection(
            Function<Event, Id> idExtractor,
            Function<Event, Id> dependencyIdExtractor) {
        return (key, event) -> {
            Id dependencyId = dependencyIdExtractor.apply(event);
            return dependencyId == null ? idExtractor.apply(event) : dependencyId;
        };
    }

    /**
     * Sorts the events and automatically filters the duplicate events (based on the {@code eventIdExtractor}).
     * <p>
     * If the {@code input} is already partitioned so that each partition's data is self-contained then consider using {@link LogicallyPartitionedStreamTopologicalSorter} instead.
     * <p>
     * The original keys will be replaced by the ID of the top level event of each hierarchy in order to deliver self-contained partitions as a result. Values remain unchanged.
     * <p>
     * The returned stream will not require repartition. However the sorter will use 2 state stores to buffer waiting events as well as one extra topic used for internal communication between partitions and one topic to partition the output.
     * <p>
     * The events must be already partition by keys using {@link #keySelection(Function, Function)}. The values must not be null.
     * <p>
     * The ID of an event should never be null. The dependency ID should be null iff the event is independent.
     * <p>
     * There should be an already created topic with name {@code topologyPrefix + "internal-resolved-events-repartition-topic"} with the same number of partitions are the input stream.
     *
     * @param <Id> should have proper implementation of {@link Object#hashCode()} and {@link Object#equals(Object)}.
     * @see #keySelection(Function, Function)
     */
    public static <Id, Event> KStream<Id, Event> sort(
            KStream<Id, Event> input,
            StreamsBuilder builder,
            Serde<Id> idSerde,
            Serde<Event> eventSerde,
            String topologyPrefix,
            Function<Event, Id> eventIdExtractor,
            Function<Event, Id> dependencyEventIdExtractor,
            JoinWindows windows,
            WindowBytesStoreSupplierSupplier storeSupplierSupplier) {
        return new RelationallyPartitionedStreamTopologicalSorter<>(input, builder, idSerde, eventSerde, topologyPrefix, eventIdExtractor, dependencyEventIdExtractor, windows, storeSupplierSupplier)
                .sort();
    }

    private KStream<Id, Event> sort() {
        String resolvedEventsTopic = topologyPrefix + "internal-resolved-events-repartition-topic";

        StoreBuilder<WindowStore<Id, Id>> resolvedEventsStore = windowStoreBuilder(
                storeSupplierSupplier.supply(
                        topologyPrefix + "resolved-events",
                        Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                        Duration.ofMillis(windows.size()),
                        false),
                idSerde,
                idSerde)
                .withCachingEnabled();
        StoreBuilder<WindowStore<Id, Event>> waitingEventsStore = windowStoreBuilder(
                storeSupplierSupplier.supply(
                        topologyPrefix + "waiting-events",
                        Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                        Duration.ofMillis(windows.size()),
                        true),
                idSerde,
                eventSerde)
                .withCachingDisabled(); // not supported when using retainDuplicates = true

        builder.addStateStore(resolvedEventsStore);
        builder.addStateStore(waitingEventsStore);

        Predicate<Id, Event> isIndependent = (key, event) -> dependencyEventIdExtractor.apply(event) == null;

        @SuppressWarnings("unchecked")
        KStream<Id, Event>[] branches = input.branch(
                Named.as(topologyPrefix + "split-into-independent-and-dependent-events"),
                isIndependent,
                (key, event) -> true);

        KStream<Id, Event> independentEvents = branches[0];
        KStream<Id, Event> dependentEvents = branches[1];
        KStream<Id, Id> resolvedDependentsToCorrelationId = builder.stream(
                resolvedEventsTopic, Consumed.with(idSerde, idSerde));

        KStream<Id, Event> independentAndTheirDirectWaitingEvents = independentEvents
                .flatTransform(() -> new BaseTransformer<Id, Event, Iterable<KeyValue<Id, Event>>>(
                                resolvedEventsStore.name(), waitingEventsStore.name()) {

                            @Override
                            public Iterable<KeyValue<Id, Event>> transform(Id id, Event event) {
                                return independentAndItsDirectWaitingEvents(id, event);
                            }
                        },
                        Named.as(topologyPrefix + "independent-and-their-direct-waiting-events"),
                        resolvedEventsStore.name(),
                        waitingEventsStore.name());

        KStream<Id, Event> resolvedDependentsDirectWaitingEvents = resolvedDependentsToCorrelationId
                .flatTransform(() -> new BaseTransformer<Id, Id, Iterable<KeyValue<Id, Event>>>(
                                resolvedEventsStore.name(), waitingEventsStore.name()) {

                            @Override
                            public Iterable<KeyValue<Id, Event>> transform(Id id, Id correlationId) {
                                return resolvedDependentDirectWaitingEvents(id, correlationId);
                            }
                        },
                        Named.as(topologyPrefix + "resolved-dependents-direct-waiting-events"),
                        resolvedEventsStore.name(),
                        waitingEventsStore.name());

        KStream<Id, Event> immediatelyResolvedDependentEvents = dependentEvents
                .transform(() -> new BaseTransformer<Id, Event, KeyValue<Id, Event>>(
                                resolvedEventsStore.name(), waitingEventsStore.name()) {

                            @Override
                            public KeyValue<Id, Event> transform(Id dependencyId, Event event) {
                                return immediatelyResolvedDependent(dependencyId, event);
                            }
                        },
                        Named.as(topologyPrefix + "immediately-resolved-dependent-events"),
                        resolvedEventsStore.name(),
                        waitingEventsStore.name());

        KStream<Id, Event> output = independentAndTheirDirectWaitingEvents
                .merge(resolvedDependentsDirectWaitingEvents, Named.as(topologyPrefix + "intermediate-merge"))
                .merge(immediatelyResolvedDependentEvents, Named.as(topologyPrefix + "final-merge"))
                .repartition(Repartitioned.with(idSerde, eventSerde).withName(topologyPrefix + "repartition-output"));

        output
                .filterNot(isIndependent, Named.as(topologyPrefix + "skip-independent-events"))
                .map(
                        (correlationId, event) -> pair(eventIdExtractor.apply(event), correlationId),
                        Named.as(topologyPrefix + "adapt-partition-key-and-format"))
                .to(resolvedEventsTopic, Produced.with(idSerde, idSerde).withName(topologyPrefix + "repartition-resolved-events"));

        return output;
    }

    private abstract class BaseTransformer<K, V, R> implements Transformer<K, V, R> {

        private final String resolvedEventsStoreName;
        private final String waitingEventsStoreName;

        private ProcessorContext context;
        private WindowStore<Id, Id> resolvedEventsWithCorrelationId;
        private WindowStore<Id, Event> waitingEventsByDependencyId;

        private BaseTransformer(String resolvedEventsStoreName, String waitingEventsStoreName) {
            this.resolvedEventsStoreName = resolvedEventsStoreName;
            this.waitingEventsStoreName = waitingEventsStoreName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.resolvedEventsWithCorrelationId = (WindowStore<Id, Id>) context.getStateStore(resolvedEventsStoreName);
            this.waitingEventsByDependencyId = (WindowStore<Id, Event>) context.getStateStore(waitingEventsStoreName);
        }

        final Iterable<KeyValue<Id, Event>> independentAndItsDirectWaitingEvents(Id id, Event event) {
            if (isResolved(id)) {
                return null;
            }

            resolvedEventsWithCorrelationId.put(id, id, context.timestamp());

            List<KeyValue<Id, Event>> result = new ArrayList<>();
            result.add(pair(id, event));
            result.addAll(fetchUniqueDependentEvents(id, id));

            return result;
        }

        final Iterable<KeyValue<Id, Event>> resolvedDependentDirectWaitingEvents(Id id, Id correlationId) {
            resolvedEventsWithCorrelationId.put(id, correlationId, context.timestamp());

            return fetchUniqueDependentEvents(id, correlationId);
        }

        private List<KeyValue<Id, Event>> fetchUniqueDependentEvents(Id dependencyId, Id correlationId) {
            return RelationallyPartitionedStreamTopologicalSorter.this.fetchUniqueDependentEvents(
                    waitingEventsByDependencyId, dependencyId, context.timestamp(), eventIdExtractor).stream()
                    .map(pair -> pair(correlationId, pair.value))
                    .collect(toList());
        }

        final KeyValue<Id, Event> immediatelyResolvedDependent(Id dependencyId, Event event) {
            long timestamp = context.timestamp();
            Id correlationId = fetch(resolvedEventsWithCorrelationId, dependencyId, timestamp,
                    iterator -> iterator.hasNext() ? iterator.next().value : null);

            if (correlationId == null) {
                waitingEventsByDependencyId.put(dependencyId, event, timestamp);
                return null;
            }

            Id id = eventIdExtractor.apply(event);
            if (isResolved(id)) {
                return null;
            }

            if (fetch(waitingEventsByDependencyId, dependencyId, timestamp,
                    iterator -> toStream(iterator)
                            .anyMatch(pair -> id.equals(eventIdExtractor.apply(pair.value))))) {
                /*
                 * The dependency is resolved and the event appears to be already added as waiting but it's not resolved yet.
                 * Therefore the event must have been already propagated by the dependency and
                 * the notification through the resolved events topic should arrive in this same partition but it's still on the way.
                 * In that case skip the event to avoiding duplication.
                 */
                return null;
            }

            resolvedEventsWithCorrelationId.put(id, correlationId, context.timestamp());

            return pair(correlationId, event);
        }

        final boolean isResolved(Id id) {
            return containsKey(resolvedEventsWithCorrelationId, id, context.timestamp());
        }

        @Override
        public final void close() {
        }
    }

    private RelationallyPartitionedStreamTopologicalSorter(KStream<Id, Event> input, StreamsBuilder builder, Serde<Id> idSerde, Serde<Event> eventSerde, String topologyPrefix, Function<Event, Id> eventIdExtractor, Function<Event, Id> dependencyEventIdExtractor, JoinWindows windows, WindowBytesStoreSupplierSupplier storeSupplierSupplier) {
        super(input, builder, idSerde, eventSerde, topologyPrefix, eventIdExtractor, dependencyEventIdExtractor, windows, storeSupplierSupplier);
    }
}

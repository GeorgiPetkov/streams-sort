package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;

import java.lang.reflect.Field;

public final class KStreamHacks {

    public static <K, V> Serde<K> getKeySerde(KStream<K, V> stream) {
        return cast(stream).keySerde();
    }

    public static <K, V> KStream<K, V> withSerdes(KStream<K, V> stream, Serde<K> keySerde, Serde<V> valueSerde) {
        KStreamImpl<K, V> streamImpl = cast(stream);
        return new KStreamImpl<>(
                streamImpl.name,
                keySerde,
                valueSerde,
                streamImpl.subTopologySourceNodes,
                requiresRepartition(stream),
                streamImpl.streamsGraphNode,
                streamImpl.builder);
    }

    public static <K, V> KStream<K, V> markNotRequiringRepartition(KStream<K, V> stream) {
        KStreamImpl<K, V> streamImpl = cast(stream);
        return new KStreamImpl<>(
                streamImpl.name,
                streamImpl.keySerde(),
                streamImpl.valueSerde(),
                streamImpl.subTopologySourceNodes,
                false,
                streamImpl.streamsGraphNode,
                streamImpl.builder);
    }

    public static boolean requiresRepartition(KStream<?, ?> stream) {
        try {
            Field repartitionRequiredField = KStreamImpl.class.getDeclaredField("repartitionRequired");
            boolean isAccessible = repartitionRequiredField.canAccess(cast(stream));
            repartitionRequiredField.setAccessible(true);

            try {
                return repartitionRequiredField.getBoolean(stream);
            } finally {
                repartitionRequiredField.setAccessible(isAccessible);
            }
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    private static <K, V> KStreamImpl<K, V> cast(KStream<K, V> stream) {
        return (KStreamImpl<K, V>) stream;
    }

    private KStreamHacks() {
    }
}

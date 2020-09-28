package georgi.petkov.streams.sort;

import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;

public interface WindowBytesStoreSupplierSupplier {

    WindowBytesStoreSupplier supply(
            String name,
            Duration retentionPeriod,
            Duration windowSize,
            boolean retainDuplicates);
}
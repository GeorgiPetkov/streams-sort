package georgi.petkov.streams.sort;

import lombok.Value;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Value
class Event {

    UUID id;
    UUID dependencyId;
    String data;

    Event(UUID id, UUID dependencyId, String data) {
        this.id = requireNonNull(id);
        this.dependencyId = dependencyId;
        this.data = requireNonNull(data);
    }
}

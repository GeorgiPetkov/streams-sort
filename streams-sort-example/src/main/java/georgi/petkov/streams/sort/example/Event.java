package georgi.petkov.streams.sort.example;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(access = PRIVATE)
class Event {

    UUID id;
    UUID dependencyId;

    static Event createIndependentEvent(UUID id) {
        return new Event(requireNonNull(id), null);
    }

    static Event createDependentEvent(UUID id, Event dependency) {
        return new Event(requireNonNull(id), requireNonNull(dependency.getId()));
    }

    final boolean isDependent() {
        return this.getDependencyId() != null;
    }
}

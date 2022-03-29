package io.github.ferdinandswoboda.folo;

import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Complete, bidirectional mapping of {@code L} objects to {@code R} objects, representing a {@link
 * Relation relation}.
 *
 * @param <L> The Java class that the left-hand side of the relation is mapped to.
 * @param <R> The Java class that the right-hand side of the relation is mapped to.
 */
@Value.Immutable(builder = false)
@Value.Style(
    allParameters = true,
    typeAbstract = "*Interface",
    typeImmutable = "*",
    visibility = Value.Style.ImplementationVisibility.PACKAGE)
interface ObjectMappingInterface<L, R> {

  /** Returns a map of all {@code L} objects to their related {@code R} objects. */
  Map<L, List<R>> toSuccessors();

  /** Returns a reverse map of all {@code R} objects to their related {@code L} objects. */
  Map<R, List<L>> toPredecessors();
}

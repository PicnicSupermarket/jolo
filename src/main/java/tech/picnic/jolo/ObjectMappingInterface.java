package tech.picnic.jolo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  ImmutableMap<L, ImmutableList<R>> toSuccessors();

  /** Returns a reverse map of all {@code R} objects to their related {@code L} objects. */
  ImmutableMap<R, ImmutableList<L>> toPredecessors();
}

package tech.picnic.jolo;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static tech.picnic.jolo.Util.validate;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import javax.annotation.Nullable;
import org.jooq.Record;

/**
 * Mutable graph of objects loaded by {@link Entity entities}. Objects are linked by {@link IdPair
 * ID pairs} loaded by {@link Relation relations}.
 *
 * @apiNote Access is not synchronized.
 */
final class ObjectGraph {
  private final Table<Entity<?, ?>, Long, Object> entityAndIdToObject;
  private final SetMultimap<Relation<?, ?>, IdPair> relationToLinks;

  ObjectGraph() {
    entityAndIdToObject = HashBasedTable.create();
    relationToLinks = MultimapBuilder.hashKeys().linkedHashSetValues().build();
  }

  /**
   * Add an object loaded by the given {@link Entity entity} with the given ID. If an object of the
   * same entity and with the same ID already exists, the given object is ignored.
   */
  <E> void add(Entity<? extends E, ?> entity, long id, E object) {
    if (!entityAndIdToObject.contains(entity, id)) {
      entityAndIdToObject.put(entity, id, object);
    }
  }

  /** Add links loaded by the given {@link Relation relation}. */
  <L, R> void add(Relation<L, R> relation, Set<IdPair> links) {
    relationToLinks.putAll(relation, links);
  }

  /**
   * Merges the other graph into this one.
   *
   * @param other The other object graph to merge with.
   * @apiNote This operation is associative but not commutative. Existing objects loaded by the same
   *     entity and with the same ID take precedence.
   */
  void merge(ObjectGraph other) {
    requireNonNull(other);
    for (var cell : other.entityAndIdToObject.cellSet()) {
      add(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
    }
    relationToLinks.putAll(other.relationToLinks);
  }

  /**
   * Returns all {@link ObjectMapping objects} loaded by the given {@link Relation relation's} left
   * and right entities.
   */
  @SuppressWarnings("unchecked")
  <L, R> ObjectMapping<L, R> getObjectMapping(Relation<L, R> relation) {
    Map<Long, L> leftObjectsById = (Map<Long, L>) entityAndIdToObject.row(relation.getLeft());
    Map<Long, R> rightObjectsById = (Map<Long, R>) entityAndIdToObject.row(relation.getRight());

    Map<L, ImmutableList<R>> objectToSuccessors =
        relationToLinks.get(relation).stream()
            .collect(
                groupingBy(
                    idPair -> getObject(relation.getLeft(), leftObjectsById, idPair.getLeftId()),
                    mapping(
                        idPair ->
                            getObject(relation.getRight(), rightObjectsById, idPair.getRightId()),
                        toImmutableList())));
    leftObjectsById.values().forEach(o -> objectToSuccessors.putIfAbsent(o, ImmutableList.of()));

    Map<R, ImmutableList<L>> objectToPredecessors =
        relationToLinks.get(relation).stream()
            .collect(
                groupingBy(
                    idPair -> getObject(relation.getRight(), rightObjectsById, idPair.getRightId()),
                    mapping(
                        idPair ->
                            getObject(relation.getLeft(), leftObjectsById, idPair.getLeftId()),
                        toImmutableList())));
    rightObjectsById.values().forEach(o -> objectToPredecessors.putIfAbsent(o, ImmutableList.of()));

    return ObjectMapping.of(objectToSuccessors, objectToPredecessors);
  }

  /** Objects loaded by the given {@link Entity entity}. */
  @SuppressWarnings("unchecked")
  <E> Collection<E> getObjects(Entity<E, ? extends Record> entity) {
    return (Collection<E>) entityAndIdToObject.row(entity).values();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectGraph that = (ObjectGraph) o;
    return entityAndIdToObject.equals(that.entityAndIdToObject)
        && relationToLinks.equals(that.relationToLinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityAndIdToObject, relationToLinks);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ObjectGraph.class.getSimpleName() + "[", "]")
        .add("entityAndIdToObject=" + entityAndIdToObject)
        .add("relationToEdges=" + relationToLinks)
        .toString();
  }

  private static <E> E getObject(Entity<E, ?> entity, Map<Long, E> objectsById, Long id) {
    E result = objectsById.get(id);
    validate(result != null, "Unknown id requested from table %s: %s", entity, id);
    return result;
  }
}

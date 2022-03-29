package io.github.ferdinandswoboda.folo;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.jooq.Record;

/**
 * Mutable graph of objects loaded by {@link Entity entities}. Objects are linked by {@link IdPair
 * ID pairs} loaded by {@link Relation relations}.
 *
 * @apiNote Access is not synchronized.
 */
final class ObjectGraph {
  private final Map<Entity<?, ?>, Map<Long, Object>> entityAndIdToObject;
  private final Map<Relation<?, ?>, Set<IdPair>> relationToLinks;

  ObjectGraph() {
    entityAndIdToObject = new LinkedHashMap<>(8);
    relationToLinks = new LinkedHashMap<>(8);
  }

  /**
   * Add an object loaded by the given {@link Entity entity} with the given ID. If an object of the
   * same entity and with the same ID already exists, the given object is ignored.
   */
  <E> void add(Entity<? extends E, ?> entity, long id, Supplier<E> object) {
    entityAndIdToObject.compute(
        entity,
        (e, idToObject) -> {
          if (idToObject == null) {
            return new LinkedHashMap<>(Map.of(id, object.get()));
          } else {
            idToObject.computeIfAbsent(id, key -> object.get());
            return idToObject;
          }
        });
  }

  /** Add links loaded by the given {@link Relation relation}. */
  <L, R> void add(Relation<L, R> relation, Set<IdPair> links) {
    relationToLinks.compute(
        relation,
        (rel, currentLinks) -> {
          if (currentLinks == null) {
            return new LinkedHashSet<>(links);
          } else {
            currentLinks.addAll(links);
            return currentLinks;
          }
        });
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
    for (var entry : other.entityAndIdToObject.entrySet()) {
      entry.getValue().forEach((key, value) -> add(entry.getKey(), key, () -> value));
    }
    other.relationToLinks.forEach(this::add);
  }

  /**
   * Returns all {@link ObjectMapping objects} loaded by the given {@link Relation relation's} left
   * and right entities.
   */
  @SuppressWarnings("unchecked")
  <L, R> ObjectMapping<L, R> getObjectMapping(Relation<L, R> relation) {
    Map<Long, L> leftObjectsById =
        (Map<Long, L>) entityAndIdToObject.getOrDefault(relation.getLeft(), Map.of());
    Map<Long, R> rightObjectsById =
        (Map<Long, R>) entityAndIdToObject.getOrDefault(relation.getRight(), Map.of());

    Map<L, List<R>> objectToSuccessors =
        relationToLinks.getOrDefault(relation, Set.of()).stream()
            .collect(
                groupingBy(
                    idPair -> getObject(relation.getLeft(), leftObjectsById, idPair.getLeftId()),
                    mapping(
                        idPair ->
                            getObject(relation.getRight(), rightObjectsById, idPair.getRightId()),
                        toUnmodifiableList())));
    leftObjectsById.values().forEach(o -> objectToSuccessors.putIfAbsent(o, List.of()));

    Map<R, List<L>> objectToPredecessors =
        relationToLinks.getOrDefault(relation, Set.of()).stream()
            .collect(
                groupingBy(
                    idPair -> getObject(relation.getRight(), rightObjectsById, idPair.getRightId()),
                    mapping(
                        idPair ->
                            getObject(relation.getLeft(), leftObjectsById, idPair.getLeftId()),
                        toUnmodifiableList())));
    rightObjectsById.values().forEach(o -> objectToPredecessors.putIfAbsent(o, List.of()));

    return ObjectMapping.of(objectToSuccessors, objectToPredecessors);
  }

  /** Objects loaded by the given {@link Entity entity}. */
  @SuppressWarnings("unchecked")
  <E> Collection<E> getObjects(Entity<E, ? extends Record> entity) {
    return (Collection<E>) entityAndIdToObject.getOrDefault(entity, Map.of()).values();
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

  @SuppressWarnings("NullAway")
  private static <E> E getObject(Entity<E, ?> entity, Map<Long, E> objectsById, Long id) {
    E result = objectsById.get(id);
    Util.validate(result != null, "Unknown id requested from table %s: %s", entity, id);
    return result;
  }
}

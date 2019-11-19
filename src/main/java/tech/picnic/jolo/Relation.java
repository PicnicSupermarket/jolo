package tech.picnic.jolo;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import org.jooq.Field;
import org.jooq.Record;

/**
 * Represents a relation between two {@link Entity entities}. This class is used to store a relation
 * (pairs of primary keys) loaded from a data set.
 *
 * @param <L> The Java class that the left-hand side of the relation is mapped to
 * @param <R> The Java class that the right-hand side of the relation is mapped to.
 */
final class Relation<L, R, K> {
  enum Arity {
    ZERO_OR_ONE,
    ONE,
    MANY
  }

  private final Set<Pair<K>> pairs = new LinkedHashSet<>();
  private final Entity<L, ?, K> left;
  private final Entity<R, ?, K> right;
  private final Field<K> leftKey;
  private final Field<K> rightKey;
  private final Arity leftArity;
  private final Arity rightArity;
  private final Optional<BiConsumer<L, ?>> leftSetter;
  private final Optional<BiConsumer<R, ?>> rightSetter;
  private final BiConsumer<Record, Set<Pair<K>>> relationLoader;
  private final boolean relationLoaderIsCustom;

  @SuppressWarnings("ConstructorLeaksThis")
  Relation(
      Entity<L, ?, K> left,
      Entity<R, ?, K> right,
      Field<K> leftKey,
      Field<K> rightKey,
      Arity leftArity,
      Arity rightArity,
      Optional<BiConsumer<L, ?>> leftSetter,
      Optional<BiConsumer<R, ?>> rightSetter,
      Optional<BiConsumer<Record, Set<Pair<K>>>> relationLoader) {
    this.left = left;
    this.right = right;
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftArity = leftArity;
    this.rightArity = rightArity;
    this.leftSetter = leftSetter;
    this.rightSetter = rightSetter;
    this.relationLoader = relationLoader.orElse(this::foreignKeyRelationLoader);
    this.relationLoaderIsCustom = relationLoader.isPresent();
  }

  /** Copies this relation, discarding any state. This method is used in a prototype pattern. */
  @SuppressWarnings("unchecked")
  Relation<L, R, K> copy(Map<Entity<?, ?, ?>, Entity<?, ?, ?>> newEntities) {
    Entity<L, ?, K> newLeft = (Entity<L, ?, K>) newEntities.get(left);
    assert newLeft != null : "Attempt to create copy without new left entity";
    Entity<R, ?, K> newRight = (Entity<R, ?, K>) newEntities.get(right);
    assert newRight != null : "Attempt to create copy without new right entity";
    return new Relation<>(
        newLeft,
        newRight,
        leftKey,
        rightKey,
        leftArity,
        rightArity,
        leftSetter,
        rightSetter,
        relationLoaderIsCustom ? Optional.of(relationLoader) : Optional.empty());
  }

  /** Attempts to load a relation from the given record. */
  void load(Record record) {
    relationLoader.accept(record, pairs);
  }

  /**
   * Given the relation pairs stored in this object, setters are called on the objects loaded by the
   * left and right {@link Entity}.
   */
  void link() {
    leftSetter.ifPresent(this::linkLeft);
    rightSetter.ifPresent(this::linkRight);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void linkLeft(BiConsumer setter) {
    switch (rightArity) {
      case MANY:
        linkMany(left.getEntityMap(), setter, getPostSets());
        break;
      case ONE:
        linkOne(left.getEntityMap(), setter, getSuccessors());
        break;
      case ZERO_OR_ONE:
        linkOptional(left.getEntityMap(), setter, getSuccessors());
        break;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void linkRight(BiConsumer setter) {
    switch (leftArity) {
      case MANY:
        linkMany(right.getEntityMap(), setter, getPreSets());
        break;
      case ONE:
        linkOne(right.getEntityMap(), setter, getPredecessors());
        break;
      case ZERO_OR_ONE:
        linkOptional(right.getEntityMap(), setter, getPredecessors());
        break;
    }
  }

  private static <T, U, K> void linkOne(
      Map<K, T> entities, BiConsumer<T, U> setter, Map<K, U> successors) {
    entities.forEach((id, e) -> setter.accept(e, successors.get(id)));
  }

  private static <T, U, K> void linkOptional(
      Map<K, T> entities, BiConsumer<T, Optional<U>> setter, Map<K, U> successors) {
    entities.forEach((id, e) -> setter.accept(e, Optional.ofNullable(successors.get(id))));
  }

  private static <T, U, K> void linkMany(
      Map<K, T> entities, BiConsumer<T, Collection<U>> setter, Map<K, List<U>> successors) {
    entities.forEach((id, e) -> setter.accept(e, successors.getOrDefault(id, emptyList())));
  }

  private Map<K, List<L>> getPreSets() {
    return pairs.stream().collect(toMultiset(Pair::getRightId, p -> left.get(p.getLeftId())));
  }

  private Map<K, List<R>> getPostSets() {
    return pairs.stream().collect(toMultiset(Pair::getLeftId, p -> right.get(p.getRightId())));
  }

  private static <T, K, V> Collector<T, ?, Map<K, List<V>>> toMultiset(
      Function<T, K> keyFunction, Function<T, V> valueFunction) {
    return groupingBy(keyFunction, mapping(valueFunction, toList()));
  }

  private Map<K, L> getPredecessors() {
    return pairs.stream()
        .collect(toMap(Pair::getRightId, p -> left.get(p.getLeftId()), this::unexpectedPair));
  }

  private Map<K, R> getSuccessors() {
    return pairs.stream()
        .collect(toMap(Pair::getLeftId, p -> right.get(p.getRightId()), this::unexpectedPair));
  }

  private <T> T unexpectedPair(T oldValue, T newValue) {
    throw new ValidationException(
        String.format(
            "N-to-1 relation between %s (%s) and %s (%s) contains (x, %s) and (x, %s)",
            left, right, leftArity, rightArity, oldValue, newValue));
  }

  /**
   * Attempts to load a single pair from this relation from the given record. This is done by
   * looking up the two key fields associated with this record (either a primary key and a foreign
   * key, or two foreign keys in case of a many-to-may relation), and if both can be found, the two
   * values are considered to represent a pair that is part of the relation.
   */
  private void foreignKeyRelationLoader(Record record, Set<Pair<K>> sink) {
    K leftId = record.get(leftKey);
    K rightId = record.get(rightKey);
    if (leftId != null && rightId != null) {
      sink.add(Relation.Pair.of(leftId, rightId));
    }
  }

  static final class Pair<K> {
    private final K leftId;
    private final K rightId;

    Pair(K leftId, K rightId) {
      this.leftId = leftId;
      this.rightId = rightId;
    }

    static <K> Pair<K> of(K leftId, K rightId) {
      return new Pair<>(leftId, rightId);
    }

    K getLeftId() {
      return leftId;
    }

    K getRightId() {
      return rightId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pair<?> pair = (Pair<?>) o;
      return leftId == pair.leftId && rightId == pair.rightId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(leftId, rightId);
    }
  }
}

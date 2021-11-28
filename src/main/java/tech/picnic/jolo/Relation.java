package tech.picnic.jolo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.toOptional;
import static tech.picnic.jolo.Util.validate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.Record;

/**
 * Represents a relation between two {@link Entity entities}. This class is used to extract links
 * between a data set's rows and set their corresponding objects' references.
 *
 * @param <L> The Java class that the left-hand side of the relation is mapped to.
 * @param <R> The Java class that the right-hand side of the relation is mapped to.
 */
final class Relation<L, R> {
  enum Arity {
    ZERO_OR_ONE,
    ONE,
    MANY
  }

  private final Entity<L, ?> left;
  private final Entity<R, ?> right;
  private final Field<Long> leftKey;
  private final Field<Long> rightKey;
  private final Arity leftArity;
  private final Arity rightArity;
  private final Optional<BiConsumer<L, ?>> leftSetter;
  private final Optional<BiConsumer<R, ?>> rightSetter;
  private final Function<Record, Set<IdPair>> relationLoader;

  private final int hashCode;

  @SuppressWarnings("ConstructorLeaksThis")
  Relation(
      Entity<L, ?> left,
      Entity<R, ?> right,
      Field<Long> leftKey,
      Field<Long> rightKey,
      Arity leftArity,
      Arity rightArity,
      Optional<BiConsumer<L, ?>> leftSetter,
      Optional<BiConsumer<R, ?>> rightSetter,
      Optional<Function<Record, Set<IdPair>>> relationLoader) {
    this.left = left;
    this.right = right;
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.leftArity = leftArity;
    this.rightArity = rightArity;
    this.leftSetter = leftSetter;
    this.rightSetter = rightSetter;
    this.relationLoader = relationLoader.orElse(this::foreignKeyRelationLoader);
    this.hashCode =
        Objects.hash(
            this.left,
            this.right,
            this.leftKey,
            this.rightKey,
            this.leftArity,
            this.rightArity,
            this.leftSetter,
            this.rightSetter,
            this.relationLoader);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Relation<?, ?> relation = (Relation<?, ?>) o;
    return Objects.equals(left, relation.left)
        && Objects.equals(right, relation.right)
        && Objects.equals(leftKey, relation.leftKey)
        && Objects.equals(rightKey, relation.rightKey)
        && leftArity == relation.leftArity
        && rightArity == relation.rightArity
        && Objects.equals(leftSetter, relation.leftSetter)
        && Objects.equals(rightSetter, relation.rightSetter)
        && Objects.equals(relationLoader, relation.relationLoader);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format(
        "%s-to-%s Relation<%s (%s), %s (%s)>",
        leftArity,
        rightArity,
        left.getTable().getName(),
        leftKey.getName(),
        right.getTable().getName(),
        rightKey.getName());
  }

  @SuppressWarnings("NoFunctionalReturnType")
  Function<Record, Set<IdPair>> getRelationLoader() {
    return relationLoader;
  }

  Entity<L, ?> getLeft() {
    return left;
  }

  Entity<R, ?> getRight() {
    return right;
  }

  /** Given an {@link ObjectMapping object mapping}, setters are called on the objects. */
  void link(ObjectMapping<L, R> objectMappping) {
    leftSetter.ifPresent(setter -> link(setter, objectMappping.toSuccessors(), rightArity));
    rightSetter.ifPresent(setter -> link(setter, objectMappping.toPredecessors(), leftArity));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private <T, U> void link(
      BiConsumer setter, ImmutableMap<T, ImmutableList<U>> objectMapping, Arity arity) {
    switch (arity) {
      case MANY:
        linkMany(objectMapping, setter);
        break;
      case ONE:
        linkOne(objectMapping, setter);
        break;
      case ZERO_OR_ONE:
        linkOptional(objectMapping, setter);
        break;
    }
  }

  private <T, U> void linkOne(
      ImmutableMap<T, ImmutableList<U>> objectMapping, BiConsumer<T, U> setter) {
    objectMapping.forEach(
        (object, successors) -> {
          validate(
              successors.size() <= 1,
              "N-to-1 relation between %s (%s) and %s (%s) contains conflicting tuples",
              left,
              leftArity,
              right,
              rightArity);
          checkArgument(!successors.isEmpty());
          setter.accept(object, successors.get(0));
        });
  }

  private <T, U> void linkOptional(
      ImmutableMap<T, ImmutableList<U>> objectMapping, BiConsumer<T, Optional<U>> setter) {
    objectMapping.forEach(
        (object, successors) -> {
          validate(
              successors.size() <= 1,
              "N-to-1 relation between %s (%s) and %s (%s) contains conflicting tuples",
              left,
              leftArity,
              right,
              rightArity);
          setter.accept(object, successors.stream().collect(toOptional()));
        });
  }

  private static <T, U> void linkMany(
      ImmutableMap<T, ImmutableList<U>> objectMapping, BiConsumer<T, Collection<U>> setter) {
    objectMapping.forEach(setter);
  }

  /**
   * Attempts to load a single pair from this relation from the given record. This is done by
   * looking up the two key fields associated with this record (either a primary key and a foreign
   * key, or two foreign keys in case of a many-to-may relation), and if both can be found, the two
   * values are considered to represent a pair that is part of the relation.
   */
  private ImmutableSet<IdPair> foreignKeyRelationLoader(Record record) {
    Long leftId = record.get(leftKey);
    Long rightId = record.get(rightKey);
    ImmutableSet.Builder<IdPair> sink = ImmutableSet.builder();
    if (leftId != null && rightId != null) {
      sink.add(IdPair.of(leftId, rightId));
    }
    return sink.build();
  }
}

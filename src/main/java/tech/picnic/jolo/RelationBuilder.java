package tech.picnic.jolo;

import static tech.picnic.jolo.Util.validate;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Record;
import org.jooq.TableField;
import tech.picnic.jolo.Relation.Arity;

/**
 * Class used to specify a {@link Relation}. Cannot be instantiated directly, but is created as part
 * of the fluent API {@link LoaderFactory#create(Entity)}.
 */
public final class RelationBuilder<T, L, R, K> {
  private final LoaderFactoryBuilderImpl<T> builder;
  private final Entity<L, ?, K> left;
  private final Entity<R, ?, K> right;
  @Nullable private Field<K> leftKey;
  @Nullable private Field<K> rightKey;
  @Nullable private Arity leftArity;
  @Nullable private Arity rightArity;
  @Nullable private BiConsumer<L, ?> leftSetter;
  @Nullable private BiConsumer<R, ?> rightSetter;
  private Optional<BiConsumer<Record, Set<Relation.Pair<K>>>> relationLoader = Optional.empty();

  RelationBuilder(
      LoaderFactoryBuilderImpl<T> builder, Entity<L, ?, K> left, Entity<R, ?, K> right) {
    this.builder = builder;
    this.left = left;
    this.right = right;
  }

  /** Shorthand for {@code .and().build()}, to make the API read more naturally. */
  public LoaderFactory<T> build() {
    return and().build();
  }

  /**
   * Finalises the current relation definition and returns to the loader builder that the new
   * relation was created for.
   */
  public LoaderFactoryBuilder<T> and() {
    validate(
        leftSetter != null || rightSetter != null,
        "Relationship between %s and %s has no setters",
        left,
        right);
    assert leftKey != null : "Left key was not set";
    assert rightKey != null : "Right key was not set";
    assert leftArity != null : "Left arity was not set";
    assert rightArity != null : "Right arity was not set";
    return builder.addRelation(
        new Relation<>(
            left,
            right,
            leftKey,
            rightKey,
            leftArity,
            rightArity,
            Optional.ofNullable(leftSetter),
            Optional.ofNullable(rightSetter),
            relationLoader));
  }

  /**
   * Specifies that the given field is a not-null foreign key for an optional one-to-one relation.
   */
  public RelationBuilder<T, L, R, K> oneToZeroOrOne(TableField<?, K> field) {
    setType(field, checkKey(field), Arity.ZERO_OR_ONE, Arity.ONE);
    return this;
  }

  /** Specifies that the given field is a nullable foreign key for a one-to-one relation. */
  public RelationBuilder<T, L, R, K> optionalOneToOne(TableField<?, K> field) {
    setType(field, checkKey(field), Arity.ZERO_OR_ONE, Arity.ZERO_OR_ONE);
    return this;
  }

  /** Specifies that the given field is a not-null foreign key for a one-to-one relation. */
  public RelationBuilder<T, L, R, K> oneToOne(TableField<?, K> field) {
    setType(field, checkKey(field), Arity.ONE, Arity.ONE);
    return this;
  }

  /** Specifies that the given field is a nullable foreign key for a one-to-many relation. */
  public RelationBuilder<T, L, R, K> zeroOrOneToMany(TableField<?, K> field) {
    setType(field, checkKey(field), Arity.MANY, Arity.ZERO_OR_ONE);
    return this;
  }

  /** Specifies that the given field is a not-null foreign key for a one-to-many relation. */
  public RelationBuilder<T, L, R, K> oneToMany(TableField<?, K> field) {
    setType(field, checkKey(field), Arity.MANY, Arity.ONE);
    return this;
  }

  /** Specifies that the given fields constitute a many-to-many relation. */
  public RelationBuilder<T, L, R, K> manyToMany(TableField<?, K> field1, TableField<?, K> field2) {
    // Requiring the fields to be from the same table is not strictly necessary.
    validate(
        field1.getTable().equals(field2.getTable()),
        "Fields defining a many-to-many relation should come from the same table");
    checkKey(field1, left);
    checkKey(field2, right);
    setType(Arity.MANY, Arity.MANY, field1, field2);
    return this;
  }

  /** Specifies a setter for the left-hand side of a *-to-1 relation. */
  public RelationBuilder<T, L, R, K> setOneLeft(BiConsumer<L, R> setter) {
    validate(rightArity == Arity.ONE, "Right arity is %s", rightArity);
    leftSetter = setter;
    return this;
  }

  /** Specifies a setter for the right-hand side of a 1-to-* relation. */
  public RelationBuilder<T, L, R, K> setOneRight(BiConsumer<R, L> setter) {
    validate(leftArity == Arity.ONE, "Left arity is %s", leftArity);
    rightSetter = setter;
    return this;
  }

  /** Specifies a setter for the left-hand side of a *-to-0..1 relation. */
  public RelationBuilder<T, L, R, K> setZeroOrOneLeft(BiConsumer<L, Optional<R>> setter) {
    validate(rightArity == Arity.ZERO_OR_ONE, "Right arity is %s", rightArity);
    leftSetter = setter;
    return this;
  }

  /** Specifies a setter for the right-hand side of a 0..1-to-* relation. */
  public RelationBuilder<T, L, R, K> setZeroOrOneRight(BiConsumer<R, Optional<L>> setter) {
    validate(leftArity == Arity.ZERO_OR_ONE, "Left arity is %s", leftArity);
    rightSetter = setter;
    return this;
  }

  /** Specifies a setter for the left-hand side of a *-to-many relation. */
  public RelationBuilder<T, L, R, K> setManyLeft(BiConsumer<L, List<R>> setter) {
    validate(rightArity == Arity.MANY, "Right arity is %s", rightArity);
    leftSetter = setter;
    return this;
  }

  /** Specifies a setter for the right-hand side of a many-to-* relation. */
  public RelationBuilder<T, L, R, K> setManyRight(BiConsumer<R, List<L>> setter) {
    validate(leftArity == Arity.MANY, "Left arity is %s", leftArity);
    rightSetter = setter;
    return this;
  }

  /** Specifies a function to programmatically identify relation pairs in loaded records. */
  public RelationBuilder<T, L, R, K> setRelationLoader(
      BiConsumer<Record, Set<Relation.Pair<K>>> relationLoader) {
    this.relationLoader = Optional.of(relationLoader);
    return this;
  }

  private Entity<?, ?, ?> checkKey(TableField<?, K> field) {
    Entity<?, ?, ?> referencedSide = field.getTable().equals(left.getTable()) ? right : left;
    validate(
        field.getTable().equals((referencedSide == left ? right : left).getTable()),
        "Foreign key should be a field of %s or %s",
        left.getTable(),
        right.getTable());
    checkKey(field, referencedSide);
    return referencedSide;
  }

  private void checkKey(TableField<?, K> field, Entity<?, ?, ?> entity) {
    validate(
        field.getTable().getReferencesTo(entity.getTable()).stream()
            .map(ForeignKey::getFields)
            .filter(fk -> fk.size() == 1)
            .map(fk -> fk.get(0))
            .anyMatch(fk -> field.equals(field.getTable().field(fk))),
        "%s is not a foreign key into %s",
        field,
        entity.getTable());
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private void setType(Arity leftArity, Arity rightArity, Field<K> leftKey, Field<K> rightKey) {
    validate(this.leftKey == null, "Relationship type already set");
    this.leftArity = leftArity;
    this.rightArity = rightArity;
    this.leftKey = leftKey;
    this.rightKey = rightKey;
  }

  private void setType(
      TableField<?, K> field,
      Entity<?, ?, ?> referencedSide,
      Arity referentArity,
      Arity referencedArity) {
    if (referencedSide == left) {
      setType(referencedArity, referentArity, field, right.getPrimaryKey());
    } else {
      setType(referentArity, referencedArity, left.getPrimaryKey(), field);
    }
  }
}

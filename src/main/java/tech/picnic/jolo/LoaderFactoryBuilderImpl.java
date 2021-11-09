package tech.picnic.jolo;

import static tech.picnic.jolo.Util.getForeignKey;
import static tech.picnic.jolo.Util.getOptionalForeignKey;
import static tech.picnic.jolo.Util.validate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;

/**
 * Creates a {@link Loader}. Cannot be instantiated directly; use {@link LoaderFactory#create}
 * instead.
 */
final class LoaderFactoryBuilderImpl<T> implements LoaderFactoryBuilder<T>, LoaderFactory<T> {
  private final Entity<T, ?> entity;
  private final Set<Entity<?, ?>> entities = new HashSet<>();
  private final List<Relation<?, ?>> relations = new ArrayList<>();

  LoaderFactoryBuilderImpl(Entity<T, ?> entity) {
    this.entity = entity;
    entities.add(entity);
  }

  @Override
  public LoaderFactory<T> build() {
    return this;
  }

  /**
   * Creates a new {@link Loader} with the entities and relations specified in this builder. The
   * resulting loader can be used as a {@link Record record} {@link java.util.stream.Collector
   * collector}.
   *
   * @see Loader
   */
  @Override
  public Loader<T> newLoader() {
    return new Loader<>(entity, entities, relations);
  }

  /**
   * Specifies that there is a relation between two entities. The entities that are passed in are
   * automatically deserialised by the loaders created by {@link #newLoader}. This method returns a
   * builder that allows you to specify further details about the relation, and about how it is
   * loaded.
   */
  @Override
  public <L, R> RelationBuilder<T, L, R> relation(Entity<L, ?> left, Entity<R, ?> right) {
    addEntity(left);
    addEntity(right);
    return new RelationBuilder<>(this, left, right);
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToMany(
      Entity<L, L2> left, Entity<R, R2> right) {
    return relation(left, right).oneToMany(getForeignKey(right.getTable(), left.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToOne(
      Entity<L, L2> left, Entity<R, R2> right) {
    return relation(left, right)
        .oneToOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToZeroOrOne(
      Entity<L, L2> left, Entity<R, R2> right) {
    return relation(left, right)
        .oneToZeroOrOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> optionalOneToOne(
      Entity<L, L2> left, Entity<R, R2> right) {
    return relation(left, right)
        .optionalOneToOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> zeroOrOneToMany(
      Entity<L, L2> left, Entity<R, R2> right) {
    return relation(left, right).zeroOrOneToMany(getForeignKey(right.getTable(), left.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> manyToMany(
      Entity<L, L2> left, Entity<R, R2> right, Table<?> relation) {
    TableField<?, Long> leftKey = getForeignKey(relation, left.getTable());
    TableField<?, Long> rightKey = getForeignKey(relation, right.getTable());
    return relation(left, right).manyToMany(leftKey, rightKey);
  }

  @SuppressWarnings("NullAway")
  private static <L extends Record, R extends Record> TableField<?, Long> getForeignKeySymmetric(
      Table<L> left, Table<R> right) {
    TableField<?, Long> leftKey = getOptionalForeignKey(right, left).orElse(null);
    TableField<?, Long> rightKey =
        leftKey == null
            ? getForeignKey(left, right)
            : getOptionalForeignKey(left, right).orElse(null);
    validate(
        leftKey == null || rightKey == null || leftKey.equals(rightKey),
        "One-to-one relationship between %s and %s is ambiguous, "
            + "please specify the foreign key explicitly",
        left.getName(),
        right.getName());
    return leftKey == null ? rightKey : leftKey;
  }

  /**
   * Used by {@link RelationBuilder} to return completed {@link Relation relations} to this builder.
   */
  LoaderFactoryBuilderImpl<T> addRelation(Relation<?, ?> relation) {
    relations.add(relation);
    return this;
  }

  private void addEntity(Entity<?, ?> newEntity) {
    boolean primaryKeyIdentifiesUniqueEntity =
        entities.stream()
            .filter(e -> e.getPrimaryKey().equals(newEntity.getPrimaryKey()))
            .allMatch(e -> newEntity == e);
    validate(
        primaryKeyIdentifiesUniqueEntity, "Distinct entities cannot refer to the same primary key");
    entities.add(newEntity);
  }
}

package tech.picnic.jolo;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static tech.picnic.jolo.Util.getForeignKey;
import static tech.picnic.jolo.Util.getOptionalForeignKey;
import static tech.picnic.jolo.Util.validate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;

/**
 * Creates a {@link Loader}. Cannot be instantiated directly; use {@link LoaderFactory#create}
 * instead.
 */
final class LoaderFactoryBuilderImpl<T> implements LoaderFactoryBuilder<T>, LoaderFactory<T> {
  private final Entity<T, ?, ?> entity;
  private final Set<Entity<?, ?, ?>> entities = new HashSet<>();
  private final List<Relation<?, ?, ?>> relations = new ArrayList<>();

  LoaderFactoryBuilderImpl(Entity<T, ?, ?> entity) {
    this.entity = entity;
    entities.add(entity);
  }

  @Override
  public LoaderFactory<T> build() {
    return this;
  }

  /**
   * Creates a new {@link Loader} with the entities and relations specified in this builder. The
   * resulting loader can be used as a jOOQ record handler.
   *
   * @see Loader
   */
  @Override
  public Loader<T> newLoader() {
    // We use a prototype pattern to create new entities / relations that keep state about the
    // deserialisation process, by calling Entity#copy and Relation#copy.
    Map<Entity<?, ?, ?>, Entity<?, ?, ?>> newEntities =
        entities.stream().collect(toMap(identity(), Entity::copy));
    @SuppressWarnings("unchecked")
    Entity<T, ?, ?> mainEntity = (Entity<T, ?, ?>) newEntities.get(entity);
    assert mainEntity != null : "Main entity was not copied";
    return new Loader<>(
        mainEntity,
        entities.stream().map(newEntities::get).collect(toSet()),
        relations.stream().map(r -> r.copy(newEntities)).collect(toList()));
  }

  /**
   * Specifies that there is a relation between two entities. The entities that are passed in are
   * automatically deserialised by the loaders created by {@link #newLoader}. This method returns a
   * builder that allows you to specify further details about the relation, and about how it is
   * loaded.
   */
  @Override
  public <L, R, K> RelationBuilder<T, L, R, K> relation(
      Entity<L, ?, K> left, Entity<R, ?, K> right) {
    addEntity(left);
    addEntity(right);
    return new RelationBuilder<>(this, left, right);
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToMany(
      Entity<L, L2, K> left, Entity<R, R2, K> right) {
    return relation(left, right).oneToMany(getForeignKey(right.getTable(), left.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToOne(
      Entity<L, L2, K> left, Entity<R, R2, K> right) {
    return relation(left, right)
        .oneToOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToZeroOrOne(
      Entity<L, L2, K> left, Entity<R, R2, K> right) {
    return relation(left, right)
        .oneToZeroOrOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K>
      RelationBuilder<T, L, R, K> optionalOneToOne(Entity<L, L2, K> left, Entity<R, R2, K> right) {
    return relation(left, right)
        .optionalOneToOne(getForeignKeySymmetric(left.getTable(), right.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K>
      RelationBuilder<T, L, R, K> zeroOrOneToMany(Entity<L, L2, K> left, Entity<R, R2, K> right) {
    return relation(left, right).zeroOrOneToMany(getForeignKey(right.getTable(), left.getTable()));
  }

  @Override
  public <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> manyToMany(
      Entity<L, L2, K> left, Entity<R, R2, K> right, Table<?> relation) {
    TableField<?, K> leftKey = getForeignKey(relation, left.getTable());
    TableField<?, K> rightKey = getForeignKey(relation, right.getTable());
    return relation(left, right).manyToMany(leftKey, rightKey);
  }

  @SuppressWarnings("unchecked")
  private static <L extends Record, R extends Record, K> TableField<?, K> getForeignKeySymmetric(
      Table<L> left, Table<R> right) {
    TableField<?, K> leftKey = (TableField<?, K>) getOptionalForeignKey(right, left).orElse(null);
    TableField<?, K> rightKey =
        leftKey == null
            ? getForeignKey(left, right)
            : (TableField<?, K>) getOptionalForeignKey(left, right).orElse(null);
    validate(
        leftKey == null || rightKey == null || leftKey.equals(rightKey),
        "One-to-one relationship between %s and %s is ambiguous, "
            + "please specify the foreign key explicitly",
        left.getName(),
        right.getName());
    return leftKey == null ? rightKey : leftKey;
  }

  /**
   * Used by {@link RelationBuilder} to return completed {@link Relation} prototypes to this
   * builder.
   */
  LoaderFactoryBuilderImpl<T> addRelation(Relation<?, ?, ?> relation) {
    relations.add(relation);
    return this;
  }

  private void addEntity(Entity<?, ?, ?> newEntity) {
    boolean primaryKeyIdentifiesUniqueEntity =
        entities.stream()
            .filter(e -> e.getPrimaryKey().equals(newEntity.getPrimaryKey()))
            .allMatch(e -> newEntity == e);
    validate(
        primaryKeyIdentifiesUniqueEntity, "Distinct entities cannot refer to the same primary key");
    entities.add(newEntity);
  }
}

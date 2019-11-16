package tech.picnic.jolo;

import org.jooq.Record;
import org.jooq.Table;

/**
 * Creates a {@link Loader}. Cannot be instantiated directly; use {@link LoaderFactory#create}
 * instead.
 */
public interface LoaderFactoryBuilder<T> {

  LoaderFactory<T> build();

  /**
   * Specifies that there is a relation between two entities. The entities that are passed in are
   * automatically deserialised by the loaders created by {@link LoaderFactory#newLoader}. This
   * method returns a builder that allows you to specify further details about the relation, and
   * about how it is loaded.
   */
  <L, R, K> RelationBuilder<T, L, R, K> relation(Entity<L, ?, K> left, Entity<R, ?, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToMany(
      Entity<L, L2, K> left, Entity<R, R2, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToOne(
      Entity<L, L2, K> left, Entity<R, R2, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> oneToZeroOrOne(
      Entity<L, L2, K> left, Entity<R, R2, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> optionalOneToOne(
      Entity<L, L2, K> left, Entity<R, R2, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> zeroOrOneToMany(
      Entity<L, L2, K> left, Entity<R, R2, K> right);

  <L, R, L2 extends Record, R2 extends Record, K> RelationBuilder<T, L, R, K> manyToMany(
      Entity<L, L2, K> left, Entity<R, R2, K> right, Table<?> relation);
}

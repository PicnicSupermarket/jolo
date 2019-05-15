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
  <L, R> RelationBuilder<T, L, R> relation(Entity<L, ?> left, Entity<R, ?> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToMany(
      Entity<L, L2> left, Entity<R, R2> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToOne(
      Entity<L, L2> left, Entity<R, R2> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> oneToZeroOrOne(
      Entity<L, L2> left, Entity<R, R2> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> optionalOneToOne(
      Entity<L, L2> left, Entity<R, R2> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> zeroOrOneToMany(
      Entity<L, L2> left, Entity<R, R2> right);

  <L, R, L2 extends Record, R2 extends Record> RelationBuilder<T, L, R> manyToMany(
      Entity<L, L2> left, Entity<R, R2> right, Table<?> relation);
}

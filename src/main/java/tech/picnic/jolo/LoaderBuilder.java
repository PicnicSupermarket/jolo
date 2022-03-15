package tech.picnic.jolo;

import org.jooq.Record;
import org.jooq.Table;

/** Interface implemented by classes that can create {@link Loader} objects. */
public interface LoaderBuilder<T> {

  /** Creates a new {@link Loader}. */
  Loader<T> build();

  /**
   * Specifies that there is a relation between two entities. The entities that are passed in are
   * automatically deserialised by the loaders created by {@link LoaderBuilder#build()}. This method
   * returns a builder that allows you to specify further details about the relation, and about how
   * it is loaded.
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

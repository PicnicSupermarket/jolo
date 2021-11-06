package tech.picnic.jolo;

import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.jooq.Record;

/**
 * {@link Record} {@link Collector} that loads entity-relation graphs from a data set. To create a
 * {@code Loader}, use {@link LoaderFactory#create}. The loader can be used as follows:
 *
 * <pre>{@code
 * // In static initialisation code, set up the loader factory
 * private static final Entity<T> MY_ENTITY = ...;
 * private static final LoaderFactory<T> FACTORY =
 *     LoaderFactory.create(MY_ENTITY)
 *             .relation(...)
 *             .oneToMany(...)
 *             .setOneLeft(...)
 *             .and()
 *             .relation(...)
 *             ...;
 *
 * // At runtime:
 * Query query = ...;
 * List<T> result = query.collect(FACTORY.newLoader());
 * }</pre>
 *
 * <p>It is highly recommended to initialise the loader factory as early as possible, because during
 * initialisation a number of validations are performed. Initialising at application start-up
 * therefore makes it possible to detect any misconfiguration before the query is first executed.
 */
public final class Loader<T> implements Collector<Record, ObjectGraph, List<T>> {
  private final Entity<T, ?> mainEntity;
  private final Set<Entity<?, ?>> entities;
  private final List<Relation<?, ?>> relations;

  Loader(Entity<T, ?> mainEntity, Set<Entity<?, ?>> entities, List<Relation<?, ?>> relations) {
    this.mainEntity = mainEntity;
    this.entities = entities;
    this.relations = relations;
  }

  @Override
  @SuppressWarnings("NoFunctionalReturnType")
  public Supplier<ObjectGraph> supplier() {
    return ObjectGraph::new;
  }

  @Override
  @SuppressWarnings("NoFunctionalReturnType")
  public BiConsumer<ObjectGraph, Record> accumulator() {
    return (objectGraph, record) -> {
      for (Entity<?, ?> entity : entities) {
        entity.getId(record).ifPresent(id -> objectGraph.add(entity, id, entity.load(record)));
      }
      for (Relation<?, ?> relation : relations) {
        objectGraph.add(relation, relation.getRelationLoader().apply(record));
      }
    };
  }

  @Override
  @SuppressWarnings("NoFunctionalReturnType")
  public BinaryOperator<ObjectGraph> combiner() {
    return (first, second) -> {
      first.merge(second);
      return first;
    };
  }

  @SuppressWarnings({"unchecked", "rawtypes", "NoFunctionalReturnType"})
  @Override
  public Function<ObjectGraph, List<T>> finisher() {
    return objectGraph -> {
      for (Relation relation : relations) {
        relation.link(objectGraph.getObjectMapping(relation));
      }
      return new ArrayList<>(objectGraph.getObjects(mainEntity));
    };
  }

  @Override
  public Set<Characteristics> characteristics() {
    return emptySet();
  }
}

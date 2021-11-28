package tech.picnic.jolo;

import static java.util.Collections.emptySet;

import com.google.common.collect.ImmutableSet;
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
 * {@code Loader}, use {@link Loader#create}. The loader can be used as follows:
 *
 * <pre>{@code
 * // In static initialisation code, set up the loader
 * private static final Entity<T> MY_ENTITY = ...;
 * private static final Loader<T> LOADER =
 *     Loader.create(MY_ENTITY)
 *             .relation(...)
 *             .oneToMany(...)
 *             .setOneLeft(...)
 *             .and()
 *             .relation(...)
 *             ...;
 *
 * // At runtime:
 * Query query = ...;
 * List<T> result = query.collect(toLinkedObjects(LOADER));
 * }</pre>
 *
 * <p>It is highly recommended to initialise the loader as early as possible, because during
 * initialisation a number of validations are performed. Initialising at application start-up
 * therefore makes it possible to detect any misconfiguration before the query is first executed.
 */
public final class Loader<T> implements Collector<Record, ObjectGraph, List<T>> {
  private final Entity<T, ?> mainEntity;
  private final ImmutableSet<Entity<?, ?>> entities;
  private final ImmutableSet<Relation<?, ?>> relations;

  Loader(Entity<T, ?> mainEntity, Set<Entity<?, ?>> entities, List<Relation<?, ?>> relations) {
    this.mainEntity = mainEntity;
    this.entities = ImmutableSet.copyOf(entities);
    this.relations = ImmutableSet.copyOf(relations);
  }

  static <T> LoaderBuilder<T> create(Entity<T, ?> mainEntity) {
    return new LoaderBuilderImpl<>(mainEntity);
  }

  /**
   * Convenience function for improved readability when calling the loader as a parameter of {@link
   * java.util.stream.Stream#collect(Collector)}. It returns the given loader.
   */
  public static <T> Loader<T> toLinkedObjects(Loader<T> loader) {
    return loader;
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

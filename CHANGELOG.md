# Changelog

## v0.0.3

### Release date:

2021/11/14

### Changes:

This release introduces a backwards-incompatible makeover of the public API by simplifying the main `Loader` API,
requiring JDK v11+ and Jooq v3.15+ and dropping multiple public methods:

- Require JDK v11+ and Jooq v3.15+.
- Implement `Collector<Record, ObjectGraph, List<T>>` for `Loader<T>` to be used
  with `org.jooq.ResultQuery#collect(Collector<? super R, A, X> collector)`. See the [README.md] for updated usage
  advice.
- Make `Entity`, `Relation` and `Loader` effectively immutable.
- Remove or hide `Entity#get(long id)`, `Entity#getEntities()`, `Entity#getEntityMap()`. `Entity#copy()`
  , `Entity#load(Record record)`
- Remove `Loader#next(Record record)`, `Loader#get()`, `Loader#stream()`, `Loader#getSet()`, `Loader#getList()`
  , `Loader#getOne()`, `Loader#getOptional()`, `Loader#collect(Collector<? super T, A, R> collector)`.
- Custom relation loaders have to implement `Function<Record, Set<IdPair>`.
- Add additional validation to cover missing entity candidates for the right side of an N-to-1 relation.
- Provide improved validation failure messages.

## v0.0.2

### Release date:

2020/03/01

### Changes:

- Introduce `Loader#collect(Collector<? super T, A, R> collector)`

## v0.0.1

### Release date:

2019/08/13

### Changes:

Initial release

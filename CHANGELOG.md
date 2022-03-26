# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]



## [v1.0.0]

This release marks the initial release of Folo - a fork of [Jolo]. Compared to [Jolo v0.0.2] it introduces a backwards-incompatible
makeover of the public API by simplifying the main `Loader` API, requiring JDK v11+ and Jooq v3.15+ and dropping
multiple public methods.

### Added

- Implement `Collector<Record, ObjectGraph, List<T>>` for `Loader<T>` to be used
  with `org.jooq.ResultQuery#collect(Collector<? super R, A, X> collector)`. See the [readme](README.md) for updated
  usage advice.
- Add additional validation to cover missing entity candidates for the right side of an N-to-1 relation. In case of
  validation exceptions make sure you define the loader in accordance with your query.
- Support building and running on JDK v17.

### Changed

- Require JDK v11+ and Jooq v3.15+.
- Make `Entity`, `Relation` and `Loader` effectively immutable.
- Custom relation loaders have to implement `Function<Record, Set<IdPair>` instead of `BiConsumer<Record, Set<IdPair>>`.
- Provide improved validation failure messages in case conflicting tuples are encountered during linking.
- Upgrade several internal dependencies.

### Removed

- `Entity#get(long id)`, `Entity#getEntities()`, `Entity#getEntityMap()`. `Entity#copy()`, `Entity#load(Record record)`
- `Loader#next(Record record)`, `Loader#get()`, `Loader#stream()`, `Loader#getSet()`, `Loader#getList()`
  , `Loader#getOne()`, `Loader#getOptional()`, `Loader#collect(Collector<? super T, A, R> collector)`.

### Upgrade Notes

Coming from [Jolo v0.0.2] perform these case-sensitive
string search & replace operations in places where you've previously used Jolo:

- `LoaderFactory.create` -> `Loader.of`
- `LoaderFactory` -> `Loader`
- drop occurrences of `.newLoader()`
- `.fetchInto(` -> `.collect(`

Optionally, simplify e.g. `.collect(loader).stream().collect(toImmutableSet())`
to `.collect(collectingAndThen(loader, ImmutableSet::copyOf))` and/or append `java.util.stream` operators as needed.

[Unreleased]: https://github.com/ferdinand-swoboda/folo/compare/v1.0.0...HEAD

[v1.0.0]: https://github.com/ferdinand-swoboda/folo/compare/v0.0.2...v1.0.0

[Jolo]: https://github.com/PicnicSupermarket/jolo

[Jolo v0.0.2]: https://github.com/PicnicSupermarket/jolo/releases/tag/v0.0.2

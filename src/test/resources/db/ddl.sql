CREATE SCHEMA base;

CREATE TABLE base.Foo
(
    id            BIGSERIAL PRIMARY KEY,
    foo           INT,
    relatedFooIds BIGINT[]
);

CREATE TABLE base.Bar
(
    id         BIGSERIAL PRIMARY KEY,
    fooId      BIGINT REFERENCES Foo,
    bar        INT,
    otherBarId BIGINT REFERENCES Bar
);

CREATE TABLE base.FooBar
(
    fooId BIGINT REFERENCES Foo,
    barId BIGINT REFERENCES Bar
);

CREATE TABLE base.Baz
(
    id    BIGSERIAL PRIMARY KEY,
    barId BIGINT REFERENCES Bar
);

ALTER TABLE base.Bar
    ADD COLUMN bazId BIGINT;
ALTER TABLE base.Bar
    ADD FOREIGN KEY (bazId) REFERENCES Baz;

CREATE SCHEMA other;

CREATE TABLE other.Foo
(
    id  BIGSERIAL PRIMARY KEY,
    foo INT
);

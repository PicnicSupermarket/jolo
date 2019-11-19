package tech.picnic.jolo;

import com.google.errorprone.annotations.FormatMethod;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;

final class Util {
  private Util() {}

  static <R extends Record, K> TableField<R, K> getPrimaryKey(Table<R> table) {
    return getKey(table, table.getPrimaryKey().getFields(), "primary");
  }

  static <L extends Record, R extends Record, K> TableField<?, K> getForeignKey(
      Table<L> from, Table<R> into) {
    return Util.<L, R, K>getOptionalForeignKey(from, into)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Table %s has no foreign key into %s", from, into)));
  }

  static <L extends Record, R extends Record, K> Optional<TableField<?, K>> getOptionalForeignKey(
      Table<L> from, Table<R> into) {
    Table<L> fromTable = Util.unalias(from);
    Table<R> intoTable = Util.unalias(into);
    List<ForeignKey<L, R>> keys = fromTable.getReferencesTo(intoTable);
    if (keys.isEmpty()) {
      return Optional.empty();
    }
    validate(
        keys.size() == 1,
        "One-to-* relationship between %s and %s is ambiguous, "
            + "please specify the foreign key explicitly",
        fromTable.getName(),
        intoTable.getName());
    return Optional.of(getKey(from, keys.get(0).getFields(), "foreign"));
  }

  static boolean equalFieldNames(@Nullable Field<?> left, @Nullable Field<?> right) {
    if (left == null) {
      return right == null;
    }
    if (right == null) {
      return false;
    }
    return left.getQualifiedName().equals(right.getQualifiedName());
  }

  @FormatMethod
  static void validate(boolean condition, String message, @Nullable Object... args) {
    if (!condition) {
      throw new ValidationException(String.format(message, args));
    }
  }

  private static <R extends Record, K> TableField<R, K> getKey(
      Table<R> table, List<TableField<R, ?>> fields, String keyType) {
    validate(fields.size() == 1, "Compound %s keys are not supported", keyType);
    validate(
        Long.class.equals(fields.get(0).getType()) || UUID.class.equals(fields.get(0).getType()),
        "Only %s keys of type Long or UUID are supported",
        keyType);
    @SuppressWarnings("unchecked")
    TableField<R, K> field = (TableField<R, K>) table.field(fields.get(0));
    return field;
  }

  private static <R extends Record> Table<R> unalias(Table<R> table) {
    UniqueKey<R> primaryKey = table.getPrimaryKey();
    return primaryKey == null ? table : primaryKey.getTable();
  }
}

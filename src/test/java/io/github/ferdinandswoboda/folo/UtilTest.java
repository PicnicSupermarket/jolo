package io.github.ferdinandswoboda.folo;

import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.FOO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ferdinandswoboda.folo.data.schema.other.Tables;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public final class UtilTest {
  @Test
  public void testEqualsOnFields() {
    Field<Long> withSchema = DSL.field(DSL.name("BASE", "FOO", "ID"), Long.class);
    Field<Long> noSchema = DSL.field(DSL.name("FOO", "ID"), Long.class);
    assertTrue(FOO.ID.equals(withSchema));
    assertTrue(withSchema.equals(FOO.ID));
    assertFalse(FOO.ID.equals(noSchema));
    assertFalse(noSchema.equals(FOO.ID));

    assertEquals(FOO.ID.toString(), "\"BASE\".\"FOO\".\"ID\"");
    assertEquals(withSchema.getQualifiedName(), DSL.name("BASE", "FOO", "ID"));
    assertEquals(noSchema.getQualifiedName(), DSL.name("FOO", "ID"));

    // Documenting this oddity: the qualified name of FOO.ID is not "BASE.FOO.ID", and fields
    // of the same name but from different schemata are considered equivalent.
    assertEquals(FOO.ID.getQualifiedName(), DSL.name("FOO", "ID"));
    assertFalse(FOO.ID.getQualifiedName().equals(withSchema.getQualifiedName()));
    assertTrue(FOO.ID.getQualifiedName().equals(noSchema.getQualifiedName()));
    assertFalse(FOO.ID.equals(Tables.FOO.ID));
    assertTrue(FOO.ID.getQualifiedName().equals(Tables.FOO.ID.getQualifiedName()));
  }
}

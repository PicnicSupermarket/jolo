package tech.picnic.jolo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.picnic.jolo.data.schema.Tables.FOO;

import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public final class UtilTest {
  /**
   * In order to test that jOOQ does not perform unexpected mapping of equally named fields in
   * different tables, we currently use an explicit comparison of the fields' qualified names.
   * Originally the implementation would just check {@link Field#equals}, but it turns out that this
   * operator is not symmetric. If this is ever fixed in jOOQ, we should reconsider the
   * implementation.
   *
   * @see <a href="https://github.com/jOOQ/jOOQ/issues/8509">jOOQ bug report</a>
   */
  @Test
  public void testEqualsOnFields() {
    Field<Long> withSchema = DSL.field(DSL.name("PUBLIC", "FOO", "ID"), Long.class);
    Field<Long> noSchema = DSL.field(DSL.name("FOO", "ID"), Long.class);
    assertFalse(FOO.ID.equals(withSchema));
    assertFalse(withSchema.equals(FOO.ID));
    assertFalse(FOO.ID.equals(noSchema));
    assertTrue(noSchema.equals(FOO.ID));

    // Also documenting this oddity: the qualified name of FOO.ID is not "PUBLIC.FOO.ID", even
    // though it is only considered equal to a field that has the "PUBLIC" qualifier.
    assertEquals(FOO.ID.toString(), "\"FOO\".\"ID\"");
    assertEquals(FOO.ID.getQualifiedName(), DSL.name("FOO", "ID"));
    assertEquals(withSchema.getQualifiedName(), DSL.name("PUBLIC", "FOO", "ID"));
    assertEquals(noSchema.getQualifiedName(), DSL.name("FOO", "ID"));
  }

  @Test
  public void testEqualFieldName() {
    Field<Long> identical = DSL.field(DSL.name("FOO", "ID"), Long.class);
    assertTrue(Util.equalFieldNames(FOO.ID, identical));
    assertTrue(Util.equalFieldNames(identical, FOO.ID));
  }
}

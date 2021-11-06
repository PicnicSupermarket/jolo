package tech.picnic.jolo;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.picnic.jolo.data.schema.Tables.FOO;

import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public final class UtilTest {

  @Test
  public void testEqualFieldName() {
    Field<Long> identical = DSL.field(DSL.name("FOO", "ID"), Long.class);
    assertTrue(Util.equalFieldNames(FOO.ID, identical));
    assertTrue(Util.equalFieldNames(identical, FOO.ID));
  }
}

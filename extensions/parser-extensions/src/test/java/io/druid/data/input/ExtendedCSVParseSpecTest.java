package io.druid.data.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ExtendedCSVParseSpecTest
{
  @Test
  public void testExtendedCSVParseSpecWithSplit() throws IOException
  {
    final ExtendedCSVParseSpec csvParser = new ExtendedCSVParseSpec(
        null,
        new DimensionsSpec(null, null, null),
        ":",
        Arrays.asList("time", "dim1", "dim2", "dim3"),
        Arrays.asList("time", "not-existing")
    );
    String body = "2016-03-02 09:23:00,hello:world,foo:bar,manse";
    final Map<String, Object> jsonMap = csvParser.makeParser().parse(body);
    Assert.assertEquals(
        "jsonMap",
        ImmutableMap.of(
            "time", "2016-03-02 09:23:00",
            "dim1", Arrays.asList("hello", "world"),
            "dim2", Arrays.asList("foo", "bar"),
            "dim3", "manse"
        ),
        jsonMap
    );
  }

  @Test
  public void registerTest() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModule(new ParserExtensionsModule().getJacksonModules().get(0));

    ParseSpec read = mapper.readValue(
          "  {\n"
        + "    \"format\" : \"csv_extension\",\n"
        + "    \"timestampSpec\" : {\n"
        + "      \"column\" : \"timestamp\"\n"
        + "    },\n"
        + "    \"columns\" : [\"timestamp\",\"page\",\"language\",\"user\",\"unpatrolled\",\"newPage\",\"robot\",\"anonymous\",\"namespace\",\"continent\",\"country\",\"region\",\"city\",\"added\",\"deleted\",\"delta\"],\n"
        + "    \"dimensionsSpec\" : {\n"
        + "      \"dimensions\" : [\"page\",\"language\",\"user\",\"unpatrolled\",\"newPage\",\"robot\",\"anonymous\",\"namespace\",\"continent\",\"country\",\"region\",\"city\"]\n"
        + "    }\n"
        + "  }"
        , ParseSpec.class
    );

    Assert.assertEquals(ExtendedCSVParseSpec.class, read.getClass());
  }
}
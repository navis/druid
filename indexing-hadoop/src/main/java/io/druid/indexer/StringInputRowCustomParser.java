package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.Map;

/**
 * Created by jerryjung on 1/22/16.
 */
public class StringInputRowCustomParser implements ByteBufferInputRowParser
{
  private static final Charset DEFAULT_CHARSET;
  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;
  private final Charset charset;
  private CharBuffer chars;
  private final HadoopCustomStringDecoder decoder;
  static final Logger log = new Logger(StringInputRowCustomParser.class);

  @JsonCreator
  public StringInputRowCustomParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding,
      @JsonProperty("decoder") HadoopCustomStringDecoder decoder
  )
  {
    this.chars = null;
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();
    if (encoding != null) {
      this.charset = Charset.forName(encoding);
    } else {
      this.charset = DEFAULT_CHARSET;
    }
    this.decoder = decoder;

  }


  public InputRow parse(ByteBuffer input)
  {
    return this.parseMap(this.buildStringKeyMap(input));
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return this.parseSpec;
  }

  @JsonProperty
  public HadoopCustomStringDecoder getDecoder()
  {
    return decoder;
  }

  @JsonProperty
  public String getEncoding()
  {
    return this.charset.name();
  }

  public StringInputRowCustomParser withParseSpec(ParseSpec parseSpec)
  {
    return new StringInputRowCustomParser(parseSpec, this.getEncoding(), this.getDecoder());
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();
    if (this.chars == null || this.chars.remaining() < payloadSize) {
      this.chars = CharBuffer.allocate(payloadSize);
    }

    CoderResult coderResult = this.charset.newDecoder()
                                          .onMalformedInput(CodingErrorAction.REPLACE)
                                          .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                          .decode(input, this.chars, true);
    if (coderResult.isUnderflow()) {
      this.chars.flip();

      Map theMap;
      try {
        theMap = this.parseString(this.chars.toString());
      }
      finally {
        this.chars.clear();
      }

      return theMap;
    } else {
      throw new ParseException("Failed with CoderResult[%s]", new Object[]{coderResult});
    }
  }

  private Map<String, Object> parseString(String inputString)
  {
    return this.parser.parse(inputString);
  }

  public InputRow parse(String input)
  {
    return this.parseMap(this.parseString(input));
  }

  private InputRow parseMap(Map<String, Object> theMap)
  {
    return this.parse(theMap);
  }


  public InputRow parse(Map<String, Object> theMap) {
    Object dimensions = this.parseSpec.getDimensionsSpec().hasCustomDimensions()?this.parseSpec.getDimensionsSpec().getDimensions(): Lists
        .newArrayList(Sets.difference(theMap.keySet(), this.parseSpec.getDimensionsSpec().getDimensionExclusions()));


    DateTime timestamp;
    try {
      timestamp = this.parseSpec.getTimestampSpec().extractTimestamp(theMap);
      if(timestamp == null) {
        String e = theMap.toString();
        throw new NullPointerException(String.format("Null timestamp in input: %s", new Object[]{e.length() < 100?e:e.substring(0, 100) + "..."}));
      }
    } catch (Exception var5) {
      throw new ParseException(var5, "Unparseable timestamp found!", new Object[0]);
    }


    Map<String,String> param = decoder.getParseColumn();
    List dimensionList = (List)dimensions;
    if(theMap.containsKey(param.get("columnField"))){
      String[] paramKeys = ((String)theMap.get(param.get("columnField"))).split(param.get("tokenizer"));
      String[] paramValues = ((String)theMap.get(param.get("valueField"))).split(param.get("tokenizer"));
      for(int i=0; i < paramKeys.length; i++) {
        theMap.put(paramKeys[i],paramValues[i]);
        dimensionList.add(paramKeys[i]);
      }
      theMap.remove(param.get("columnField"));
      theMap.remove(param.get("valueField"));
      dimensionList.remove(param.get("columnField"));
      dimensionList.remove(param.get("valueField"));
    }

    return new MapBasedInputRow(timestamp.getMillis(), dimensionList, theMap);
  }

  static {
    DEFAULT_CHARSET = Charsets.UTF_8;
  }
}

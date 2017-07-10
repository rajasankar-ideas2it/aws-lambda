package com.powerplay.lambda.util;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Wrapper for the fasterxml jackson library, to make using it easier.
 */
public final class JsonUtil {

    private static final JsonFactory FACTORY = new JsonFactory();

    private JsonUtil() {
        /**/
    }

    // --------------------------------- generating ---------------------------

    /**
     * Turn the Map into a JSON String. If map contains unsupported elements
     * (like incompatible JSON elements), or serialization errors occur, this
     * throws an IOException.<br>
     * <br>
     * WARNING: this method does not do any cyclic reference checks. If values
     * in this map or a sub-container contain cyclic references, it will cause a
     * stack overflow.<br>
     * <br>
     * Only subclasses of Map and Collection are supported for heirarchical
     * structures.
     *
     * @param map
     *            Map of keys/values to encode.
     * @return Json-String version of the given Map.
     * @throws IOException
     *             If map contains unsupported elements (like incompatible JSON
     *             elements), or serialization errors occur
     */
    public static String stringify(final Map<String, ? extends Object> map) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            final JsonGenerator json = FACTORY.createGenerator(writer);
            writeMap(json, map);

            // calling close prevents further data being written (which doesn't
            // matter here), but also flushes internal buffers so we can
            // properly pull data out of it.
            json.close();
            return writer.toString();
        }
    }

    /**
     * Turn the List into a JSON String. If list contains unsupported elements
     * (like incompatible JSON elements), or serialization errors occur, this
     * throws an IOException.<br>
     * <br>
     * WARNING: this method does not do any cyclic reference checks. If values
     * in this list or a sub-container contain cyclic references, it will cause
     * a stack overflow.<br>
     * <br>
     * Only subclasses of Map and Collection are supported for heirarchical
     * structures.
     *
     * @param collection
     *            Collection of values to encode.
     * @return Json-String version of the given List.
     * @throws IOException
     *             If list contains unsupported elements (like incompatible JSON
     *             elements), or serialization errors occur
     */
    public static String stringify(final Collection<? extends Object> collection) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            final JsonGenerator json = FACTORY.createGenerator(writer);
            writeCollection(json, collection);

            // calling close prevents further data being written (which doesn't
            // matter here), but also flushes internal buffers so we can
            // properly pull data out of it.
            json.close();
            return writer.toString();
        }
    }

    /**
     * Write the given map into the JsonGenerator.
     *
     * @param json
     *            JsonGenerator, which is open and ready.
     * @param map
     *            Data to stringify and append.
     * @throws IOException
     *             If writing a key or value fails.
     */
    private static void writeMap(final JsonGenerator json, final Map<?, ? extends Object> map) throws IOException {
        json.writeStartObject();
        for (final Entry<?, ? extends Object> entry : map.entrySet()) {
            json.writeFieldName(String.valueOf(entry.getKey()));
            writeObject(json, entry.getValue());
        }
        json.writeEndObject();
    }

    /**
     * Write the given list into the JsonGenerator.
     *
     * @param json
     *            JsonGenerator, which is open and ready.
     * @param list
     *            Data to stringify and append.
     * @throws IOException
     *             If writing a value fails.
     */
    private static void writeCollection(final JsonGenerator json, final Collection<? extends Object> list)
            throws IOException {
        json.writeStartArray();
        for (final Object value : list) {
            writeObject(json, value);
        }
        json.writeEndArray();
    }

    /**
     * Write the given object into the JsonGenerator.
     *
     * @param json
     *            JsonGenerator, which is open and ready.
     * @param value
     *            Value to stringify and append. Can be a primitive, or a Map or
     *            Collection.
     * @throws IOException
     *             If writing the object (or a sub-object) fails.
     */
    @SuppressWarnings("unchecked")
    private static void writeObject(final JsonGenerator json, final Object value) throws IOException {
        if (value instanceof Map) {
            writeMap(json, (Map<String, ? extends Object>) value);
        } else if (value instanceof Collection) {
            writeCollection(json, (Collection<? extends Object>) value);
        } else {
            try {
                json.writeObject(value);
            } catch (final IllegalStateException e) {
                if (value != null) {
                    json.writeObject(String.valueOf(value));
                } else {
                    // null already failed writing, this isn't normal. throw up.
                    throw e;
                }
            }
        }
    }

    // ---------------------------------- parsing -----------------------------

    /**
     * Turn the given JSON-String into a Map. Requires the string is actually a
     * string representation of a JSON object (not array). Sub-objects are also
     * Maps, and sub-arrays are Collections.
     *
     * @param string
     *            JSON-String to convert to a Map.
     * @return Map with contents parsed from the String.
     * @throws IOException
     *             If memory or parsing issues occur.
     * @throws IllegalStateException
     *             If the root element is not an object.
     */
    public static Map<String, Object> parseMap(final String string) throws IOException {
        checkNotNullOrEmpty(string, "String cannot be null/empty");

        try (JsonParser json = FACTORY.createParser(string)) {
            final JsonToken token = json.nextToken();
            if (!JsonToken.START_OBJECT.equals(token)) {
                throw new IllegalStateException(
                        "Only Maps are supported as the root object for " + "parseMap (got " + token + ")");
            }
            // we already read nextToken() == START_OBJECT, so readMap won't
            // doubly-see it
            return readMap(json);
        }
    }

    /**
     * Turn the given JSON-String into a List. Requires the string is actually a
     * string representation of a JSON array (not object). Sub-arrays are also
     * Lists, and sub-objects are Maps.
     *
     * @param string
     *            JSON-String to convert to a List.
     * @return List with contents parsed from the String.
     * @throws IOException
     *             If memory or parsing issues occur.
     * @throws IllegalStateException
     *             If the root element is not an array.
     */
    public static List<Object> parseList(final String string) throws IOException {
        try (JsonParser json = FACTORY.createParser(string)) {
            final JsonToken token = json.nextToken();
            if (!JsonToken.START_ARRAY.equals(token)) {
                throw new IllegalStateException(
                        "Only Lists are supported as the root object for " + "parseList (got " + token + ")");
            }
            // we already read nextToken() == START_ARRAY, so readList won't
            // doubly-see it
            return readList(json);
        }
    }

    /**
     * Read a map from the JsonParser. Expects that json.getCurrentToken() ==
     * JsonToken.START_OBJECT.
     *
     * @param json
     *            Prepared JsonParser
     * @return Map with contents from this section of the json string only.
     * @throws IOException
     *             If parsing or memory errors occur.
     */
    private static Map<String, Object> readMap(final JsonParser json) throws IOException {
        final Map<String, Object> map = new LinkedHashMap<String, Object>();
        JsonToken token;
        while ((token = json.nextToken()) != null && token != JsonToken.END_OBJECT) {
            final String name = json.getText();
            json.nextToken();
            final Object val = getVal(json);
            map.put(name, val);
        }
        return map;
    }

    /**
     * Read a list from the JsonParser. Expects that json.getCurrentToken() ==
     * JsonToken.START_ARRAY.
     *
     * @param json
     *            Prepared JsonParser
     * @return List with contents from this section of the json string only.
     * @throws IOException
     *             If parsing or memory errors occur.
     */
    private static List<Object> readList(final JsonParser json) throws IOException {
        final List<Object> list = new LinkedList<Object>();
        JsonToken token;
        while (!JsonToken.END_ARRAY.equals(token = json.nextToken()) && token != null) {
            final Object val = getVal(json);
            list.add(val);
        }
        return list;
    }

    /**
     * Read a value from the JsonParser. Must have json.getCurrentToken() on an
     * actual value. Returns null on END_ARRAY or END_OBJECT (or NULL,
     * obviously). Throws IllegalStateException if current token is a
     * FIELD_NAME. Will automatically parse deep objects and arrays if it comes
     * across START_OBJECT or START_ARRAY, using {@link #parseMap(String)} and
     * {@link #parseList(String)} (which recurse back into here).
     *
     * @param json
     *            Prepared JsonParser
     * @return Whatever Object the parser had at its current position, including
     *         sub-objects and sub-arrays.
     * @throws IOException
     *             If parsing or memory errors occur.
     * @throws IllegalStateException
     *             If field names appear in odd places, or other weird issues
     *             arise.
     */
    private static Object getVal(final JsonParser json) throws IOException {
        final JsonToken token = json.getCurrentToken();
        switch (token) {
        case START_OBJECT:
            return readMap(json);
        case START_ARRAY:
            return readList(json);
        case VALUE_NUMBER_INT:
            return json.getLongValue();
        case VALUE_NUMBER_FLOAT:
            return json.getDoubleValue();
        case VALUE_STRING:
            return json.getValueAsString();
        case VALUE_NULL:
            return null;

        case VALUE_TRUE:
        case VALUE_FALSE:
            return json.getValueAsBoolean();

        case END_ARRAY:
        case END_OBJECT:
            // nothing more to do here, object/array has finished. caller
            // should detect this beforehand, but if not, return null.
            return null;

        case FIELD_NAME:
            // should not happen, indicates malformed json (or possibly a
            // bug in our wrapper)
            throw new IllegalStateException("Unexpected FIELD_NAME while reading a value");

        case NOT_AVAILABLE:
        case VALUE_EMBEDDED_OBJECT:
        default:
            // none of these can happen, we are using a blocking stream,
            // and standard json decoders, and no other values are possible
            // for JsonToken. Just here for completeness.
            throw new IllegalStateException("Unexpected FIELD_NAME while reading a value");
        }
    }

    /**
     * Makes sure str is not null or empty. Calls
     * {@link StringUtil#isNullOrEmpty(String)} to determine this.
     *
     * @param str
     *            The string to check.
     * @param errorMessage
     *            The message to display.
     */
    private static void checkNotNullOrEmpty(final String str, final Object errorMessage) {
        com.google.common.base.Preconditions.checkArgument(!isNullOrEmpty(str), errorMessage);
    }

    /**
     * @param str
     *            The string
     * @return {@code true} if the argument string is null or
     *         {@link java.lang.String#isEmpty() empty}
     */
    private static boolean isNullOrEmpty(final String str) {
        return (str == null || str.isEmpty());
    }
}

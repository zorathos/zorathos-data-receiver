package org.datacenter.receiver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serial;
import java.util.Iterator;
import java.util.List;

/**
 * @author : [wangminan]
 * @description : 处理包含JSON数组的文件，每个文件包含一个List<T>结构
 */
@Slf4j
public class JsonArrayFileInputFormat<T> extends SimpleStreamFormat<T> {
    @Serial
    private static final long serialVersionUID = 895649L;
    public static final String DEFAULT_CHARSET_NAME = "UTF-8";
    private final String charsetName;
    protected final Class<T> modelClass;
    private static final ObjectMapper mapper = DataReceiverUtil.mapper;

    public JsonArrayFileInputFormat(Class<T> modelClass) {
        this(DEFAULT_CHARSET_NAME, modelClass);
    }

    public JsonArrayFileInputFormat(String charsetName, Class<T> modelClass) {
        log.info("JsonArrayFileInputFormat initializing for class: {} with charset: {}", modelClass, charsetName);
        this.charsetName = charsetName;
        this.modelClass = modelClass;
    }

    public StreamFormat.Reader<T> createReader(Configuration config,
                                               FSDataInputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, this.charsetName));
        return new JsonArrayFileInputFormat.Reader<>(reader, modelClass, mapper);
    }

    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(modelClass);
    }

    @PublicEvolving
    public static final class Reader<T> implements StreamFormat.Reader<T> {
        private final BufferedReader reader;
        private final Class<T> modelClass;
        private final ObjectMapper mapper;
        private Iterator<T> iterator;

        Reader(BufferedReader reader, Class<T> modelClass, ObjectMapper mapper) {
            this.reader = reader;
            this.modelClass = modelClass;
            this.mapper = mapper;
        }

        @Nullable
        public T read() throws IOException {
            if (iterator == null) {
                // 首次调用时，解析整个JSON数组
                try {
                    List<T> items = mapper.readValue(
                            reader,
                            mapper.getTypeFactory().constructCollectionType(List.class, modelClass)
                    );
                    iterator = items.iterator();
                } catch (Exception e) {
                    throw new IOException("Failed to parse JSON array to List<" +
                            modelClass.getName() + ">: " + e.getMessage(), e);
                }
            }

            // 逐个返回数组元素
            return iterator.hasNext() ? iterator.next() : null;
        }

        public void close() throws IOException {
            this.reader.close();
        }
    }
}

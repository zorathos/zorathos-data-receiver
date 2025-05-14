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

/**
 * @author : [wangminan]
 * @description : 以单个JSON为一个文件 不能使用标准TextLineFormat 借用arktouros逻辑处理
 */
@Slf4j
public class JsonStringFileInputFormat<T> extends SimpleStreamFormat<T> {
    @Serial
    private static final long serialVersionUID = 1L;
    public static final String DEFAULT_CHARSET_NAME = "UTF-8";
    private final String charsetName;
    protected final Class<T> modelClass;
    private static final ObjectMapper mapper = DataReceiverUtil.mapper;

    public JsonStringFileInputFormat(Class<T> modelClass) {
        this(DEFAULT_CHARSET_NAME, modelClass);
    }

    public JsonStringFileInputFormat(String charsetName, Class<T> modelClass) {
        log.info("JsonStringFileInputFormat initializing for class: {} with charset: {}", modelClass, charsetName);
        this.charsetName = charsetName;
        this.modelClass = modelClass;
    }

    public StreamFormat.Reader<T> createReader(Configuration config,
                                               FSDataInputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, this.charsetName));
        return new JsonStringFileInputFormat.Reader<>(reader, modelClass, mapper);
    }

    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(modelClass);
    }

    @PublicEvolving
    public static final class Reader<T> implements StreamFormat.Reader<T> {
        private final BufferedReader reader;
        private final Class<T> modelClass;
        private final ObjectMapper mapper;

        Reader(BufferedReader reader, Class<T> modelClass, ObjectMapper mapper) {
            this.reader = reader;
            this.modelClass = modelClass;
            this.mapper = mapper;
        }

        @Nullable
        public T read() throws IOException {
            return mapper.readValue(reader, modelClass);
        }

        public void close() throws IOException {
            this.reader.close();
        }
    }
}

package com.fei.util;

import com.fei.info.Personas;
import com.ibm.icu.impl.Row;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;

/**
 * @Description: 自定义反序列化 schema，让source拿到方便处理的结构的数据
 * @ClassName: CustomKafkaDeserSchema
 * @Author chengfei
 * @DateTime 2022/2/15 11:19
 **/
public class CustomKafkaDeserSchema implements KafkaDeserializationSchema<Personas> {
    //    private Logger log= LoggerFactory.getLogger(CustomKafkaDeserSchema.class);
    @Override
    public boolean isEndOfStream(Personas nextElement) {
        return false;
    }

    /**
     * @param record
     * @Description: 具体的格式化数据到source中
     * @return:{@link scala.Tuple2<java.lang.String, java.lang.String>}
     * @DateTime: 2022/7/26 9:57
     */
    @Override
    public Personas deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String[] split = new String(record.value()).split(",");
//        return Row.R5<String, String, Integer, Long, String> of = Row.of(split[0], split[1], Integer.valueOf(split[2]).intValue(), Long.valueOf(split[3]).longValue(), split[4]);
        return new Personas(split[0],split[1],Integer.valueOf(split[2]).intValue(),Long.valueOf(split[3]).longValue(),split[4]);
    }

    /**
     * @param
     * @Description: 数据输出的类型
     * @return:{@link org.apache.flink.api.common.typeinfo.TypeInformation<scala.Tuple2 < java.lang.String, java.lang.String>>}
     * @DateTime: 2022/7/26 10:08
     */
    @Override
    public TypeInformation<Personas> getProducedType() {
        // 返回反序列化后的对象类型
        return TypeInformation.of(Personas.class);
    }
}

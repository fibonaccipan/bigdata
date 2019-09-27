package com.yowaqu.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @ ClassName CombineDistinctEvaluator
 * @ Author fibonacci
 * @ Description TODO
 * @ Date 2019/9/26
 * @ Version 1.0
 */
public class CombineDistinctEvaluator extends GenericUDAFEvaluator implements Serializable {

    static final Log LOG = LogFactory.getLog(CombineDistinctEvaluator.class.getName());

    private Long serialVersionUID = 1L;
    private PrimitiveObjectInspector inputOI;
    private ListObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);
        if (m == Mode.PARTIAL1) {
            // 当m是第一阶段时 意味着返回一个分布式的 Aggregation List
            // 调terminatePartial() 方法
            inputOI = (PrimitiveObjectInspector) parameters[0];
            LOG.info("PARTIAL1 inputOI OK");
            return ObjectInspectorFactory
                    .getStandardListObjectInspector(
                            ObjectInspectorUtils.getStandardObjectInspector(inputOI)
                    );
        } else {
            if(parameters[0] instanceof ListObjectInspector) {
                // 当m是ListObjectInspector时，意味着运行到了 部分或者完全的reduce汇总阶段
                internalMergeOI = (ListObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                LOG.info("MID inputOI OK");
                if(m == Mode.FINAL)
                    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                else
                    return ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
            } else {
                // 当m为Complete 无reduce阶段，直接从原始数据到结果
                // 调terminate() 方法
                inputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                LOG.info("COMPLETE input OK");
                return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
            }
        }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new MidAggBuff();
    }

    @Override //
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        ((MidAggBuff) aggregationBuffer).reset();
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
        assert (parameters.length == 1);
        Object p = parameters[0];
        if(p != null)
            ((MidAggBuff) aggregationBuffer).put(p,inputOI);
    }

    @Override // PARTIAL1 和 PARTIAL2 阶段的汇集
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        MidAggBuff myagg = (MidAggBuff) aggregationBuffer;
        ArrayList<Object> rslt = new ArrayList<Object>(myagg.getStrLength());
        rslt.addAll(myagg.getStr());
        return rslt;
    }

    @Override // PARTIAL2 和 FINAN 阶段的 merge
    public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
        MidAggBuff myagg = (MidAggBuff) aggregationBuffer;
        ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
        if(partialResult != null)
            for(Object i:partialResult)
                myagg.put(i,inputOI);
    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        return ((MidAggBuff) aggregationBuffer).getDistinctStr();
    }
}

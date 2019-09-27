package com.yowaqu.udaf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @ ClassName MidAggBuff
 * @ Author fibonacci
 * @ Description ： 存储UDAF中间结果，并可以进行处理
 * @ Date 2019/9/27
 * @ Version 1.0
 */
public class MidAggBuff extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    private ArrayList<Object> str = new ArrayList<Object>();

    protected void put (Object p, PrimitiveObjectInspector inputOI){
        str.add(ObjectInspectorUtils.copyToStandardJavaObject(p,inputOI));
    }

    protected void reset(){str.clear();}

    protected ArrayList<Object> getStr() {return str;}

    protected String getDistinctStr(){
        String strFull = StringUtils.join(str,",");
        Set<String> set = new HashSet<String>();
        for(String elmt:strFull.split(","))
            set.add(elmt);
        // addAll 待实验
        //  set.addAll(Arrays.asList(strFull.split(",")));
        return StringUtils.join(set.toArray(),",");
    }

    protected int getStrLength(){return str.size();}
}

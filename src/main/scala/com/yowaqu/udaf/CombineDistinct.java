package com.yowaqu.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * @ ClassName CombineDistinct
 * @ Author fibonacci
 * @ Description : HIVE UDAF ,将同一组内的 字符串拼接起来并去重
 * @ Date 2019/9/26
 * @ Version 1.0
 */
@Description(name = "CombineDistinct",value = "_FUNC_(X) - return multiple concat and get distinct")
public class CombineDistinct extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(CombineDistinct.class.getName());
    public CombineDistinct(){}

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[]parameters) throws SemanticException{
        if(parameters.length != 1){
            throw new UDFArgumentTypeException(parameters.length - 1,"Exactly ne argument is expected.");
        }
        if(parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,"Only primitive type arguments are accepted but "
            + parameters[0].getTypeName() +"was passed as parameter 1.");
        }
        return new CombineDistinctEvaluator();
    }
}

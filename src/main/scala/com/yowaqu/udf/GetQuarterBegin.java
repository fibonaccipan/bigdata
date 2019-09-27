package com.yowaqu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @ ClassName GetQuarterBegin
 * @ Author fibonacci
 * @ Description : hive UDF 对每一行 使用的函数
 * @ Date 2019/9/27
 * @ Version 1.0
 */
public class GetQuarterBegin extends GenericUDF {
    private transient TextConverter converter;
    private Text result = new Text();
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1 )
            throw new UDFArgumentException("GetQuarterBegin() require 1 argument , got "+arguments.length);

        PrimitiveObjectInspector argumentOI;

        if(arguments[0] instanceof PrimitiveObjectInspector)
            argumentOI = (PrimitiveObjectInspector) arguments[0];
        else
            throw new UDFArgumentException("GQB only takes primitive types, got" + arguments[0].getTypeName());

        switch (argumentOI.getPrimitiveCategory()){
            case STRING:
            case CHAR:
            case VARCHAR:
                break;
            default:
                throw new UDFArgumentException(
                        "GetQuarterBegin only takes STRING/CHAR/VARCHAR types , got"+argumentOI.getPrimitiveCategory());

        }
        converter = new TextConverter(argumentOI);
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object dateObject = arguments[0].get();
        if(dateObject == null)
            return null;

        String date = ((Text) converter.convert(dateObject)).toString();

        if(date == null)
            return null;

        result.set(getBegin(date));
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("GetQuarterBegin",children);
    }

    private String getBegin(String date){
        int year = Integer.parseInt(date.substring(0,4));
        String month = date.substring(4,6);
        int mon = ((int)(Integer.parseInt(month) / 3.1)) * 3;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR,year);
        cal.set(Calendar.MONTH,mon);
        cal.set(Calendar.DAY_OF_MONTH,1);
        return dateFormat.format(cal.getTime());
    }
}

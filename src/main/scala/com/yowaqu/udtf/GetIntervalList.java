package com.yowaqu.udtf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @ ClassName GetIntervalList
 * @ Author fibonacci
 * @ Description:  hive UDTF ,transform a one row date interval to multiple row list
 * @ Date 2019/9/27
 * @ Version 1.0
 * @ udtf Example:
 * select
 * a.id,a.start_date,a.end_date,d.detail
 * from temp_tab a
 * lateral view getIntervalList(a.start_date,'yyyyMMdd',a.end_date,'yyyyMMdd') d as detail
 */
@Description(name = "GetIntervalList",value = "_FUNC_(date1,format1,date2,format2) - get two column of date" +
        ", return multiple rows. represent every day between this two")
public class GetIntervalList extends GenericUDTF {
    private transient ObjectInspector inputOI = null;
    private static Log LOG = LogFactory.getLog(GetIntervalList.class.getName());

    @Override
    public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 4)
            throw new UDFArgumentException("GetIntervalList needs 4 arguments !");

        for(ObjectInspector arg:arguments)
            if(arg.getCategory() != ObjectInspector.Category.PRIMITIVE
                    && ((PrimitiveObjectInspector) arg).getPrimitiveCategory() !=
                    PrimitiveObjectInspector.PrimitiveCategory.STRING)
                throw new UDFArgumentException("GetIntervalList() only take a String as a parameter");
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }


    @Override
    public void process(Object[] objects) throws HiveException {
        String startDate = objects[0].toString();
        String startFormat = objects[1].toString();
        String endDate = objects[2].toString();
        String endFormat = objects[3].toString();
        List<String> allDayList = getIntervalList(startDate,startFormat,endDate,endFormat);
        List<String> row = new ArrayList<String>();
        for(String s:allDayList){
            row.add(s);
            forward(row.toArray());
            row.clear();
        }
    }

    private List<String> getIntervalList(String start,String startFmt,String end,String endFmt){
        SimpleDateFormat startFormat = new SimpleDateFormat(startFmt);
        SimpleDateFormat endFormat = new SimpleDateFormat(endFmt);
        List<String> list = new ArrayList<String>();
        Date startDate;
        Date endDate;
        Calendar cal = Calendar.getInstance();
        try{
            startDate = startFormat.parse(start);
            endDate = endFormat.parse(end);
            cal.setTime(startDate);
        } catch (ParseException e){
            LOG.error("Date parse error:" + e);
            throw new RuntimeException("parse error !");
        }
        while (!cal.getTime().after(endDate)){
            list.add(startFormat.format(cal.getTime()));
            cal.add(Calendar.DAY_OF_MONTH,1);
        }
        return list;
    }

    @Override
    public void close() throws HiveException {}
}

package com.yowaqu.sshbase;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
  * @ClassName Order 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-16
  * @Version 1.0
  */
class Order implements Serializable {
    private String id;
    private String order_itemid;
    private String dept_code;
    private String brand_code;
    private String general_gds_code;
    private String general_gds_name;
    private String area_code;
    private String area_name;
    private String city_code;
    private String city_name;
    private String pay_amount;
    private String time;


//    public String getDayHour(){}

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrder_itemid() {
        return order_itemid;
    }

    public void setOrder_itemid(String order_itemid) {
        this.order_itemid = order_itemid;
    }

    public String getDept_code() {
        return dept_code;
    }

    public void setDept_code(String dept_code) {
        this.dept_code = dept_code;
    }

    public String getBrand_code() {
        return brand_code;
    }

    public void setBrand_code(String brand_code) {
        this.brand_code = brand_code;
    }

    public String getGeneral_gds_code() {
        return general_gds_code;
    }

    public void setGeneral_gds_code(String general_gds_code) {
        this.general_gds_code = general_gds_code;
    }

    public String getGeneral_gds_name() {
        return general_gds_name;
    }

    public void setGeneral_gds_name(String general_gds_name) {
        this.general_gds_name = general_gds_name;
    }

    public String getArea_code() {
        return area_code;
    }

    public void setArea_code(String area_code) {
        this.area_code = area_code;
    }

    public String getArea_name() {
        return area_name;
    }

    public void setArea_name(String area_name) {
        this.area_name = area_name;
    }

    public String getCity_code() {
        return city_code;
    }

    public void setCity_code(String city_code) {
        this.city_code = city_code;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getPay_amount() {
        return pay_amount;
    }

    public void setPay_amount(String pay_amount) {
        this.pay_amount = pay_amount;
    }
}
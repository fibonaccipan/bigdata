package com.yowaqu.sshbase;

import com.alibaba.fastjson.JSON;

/**
 * @ClassName fastjsonTest2
 * @Author fibonacci
 * @Description TODO
 * @Date 19-7-27
 * @Version 1.0
 */
public class fastjsonTest2 {
    public static void main(String[] args) {
        Order o = new Order();
        o.setArea_code("10003");
        o.setCity_code("025");
        o.setCity_name("南京市");
        System.out.println(o);
        System.out.println(JSON.toJSONString(o,true));
    }
}

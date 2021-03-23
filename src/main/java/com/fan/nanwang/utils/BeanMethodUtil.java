package com.fan.nanwang.utils;

import java.lang.reflect.Method;

public class BeanMethodUtil<T> {

    public static<T> void set(T bean,String fun,Object value) {
        try {
            Method method = bean.getClass().getMethod(fun,Class.forName(value.getClass().getCanonicalName()));
            method.invoke(bean, value);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static<T> Object get(T bean,String fun) {
        try {
            Method method = bean.getClass().getMethod(fun);
            return method.invoke(bean);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
}

package com.fei.info;

/**
 * @Description:
 * @ClassName: Personas
 * @Author chengfei
 * @DateTime 2023/5/23 10:20
 **/
public class Personas {
    // id
    private String uuid;
    // 名称
    private String name;
    // 年龄
    private int age;
    // 时间戳
    private long dt;
    // 分区字段(yyyy-mm-dd)
    private String pt;

    public Personas(){}

    public Personas(String uuid, String name, int age, long dt, String pt) {
        this.uuid = uuid;
        this.name = name;
        this.age = age;
        this.dt = dt;
        this.pt = pt;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public long getDt() {
        return dt;
    }

    public void setDt(long dt) {
        this.dt = dt;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }
}

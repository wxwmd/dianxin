package com.ms.spark.bean;

public class CallLog {
    private String caller1;
    private String caller2;
    private String callTime;
    private int duration;

    public CallLog(String caller1, String caller2, String callTime, int duration) {
        this.caller1 = caller1;
        this.caller2 = caller2;
        this.callTime = callTime;
        this.duration = duration;
    }

    public String getCaller1() {
        return caller1;
    }

    public void setCaller1(String caller1) {
        this.caller1 = caller1;
    }

    public String getCaller2() {
        return caller2;
    }

    public void setCaller2(String caller2) {
        this.caller2 = caller2;
    }

    public String getCallTime() {
        return callTime;
    }

    public void setCallTime(String callTime) {
        this.callTime = callTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "CallLog{" +
                "caller1='" + caller1 + '\'' +
                ", caller2='" + caller2 + '\'' +
                ", callTime='" + callTime + '\'' +
                ", duration=" + duration +
                '}';
    }
}

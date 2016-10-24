package com.objectpartners.common.domain;

import java.io.Serializable;

public class CallFrequency implements Serializable {
    static final long serialVersionUID = 1000L;

    private Integer count;
    private String calltype;

    public CallFrequency() {
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getCalltype() {
        return calltype;
    }

    public void setCalltype(String calltype) {
        this.calltype = calltype;
    }
}

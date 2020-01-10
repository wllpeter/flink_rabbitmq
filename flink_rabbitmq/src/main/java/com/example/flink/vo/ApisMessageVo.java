package com.example.flink.vo;

import java.io.Serializable;

/**
 * Created by 86131 on 2020/1/10.
 */
public class ApisMessageVo implements Serializable{
    private static final long serialVersionUID = -2757568524808184059L;
    private String communicationType;
    private String communicationAddress;

    public String getCommunicationType() {
        return communicationType;
    }

    public void setCommunicationType(String communicationType) {
        this.communicationType = communicationType;
    }

    public String getCommunicationAddress() {
        return communicationAddress;
    }

    public void setCommunicationAddress(String communicationAddress) {
        this.communicationAddress = communicationAddress;
    }
}

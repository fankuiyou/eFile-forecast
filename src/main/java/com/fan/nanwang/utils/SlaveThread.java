package com.fan.nanwang.utils;


// SlaveThread类为子线程的父类
public abstract class SlaveThread extends Thread {
    private ControlSlave m_ControlSlave = new ControlSlave();

    public final void joinMaster(ControlMaster m) {
        m.joinSlave(this.m_ControlSlave);
    }
    public final void run() {
        try {
            slaveRun();
        } finally {
            this.m_ControlSlave.done();
        }
    }

    public abstract void slaveRun();
}
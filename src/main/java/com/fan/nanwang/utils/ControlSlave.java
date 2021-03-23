package com.fan.nanwang.utils;


import java.util.ArrayList;
import java.util.List;

//   子线程关联到管理类
public final class ControlSlave {
    private List listMaster = new ArrayList();

    public void joinMaster(ControlMaster m) {
        if (m == null)
            return;
        synchronized (this) {
            if (this.listMaster.contains(m))
                return;
            this.listMaster.add(m);
        }
        m.joinSlave(this);
    }
    public final void done() {
        synchronized (this) {
            for (int i = 0; i < this.listMaster.size(); i++) {
                ((ControlMaster)this.listMaster.get(i)).slaveDone(this);
            }
            this.listMaster.clear();
        }
    }
}
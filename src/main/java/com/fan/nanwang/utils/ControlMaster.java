package com.fan.nanwang.utils;

import java.util.ArrayList;
import java.util.List;

public final class ControlMaster {
    private boolean exiting = false;
    private List listSlave = new ArrayList();

    public void joinSlave(ControlSlave s) { if (s == null)
        return;
        synchronized (this) {
            if (this.listSlave.contains(s))
                return;
            this.listSlave.add(s);
        }
        s.joinMaster(this); }

    public void exiting() {
        this.exiting = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void safeDone() {
        synchronized (this) {
            do {
                try {
                    wait();
                }
                catch (Exception localException)
                {
                }
                if (this.exiting) break;
            }while (this.listSlave.size() > 0);
        }
    }

    final void slaveDone(ControlSlave s) {
        synchronized (this) {
            this.listSlave.remove(s);
            notifyAll();
        }
    }
}
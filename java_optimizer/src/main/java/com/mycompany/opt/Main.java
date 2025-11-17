package com.mycompany.opt;

import com.google.ortools.Loader;
import com.google.ortools.sat.*;

public class Main {
    static {
        Loader.loadNativeLibraries();
    }

    public static void main(String[] args) {
        CpModel model = new CpModel();
        IntVar x = model.newBoolVar("x");
        model.maximize(x);

        CpSolver solver = new CpSolver();
        CpSolverStatus status = solver.solve(model);

        System.out.println("Status: " + status);
        if (status == CpSolverStatus.OPTIMAL || status == CpSolverStatus.FEASIBLE) {
            System.out.println("Giá trị tối ưu x = " + solver.value(x));
        } else {
            System.out.println("Không tìm được nghiệm.");
        }
    }
}

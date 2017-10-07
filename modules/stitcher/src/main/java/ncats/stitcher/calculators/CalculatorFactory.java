package ncats.stitcher.calculators;

import java.util.ArrayList;
import java.util.List;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Stitch;
import ncats.stitcher.calculators.ApprovalCalculator;
import ncats.stitcher.calculators.StitchCalculator;

public class CalculatorFactory {
    private EntityFactory ef;

    private List<StitchCalculator> calcs = new ArrayList<>();

    private static CalculatorFactory _instance = null;

    public CalculatorFactory(EntityFactory ef) {
        this.ef = ef;
        
        //For now, add these explicitly
        //eventually, move this to setup place
        
        calcs.add(new ApprovalCalculator(ef));
        /*
         * calcs.add(s->{ s.set("justatest","somevalue"); });
         */
    }

    public void process(Stitch s) {
        calcs.forEach(c -> {
            c.accept(s);
        });
    }

    public static CalculatorFactory getCalculatorFactory(EntityFactory ef) {
        if (_instance != null)
            return _instance;
        _instance = new CalculatorFactory(ef);
        return _instance;
    }

}

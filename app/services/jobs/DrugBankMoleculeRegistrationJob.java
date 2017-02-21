package services.jobs;

import java.io.IOException;
import ncats.stitcher.GraphDb;
import ncats.stitcher.impl.MoleculeEntityFactory;
import ncats.stitcher.impl.DrugBankEntityFactory;

public class DrugBankMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public DrugBankMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new DrugBankEntityFactory (getGraphDb ());
    }
}

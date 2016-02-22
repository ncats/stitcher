package services.jobs;

import java.io.IOException;
import ix.curation.GraphDb;
import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.impl.DrugBankEntityFactory;

public class DrugBankMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public DrugBankMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new DrugBankEntityFactory (getGraphDb ());
    }
}

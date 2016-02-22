package services.jobs;

import java.io.IOException;
import ix.curation.GraphDb;
import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.impl.IntegrityMoleculeEntityFactory;

public class IntegrityMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public IntegrityMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new IntegrityMoleculeEntityFactory (getGraphDb ());
    }
}

package services.jobs;

import java.io.IOException;
import ncats.stitcher.GraphDb;
import ncats.stitcher.impl.MoleculeEntityFactory;
import ncats.stitcher.impl.IntegrityMoleculeEntityFactory;

public class IntegrityMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public IntegrityMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new IntegrityMoleculeEntityFactory (getGraphDb ());
    }
}

package services.jobs;

import java.io.IOException;
import ncats.stitcher.impl.MoleculeEntityFactory;
import ncats.stitcher.impl.SRSJsonEntityFactory;
import ncats.stitcher.GraphDb;


public class SRSJsonRegistrationJob extends MoleculeRegistrationJob {
    public SRSJsonRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new SRSJsonEntityFactory (getGraphDb ());
    }
}

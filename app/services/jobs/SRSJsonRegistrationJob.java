package services.jobs;

import java.io.IOException;
import play.Logger;

import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.impl.SRSJsonEntityFactory;
import ix.curation.GraphDb;


public class SRSJsonRegistrationJob extends MoleculeRegistrationJob {
    public SRSJsonRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new SRSJsonEntityFactory (getGraphDb ());
    }
}

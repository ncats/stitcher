package services.jobs;

import java.io.IOException;
import ncats.stitcher.GraphDb;
import ncats.stitcher.impl.MoleculeEntityFactory;
import ncats.stitcher.impl.NPCEntityFactory;

public class NPCMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public NPCMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new NPCEntityFactory (getGraphDb ());
    }
}

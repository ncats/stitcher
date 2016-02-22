package services.jobs;

import java.io.IOException;
import ix.curation.GraphDb;
import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.impl.NPCEntityFactory;

public class NPCMoleculeRegistrationJob extends MoleculeRegistrationJob {
    public NPCMoleculeRegistrationJob () {
    }

    @Override
    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new NPCEntityFactory (getGraphDb ());
    }
}

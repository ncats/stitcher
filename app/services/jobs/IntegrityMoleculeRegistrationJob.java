package services.jobs;

import java.io.IOException;
import play.Logger;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

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

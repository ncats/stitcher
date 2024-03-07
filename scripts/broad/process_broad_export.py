import csv

input_data = csv.reader(snakemake.input[0])

with open(snakemake.input[0], mode='r') as input_file:
    with open(snakemake.output[0], mode='wt') as output_file:
        reader = csv.reader(input_file, delimiter='\t')
        writer = csv.writer(output_file, delimiter='\t')
        writer.writerow(['broad_name','smiles','status','target','moa','diseaseArea','indication'])
        # ignore header
        _ = next(reader)
        line = next(reader)
        try:
            while line:
                # new headers
                # 'Name', 'MOA', 'Target', 'Disease Area', 'Indication', 'SMILES', 'InChIKey', 'Phase'
                # old headers
                # broad_name	name	synonyms	smiles	status	statusSource	target	targetSource	moa	diseaseArea	indication	indicationSource
                Name, MOA, Target, Disease_Area, Indication, SMILES, InChIKey, Phase = line
                if SMILES:
                    smiles_array = [value.strip() for value in SMILES.split(',')]
                    smiles = smiles_array[0]
                else:
                    smiles = None

                if Target:
                    target_array = [value.strip() for value in Target.split(',')]
                    target = '|'.join(target_array)
                else:
                    target = None

                writer.writerow([Name, smiles, Phase, target, MOA, Disease_Area, Indication])
                line = next(reader)
        except StopIteration:
            pass

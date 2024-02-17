import minds

query = "SELECT * FROM nihnci.clinical WHERE project_id = 'TCGA-LUAD'"
cohort = minds.build_cohort(query=query, output_dir="Z:\\datasets\\TCGA-LUAD")
cohort.download()
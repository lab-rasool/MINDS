import minds

query = "SELECT * FROM nihnci.clinical WHERE project_id = 'TCGA-LUAD'"
cohort = minds.build_cohort(query=query, output_dir="D:\TCGA-LUAD")
cohort.download()

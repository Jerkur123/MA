# MA
This repository contains all the code needed to run the scenario generation workflow for resilience analysis, as described in the thesis "On the Resilience of the European Power System." All required packages and their versions, primarily from the pypsa-eur environment, are listed in the file pypsa-eur.yaml.

Steps to run the scenario generation workflow:

1. Install the environment.
2. Run the Snakemake workflow. By default, the get_results option in the config file is set to True, which overrides the scenario generation and downloads the results from [Zenodo](https://zenodo.org/records/13619460). Set it to False to run the scenario generation workflow.
3. Run aggregate_data.ipynb to analyze the reproduce the plots

The contingency scenario generation workflow follows this structure:

1. The base scenario is solved using the script solve_base.py.
2. Based on the solved base scenario, the contingency scenario—defined by the duration and severity in the configuration file—is generated using the script solve {contingency_name}.py.
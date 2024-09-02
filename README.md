# MA
This repository contains all the code needed to run the scenario generation workflow for resilience analysis, as described in the thesis "On the Resilience of the European Power System." All required packages, primarily from the pypsa-eur environment, along with their versions, are listed in the file MA_JK.yaml.

The contingency scenario generation workflow follows this structure:

1. The base scenario is solved using the script solve_base.py.
2. Based on the solved base scenario, the contingency scenario—defined by the duration and severity in the configuration file—is generated using the script solve_{contingency_name}.py

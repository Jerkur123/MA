import os
import shutil
import yaml
from netCDF4 import Dataset


configfile: 'config/config.yaml'


transmission_limits = config['transmission_limit']
countries = config['countries']
contingencies = config['contingencies']
models = config['models']
horizon = config['horizon']
o = config['min_equity']

# Ensure horizon is an integer
try:
    horizon = int(horizon)
except ValueError:
    raise ValueError(f"Horizon value must be an integer, got {horizon}")

# Transform the contingencies structure into a more accessible format and extend based on sensitivity analysis
print(contingencies)
contingency_list = []
for contingency in contingencies:
    for name, params in contingency.items():
        reductiontos = []
        durations = []
        for param in params:
            if 'reductionto' in param:
                reductiontos = param['reductionto']
            if 'duration' in param:
                durations = param['duration']
        #durations = params['duration']
        
        for rt in reductiontos:
            for dur in durations:
                contingency_list.append({
                    'name': name,
                    'reductionto': rt,
                    'duration': int(dur)  # Convert duration to integer
                })

print("Extended Contingency list:", contingency_list)
print(countries)
# Create a list of dictionaries for each combination, including extended contingencies
combinations = []
for country in countries:
    for country_code, country_datas in country.items():
        for country_data in country_datas:
            if 'bus' in country_data:
                #bus = next(item['bus'] for item in country_data if 'bus' in item)
                bus = country_data['bus']
        for tl in transmission_limits:
            for model in models:
                for c in contingency_list:
                    combinations.append({
                        'country': country_code,
                        'buses': bus,
                        'contingency': c['name'],
                        'reductionto': c['reductionto'],
                        'duration': int(c['duration']),  # Convert duration to integer
                        'transmission_limit': tl,
                        'model': model
                    })

# Dictionary to hold the second values of reductionto and duration for each contingency
second_values = {}

# Parsing each contingency
for contingency_dict in contingencies:
    for contingency, params in contingency_dict.items():
        reductionto_second = None
        duration_second = None
        for param in params:
            if 'reductionto' in param:
                reductionto_second = param['reductionto'][1]  # Access the second value
            if 'duration' in param:
                duration_second = param['duration'][1]  # Access the second value
        second_values[contingency] = {
            'reductionto': reductionto_second,
            'duration': duration_second
        }

print(second_values)



rule all:
    input:
        #capacity of investment model for each scenarios
        expand(
            "results/{country}_{buses}_{transmission_limit}_{contingency}_{reductionto}_{duration}_{model}roll.nc", 
            zip,
            country=[comb['country'] for comb in combinations],
            buses=[comb['buses'] for comb in combinations],
            contingency=[comb['contingency'] for comb in combinations],
            reductionto=[comb['reductionto'] for comb in combinations],
            duration=[comb['duration'] for comb in combinations],
            transmission_limit=[comb['transmission_limit'] for comb in combinations],
            model=[comb['model'] for comb in combinations]
        )

rule solve_base:
    input:
        "resources/{country}_{buses}_{transmission_limit}_base.nc"
    output:
        "resources/{country}_{buses}_{transmission_limit}_base_solved.nc",
        "resources/{country}_{buses}_{transmission_limit}_base_roll_solved.nc"
    params:
        co2_price = config['co2_price'],
        horizon = horizon,
        o = o
    run:
        if not os.path.exists(input[0]):
            print(f"Input file {input[0]} does not exist. Skipping.")
        else:
            if not os.path.exists(output[0]) or not os.path.exists(output[1]):
                script = "scripts/solve_base.py"
                shell(f"python {script} {input[0]} {output[0]} {params.co2_price} {output[1]} {params.horizon} {params.o} {wildcards.country} {wildcards.transmission_limit} {wildcards.buses}")
            else:
                print(f"Skipping processing for {output[0]} and {output[1]} as they already exist.")

rule dynamic_solve:
    input:
        "resources/{country}_{buses}_{transmission_limit}_base_solved.nc"
    output:
        "resources/{country}_{buses}_{transmission_limit}_{contingency}_{reductionto}_{duration}_{model}.nc",
        "results/{country}_{buses}_{transmission_limit}_{contingency}_{reductionto}_{duration}_{model}roll.nc"
    params:
        #reductionto=lambda wildcards: next(c['reductionto'] for c in contingency_list if c['name'] == wildcards.contingency),
        #duration=lambda wildcards: next(c['duration'] for c in contingency_list if c['name'] == wildcards.contingency),
        #art,
        horizon = horizon,
        o = o
    run:
        
        if not os.path.exists(input[0]):
            print(f"Input file {input[0]} does not exist. Skipping.")
        else:
            if os.path.exists(output[0]) and os.path.exists(output[1]): 
                print(f"Skipping processing for {output[0]} as it already exists. Skipping")
            else:
                #print(f'country: {wildcards.country}')
                if wildcards.contingency == "pv":
                    script = "scripts/solve_pv.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}")
                elif wildcards.contingency == "wind":
                    script = "scripts/solve_wind.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}")
                elif wildcards.contingency == "noexim":
                    script = "scripts/solve_noexim.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}")
                elif wildcards.contingency == "drought":
                    script = "scripts/solve_drought.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}") 
                elif wildcards.contingency == "dispatchcut":
                    script = "scripts/solve_dispatchcut.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}")  
                elif wildcards.contingency == "windpv":
                    script = "scripts/solve_windpv.py"
                    shell(f"python {script} {input[0]} {output[0]} {output[1]} {wildcards.contingency} {wildcards.reductionto} {wildcards.duration} {wildcards.model} {params.horizon} {wildcards.country} {params.o} {wildcards.transmission_limit} {wildcards.buses}")         
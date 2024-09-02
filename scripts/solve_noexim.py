import sys
import os
import pypsa 
import matplotlib.pyplot as plt
plt.style.use("bmh")
import pandas as pd
from pypsa.plot import add_legend_patches
import gurobipy
import cartopy.crs as ccrs
from pypsa.optimization import optimize
import matplotlib.cm as cm
import numpy as np
import xarray as xr
import seaborn as sns
import re

def mod_rh_storage(n1,n,n_base):
    """
        Modify marginal cost and the initial soc of storage for the rolling horizon model 

        marginal cost of the storage unit= MSV

        initial soc of storage unit in rh models = initial of storage units in long term optimized model


        Parameters
        ----------
        n : solved pypsa.Network (perfect foresight)
        n1 : to be edited pypsa.Network (rolling horizon model)
        n_base : long term optimized base model
        """
    
    #edit marginal cost
    for sto_name in n.storage_units.index:
        #mean storage value of storage in long term optimization
        MSV = n.storage_units_t.mu_energy_balance[sto_name].mean()
        n1.storage_units.loc[sto_name,'marginal_cost'] = MSV

    #edit initial value
    for index, value in n_base.storage_units_t.state_of_charge.iloc[0,:].items():
        n1.storage_units.at[index, 'state_of_charge_initial'] = value

def add_EQ_constraints(n, o, scaling=1e-1):
    """
    Add equity constraints to the network.

    Currently this is only implemented for the electricity sector only.

    Opts must be specified in the config.yaml.

    Parameters
    ----------
    n : pypsa.Network
    o : str

    Example
    -------
    scenario:
        opts: [Co2L-EQ0.7-24h]

    Require each country or node to on average produce a minimal share
    of its total electricity consumption itself. Example: EQ0.7c demands each country
    to produce on average at least 70% of its consumption; EQ0.7 demands
    each node to produce on average at least 70% of its consumption.
    """
    # TODO: Generalize to cover myopic and other sectors?
    float_regex = "[0-9]*\.?[0-9]+"
    level = float(re.findall(float_regex, o)[0])
    if o[-1] == "c":
        ggrouper = n.generators.bus.map(n.buses.country)
        lgrouper = n.loads.bus.map(n.buses.country)
        sgrouper = n.storage_units.bus.map(n.buses.country)
    else:
        ggrouper = n.generators.bus
        lgrouper = n.loads.bus
        sgrouper = n.storage_units.bus
    load = (
        n.snapshot_weightings.generators
        @ n.loads_t.p_set.groupby(lgrouper, axis=1).sum()
    )
    inflow = (
        n.snapshot_weightings.stores
        @ n.storage_units_t.inflow.groupby(sgrouper, axis=1).sum()
    )
    inflow = inflow.reindex(load.index).fillna(0.0)
    rhs = scaling * (level * load - inflow)
    p = n.model["Generator-p"]
    lhs_gen = (
        (p * (n.snapshot_weightings.generators * scaling))
        .groupby(ggrouper.to_xarray())
        .sum()
        .sum("snapshot")
    )
    # TODO: double check that this is really needed, why do have to subtract the spillage
    if not n.storage_units_t.inflow.empty:
        spillage = n.model["StorageUnit-spill"]
        lhs_spill = (
            (spillage * (-n.snapshot_weightings.stores * scaling))
            .groupby(sgrouper.to_xarray())
            .sum()
            .sum("snapshot")
        )
        lhs = lhs_gen + lhs_spill
    else:
        lhs = lhs_gen
    n.model.add_constraints(lhs >= rhs, name="equity_min")

def extract_carriers(column_names, country):
    """
    Filters columns that match the specified carrier names.
    
    Parameters:
    - column_names: List of column names in the DataFrame.
    - country: two alphabet code of selected country. dor example 'DE'.
    
    Returns:
    - List of column names that match the carriers.
    """
    return [col for col in column_names if 'DE' in col]

def min_generation_period(n, num_days, country):
    """
    Iterates through the DataFrame and finds the start date with the minimum generation sum for all generation in a selected country over a given number of days.
    (indicator of maximum import)

    Parameters:
    - n: pypsa Network.
    - num_days: The total number of days over which to sum the generation.
    - country: two alphabet code of selected country. dor example 'DE'.

    Returns:
    - Start date with the minimum generation and the corresponding sum.
    """
    #combine storage production with generators
    df = n.generators_t.p
    sto = n.storage_units_t.p
    df = pd.concat([df,sto], axis = 1)

    # Determine the timestep in days based on the DataFrame's index
    timestep = (df.index[1] - df.index[0]).total_seconds() / 86400  # Convert seconds to days
    num_rows = round(num_days / timestep)  # Calculate number of rows needed, round to nearest whole number
    
    # Extract column names matching the specified carriers
    relevant_columns = extract_carriers(df.columns, country)
    
    min_generation = 1e50
    start_date_with_min_generation = None

    # Iterate over each row in the DataFrame
    for start_idx in range(len(df)):
        #print(start_idx)
        # Ensure we do not exceed the DataFrame's length
        if start_idx + num_rows <= len(df):
            # Sum the generation for specified carriers over the period
            current_sum = df.iloc[start_idx:start_idx + num_rows][relevant_columns].sum().sum()
            #print(df.index[start_idx],current_sum)
            # Check if the current sum is the highest found so far
            if current_sum < min_generation:
                min_generation = current_sum
                start_date_with_min_generation = df.index[start_idx]

    return start_date_with_min_generation

#allow investment only in chosen country and national lines
def allow_inv(n1,n,country):
    #delete line expansion global constraint
    if 'lv_limit' in n1.global_constraints.index:
        n1.global_constraints = n1.global_constraints.drop('lv_limit')

    #set the optimal capacity of generators from the base scenario as the new minimum capacity 
    for index, row in n1.generators.iterrows():
        if row['p_nom_extendable'] == True:  
            n1.generators.at[index, 'p_nom_min'] = n.generators.at[index, 'p_nom_opt']
            n1.generators.at[index, 'p_nom_extendable'] = True
        else:
            n1.generators.at[index, 'p_nom'] = n.generators.at[index, 'p_nom_opt']
            n1.generators.at[index, 'p_nom_extendable'] = False

    #set the optimal capacity of storage units from the base scenario as the new minimum capacity 
    for index, row in n1.storage_units.iterrows():
        if row['p_nom_extendable'] == True:
            n1.storage_units.at[index, 'p_nom_min'] = n.storage_units.at[index, 'p_nom_opt'] 
            n1.storage_units.at[index, 'p_nom_extendable'] = True
        else:
            n1.storage_units.at[index, 'p_nom'] = n.storage_units.at[index, 'p_nom_opt']
            n1.storage_units.at[index, 'p_nom_extendable'] = False

    #set the optimal capacity of lines from the base scenario as the new minimum capacity 
    for index, row in n1.lines.iterrows():
        if row['carrier'] == 'AC' and (row['bus0'][:2] == row['bus1'][:2]):
            n1.lines.at[index, 's_nom_min'] = n.lines.at[index, 's_nom_opt']
            n1.lines.at[index, 's_nom_extendable'] = True 
            n1.lines.at[index, 's_nom_max'] = n.lines.at[index, 's_nom_opt']*1.5
        else:
            n1.lines.at[index, 's_nom'] = n.lines.at[index, 's_nom_opt']
            n1.lines.at[index, 's_nom_extendable'] = False 
    
    #set the optimal capacity of links from the base scenario as the new minimum capacity 
    for index, row in n1.links.iterrows():
        if row['carrier'] == 'DC' and (row['bus0'][:2] == row['bus1'][:2]):
            n1.links.at[index, 'p_nom'] = n.links.at[index, 'p_nom_opt']
            n1.links.at[index, 'p_nom_extendable'] = False 
            n1.links.at[index, 'p_nom_max'] = n.links.at[index, 'p_nom_opt']*1.5
        else:
            n1.links.at[index, 'p_nom'] = n.links.at[index, 'p_nom_opt']
            n1.links.at[index, 'p_nom_extendable'] = False
            
             

def no_inv(n2,n):
    #set the optimal capacity of generators from the base scenario as the new minimum capacity 
    for index, value in n2.generators.p_nom_extendable.items():
        if value:  
            n2.generators.at[index, 'p_nom'] = n.generators.at[index, 'p_nom_opt']
            #n2.generators.at[index, 'p_nom_max'] = n.generators.at[index, 'p_nom_opt']
            n2.generators.at[index,'p_nom_extendable'] = False

    #set the optimal capacity of storage units from the base scenario as the new minimum capacity 
    for index, value in n2.storage_units.p_nom_extendable.items():
        if value:  
            n2.storage_units.at[index, 'p_nom'] = n.storage_units.at[index, 'p_nom_opt']
            #n2.storage_units.at[index, 'p_nom_max'] = n.storage_units.at[index, 'p_nom_opt']
            n2.storage_units.at[index,'p_nom_extendable'] = False

    #set the optimal capacity of stores from the base scenario as the new minimum capacity 
    for index, value in n2.stores.e_nom_extendable.items():
        if value:  
            n2.stores.at[index, 'e_nom'] = n.stores.at[index, 'e_nom_opt']
            #n2.stores.at[index, 'e_nom_max'] = n.stores.at[index, 'e_nom_opt']
            n2.stores.at[index, 'e_nom_extendable'] =False

    #set the optimal capacity of lines from the base scenario as the new minimum capacity 
    for index, value in n2.lines.s_nom_extendable.items():
        if value:  
            n2.lines.at[index, 's_nom'] = n.lines.at[index, 's_nom_opt']
            #n2.lines.at[index, 's_nom_max'] = n.lines.at[index, 's_nom_opt']
            n2.lines.at[index, 's_nom_extendable'] =False

    #set the optimal capacity of lines from the base scenario as the new minimum capacity 
    for index, value in n2.links.p_nom_extendable.items():
        if value:  
            n2.links.at[index, 'p_nom'] = n.links.at[index, 'p_nom_opt']
            #n2.lines.at[index, 's_nom_max'] = n.lines.at[index, 's_nom_opt']
            n2.links.at[index, 'p_nom_extendable'] =False

def set_initial_soc(n1, n):
    for index, value in n.storage_units_t.state_of_charge.iloc[0,:].items():
        n1.storage_units.at[index, 'state_of_charge_initial'] = value           

def export_statistics(n, country, n_perf=0):
    #add column country in generaror, storage unit, lines and links
    for index, row in n.generators.iterrows():
        n.generators.at[index,'country'] = row['bus'][:2]

    for index, row in n.storage_units.iterrows():
        n.storage_units.at[index,'country'] = row['bus'][:2]

    for index, row in n.lines.iterrows():
        if country in row['bus0'] or country in row['bus1']:
            n.lines.at[index, 'country'] = country
        else:
            n.lines.at[index,'country'] = row['bus0'][:2]

    for index, row in n.links.iterrows():
        if country in row['bus0'] or country in row['bus1']:
            n.links.at[index, 'country'] = country
        else:
            n.links.at[index,'country'] = row['bus0'][:2]

    #export capacity in GW
    cap = n.statistics.optimal_capacity(comps=["Generator", "StorageUnit","Line","Link","Transformer"], groupby=["carrier","country"], aggregate_groups="sum").unstack().fillna(0).droplevel(0)/1e3 #GW
    filename = f"results/cap_{n.name}.csv"
    output_path = os.path.join(os.getcwd(), filename)
    cap.to_csv(output_path) 

    gen = n.statistics.supply(comps=["Generator", "StorageUnit",], groupby=["carrier","country"], aggregate_groups="sum", aggregate_time = False).fillna(0).droplevel(0)/1e3 #GW
    filename = f"results/gen_{n.name}.csv"
    output_path = os.path.join(os.getcwd(), filename)
    gen.to_csv(output_path)

    #export system cost in Bill €
    #calc opex and capex of the chosen country
    if 'roll' in n.name:
        # marginal cost of storage units under perfect foresight is used back to calculate the system cost of storages in RH models
        n_copy = n.copy()
        n_copy.storage_units['marginal_cost']=n_perf.storage_units['marginal_cost']
        opex = n_copy.statistics.opex(comps=["Generator", "StorageUnit","Line","Link","Transformer"], groupby=["carrier","country"], aggregate_groups="sum").unstack().fillna(0).droplevel(0)
    else:
        opex = n.statistics.opex(comps=["Generator", "StorageUnit","Line","Link","Transformer"], groupby=["carrier","country"], aggregate_groups="sum").unstack().fillna(0).droplevel(0)

    capex= n.statistics.capex(comps=["Generator", "StorageUnit","Line","Link","Transformer"], groupby=["carrier","country"], aggregate_groups="sum").unstack().fillna(0).droplevel(0)

    capex_df = capex / 1e9 #Bill€
    filename = f"results/capex_{n.name}.csv"
    output_path = os.path.join(os.getcwd(), filename)
    capex_df.to_csv(output_path)

    cost_df = pd.DataFrame(columns=n.generators.country.unique(), index = n.carriers.index)

    for index, row in capex.iterrows():
        for col in capex.columns:
            cost_df.loc[index, col] = capex.loc[index, col]

    for index, value in opex.iterrows():
        for col in opex.columns:
            cost_df.at[index, col] += opex.at[index, col]

    cost_df = cost_df.dropna()
    system_cost = cost_df/1e9 # Bill€
    filename = f"results/syscost_{n.name}.csv"
    output_path = os.path.join(os.getcwd(), filename)
    system_cost.to_csv(output_path)

def solve_contingencies(input_file, output_file, output_file_roll, contingency, reductionto, duration, model, horizon, country, o, tl, bus):
    
    n = pypsa.Network(input_file)

    #scenario models
    n_new = n.copy()
    reductionto = float(reductionto)
    duration = int(duration)

    #define cut start based on the period with max generation of the chosen carrier    
    cut_start = min_generation_period(n, duration, country)
    cut_end = cut_start + pd.Timedelta(days= duration)

    #implement noexim scenario
    # find transnational AC lines and cut them
    for index,row in n_new.lines.iterrows():
        ctr_bus0 = row['bus0'][:2]
        ctr_bus1 = row['bus1'][:2]
        #set s_max_pu as series for transnational AC Lines
        if ctr_bus0 != ctr_bus1 and row['carrier'] == 'AC':
                n_new.lines_t.s_max_pu[index] = 0.7

    # find transnational DC lines
    for index,row in n_new.links.iterrows():
        ctr_bus0 = row['bus0'][:2]
        ctr_bus1 = row['bus1'][:2]
        #set s_max_pu as series for transnational DC Lines
        if ctr_bus0 != ctr_bus1 and row['carrier'] == 'DC':
                n_new.links_t.p_max_pu[index] = float(1)
                n_new.links_t.p_min_pu[index] = float(0)

    #cut AC lines
    n_new.lines_t.s_max_pu.loc[cut_start:cut_end] *= reductionto

    #cut DC lines
    n_new.links_t.p_max_pu.loc[cut_start:cut_end] *= reductionto


    #build inv and noinv model:
    if model == 'inv':
        allow_inv(n_new,n, country) 

        #solve inv model
        n_new.optimize(solver_name = "gurobi", assign_all_duals = True)

        #add min equity constraint 
        add_EQ_constraints(n_new, o)
        n_new.optimize.solve_model(solver_name='gurobi',assign_all_duals = True)
        print('equity constraint sovled and added back to the model')
        n_new.name = f'{contingency}_{country}_{bus}_{tl}_{reductionto}_{duration}_inv'
        export_statistics(n_new, country)

        #create rolling horizon model
        n_roll = n_new.copy()
        n_roll.name = f'{contingency}_{country}_{bus}_{tl}_{reductionto}_{duration}_invroll'
        no_inv(n_roll, n_new)

    elif model == 'noinv':
        no_inv(n_new,n)

        #solve noinv model
        n_new.optimize(solver_name = "gurobi",assign_all_duals = True)
        n_new.name = f'{contingency}_{country}_{bus}_{tl}_{reductionto}_{duration}_noinv'

        export_statistics(n_new, country)

        #create rolling horizon model
        n_roll = n_new.copy()
        n_roll.name = f'{contingency}_{country}_{bus}_{tl}_{reductionto}_{duration}_noinvroll'

    #solve rolling horizon    
    n_roll.storage_units['cyclic_state_of_charge'] = False
    n_roll.storage_units['cyclic_state_of_charge_per_period'] = False
    mod_rh_storage(n_roll,n_new,n)
    optimize.optimize_with_rolling_horizon(n_roll, horizon=int(horizon), overlap=0, solver_name='gurobi',assign_all_duals = True)
    export_statistics(n_roll, country, n_new)
    # Save the solved network to the output file
    n_new.export_to_netcdf(output_file)
    n_roll.export_to_netcdf(output_file_roll)
    #n_base_RH.export_to_netcdf(output_file_base_roll)

    # Also save the solved network to the resources directory
    #resource_output_file = os.path.join("resources", os.path.basename(output_file))
    #n_new.export_to_netcdf(resource_output_file)

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    output_file_roll = sys.argv[3]
    contingency = sys.argv[4]
    reductionto = sys.argv[5]
    duration = sys.argv[6]
    model = sys.argv[7]
    horizon = int(sys.argv[8])
    country = str(sys.argv[9])
    o = sys.argv[10]
    tl = sys.argv[11]
    bus = sys.argv[12]

    
solve_contingencies(input_file, output_file, output_file_roll, contingency, reductionto, duration, model, horizon, country, o, tl, bus)


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

def mod_rh_storage(n1,n):
    """
        Modify marginal cost and the initial soc of storage for the rolling horizon model 

        marginal cost of the storage unit= MSV

        initial soc of storage unit in rh models = initial of storage units in long term optimized model


        Parameters
        ----------
        n : solved pypsa.Network (perfect foresight)
        n1 : to be edited pypsa.Network (rolling horizon model)
        """
    
    #edit marginal cost
    for sto_name in n.storage_units.index:
        #mean storage value of storage in long term optimization
        MSV = n.storage_units_t.mu_energy_balance[sto_name].mean()
        n1.storage_units.loc[sto_name,'marginal_cost'] = MSV

    #edit initial value
    for index, value in n.storage_units_t.state_of_charge.iloc[0,:].items():
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

def export_statistics(n, country,n_perf=0):
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


def solve_base(input_file, output_file, output_RH, co2_price, horizon, o, country, tl, bus):
    n = pypsa.Network(input_file)
    print(tl)
    print(country)
    print(bus)

    # Set marginal cost of load shedding to 15000€/MWh
    for i in n.generators.bus:
        for index in n.generators.index:
            if not i.endswith('H2') and index.endswith('load'):
                n.generators.loc[index, 'marginal_cost'] = 15000

    # Edit load shedding's unit from kW to MW
    for index in n.generators.index:
        if index.endswith('load') and not index.endswith('H2 load'):
            n.generators.loc[index, 'sign'] = 1

    n.optimize(solver_name='gurobi')


    #add min equity constraint
    add_EQ_constraints(n, o)
    n.optimize.solve_model(solver_name='gurobi', assign_all_duals=True)

    n.name = f'{country}_{tl}_base_solved'
    export_statistics(n, country)

    #BAse RH
    n_RH = n.copy()
    n_RH.name = f'{country}_{tl}_base_roll_solved'
    mod_rh_storage(n_RH,n)
    no_inv(n_RH,n)
    n_RH.storage_units['cyclic_state_of_charge'] = False
    n_RH.storage_units['cyclic_state_of_charge_per_period'] = False
    optimize.optimize_with_rolling_horizon(n_RH, horizon=int(horizon), overlap=0, solver_name='gurobi',assign_all_duals=True)
    
    export_statistics(n_RH, country,n)

    # Save the solved network to the output file
    n.export_to_netcdf(output_file)
    n_RH.export_to_netcdf(output_RH)

    # Also save the solved network to the resources directory
   # resource_output_file = os.path.join("resources", os.path.basename(output_file))
    #n.export_to_netcdf(resource_output_file)

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    co2_price = float(sys.argv[3])
    output_RH = sys.argv[4]
    horizon = sys.argv[5]
    o = sys.argv[6]
    country = sys.argv[7]
    tl = sys.argv[8]
    bus = sys.argv[9]

    solve_base(input_file, output_file, output_RH, co2_price, horizon, o, country, tl, bus)


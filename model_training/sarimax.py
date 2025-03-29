import pygad
import numpy as np
from sktime.forecasting.sarimax import SARIMAX
from sktime.split import ExpandingWindowSplitter
from sklearn.model_selection import train_test_split, cross_val_score


from numpy.random import RandomState

import pandas as pd
import argparse
from sqlalchemy import create_engine
from azure.storage.blob import ContainerClient

def main():
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Feature engineering script for Octopus Energy data')
    
    parser.add_argument('--output_path', required=True, help='Path to save the processed data')
    parser.add_argument('--azure_blob_conn_str', required=True, help='azure_blob_conn_str')
    parser.add_argument('--target', required=True, help='y variable')
    
    args = parser.parse_args()
    seed = 1234
    state = RandomState(seed)
    blob_block = ContainerClient.from_connection_string(
    conn_str=args.azure_blob_conn_str,
    container_name='octopusenergy'
    )
    #blob_block.upload_blob(args.output_path, output, overwrite=True, encoding='utf-8')
    #read data
    df = blob_block.download_blob(args.output_path).readall()
    df = pd.read_csv(df, index_col=0)

    featurenames = [i for i in df.columns if i not in [args.target, 'hour_start']]
    X, y = df[featurenames], df.loc[:,args.target]
    
    
    #split_index = int(len(X) * 0.5)  # 50% for training, 20% for testing
    X_train, X_test = X.loc[X['hour_start'].dt.year==2023,], X.loc[X['hour_start'].dt.year==2024]
    y_train, y_test = y.loc[X['hour_start'].dt.year==2023], y.loc[X['hour_start'].dt.year==2024]



    def fitness_func(ga_instance, solution, solution_idx):
        #prep model
        order = solution[:3]
        seasonal_order = solution[3:6]
        features = solution[6:]
        mask = np.array(features, dtype = bool)
        
        selected_features = np.array(featurenames)[mask]
        X_tmp = X_train.loc[:,selected_features]
        forecaster = SARIMAX(order=order, seasonal_order=seasonal_order)
        
        score = cross_val_score(forecaster, X_tmp, y_train, scoring="rmse", cv = 5).mean()
        fitness = score
        return fitness



    m = len(featurenames)
    fitness_function = fitness_func
    # initialize with a random subset of features
    gene_space = list(state.randint(0, 8, 3)) + list(state.randint(0, 2, m))

    num_generations = 100
    num_parents_mating = 2

    sol_per_pop = 2
    num_genes = m


    parent_selection_type = "sss"
    keep_parents = 2
    crossover_type = "single_point"
    mutation_type = "random"
    mutation_percent_genes = 15

    ga_instance = pygad.GA(gene_space=gene_space,
                        num_generations=num_generations,
                        num_parents_mating=num_parents_mating,
                        fitness_func=fitness_function,
                        sol_per_pop=sol_per_pop,
                        num_genes=num_genes,
                        keep_parents=keep_parents,
                        crossover_type=crossover_type,
                        mutation_type=mutation_type,
                        mutation_percent_genes=mutation_percent_genes,
                        random_seed=seed,
                        )

    ga_instance.run()


    solution, solution_fitness, solution_idx = ga_instance.best_solution()
    print("Parameters of the best solution : {solution}".format(solution=solution))
    print("Fitness value of the best solution = {solution_fitness}".format(solution_fitness=solution_fitness))
    print(f"Number of features selected = {sum(solution)}")

    order = solution[:3]
    seasonal_order = solution[3:6]
    features = solution[6:]
    mask = np.array(features, dtype = bool)
    
    selected_features = np.array(featurenames)[mask]
    forecaster = SARIMAX(order=order, seasonal_order=seasonal_order)
    #model = forecaster.fit(X_train, y_train)
    #print(f"Performance with all the features:")
    #model.score(X_test, y_test)


    model = forecaster.fit(X_train.loc[:,selected_features], y_train, fh=14)
    cv = ExpandingWindowSplitter(
        step_length=1, fh=list(range(1,15,1)), initial_window=1
    )
    y_pred = model.update_predict(X=X_test.loc[:,selected_features], y=y_test, cv=cv, update_params=False, reset_forecaster=False)
    print(y_pred)

if __name__ == "__main__":
    main()
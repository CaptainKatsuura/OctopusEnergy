import pygad
import numpy as np
import pickle
from sktime.forecasting.sarimax import SARIMAX
from sktime.split import ExpandingWindowSplitter
from sktime.performance_metrics.forecasting import MeanSquaredError
from sktime.forecasting.model_evaluation import evaluate

from numpy.random import RandomState

import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from azure.storage.blob import ContainerClient
from sktime import show_versions

def main():
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Feature engineering script for Octopus Energy data')

    parser.add_argument('--output_path', required=True, help='Path to read the processed data')
    parser.add_argument('--azure_blob_conn_str', required=True, help='azure_blob_conn_str')
    parser.add_argument('--target', required=True, help='y variable')
    
    
    args = parser.parse_args()
    input_path = f"fe/{os.path.splitext(args.output_path.split('/')[-1])[0]}.csv"
    seed = 12345
    state = RandomState(seed)
    blob_block = ContainerClient.from_connection_string(
    conn_str=args.azure_blob_conn_str,
    container_name='octopusenergy'
    )
    #blob_block.upload_blob(args.output_path, output, overwrite=True, encoding='utf-8')
    #read data
    print(input_path)
    df = blob_block.download_blob(input_path)
    df = pd.read_csv(df, index_col=0)
    # Ensure the index is a DatetimeIndex
    df.index = pd.to_datetime(df.index)
    print(df.head())
    nan_rows = df[df.isnull().T.any()]
    if not nan_rows.empty:
        print(f"NaN rows: {nan_rows}")
        

    featurenames = [i for i in df.columns if i not in [args.target, 'date']]
    X, y = df[featurenames], df.loc[:,[args.target]]
    print(X.head())
    print(y.head())    
    
    #split_index = int(len(X) * 0.5)  # 50% for training, 20% for testing
    X_train, X_test = X.loc[X.index.year == 2024], X.loc[X.index.year == 2025]
    y_train, y_test = y.loc[y.index.year == 2024, [args.target]], y.loc[y.index.year == 2025, [args.target]]
    print(f"y_train type: {type(y_train)}")
    print(f"y_train shape: {y_train.shape}")
    print(f"y_train head:\n{y_train.head(35)}")
    print(f"X_train type: {type(X_train)}")
    print(f"X_train shape: {X_train.shape}")
    print(f"X_train head:\n{X_train.head(35)}")
    
    def fitness_func(ga_instance, solution, solution_idx):
        #prep model
        print(f'solution {solution}')
        order = solution[:3]
        features = solution[3:]
        mask = np.array(features, dtype = bool)
        
        selected_features = np.array(featurenames)[mask]
        print(f"selected_features: {selected_features}")
        X_tmp = X_train.loc[:,selected_features]
        print(f"X_tmp shape: {X_tmp.shape}")
        print(f"X_tmp head:\n{X_tmp.head()}")
        forecaster = SARIMAX(order=order, seasonal_order=None, freq='D')
        cv = ExpandingWindowSplitter(initial_window=30, step_length=30, fh=[1])
        if len(selected_features) == 0:
            results = evaluate(forecaster=forecaster, y=y_train, cv=cv, scoring=MeanSquaredError(square_root=True), error_score='raise')
        else:
            results = evaluate(forecaster=forecaster,X=X_tmp, y=y_train, cv=cv, scoring=MeanSquaredError(square_root=True), error_score='raise')
        # Extract the numeric score from the results DataFrame
        score = results["test_MeanSquaredError"].mean()  # Replace "test_MeanSquaredError" with the correct column name if needed
        # Negate the score to maximize fitness
        fitness = -score
        return fitness



    m = len(featurenames)
    fitness_function = fitness_func
    # initialize with a random subset of features
    pdq = 3+1
    feature_selection = 1+1
    gene_space = [list(range(pdq))] * 3 + [list(range(feature_selection))] * m
    print(f"gene_space: {gene_space}")

    num_generations = 5
    num_parents_mating = 2

    sol_per_pop = 2
    num_genes = len(gene_space)


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
    print(f"Number of features selected = {sum(solution[3:])}")

    order = solution[:3]
    #seasonal_order = solution[3:7]
    features = solution[3:]
    mask = np.array(features, dtype = bool)
    
    selected_features = np.array(featurenames)[mask]
    forecaster = SARIMAX(order=order, seasonal_order=None, freq='D')
    #model = forecaster.fit(X_train, y_train)
    #print(f"Performance with all the features:")
    #model.score(X_test, y_test)
    print(f"Type of X_test: {type(X_test)}")
    print(f"Type of y_test: {type(y_test)}")
    # Debug the data
    print(f"X_test shape: {X_test.shape}")
    print(f"y_test shape: {y_test.shape}")
    print(f"X_test index: {X_test.index}")
    print(f"y_test index: {y_test.index}")

    if len(selected_features) == 0:
        model = forecaster.fit(y_train, fh=[1])
    #elif len(selected_features) == 1:
    #    model = forecaster.fit(X_train.loc[:,[selected_features]], y_train, fh=[1])
    else:
        model = forecaster.fit(X_train.loc[:,selected_features], y_train, fh=[1])

    cv = ExpandingWindowSplitter(
        step_length=1, fh=[1], initial_window=30
    )

    print("Displaying version information for sktime and dependencies...")
    show_versions()
    if len(selected_features) == 0:
        y_pred = model.update_predict(y=y_test, cv=cv, update_params=False, reset_forecaster=False)
    #elif len(selected_features) == 1:
        #print(X_test.loc[:,[selected_features]])
        #y_pred = model.update_predict(X=X_test.loc[:,[selected_features]], y=y_test, cv=cv, update_params=False, reset_forecaster=False)
    else:
        print(X_test.loc[:,selected_features])
        print(f"Are indices aligned? {X_test.loc[:,selected_features].index.equals(y_test.index)}")
        y_pred = model.update_predict(X=X_test.loc[:,selected_features], y=y_test, cv=cv, update_params=False, reset_forecaster=False)

    y_pred = y_pred.to_string(index=True)
    
    blob_block.upload_blob(f'models/{args.output_path}.csv', y_pred, overwrite=True, encoding='utf-8')
    model_bytes = pickle.dumps(model)
    # Upload the serialized model to Azure Blob Storage
    blob_block.upload_blob(f'models/{args.output_path}.pkl', model_bytes, overwrite=True)

if __name__ == "__main__":
    main()
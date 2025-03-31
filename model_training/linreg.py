import pygad
import numpy as np
import pandas as pd
import argparse
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from azure.storage.blob import ContainerClient

# filepath: c:/Users/sim77/OneDrive/Desktop/AirflowWorkspace/dags/PortfolioProjects/OctopusEnergy/model_training/linreg.py

def main():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Linear regression model training script for Octopus Energy data')

    parser.add_argument('--output_path', required=True, help='Path to save the processed data')
    parser.add_argument('--azure_blob_conn_str', required=True, help='Azure Blob connection string')
    parser.add_argument('--target', required=True, help='Target variable (y)')

    args = parser.parse_args()
    input_path = f"fe/{os.path.splitext(args.output_path.split('/')[-1])[0]}.csv"
    seed = 12345
    np.random.seed(seed)

    blob_block = ContainerClient.from_connection_string(
        conn_str=args.azure_blob_conn_str,
        container_name='octopusenergy'
    )

    # Read data
    print(input_path)
    df = blob_block.download_blob(input_path)
    df = pd.read_csv(df, index_col=0)
    df.index = pd.to_datetime(df.index)

    # Create lag feature
    df['lag_y_1'] = df[args.target].shift(1)
    df.dropna(inplace=True)

    featurenames = [col for col in df.columns if col not in [args.target, 'date']]
    X, y = df[featurenames], df[[args.target]]

    # Split data into train and test sets
    X_train, X_test = X.loc[X.index.year == 2024], X.loc[X.index.year == 2025]
    y_train, y_test = y.loc[y.index.year == 2024], y.loc[y.index.year == 2025]

    def fitness_func(ga_instance, solution, solution_idx):
        # Select features based on the solution
        mask = np.array(solution, dtype=bool)
        selected_features = np.array(featurenames)[mask]
        X_tmp = X_train.loc[:, selected_features]

        # Train and evaluate the model
        tscv = TimeSeriesSplit(n_splits=5)
        rmse_scores = []
        for train_idx, val_idx in tscv.split(X_tmp):
            X_train_fold, X_val_fold = X_tmp.iloc[train_idx], X_tmp.iloc[val_idx]
            y_train_fold, y_val_fold = y_train.iloc[train_idx], y_train.iloc[val_idx]

            model = LinearRegression()
            model.fit(X_train_fold, y_train_fold)
            y_pred = model.predict(X_val_fold)
            rmse_scores.append(root_mean_squared_error(y_val_fold, y_pred))

        # Return the negative mean of the MSE scores as fitness
        return -np.mean(rmse_scores)

    # Genetic Algorithm setup
    m = len(featurenames)
    gene_space = [list(range(2))] * m  # Binary selection for each feature

    ga_instance = pygad.GA(
        gene_space=gene_space,
        num_generations=5,
        num_parents_mating=2,
        fitness_func=fitness_func,
        sol_per_pop=2,
        num_genes=m,
        keep_parents=2,
        crossover_type="single_point",
        mutation_type="random",
        mutation_percent_genes=15,
        random_seed=seed,
    )

    ga_instance.run()

    # Get the best solution
    solution, solution_fitness, solution_idx = ga_instance.best_solution()
    print("Parameters of the best solution : {solution}".format(solution=solution))
    print("Fitness value of the best solution = {solution_fitness}".format(solution_fitness=solution_fitness))
    print(f"Number of features selected = {sum(solution)}")

    # Train final model with selected features
    mask = np.array(solution, dtype=bool)
    selected_features = np.array(featurenames)[mask]
    X_train_final = X_train.loc[:, selected_features]
    X_test_final = X_test.loc[:, selected_features]

    model = LinearRegression()
    model.fit(X_train_final, y_train)
    y_pred = model.predict(X_test_final)

    # Save predictions to Azure Blob
    y_pred_df = pd.DataFrame(y_pred, index=y_test.index, columns=['Prediction'])
    y_pred['test'] = y_test.loc[:,args.target]
    y_pred_csv = y_pred_df.to_csv(index=True)
    blob_block.upload_blob(f'models/{args.output_path}.csv', y_pred_csv, overwrite=True, encoding='utf-8')

if __name__ == "__main__":
    main()
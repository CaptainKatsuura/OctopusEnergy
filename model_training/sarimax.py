import pygad
import numpy as np

from sklearn.model_selection import train_test_split, cross_val_score
from src.learner_params import target_column, model_features

from sklearn.datasets import load_breast_cancer
from sklearn.metrics import roc_auc_score

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
    
    args = parser.parse_args()
    seed = 1234
    state = RandomState(seed)

    #read data
    bc = load_breast_cancer()

    #prep model
    bst = lgbm(random_state = seed)

    function_inputs = bc.feature_names


    X, y = bc.data,bc.target
    X = pd.DataFrame(X, columns=bc.feature_names)
    X_train, X_test, y_train, y_test = train_test_split(X,
                                                        y,
                                                        random_state=seed)



    def fitness_func(ga_instance, solution, solution_idx):
        mask = np.array(solution, dtype = bool)
        selected_features = np.array(bc.feature_names)[mask]
        X_tmp = X_train.loc[:,selected_features]
        score = cross_val_score(bst, X_tmp, y_train, scoring="rmse", cv = 5).mean()
        fitness = score
        return fitness



    m = len(bc.feature_names)
    fitness_function = fitness_func
    # initialize with a random subset of features
    gene_space = list(state.randint(0, 8, 3)) + list(state.randint(0, 2, m))

    num_generations = 30
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




    model = bst.fit(X_train, y_train)
    print(f"Performance with all the features:")
    model.score(X_test, y_test)


    model = bst.fit(X_train.loc[:,selected_], y_train)
    print(f"Performance with subset of features:")
    model.score(X_test.loc[:,selected_], y_test)

if __name__ == "__main__":
    main()
# Databricks notebook source
"""
SmartLogix Transport Optimization - Route Optimization
This notebook implements route optimization using Google OR-Tools.
"""

# COMMAND ----------

# Import required libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

# Add project root to path
sys.path.append('/Workspace/Repos/your-username/SmartLogix-Transport-Optimization')

# COMMAND ----------

# Install OR-Tools if not available
%pip install ortools

# COMMAND ----------

# Import OR-Tools
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data for Optimization

# COMMAND ----------

# Load data from Delta tables
delhivery_df = spark.table("default.smartlogix.delhivery_data")
distance_df = spark.table("default.smartlogix.distance_data")
order_df = spark.table("default.smartlogix.order_data")

# Convert to Pandas for OR-Tools processing
delhivery_pandas = delhivery_df.toPandas()
distance_pandas = distance_df.toPandas()
order_pandas = order_df.toPandas()

print(f"Loaded {len(delhivery_pandas)} transport records")
print(f"Loaded {len(distance_pandas)} distance records")
print(f"Loaded {len(order_pandas)} order records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Distance Matrix

# COMMAND ----------

def create_distance_matrix(distance_df, locations):
    """Create distance matrix for given locations."""
    # Create a pivot table for distance matrix
    distance_matrix = distance_df.pivot(
        index='Source', 
        columns='Destination', 
        values='Distance(M)'
    ).fillna(0)
    
    # Ensure all locations are in the matrix
    for loc in locations:
        if loc not in distance_matrix.index:
            distance_matrix.loc[loc] = 0
        if loc not in distance_matrix.columns:
            distance_matrix[loc] = 0
    
    # Reorder matrix to match locations order
    distance_matrix = distance_matrix.reindex(locations, columns=locations)
    
    return distance_matrix.values

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Optimization Function

# COMMAND ----------

def solve_vehicle_routing(distance_matrix, num_vehicles=3, depot=0):
    """Solve Vehicle Routing Problem using OR-Tools."""
    
    # Create the routing index manager
    manager = pywrapcp.RoutingIndexManager(
        len(distance_matrix), num_vehicles, depot
    )
    
    # Create Routing Model
    routing = pywrapcp.RoutingModel(manager)
    
    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]
    
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    
    # Define cost of each arc
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    
    # Setting first solution heuristic
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    
    # Solve the problem
    solution = routing.SolveWithParameters(search_parameters)
    
    return solution, routing, manager

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Optimization

# COMMAND ----------

# Example: Optimize routes for a subset of locations
sample_locations = ['City_24', 'City_47', 'City_31', 'City_54', 'City_53']
sample_distance_matrix = create_distance_matrix(distance_pandas, sample_locations)

print("Distance Matrix:")
print(sample_distance_matrix)

# Solve the routing problem
solution, routing, manager = solve_vehicle_routing(sample_distance_matrix, num_vehicles=2, depot=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Results

# COMMAND ----------

def print_solution(solution, routing, manager, locations):
    """Print the solution."""
    print(f"Objective: {solution.ObjectiveValue()}")
    total_distance = 0
    
    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        plan_output = f"Route for vehicle {vehicle_id}:\n"
        route_distance = 0
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            plan_output += f" {locations[node_index]} ->"
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route_distance += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id
            )
        
        node_index = manager.IndexToNode(index)
        plan_output += f" {locations[node_index]}\n"
        plan_output += f"Distance of the route: {route_distance}m\n"
        print(plan_output)
        total_distance += route_distance
    
    print(f"Total distance of all routes: {total_distance}m")

# Print the solution
if solution:
    print_solution(solution, routing, manager, sample_locations)
else:
    print("No solution found!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Optimization Results

# COMMAND ----------

# Create results DataFrame
results_data = []
if solution:
    for vehicle_id in range(routing.vehicles()):
        index = routing.Start(vehicle_id)
        route = []
        
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            route.append(sample_locations[node_index])
            index = solution.Value(routing.NextVar(index))
        
        node_index = manager.IndexToNode(index)
        route.append(sample_locations[node_index])
        
        results_data.append({
            'vehicle_id': vehicle_id,
            'route': ' -> '.join(route),
            'num_stops': len(route) - 1
        })

# Convert to Spark DataFrame and save
if results_data:
    results_df = spark.createDataFrame(results_data)
    results_df.write \
        .mode("overwrite") \
        .saveAsTable("default.smartlogix.optimization_results")
    
    print("Optimization results saved to Delta table!")
    results_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Route Optimization Complete!
# MAGIC 
# MAGIC The optimization has been completed and results saved to:
# MAGIC - `default.smartlogix.optimization_results`
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Implement delay prediction models
# MAGIC 2. Create interactive dashboard
# MAGIC 3. Add real-time optimization capabilities

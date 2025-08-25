#!/usr/bin/env python3

import re
import numpy as np
import random
from typing import List, Tuple, Dict
import time
import sys
import logging
from numba import jit, njit, prange
import os

logger = logging.getLogger()

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Parllelised Ant Colony Optimisation solution for directed travelling salesman problem, entirely courtesy of Claude
@njit(nogil=True, parallel=True)
def construct_solutions_batch_numba(distances, pheromones, heuristic, n_cities, alpha, beta, n_ants, seeds, start_city=0):
    """Numba-compiled batch solution construction with GIL released"""
    all_paths = np.empty((n_ants, n_cities), dtype=np.int32)
    all_distances = np.empty(n_ants, dtype=np.float64)
    
    # Process ants in parallel
    for ant_idx in prange(n_ants):
        # Set unique seed for this ant
        np.random.seed(seeds[ant_idx])
        
        # Always start from specified city
        current_city = start_city
        all_paths[ant_idx, 0] = current_city
        
        # Track unvisited cities
        unvisited = np.ones(n_cities, dtype=np.bool_)
        unvisited[current_city] = False
        n_unvisited = n_cities - 1
        
        total_distance = 0.0
        
        for step in range(1, n_cities):
            if n_unvisited == 0:
                break
                
            # Build list of unvisited cities
            unvisited_cities = np.empty(n_unvisited, dtype=np.int32)
            idx = 0
            for city in range(n_cities):
                if unvisited[city]:
                    unvisited_cities[idx] = city
                    idx += 1
            
            # Calculate probabilities
            probabilities = np.empty(n_unvisited, dtype=np.float64)
            total_prob = 0.0
            
            for i in range(n_unvisited):
                city = unvisited_cities[i]
                pheromone_val = pheromones[current_city, city] ** alpha
                heuristic_val = heuristic[current_city, city] ** beta
                probabilities[i] = pheromone_val * heuristic_val
                total_prob += probabilities[i]
            
            # Normalize probabilities
            if total_prob > 0:
                for i in range(n_unvisited):
                    probabilities[i] /= total_prob
            else:
                # Uniform distribution
                uniform_prob = 1.0 / n_unvisited
                for i in range(n_unvisited):
                    probabilities[i] = uniform_prob
            
            # Roulette wheel selection
            r = np.random.random()
            cumulative = 0.0
            next_city_idx = 0
            
            for i in range(n_unvisited):
                cumulative += probabilities[i]
                if r <= cumulative:
                    next_city_idx = i
                    break
            
            next_city = unvisited_cities[next_city_idx]
            
            # Update path and distance
            total_distance += distances[current_city, next_city]
            all_paths[ant_idx, step] = next_city
            unvisited[next_city] = False
            current_city = next_city
            n_unvisited -= 1
        
        # Return to start
        total_distance += distances[current_city, all_paths[ant_idx, 0]]
        all_distances[ant_idx] = total_distance
    
    return all_paths, all_distances

@njit(nogil=True)
def construct_single_solution_numba(distances, pheromones, heuristic, n_cities, alpha, beta, seed, start_city=0):
    """Single ant solution construction with GIL released"""
    np.random.seed(seed)
    
    # Always start from specified city
    current_city = start_city
    path = np.empty(n_cities, dtype=np.int32)
    path[0] = current_city
    
    unvisited = np.ones(n_cities, dtype=np.bool_)
    unvisited[current_city] = False
    
    total_distance = 0.0
    
    for step in range(1, n_cities):
        # Get unvisited cities
        unvisited_cities = np.where(unvisited)[0]
        n_unvisited = len(unvisited_cities)
        
        if n_unvisited == 0:
            break
            
        # Calculate probabilities
        probabilities = np.empty(n_unvisited, dtype=np.float64)
        total_prob = 0.0
        
        for i in range(n_unvisited):
            city = unvisited_cities[i]
            pheromone_val = pheromones[current_city, city] ** alpha
            heuristic_val = heuristic[current_city, city] ** beta
            probabilities[i] = pheromone_val * heuristic_val
            total_prob += probabilities[i]
        
        if total_prob > 0:
            for i in range(n_unvisited):
                probabilities[i] /= total_prob
        else:
            uniform_prob = 1.0 / n_unvisited
            for i in range(n_unvisited):
                probabilities[i] = uniform_prob
        
        # Selection
        r = np.random.random()
        cumulative = 0.0
        next_city_idx = 0
        
        for i in range(n_unvisited):
            cumulative += probabilities[i]
            if r <= cumulative:
                next_city_idx = i
                break
        
        next_city = unvisited_cities[next_city_idx]
        total_distance += distances[current_city, next_city]
        path[step] = next_city
        unvisited[next_city] = False
        current_city = next_city
    
    total_distance += distances[current_city, path[0]]
    return path, total_distance

class FastAntColonyTSP:
    def __init__(self, distance_matrix: np.ndarray, n_ants: int = None, 
                 n_iterations: int = 100, alpha: float = 1.0, beta: float = 2.0,
                 evaporation_rate: float = 0.5, q: float = 100, 
                 use_sparse: bool = True, batch_size: int = None, 
                 symmetric: bool = True, start_city: int = 0):
        """
        Optimized Ant Colony Optimization for TSP with parallel batch processing
        
        Args:
            distance_matrix: NxN matrix of distances between cities
            n_ants: Number of ants (default: sqrt of cities for large problems)
            n_iterations: Number of iterations
            alpha: Pheromone importance factor
            beta: Heuristic importance factor  
            evaporation_rate: Pheromone evaporation rate
            q: Pheromone deposit factor
            use_sparse: Use sparse matrix optimizations
            batch_size: Process ants in batches of this size (None = all at once)
            symmetric: True for undirected graphs, False for directed (asymmetric TSP)
            start_city: City index to always start tours from (default: 0)
        """
        self.distances = distance_matrix.astype(np.float64)
        self.n_cities = len(distance_matrix)
        
        # For large problems, use fewer ants
        if n_ants is None:
            self.n_ants = max(16, min(128, int(np.sqrt(self.n_cities))))
        else:
            self.n_ants = n_ants
            
        # Set optimal batch size based on problem size and available cores
        n_cores = os.cpu_count()
        if batch_size is None:
            if self.n_cities > 1000:
                # For large problems, use smaller batches to avoid memory issues
                self.batch_size = min(self.n_ants, n_cores * 4)
            else:
                # For smaller problems, can process all ants at once
                self.batch_size = self.n_ants
        else:
            self.batch_size = min(batch_size, self.n_ants)
            
        self.n_iterations = n_iterations
        self.alpha = alpha
        self.beta = beta
        self.evaporation_rate = evaporation_rate
        self.q = q
        self.symmetric = symmetric
        self.start_city = start_city
        
        # Validate start city
        if not (0 <= start_city < self.n_cities):
            raise ValueError(f"start_city must be between 0 and {self.n_cities-1}, got {start_city}")
        
        # Initialize pheromone matrix
        self.pheromones = np.ones((self.n_cities, self.n_cities), dtype=np.float64) / self.n_cities
        
        # Precompute heuristic information
        self.heuristic = np.zeros_like(self.distances, dtype=np.float64)
        finite_mask = np.isfinite(self.distances) & (self.distances > 0)
        self.heuristic[finite_mask] = 1.0 / self.distances[finite_mask]
        
        # For sparse graphs
        if use_sparse:
            large_value = 128 # np.percentile(self.distances[finite_mask], 95) if np.any(finite_mask) else 1e6
            self.valid_connections = self.distances < large_value
            np.fill_diagonal(self.valid_connections, False)
        else:
            self.valid_connections = None
        
        self.best_path = None
        self.best_distance = float('inf')
        self.convergence_data = []
        
        logger.info(f"Initialized ACO with {self.n_ants} ants for {self.n_cities} cities")
        logger.info(f"Starting city: {self.start_city}")
        logger.info(f"Graph type: {'Symmetric (undirected)' if self.symmetric else 'Asymmetric (directed)'}")
        logger.info(f"Using batch size: {self.batch_size} (numba parallel processing)")
        if use_sparse and hasattr(self, 'valid_connections'):
            density = np.mean(self.valid_connections)
            logger.info(f"Graph density: {density:.3f} ({np.sum(self.valid_connections)} valid edges)")
    
    def _construct_solutions_batch(self) -> Tuple[List[List[int]], List[float]]:
        """Construct solutions using numba parallel batch processing"""
        all_paths = []
        all_distances = []
        
        # Process ants in batches
        for start_idx in range(0, self.n_ants, self.batch_size):
            end_idx = min(start_idx + self.batch_size, self.n_ants)
            batch_size = end_idx - start_idx
            
            # Generate unique seeds for this batch
            seeds = np.random.randint(0, 2**31, size=batch_size)
            
            # Process batch in parallel
            batch_paths, batch_distances = construct_solutions_batch_numba(
                self.distances, self.pheromones, self.heuristic, 
                self.n_cities, self.alpha, self.beta, batch_size, seeds, self.start_city
            )
            
            # Convert to lists and append
            for i in range(batch_size):
                all_paths.append(batch_paths[i].tolist())
                all_distances.append(float(batch_distances[i]))
        
        return all_paths, all_distances
    
    def _construct_solution_single(self) -> Tuple[List[int], float]:
        """Single ant solution construction"""
        seed = np.random.randint(0, 2**31)
        path_array, distance = construct_single_solution_numba(
            self.distances, self.pheromones, self.heuristic, 
            self.n_cities, self.alpha, self.beta, seed, self.start_city
        )
        return path_array.tolist(), float(distance)
    
    def _update_pheromones_vectorized(self, all_paths: List[List[int]], all_distances: List[float]):
        """Vectorized pheromone update - handles both symmetric and asymmetric cases"""
        # Evaporation
        self.pheromones *= (1 - self.evaporation_rate)
        
        # Batch deposit pheromones
        for path, distance in zip(all_paths, all_distances):
            if distance > 0:
                pheromone_deposit = self.q / distance
                path_array = np.array(path)
                from_cities = path_array
                to_cities = np.roll(path_array, -1)
                
                # Always update the direction traveled
                self.pheromones[from_cities, to_cities] += pheromone_deposit
                
                # For symmetric graphs, also update reverse direction
                if self.symmetric:
                    self.pheromones[to_cities, from_cities] += pheromone_deposit
    
    def solve(self, verbose: bool = True, early_stopping: int = 50) -> Tuple[List[int], float]:
        """Solve TSP using optimized batch ACO"""
        start_time = time.time()
        no_improvement = 0
        
        # Warm up numba compilation
        if verbose:
            logger.info("Warming up numba compilation...")
            _ = self._construct_solution_single()
            logger.info("Compilation complete, starting optimization...")
        
        for iteration in range(self.n_iterations):
            iteration_start = time.time()
            
            # Construct solutions in batches
            all_paths, all_distances = self._construct_solutions_batch()
            
            # Update best solution
            iteration_best_idx = np.argmin(all_distances)
            iteration_best_distance = all_distances[iteration_best_idx]
            
            if iteration_best_distance < self.best_distance:
                self.best_distance = iteration_best_distance
                self.best_path = all_paths[iteration_best_idx].copy()
                no_improvement = 0
            else:
                no_improvement += 1
            
            # Update pheromones
            self._update_pheromones_vectorized(all_paths, all_distances)
            
            # Record convergence
            avg_distance = np.mean(all_distances)
            self.convergence_data.append({
                'iteration': iteration,
                'best_distance': self.best_distance,
                'avg_distance': avg_distance
            })
            
            iteration_time = time.time() - iteration_start
            
            if verbose and (iteration + 1) % 5 == 0:
                elapsed = time.time() - start_time
                ants_per_sec = self.n_ants / iteration_time
                logger.info(f"Iteration {iteration + 1}: Best = {self.best_distance:.2f}, "
                      f"Avg = {avg_distance:.2f}, Time = {elapsed:.1f}s, "
                      f"Speed = {ants_per_sec:.1f} ants/sec (batch parallel)")
            
            # Early stopping
            if early_stopping > 0 and no_improvement >= early_stopping:
                if verbose:
                    logger.info(f"Early stopping at iteration {iteration + 1}")
                break
        
        return self.best_path, self.best_distance

if __name__ == "__main__":
    distances = None
    n = 0

    f = open("hq-matrix2")
    for j, line in enumerate(f.readlines()):
        row = re.sub(r'^.*\|', '', line).split()
        if distances is None:
            n = len(row)
            distances = np.zeros((n, n), dtype=int)
        distances[j] = row
    f.close()

    # Initialize ACO solver
    aco = FastAntColonyTSP(
        distance_matrix=distances,
        n_ants=100, # apparently 100 ants should be enough, despite the number of cities
        n_iterations=100,
        alpha=1.0,
        beta=2.0,
        evaporation_rate=0.3,
        q=100,
        symmetric=False,
    )

    logger.info("Starting ACO optimization...")
    best_path, best_distance = aco.solve(verbose=True)

    logger.info(f"\nBest solution found:")
    logger.info(f"Distance: {best_distance:.2f}")
    logger.info(f"Path length: {len(best_path)}")

    # N.B. m[i][j] gives the weight from `i` to `j`.
    total_dist = 0
    for i, p in enumerate(best_path):
        if i < len(best_path) - 1:
            logger.info(f"{best_path[i]} -> {best_path[i + 1]} dist = {distances[best_path[i]][best_path[i+1]]} rev_dist = {distances[best_path[i+1]][best_path[i]]}")
            total_dist += distances[best_path[i]][best_path[i+1]]
        else:
            logger.info(f"{best_path[i]} -> {best_path[0]} dist = {distances[best_path[i]][best_path[0]]} rev_dist = {distances[best_path[0]][best_path[i]]}")
            total_dist += distances[best_path[i]][best_path[0]]

    logger.info(f"Our observed distance (i->j): {total_dist}")
    if total_dist != best_distance:
        logger.fatal(f"observerd distance {total_dist} doesn't match {best_distance}")
        sys.exit(1)

    # set the new ordering
    segments = [None] * n
    f = open("hq-segs2")
    for j, line in enumerate(f.readlines()):
        match = re.match(r'.*segment #(.*?) (\d+) -> (\d+)', line)
        seg_id = int(match[1])
        start_sg_id = int(match[2])
        end_sg_id = int(match[3])
        ordering = best_path.index(seg_id)
        segments[seg_id] = [ start_sg_id, end_sg_id, ordering ]
        logger.info(f"segment #{seg_id} { [ start_sg_id, end_sg_id, ordering ] }")
    f.close()

    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect("dbname=test") #, cursor_factory=LoggingCursor)
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    execute_values(
        cursor,
        """
        UPDATE minhashes SET ordering = data.ordering
        FROM (VALUES %s) AS data(start_sg_id, end_sg_id, ordering)
        WHERE minhashes.sg_id >= data.start_sg_id
        AND minhashes.sg_id <= data.end_sg_id
        AND room_id = '!OGEhHVWSdvArJzumhm:matrix.org'
        """,
        segments,
        template=None,
        page_size=1000
    )

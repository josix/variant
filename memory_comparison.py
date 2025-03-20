#!/usr/bin/env python3
"""
Memory comparison between raw Python dictionaries and Variant objects.
This script measures and compares memory usage of equivalent data structures.
"""

import sys
import json
import random
import string
import gc
from typing import Dict, Any, List, Tuple
import matplotlib.pyplot as plt
import numpy as np
from variant_builder import VariantBuilder
from variant import Variant


def generate_random_string(length: int) -> str:
    """Generate a random string of fixed length."""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


def generate_random_dict(depth: int = 3, breadth: int = 5, 
                         str_length: int = 10, max_int: int = 1000000) -> Dict[str, Any]:
    """
    Generate a random nested dictionary with specified depth and breadth.
    
    Args:
        depth: Maximum nesting level
        breadth: Maximum number of keys at each level
        str_length: Length of random strings
        max_int: Maximum integer value
    
    Returns:
        A randomly generated dictionary
    """
    if depth <= 0:
        # Base case: generate a leaf value
        value_type = random.choice(['str', 'int', 'float', 'bool', 'null'])
        if value_type == 'str':
            return generate_random_string(str_length)
        elif value_type == 'int':
            return random.randint(0, max_int)
        elif value_type == 'float':
            return random.random() * max_int
        elif value_type == 'bool':
            return random.choice([True, False])
        else:  # null
            return None
    
    # Decide whether to create a dict or a list
    container_type = random.choice(['dict', 'list'])
    
    if container_type == 'dict':
        # Create a dictionary with random keys and values
        result = {}
        num_keys = random.randint(1, breadth)
        for _ in range(num_keys):
            key = generate_random_string(random.randint(3, str_length))
            result[key] = generate_random_dict(depth - 1, breadth, str_length, max_int)
        return result
    else:  # list
        # Create a list with random values
        result = []
        num_items = random.randint(1, breadth)
        for _ in range(num_items):
            result.append(generate_random_dict(depth - 1, breadth, str_length, max_int))
        return result


def dict_to_variant(d: Dict[str, Any]) -> Variant:
    """Convert a dictionary to a Variant object."""
    json_str = json.dumps(d)
    return VariantBuilder.parse_json(json_str)


def run_comparison(num_samples: int = 5, 
                   depths: List[int] = None, 
                   breadths: List[int] = None) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
    """
    Run memory usage comparison between dictionaries and Variants.
    
    Args:
        num_samples: Number of samples to average for each configuration
        depths: List of depth values to test
        breadths: List of breadth values to test
    
    Returns:
        Two dictionaries containing memory usage results for dictionaries and variants
    """
    if depths is None:
        depths = [2, 3, 4]
    if breadths is None:
        breadths = [5, 10, 15]
    
    dict_memory = {}
    variant_memory = {}
    
    for depth in depths:
        for breadth in breadths:
            key = f"depth={depth},breadth={breadth}"
            dict_memory[key] = []
            variant_memory[key] = []
            
            print(f"Testing configuration: {key}")
            
            for i in range(num_samples):
                print(f"  Sample {i+1}/{num_samples}...")
                
                # Generate a random dictionary
                random_dict = generate_random_dict(depth=depth, breadth=breadth)
                
                # Measure memory for dictionary
                dict_mem = sys.getsizeof(random_dict)
                dict_memory[key].append(dict_mem)
                # Measure memory for variant
                variant = dict_to_variant(random_dict)
                variant_mem = sys.getsizeof(variant)
                variant_memory[key].append(variant_mem)
                
                # Print current results
                print(f"    Dictionary: {dict_mem / 1024:.2f} KB, Variant: {variant_mem / 1024:.2f} KB")
                
                # Clean up to avoid memory accumulation
                del random_dict
                del variant
                gc.collect()
    
    return dict_memory, variant_memory


def plot_results(dict_memory: Dict[str, List[float]], 
                 variant_memory: Dict[str, List[float]], 
                 output_file: str = "memory_comparison.png"):
    """
    Plot the memory comparison results.
    
    Args:
        dict_memory: Dictionary memory measurements
        variant_memory: Variant memory measurements
        output_file: File to save the plot to
    """
    # Calculate averages
    dict_avgs = {k: sum(v) / len(v) / 1024 for k, v in dict_memory.items()}  # Convert to KB
    variant_avgs = {k: sum(v) / len(v) / 1024 for k, v in variant_memory.items()}  # Convert to KB
    
    # Sort keys for consistent ordering
    keys = sorted(dict_avgs.keys())
    
    # Prepare data for plotting
    x = np.arange(len(keys))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create bars
    dict_bars = ax.bar(x - width/2, [dict_avgs[k] for k in keys], width, label='Dictionary')
    variant_bars = ax.bar(x + width/2, [variant_avgs[k] for k in keys], width, label='Variant')
    
    # Add labels and title
    ax.set_xlabel('Configuration')
    ax.set_ylabel('Memory Usage (KB)')
    ax.set_title('Memory Usage Comparison: Dictionary vs Variant')
    ax.set_xticks(x)
    ax.set_xticklabels(keys, rotation=45, ha='right')
    ax.legend()
    
    # Add value labels on bars
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')
    
    add_labels(dict_bars)
    add_labels(variant_bars)
    
    # Add ratio text
    for i, key in enumerate(keys):
        dict_val = dict_avgs[key]
        variant_val = variant_avgs[key]
        ratio = variant_val / dict_val if dict_val > 0 else float('inf')
        ax.text(i, max(dict_val, variant_val) + 5, 
                f'Ratio: {ratio:.2f}x', 
                ha='center', va='bottom')
    
    # Adjust layout and save
    fig.tight_layout()
    plt.savefig(output_file)
    print(f"Plot saved to {output_file}")
    
    # Also return the figure for display in notebooks
    return fig


def print_summary(dict_memory: Dict[str, List[float]], variant_memory: Dict[str, List[float]]):
    """Print a summary of the memory comparison results."""
    print("\nSUMMARY OF MEMORY USAGE COMPARISON")
    print("=" * 80)
    print(f"{'Configuration':<25} {'Dict (KB)':<15} {'Variant (KB)':<15} {'Ratio (V/D)':<15}")
    print("-" * 80)
    
    # Calculate averages and print
    for key in sorted(dict_memory.keys()):
        dict_avg = sum(dict_memory[key]) / len(dict_memory[key]) / 1024  # Convert to KB
        variant_avg = sum(variant_memory[key]) / len(variant_memory[key]) / 1024  # Convert to KB
        ratio = variant_avg / dict_avg if dict_avg > 0 else float('inf')
        
        print(f"{key:<25} {dict_avg:<15.2f} {variant_avg:<15.2f} {ratio:<15.2f}x")
    
    print("=" * 80)


def main():
    """Main function to run the memory comparison."""
    print("Starting memory comparison between dictionaries and Variants...")
    
    # Define test configurations
    depths = [2, 3, 4]
    breadths = [5, 10, 20, 30]
    num_samples = 50
    
    # Run comparison
    dict_memory, variant_memory = run_comparison(
        num_samples=num_samples,
        depths=depths,
        breadths=breadths
    )
    
    # Print summary
    print_summary(dict_memory, variant_memory)
    
    # Plot results
    plot_results(dict_memory, variant_memory)


if __name__ == "__main__":
    main()
